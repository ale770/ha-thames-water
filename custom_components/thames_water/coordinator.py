"""DataUpdateCoordinator for the Thames Water integration."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
import datetime
from datetime import timedelta
import logging

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.models import (
    StatisticData,
    StatisticMeanType,
    StatisticMetaData,
)
from homeassistant.components.recorder.statistics import (
    async_add_external_statistics,
    get_last_statistics,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfVolume
from homeassistant.core import HomeAssistant
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed
from homeassistant.util import dt as dt_util

from .const import DEFAULT_LITER_COST, DOMAIN
from .thameswaterclient import ThamesWater

_LOGGER = logging.getLogger(__name__)


@dataclass
class DayData:
    """Aggregated metrics for a single day."""

    date: datetime.date
    total_usage: float
    min_usage: float
    last_read: float


@dataclass
class ThamesWaterData:
    """All data returned by the coordinator on each refresh."""

    latest_day: DayData | None
    latest_reading: float
    last_data_time: datetime.datetime


def _process_day_lines(
    day_dt: datetime.datetime,
    lines: list,
    readings: list[dict],
) -> tuple[float, DayData]:
    """Process hourly lines for a day.

    Appends to the shared readings list and returns (last_read, DayData).
    """
    total_usage = 0.0
    hourly_usages: list[float] = []
    last_read = 0.0

    for line in lines:
        time_str = line.Label
        usage = line.Usage
        last_read = line.Read

        t = dt_util.parse_time(time_str)
        if t is None:
            _LOGGER.error("Error parsing time %s", time_str)
            continue

        naive_datetime = datetime.datetime(
            day_dt.year, day_dt.month, day_dt.day, t.hour, t.minute
        )
        readings.append({"dt": naive_datetime, "state": usage})
        total_usage += usage
        hourly_usages.append(usage)

    min_usage = min(hourly_usages) if hourly_usages else 0.0

    return last_read, DayData(
        date=day_dt.date(),
        total_usage=total_usage,
        min_usage=min_usage,
        last_read=last_read,
    )


def _generate_statistics_from_readings(
    readings: list[dict],
    cumulative_start: float = 0.0,
    liter_cost: float | None = None,
) -> list[StatisticData]:
    """Convert pre-sorted hourly readings into StatisticData entries with cumulative sums."""
    cumulative = cumulative_start
    stats: list[StatisticData] = []
    for elem in readings:
        hour_ts = elem["dt"].replace(minute=0, second=0, microsecond=0)
        value = elem["state"] if liter_cost is None else elem["state"] * liter_cost
        cumulative += value
        stats.append(
            StatisticData(
                start=dt_util.as_utc(hour_ts),
                state=value,
                sum=cumulative,
            )
        )
    return stats


class ThamesWaterCoordinator(DataUpdateCoordinator[ThamesWaterData]):
    """Coordinator for the Thames Water integration."""

    def __init__(self, hass: HomeAssistant, config_entry: ConfigEntry) -> None:
        """Initialise the coordinator."""
        super().__init__(
            hass,
            _LOGGER,
            name="Thames Water",
            config_entry=config_entry,
            update_interval=None,  # Updates are triggered manually at scheduled hours.
        )
        self._tw_client: ThamesWater | None = None
    async def _async_update_data(self) -> ThamesWaterData:
        """Fetch data, compute aggregates, and inject external statistics."""
        consumption_stat_id = f"{DOMAIN}:thameswater_consumption"
        cost_stat_id = f"{DOMAIN}:thameswater_cost"

        last_stats = None
        last_cost_stats = None

        # --- Read last known statistics from the recorder (parallel) ---
        recorder = get_instance(self.hass)
        _STAT_KEYS = {"sum"}
        try:
            async with asyncio.timeout(30):
                results = await asyncio.gather(
                    recorder.async_add_executor_job(
                        get_last_statistics, self.hass, 1, consumption_stat_id, True, _STAT_KEYS
                    ),
                    recorder.async_add_executor_job(
                        get_last_statistics, self.hass, 1, cost_stat_id, True, _STAT_KEYS
                    ),
                    return_exceptions=True,
                )
        except TimeoutError:
            _LOGGER.warning("Timeout while fetching last statistics")
        else:
            raw_last, raw_last_cost = results
            if isinstance(raw_last, Exception):
                _LOGGER.error("Error fetching consumption statistics: %s", raw_last)
            elif raw_last.get(consumption_stat_id):
                last_stats = raw_last[consumption_stat_id][0]

            if isinstance(raw_last_cost, Exception):
                _LOGGER.error("Error fetching cost statistics: %s", raw_last_cost)
            elif raw_last_cost.get(cost_stat_id):
                last_cost_stats = raw_last_cost[cost_stat_id][0]

        # --- Determine fetch date range ---
        end_dt = dt_util.now() - timedelta(days=3)

        if last_stats and last_stats.get("sum") is not None and last_stats.get("start"):
            last_stat_start_utc = dt_util.as_utc(
                datetime.datetime.fromtimestamp(last_stats["start"])
            )
            start_dt = last_stat_start_utc
        else:
            last_stat_start_utc = None
            start_dt = end_dt - timedelta(days=30)

        current_date = start_dt.date()
        end_date = end_dt.date()

        no_data_before_str = self.config_entry.data.get("no_data_before", "").strip()
        if no_data_before_str:
            no_data_before = dt_util.parse_date(no_data_before_str)
            if no_data_before is None:
                _LOGGER.warning(
                    "Invalid no_data_before date '%s', ignoring", no_data_before_str
                )
            elif current_date < no_data_before:
                current_date = no_data_before

        # --- Authenticate (reuse cached client; re-create only if not yet initialised) ---
        config = self.config_entry.data
        if self._tw_client is None:
            try:
                _LOGGER.debug("Creating Thames Water client")
                async with asyncio.timeout(120):
                    self._tw_client = await self.hass.async_add_executor_job(
                        ThamesWater,
                        config["username"],
                        config["password"],
                        config["account_number"],
                    )
            except TimeoutError as err:
                raise UpdateFailed("Timeout creating Thames Water client") from err
            except asyncio.CancelledError:
                raise
            except Exception as err:
                raise UpdateFailed(f"Error creating Thames Water client: {err}") from err

        # --- Fetch daily data ---
        readings: list[dict] = []
        latest_reading = 0.0
        latest_day_data: DayData | None = None
        pending_incomplete_days: list[tuple[datetime.datetime, list]] = []

        meter_id = config["meter_id"]

        while current_date <= end_date:
            year, month, day = current_date.year, current_date.month, current_date.day
            current_date += timedelta(days=1)

            d = datetime.datetime(year, month, day)
            _LOGGER.debug("Fetching data for %s/%s/%s", day, month, year)

            try:
                async with asyncio.timeout(30):
                    data = await self.hass.async_add_executor_job(
                        self._tw_client.get_meter_usage,
                        meter_id,
                        d,
                        d,
                    )
            except TimeoutError:
                _LOGGER.warning(
                    "Timeout fetching data for %s/%s/%s", day, month, year
                )
                break
            except Exception as err:
                _LOGGER.warning(
                    "Could not get data for %s/%s/%s: %s", day, month, year, err
                )
                self._tw_client = None  # Force re-authentication on next poll.
                break

            if data is None:
                _LOGGER.warning(
                    "Skipping %s/%s/%s — no response payload", day, month, year
                )
                break
            if data.IsError:
                _LOGGER.warning(
                    "Skipping %s/%s/%s — Thames Water reported an error", day, month, year
                )
                continue
            if data.IsDataAvailable is False or data.Lines is None:
                _LOGGER.warning(
                    "Skipping %s/%s/%s — Thames Water reported no data", day, month, year
                )
                continue

            lines = data.Lines

            if len(lines) < 24:
                _LOGGER.warning(
                    "Deferring %s/%s/%s — only %d/24 hours available",
                    day, month, year, len(lines),
                )
                pending_incomplete_days.append((d, lines))
                continue

            # A complete day arrived — flush any previously deferred incomplete days.
            if pending_incomplete_days:
                for prev_day, prev_lines in pending_incomplete_days:
                    _LOGGER.warning(
                        "Assuming %s/%s/%s is broken (%d/24 hours) because %s/%s/%s is complete",
                        prev_day.day, prev_day.month, prev_day.year,
                        len(prev_lines), day, month, year,
                    )
                    prev_read, prev_data = _process_day_lines(prev_day, prev_lines, readings)
                    latest_reading = prev_read
                    latest_day_data = prev_data
                pending_incomplete_days = []

            latest_reading, latest_day_data = _process_day_lines(d, lines, readings)

        _LOGGER.info("Fetched %d historical hourly entries", len(readings))

        # Capture the actual newest datapoint timestamp before the readings list is
        # filtered down to only new entries below.
        last_raw_dt = readings[-1]["dt"] if readings else None

        # --- Determine cumulative starting points ---
        liter_cost = float(
            self.config_entry.options.get(
                "liter_cost",
                self.config_entry.data.get("liter_cost", DEFAULT_LITER_COST),
            )
        )

        if last_stat_start_utc is not None and last_stats and last_stats.get("sum") is not None:
            initial_cumulative = last_stats["sum"]
            readings = [r for r in readings if dt_util.as_utc(r["dt"]) > last_stat_start_utc]
        else:
            initial_cumulative = 0.0

        initial_cost_cumulative = (
            last_cost_stats["sum"]
            if last_cost_stats and last_cost_stats.get("sum") is not None
            else 0.0
        )

        last_data_time = (
            dt_util.as_local(last_raw_dt)
            if last_raw_dt
            else dt_util.now()
        )

        # Preserve previous values if there is nothing new to inject.
        if not readings:
            _LOGGER.warning("No new readings available")
            prev = self.data
            return ThamesWaterData(
                latest_day=latest_day_data or (prev.latest_day if prev else None),
                latest_reading=latest_reading or (prev.latest_reading if prev else 0.0),
                last_data_time=last_data_time if latest_day_data else (prev.last_data_time if prev else dt_util.now()),
            )

        # --- Build and inject statistics ---
        # readings is accumulated in chronological order (day by day), so no sort needed.
        stats = _generate_statistics_from_readings(
            readings, cumulative_start=initial_cumulative
        )
        cost_stats = _generate_statistics_from_readings(
            readings, cumulative_start=initial_cost_cumulative, liter_cost=liter_cost
        )

        metadata_consumption = StatisticMetaData(
            has_mean=False,
            has_sum=True,
            name="Thames Water Consumption",
            source=DOMAIN,
            statistic_id=consumption_stat_id,
            unit_of_measurement=UnitOfVolume.LITERS,
            mean_type=StatisticMeanType.NONE,
            unit_class="volume",
        )
        metadata_cost = StatisticMetaData(
            has_mean=False,
            has_sum=True,
            name="Thames Water Cost",
            source=DOMAIN,
            statistic_id=cost_stat_id,
            unit_of_measurement="GBP",
            mean_type=StatisticMeanType.NONE,
            unit_class=None,
        )

        try:
            async_add_external_statistics(self.hass, metadata_consumption, stats)
            async_add_external_statistics(self.hass, metadata_cost, cost_stats)
        except Exception as err:
            _LOGGER.error("Error writing statistics to database: %s", err)
            raise UpdateFailed(f"Error writing statistics: {err}") from err

        # Keep previous reading if this fetch didn't yield a new one.
        if latest_reading == 0.0 and self.data is not None:
            latest_reading = self.data.latest_reading

        return ThamesWaterData(
            latest_day=latest_day_data,
            latest_reading=latest_reading,
            last_data_time=last_data_time,
        )
