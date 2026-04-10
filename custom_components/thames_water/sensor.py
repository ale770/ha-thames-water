"""Platform for sensor integration."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta
import logging
from operator import itemgetter
import random

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
from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfVolume
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady
from homeassistant.helpers.event import async_track_time_change
from homeassistant.util import dt as dt_util

from .const import DEFAULT_LITER_COST, DOMAIN
from .entity import ThamesWaterEntity
from .thameswaterclient import ThamesWater

_LOGGER = logging.getLogger(__name__)
UPDATE_HOURS = [15, 23]


@dataclass
class ThamesWaterSensorData:
    """Shared runtime state for Thames Water sensors."""

    usage: float | None = None
    meter_read: float | None = None


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities
) -> bool:
    """Set up the Thames Water sensor platform."""
    data = ThamesWaterData(hass, entry)
    usage_sensor = ThamesWaterUsageSensor(
        hass,
        entry,
        data,
    )
    meter_read_sensor = ThamesWaterMeterReadSensor(entry, data)

    async_add_entities([usage_sensor, meter_read_sensor], update_before_add=False)

    if entry.data.get("fetch_hours"):
        try:
            update_hours = [
                int(h.strip()) for h in entry.data["fetch_hours"].split(",")
            ]
        except (ValueError, AttributeError):
            _LOGGER.warning("Invalid fetch_hours configuration, using defaults")
            update_hours = UPDATE_HOURS
    else:
        update_hours = UPDATE_HOURS

    # Schedule the sensor to update every day at UPDATE_HOURS.
    rand_minute = random.randint(0, 10)
    unsubscribe = async_track_time_change(
        hass,
        data.async_update_callback,
        hour=update_hours,
        minute=rand_minute,
        second=0,
    )
    entry.async_on_unload(unsubscribe)

    # Run an initial refresh in the background so setup can complete quickly.
    initial_update_task = hass.async_create_task(data.async_update_callback(None))
    entry.async_on_unload(initial_update_task.cancel)
    return True


def _generate_statistics_from_readings(
    readings: list[dict],
    cumulative_start: float = 0.0,
    liter_cost: float | None = None,
) -> list[StatisticData]:
    """Convert a list of (datetime, reading) entries into StatisticData entries."""
    sorted_readings = sorted(readings, key=lambda x: x["dt"])
    cumulative = cumulative_start
    stats: list[StatisticData] = []
    for elem in sorted_readings:
        # Normalize the start timestamp to the hour
        hour_ts = elem["dt"].replace(minute=0, second=0, microsecond=0)
        if liter_cost is None:
            value = elem["state"]
        else:
            value = elem["state"] * liter_cost
        cumulative += value
        stats.append(
            StatisticData(
                start=dt_util.as_utc(hour_ts),
                state=value,
                sum=cumulative,
            )
        )
    return stats


class ThamesWaterData:
    """Shared updater for Thames Water sensor entities."""

    def __init__(self, hass: HomeAssistant, config_entry: ConfigEntry) -> None:
        """Initialize the shared Thames Water updater."""
        self._hass = hass
        self._config_entry = config_entry
        self._entities: list[SensorEntity] = []
        self._values = ThamesWaterSensorData()

        username = config_entry.data.get("username")
        password = config_entry.data.get("password")
        account_number = config_entry.data.get("account_number")
        meter_id = config_entry.data.get("meter_id")

        # Validate required fields and log errors
        if not username:
            _LOGGER.error(
                "Username not found in config entry data. Available keys: %s",
                list(config_entry.data.keys()),
            )
            raise ConfigEntryNotReady(
                "Username not configured. Please remove and re-add the integration."
            )

        if not password:
            _LOGGER.error(
                "Password not found in config entry data. Available keys: %s",
                list(config_entry.data.keys()),
            )
            raise ConfigEntryNotReady(
                "Password not configured. Please remove and re-add the integration."
            )

        if not account_number:
            _LOGGER.error(
                "Account number not found in config entry data. Available keys: %s",
                list(config_entry.data.keys()),
            )
            raise ConfigEntryNotReady(
                "Account number not configured. Please remove and re-add the integration."
            )

        if not meter_id:
            _LOGGER.error(
                "Meter ID not found in config entry data. Available keys: %s",
                list(config_entry.data.keys()),
            )
            raise ConfigEntryNotReady(
                "Meter ID not configured. Please remove and re-add the integration."
            )

        self._username: str = username
        self._password: str = password
        self._account_number: int = account_number
        self._meter_id: int = meter_id

    def register_entity(self, entity: SensorEntity) -> None:
        """Track entities that should be updated after a refresh."""
        self._entities.append(entity)

    async def async_update_callback(self, ts) -> None:
        """Update the shared sensor state."""
        try:
            await self.async_update()
            for entity in self._entities:
                entity.async_write_ha_state()
        except asyncio.CancelledError:
            _LOGGER.debug("Thames Water sensor update callback was cancelled")
            raise
        except Exception as err:
            _LOGGER.error("Unexpected error in Thames Water update callback: %s", err)

    async def async_update(self):
        """Fetch data, build hourly statistics, and update shared sensor values."""
        consumption_stat_id = f"{DOMAIN}:thameswater_consumption"
        cost_stat_id = f"{DOMAIN}:thameswater_cost"

        last_stats = None
        last_cost_stats = None

        try:
            async with asyncio.timeout(30):
                last_stats = await get_instance(self._hass).async_add_executor_job(
                    get_last_statistics,
                    self._hass,
                    1,
                    consumption_stat_id,
                    True,
                    {"sum"},
                )
            async with asyncio.timeout(30):
                last_cost_stats = await get_instance(self._hass).async_add_executor_job(
                    get_last_statistics,
                    self._hass,
                    1,
                    cost_stat_id,
                    True,
                    {"sum"},
                )

            # If a previous value exists, use its "sum" as the starting cumulative.
            if len(last_stats.get(consumption_stat_id, [])) > 0:
                last_stats = last_stats[consumption_stat_id]
                last_stats = sorted(last_stats, key=itemgetter("start"), reverse=False)[
                    0
                ]
            # If a previous value exists, use its "sum" as the starting cumulative.
            if len(last_cost_stats.get(cost_stat_id, [])) > 0:
                last_cost_stats = last_cost_stats[cost_stat_id]
                last_cost_stats = sorted(
                    last_cost_stats, key=itemgetter("start"), reverse=False
                )[0]

        except TimeoutError:
            _LOGGER.warning(
                "Timeout while fetching last statistics for Thames Water integration"
            )
            last_stats = None
            last_cost_stats = None
        except (Exception) as err:
            _LOGGER.error("Error fetching last statistics: %s", err)
            last_stats = None
            last_cost_stats = None

        # Data is available from at least 3 days ago.
        end_dt = datetime.now() - timedelta(days=3)
        if (
            last_stats is not None
            and last_stats.get("sum") is not None
            and last_stats.get("start") is not None
        ):
            start_dt = dt_util.as_utc(datetime.fromtimestamp(last_stats.get("start")))
        else:
            start_dt = end_dt - timedelta(days=30)

        current_date = start_dt.date()
        end_date = end_dt.date()

        no_data_before_str = self._config_entry.data.get("no_data_before", "").strip()
        if no_data_before_str:
            try:
                no_data_before = datetime.strptime(no_data_before_str, "%Y-%m-%d").date()
                if current_date < no_data_before:
                    current_date = no_data_before
            except ValueError:
                _LOGGER.warning("Invalid no_data_before date '%s', ignoring", no_data_before_str)

        try:
            _LOGGER.debug("Creating Thames Water Client")
            async with asyncio.timeout(120):
                tw_client = await self._hass.async_add_executor_job(
                    ThamesWater,
                    self._username,
                    self._password,
                    self._account_number,
                )
        except TimeoutError:
            _LOGGER.error("Timeout creating Thames Water client")
            return
        except asyncio.CancelledError:
            _LOGGER.warning("Thames Water client creation was cancelled")
            raise
        except Exception as err:
            _LOGGER.error("Error creating Thames Water client: %s", err)
            return

        # readings holds all hourly data for the entire period.
        readings: list[dict] = []
        latest_usage = 0
        latest_meter_read: float | None = None
        pending_incomplete_days: list[tuple[datetime, list]] = []

        def _append_lines(day_dt: datetime, lines: list) -> tuple[int, float | None]:
            """Append hourly lines for a day and return total usage and latest read."""
            day_usage = 0
            day_last_read: float | None = None
            for line in lines:
                time_str = line.Label
                usage = line.Usage
                day_usage += usage
                day_last_read = line.Read
                try:
                    hour, minute = map(int, time_str.split(":"))
                except (ValueError, AttributeError) as err:
                    _LOGGER.error("Error parsing time %s: %s", time_str, err)
                    continue

                naive_datetime = datetime(
                    day_dt.year, day_dt.month, day_dt.day, hour, minute
                )
                readings.append(
                    {
                        "dt": naive_datetime,
                        "state": usage,  # Usage in Liters per hour
                    }
                )
            return day_usage, day_last_read

        while current_date <= end_date:
            year = current_date.year
            month = current_date.month
            day = current_date.day
            current_date = current_date + timedelta(days=1)

            d = datetime(year, month, day)
            _LOGGER.debug("Fetching data for %s/%s/%s", day, month, year)

            try:
                async with asyncio.timeout(30):
                    data = await self._hass.async_add_executor_job(
                        tw_client.get_meter_usage,
                        self._meter_id,
                        d,
                        d,
                    )
            except TimeoutError:
                _LOGGER.warning("Timeout fetching data for %s/%s/%s", day, month, year)
                break
            except Exception as err:
                _LOGGER.warning(
                    "Could not get data for %s/%s/%s: %s", day, month, year, err
                )
                break

            if data is None:
                _LOGGER.warning(
                    "Skipping %s/%s/%s because no response payload was returned",
                    day,
                    month,
                    year,
                )
                break
            if data.IsError:
                _LOGGER.warning(
                    "Skipping %s/%s/%s because Thames Water reported an error payload",
                    day,
                    month,
                    year,
                )
                continue
            if data.IsDataAvailable is False or data.Lines is None:
                _LOGGER.warning(
                    "Skipping %s/%s/%s because Thames Water reported no data",
                    day,
                    month,
                    year,
                )
                continue

            # Process the returned data; expect a "Lines" list.
            lines = data.Lines

            if len(lines) < 24:
                _LOGGER.warning(
                    "Deferring %s/%s/%s - only %d/24 hours available; waiting for next day to confirm",
                    day,
                    month,
                    year,
                    len(lines),
                )
                pending_incomplete_days.append((d, lines))
                continue

            # If prior days were incomplete but today is complete, assume the gap days are broken
            # and insert whatever data we have for them once we see a complete day.
            if pending_incomplete_days:
                for prev_day, prev_lines in pending_incomplete_days:
                    _LOGGER.warning(
                        "Assuming %s/%s/%s is broken (only %d/24 hours) because %s/%s/%s is complete",
                        prev_day.day,
                        prev_day.month,
                        prev_day.year,
                        len(prev_lines),
                        day,
                        month,
                        year,
                    )
                    latest_usage, latest_meter_read = _append_lines(prev_day, prev_lines)
                pending_incomplete_days = []

            latest_usage, latest_meter_read = _append_lines(d, lines)

        _LOGGER.info("Fetched %d historical entries", len(readings))

        liter_cost = self._config_entry.options.get(
            "liter_cost", self._config_entry.data.get("liter_cost", DEFAULT_LITER_COST)
        )

        _LOGGER.debug("Using Liter Cost: %s", liter_cost)

        if (
            last_stats is not None
            and last_stats.get("sum") is not None
            and last_stats.get("start") is not None
        ):
            initial_cumulative = last_stats.get("sum")
            # Discard all readings before last_stats["start"].
            start_ts = dt_util.as_utc(datetime.fromtimestamp(last_stats.get("start")))

            try:
                # Attempt to restore state if None.
                if self._values.usage is None and len(readings) > 0:
                    last_recorded_date = (
                        start_ts.date() - timedelta(days=1)
                        if start_ts.hour == 0
                        else start_ts.date()
                    )
                    daily_total = sum(
                        r["state"]
                        for r in readings
                        if r["dt"].date() == last_recorded_date
                    )
                    if daily_total > 0:
                        self._values.usage = daily_total
                        _LOGGER.debug(
                            "Restored state from last recorded day %s: %s L",
                            last_recorded_date,
                            self._values.usage,
                        )
            except Exception as err:
                _LOGGER.error("Failed to restore state from last recorded day: %s", err)

            readings = [r for r in readings if dt_util.as_utc(r["dt"]) > start_ts]
        else:
            initial_cumulative = 0.0

        if last_cost_stats is not None and last_cost_stats.get("sum") is not None:
            initial_cost_cumulative = last_cost_stats.get("sum")
        else:
            initial_cost_cumulative = 0.0

        if len(readings) == 0:
            _LOGGER.warning("No new readings available")
            return

        # Generate new StatisticData entries using the previous cumulative sum.
        stats = _generate_statistics_from_readings(
            readings, cumulative_start=initial_cumulative
        )
        cost_stats = _generate_statistics_from_readings(
            readings,
            cumulative_start=initial_cost_cumulative,
            liter_cost=float(liter_cost),
        )
        if latest_usage > 0:
            self._values.usage = latest_usage
        if latest_meter_read is not None:
            self._values.meter_read = latest_meter_read

        # Build per-hour statistics from each reading.
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
            async_add_external_statistics(self._hass, metadata_consumption, stats)
            async_add_external_statistics(self._hass, metadata_cost, cost_stats)
        except Exception as err:
            _LOGGER.error("Error writing statistics to database: %s", err)
            raise


class ThamesWaterUsageSensor(ThamesWaterEntity, SensorEntity):
    """Thames Water usage sensor."""

    _attr_state_class = SensorStateClass.TOTAL
    _attr_device_class = SensorDeviceClass.WATER
    _attr_native_unit_of_measurement = UnitOfVolume.LITERS
    _attr_name = "Last Usage"

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: ConfigEntry,
        data: ThamesWaterData,
    ) -> None:
        """Initialize the sensor."""
        self._hass = hass
        self._config_entry = config_entry
        self._data = data
        self._attr_unique_id = f"water_usage_{self._data._meter_id}"
        self._attr_should_poll = False
        self._data.register_entity(self)

    @property
    def state(self) -> float | None:
        """Return the sensor state (latest hourly consumption in Liters)."""
        return self._data._values.usage


class ThamesWaterMeterReadSensor(ThamesWaterEntity, SensorEntity):
    """Sensor exposing the latest raw meter read returned by Thames Water."""

    _attr_name = "Last Read"

    def __init__(self, config_entry: ConfigEntry, data: ThamesWaterData) -> None:
        """Initialize the meter read sensor."""
        self._config_entry = config_entry
        self._data = data
        self._attr_unique_id = f"meter_read_{self._data._meter_id}"
        self._attr_should_poll = False
        self._data.register_entity(self)

    @property
    def state(self) -> float | None:
        """Return the latest meter read."""
        return self._data._values.meter_read
