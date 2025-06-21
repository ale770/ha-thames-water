"""Platform for sensor integration."""

from __future__ import annotations

from datetime import datetime, timedelta
import logging
import async_timeout
from operator import itemgetter
import random

from thameswaterclient import ThamesWater

from homeassistant.components.recorder import get_instance
from homeassistant.components.recorder.models import StatisticData, StatisticMetaData
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
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.event import async_track_time_change
from homeassistant.util import dt as dt_util

from .const import DOMAIN
from .entity import ThamesWaterEntity

_LOGGER = logging.getLogger(__name__)
UPDATE_HOURS = [15,23]

async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities
) -> bool:
    """Set up the Thames Water sensor platform."""
    sensor = ThamesWaterSensor(
        hass,
        entry,
    )

    async_add_entities([sensor], update_before_add=True)

    if "fetch_hours" in entry.data and entry.data["fetch_hours"]:
        update_hours = entry.data["fetch_hours"].split(",")
    else:
        update_hours = UPDATE_HOURS

    # Schedule the sensor to update every day at UPDATE_HOURS.
    rand_minute = random.randint(0, 10)
    async_track_time_change(
        hass,
        sensor.async_update_callback,
        hour=update_hours,
        minute=rand_minute,
        second=0,
    )
    return True


def _generate_statistics_from_readings(
    readings: list[tuple[datetime, float]],
    cumulative_start: float = 0.0,
    liter_cost: float = None,
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


class ThamesWaterSensor(ThamesWaterEntity, SensorEntity):
    """Thames Water Sensor class."""

    _attr_state_class = SensorStateClass.TOTAL
    _attr_device_class = SensorDeviceClass.WATER
    _attr_native_unit_of_measurement = UnitOfVolume.LITERS
    _attr_name = "Thames Water Sensor"

    def __init__(
        self,
        hass: HomeAssistant,
        config_entry: ConfigEntry,
    ) -> None:
        """Initialize the sensor."""
        self._hass = hass
        self._config_entry = config_entry
        self._state: float | None = None

        self._username = config_entry.data["username"]
        self._password = config_entry.data["password"]
        self._account_number = config_entry.data["account_number"]
        self._meter_id = config_entry.data["meter_id"]
        self._cookies_dict = None

        self._attr_unique_id = f"water_usage_{self._meter_id}"
        self._attr_should_poll = False

    @property
    def state(self) -> float | None:
        """Return the sensor state (latest hourly consumption in Liters)."""
        return self._state

    @callback
    async def async_update_callback(self, ts) -> None:
        """Update the sensor state."""
        await self.async_update()
        self.async_write_ha_state()

    async def async_update(self):
        """Fetch data, build hourly statistics, and inject external statistics."""
        consumption_stat_id = f"{DOMAIN}:thameswater_consumption"
        cost_stat_id = f"{DOMAIN}:thameswater_cost"

        try:
            async with async_timeout.timeout(5):  # seconds
                last_stats = await get_instance(self.hass).async_add_executor_job(
                    get_last_statistics, self.hass, 1, consumption_stat_id, True, {"sum"}
                )
            async with async_timeout.timeout(5):
                last_cost_stats = await get_instance(self.hass).async_add_executor_job(
                    get_last_statistics, self.hass, 1, cost_stat_id, True, {"sum"}
                )
            # If a previous value exists, use its "sum" as the starting cumulative.
            if len(last_stats.get(consumption_stat_id, [])) > 0:
                last_stats = last_stats[consumption_stat_id]
                last_stats = sorted(last_stats, key=itemgetter("start"), reverse=False)[0]
            # If a previous value exists, use its "sum" as the starting cumulative.
            if len(last_cost_stats.get(cost_stat_id, [])) > 0:
                last_cost_stats = last_cost_stats[cost_stat_id]
                last_cost_stats = sorted(last_cost_stats, key=itemgetter("start"), reverse=False)[0]

        except AttributeError:
            last_stats = None
        except asyncio.TimeoutError:
            _LOGGER.warning("Timeout while fetching last statistics for Thames Water integration")
            last_stats = None
            last_cost_stats = None

        # Data is available from at least 3 days ago.
        end_dt = datetime.now() - timedelta(days=3)
        if last_stats is not None and last_stats.get("sum") is not None:
            start_dt = dt_util.as_utc(datetime.fromtimestamp(last_stats.get("start")))
        else:
            start_dt = end_dt - timedelta(days=30)

        current_date = start_dt.date()
        end_date = end_dt.date()

        try:
            _LOGGER.debug("Creating Thames Water Client")
            tw_client = await self._hass.async_add_executor_job(
                ThamesWater,
                self._username,
                self._password,
                self._account_number,
            )
        except Exception as err:
            _LOGGER.error("Error creating Thames Water client: %s", err)
            return

        # readings holds all hourly data for the entire period.
        readings: list[dict] = []
        latest_usage = 0
        while current_date <= end_date:
            year = current_date.year
            month = current_date.month
            day = current_date.day
            current_date = current_date + timedelta(days=1)

            d = datetime(year, month, day)
            _LOGGER.debug("Fetching data for %s/%s/%s", day, month, year)
            try:
                data = await self._hass.async_add_executor_job(
                    tw_client.get_meter_usage,
                    self._meter_id,
                    d,
                    d,
                )
            except (
                Exception
            ):  # If data is not yet available, it will raise an exception.
                data = None
                _LOGGER.warning("Could not get data for %s/%s/%s", day, month, year)

            if (
                data is None
                or data.Lines is None
                or data.IsDataAvailable is False
                or data.IsError
            ):
                continue

            # Process the returned data; expect a "Lines" list.
            lines = data.Lines
            latest_usage = 0
            for line in lines:
                time_str = line.Label
                usage = line.Usage
                latest_usage += usage
                try:
                    hour, minute = map(int, time_str.split(":"))
                except Exception as err:
                    _LOGGER.error("Error parsing time %s: %s", time_str, err)
                    continue
                naive_datetime = datetime(year, month, day, hour, minute)
                readings.append(
                    {
                        "dt": naive_datetime,
                        "state": usage,  # Usage in Liters per hour
                    }
                )

        _LOGGER.info("Fetched %d historical entries", len(readings))
        # Clear temporary cookies.
        self._cookies_dict = None

        liter_cost = self._config_entry.options.get(
            "liter_cost", self._config_entry.data.get("liter_cost")
        )

        _LOGGER.debug(
            "Using Liter Cost: %s",
            liter_cost,
        )
        if liter_cost is None:
            liter_cost = 0

        if last_stats is not None and last_stats.get("sum") is not None:
            initial_cumulative = last_stats["sum"]
            # Discard all readings before last_stats["start"].
            start_ts = dt_util.as_utc(datetime.fromtimestamp(last_stats.get("start")))
            readings = [r for r in readings if dt_util.as_utc(r["dt"]) > start_ts]
        else:
            initial_cumulative = 0.0

        if last_cost_stats is not None and last_cost_stats.get("sum") is not None:
            initial_cost_cumulative = last_cost_stats["sum"]
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
            self._state = latest_usage

        # Build per-hour statistics from each reading.
        metadata_consumption = StatisticMetaData(
            has_mean=False,
            has_sum=True,
            name="Thames Water Consumption",
            source=DOMAIN,
            statistic_id=consumption_stat_id,
            unit_of_measurement=UnitOfVolume.LITERS,
        )
        metadata_cost = StatisticMetaData(
            has_mean=False,
            has_sum=True,
            name="Thames Water Cost",
            source=DOMAIN,
            statistic_id=cost_stat_id,
            unit_of_measurement="GBP",
        )
        async_add_external_statistics(self._hass, metadata_consumption, stats)
        async_add_external_statistics(self._hass, metadata_cost, cost_stats)
