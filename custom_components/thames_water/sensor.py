"""Sensor platform for the Thames Water integration."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
import logging
import random
from typing import Any

from homeassistant.components.sensor import (
    SensorDeviceClass,
    SensorEntity,
    SensorEntityDescription,
    SensorStateClass,
)
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfVolume
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity_platform import AddEntitiesCallback
from homeassistant.helpers.event import async_track_time_change
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import ThamesWaterCoordinator, ThamesWaterData
from .entity import ThamesWaterEntity

_LOGGER = logging.getLogger(__name__)
UPDATE_HOURS = [15, 23]


@dataclass(frozen=True, kw_only=True)
class ThamesWaterSensorEntityDescription(SensorEntityDescription):
    """Extends SensorEntityDescription with a value accessor."""

    value_fn: Callable[[ThamesWaterData], Any]


SENSOR_DESCRIPTIONS: tuple[ThamesWaterSensorEntityDescription, ...] = (
    ThamesWaterSensorEntityDescription(
        key="water_usage",
        translation_key="water_usage",
        native_unit_of_measurement=UnitOfVolume.LITERS,
        device_class=SensorDeviceClass.WATER,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=0,
        value_fn=lambda data: data.latest_day.total_usage if data.latest_day else None,
    ),
    ThamesWaterSensorEntityDescription(
        key="min_daily_flow",
        translation_key="min_daily_flow",
        native_unit_of_measurement=UnitOfVolume.LITERS,
        device_class=SensorDeviceClass.WATER,
        state_class=SensorStateClass.MEASUREMENT,
        suggested_display_precision=0,
        value_fn=lambda data: data.latest_day.min_usage if data.latest_day else None,
    ),
    ThamesWaterSensorEntityDescription(
        key="last_data_date",
        translation_key="last_data_date",
        device_class=SensorDeviceClass.TIMESTAMP,
        value_fn=lambda data: data.last_data_time,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> bool:
    """Set up Thames Water sensor platform."""
    coordinator: ThamesWaterCoordinator = hass.data[DOMAIN][entry.entry_id]
    meter_id = entry.data.get("meter_id", "")

    entities: list[SensorEntity] = [
        ThamesWaterSensor(coordinator, meter_id),
        *[
            ThamesWaterCoordinatorSensor(coordinator, description, meter_id)
            for description in SENSOR_DESCRIPTIONS
        ],
    ]
    async_add_entities(entities)

    # Schedule refreshes at the configured hours each day.
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

    rand_minute = random.randint(0, 10)

    async def _refresh_callback(ts) -> None:
        try:
            await coordinator.async_request_refresh()
        except asyncio.CancelledError:
            raise
        except Exception as err:
            _LOGGER.error("Unexpected error during scheduled Thames Water refresh: %s", err)

    unsubscribe = async_track_time_change(
        hass,
        _refresh_callback,
        hour=update_hours,
        minute=rand_minute,
        second=0,
    )
    entry.async_on_unload(unsubscribe)
    return True


class ThamesWaterSensor(
    CoordinatorEntity[ThamesWaterCoordinator],
    ThamesWaterEntity,
    SensorEntity,
):
    """The primary Thames Water meter reading sensor (cumulative meter odometer)."""

    _attr_state_class = SensorStateClass.TOTAL_INCREASING
    _attr_device_class = SensorDeviceClass.WATER
    _attr_native_unit_of_measurement = UnitOfVolume.LITERS
    _attr_suggested_display_precision = 0
    _attr_has_entity_name = True
    _attr_translation_key = "meter_reading"

    def __init__(
        self,
        coordinator: ThamesWaterCoordinator,
        meter_id: str,
    ) -> None:
        """Initialise the sensor."""
        super().__init__(coordinator)
        self._attr_unique_id = f"meter_read_{meter_id}"

    @property
    def native_value(self) -> float | None:
        """Return the latest cumulative meter reading in litres."""
        if self.coordinator.data is None:
            return None
        return self.coordinator.data.latest_reading


class ThamesWaterCoordinatorSensor(
    CoordinatorEntity[ThamesWaterCoordinator],
    ThamesWaterEntity,
    SensorEntity,
):
    """Generic sensor driven by a ThamesWaterSensorEntityDescription."""

    _attr_has_entity_name = True

    def __init__(
        self,
        coordinator: ThamesWaterCoordinator,
        description: ThamesWaterSensorEntityDescription,
        meter_id: str,
    ) -> None:
        """Initialise the sensor."""
        super().__init__(coordinator)
        self.entity_description = description
        self._attr_unique_id = f"{description.key}_{meter_id}"

    @property
    def native_value(self) -> Any:
        """Return the sensor value via the description's value_fn."""
        if self.coordinator.data is None:
            return None
        return self.entity_description.value_fn(self.coordinator.data)
