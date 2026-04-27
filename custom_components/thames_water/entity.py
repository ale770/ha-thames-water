"""Entity for the Thames Water integration."""

from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.entity import Entity

from .const import DOMAIN


class ThamesWaterEntity(Entity):
    """Base class for Thames Water entities."""

    _attr_device_info = DeviceInfo(
        identifiers={(DOMAIN, "thames_water")},
        manufacturer="Thames Water",
        model="Smart Water Meter",
        name="Thames Water Meter",
    )
