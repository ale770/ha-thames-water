"""Init for the Thames Water integration."""

from homeassistant.config_entries import ConfigEntry
from homeassistant.const import Platform
from homeassistant.core import HomeAssistant

from .const import DOMAIN
from .coordinator import ThamesWaterCoordinator

PLATFORMS = [Platform.SENSOR, Platform.NUMBER]


async def async_setup(hass: HomeAssistant, config: dict):
    """Set up the Thames Water component."""
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry):
    """Set up Thames Water from a config entry."""
    hass.data.setdefault(DOMAIN, {})

    coordinator = ThamesWaterCoordinator(hass, entry)
    hass.data[DOMAIN][entry.entry_id] = coordinator

    # Forward platform setups first so their modules are imported before the
    # first coordinator refresh runs (avoids blocking-import warnings in HA 2025+).
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)

    # First refresh runs last; raises ConfigEntryNotReady on failure.
    await coordinator.async_config_entry_first_refresh()

    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry):
    """Unload a config entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        hass.data[DOMAIN].pop(entry.entry_id)
    return unload_ok
