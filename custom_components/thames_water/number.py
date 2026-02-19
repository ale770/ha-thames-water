"""Number platform for the Thames Water integration."""

import logging

from homeassistant.components.number import NumberEntity, NumberMode
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DEFAULT_LITER_COST
from .entity import ThamesWaterEntity

_LOGGER = logging.getLogger(__name__)


async def async_setup_entry(
    _hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    """Set up the number entities for Thames Water."""
    # Use options first, then entry data, then fallback to default.
    if "liter_cost" in entry.options:
        liter_cost = entry.options["liter_cost"]
    elif "liter_cost" in entry.data:
        liter_cost = entry.data["liter_cost"]
    else:
        liter_cost = DEFAULT_LITER_COST

    entities = [
        ThamesWaterLiterCost(entry, initial_value=liter_cost),
    ]
    async_add_entities(entities)


class ThamesWaterLiterCost(ThamesWaterEntity, NumberEntity):
    """Number entity representing the water liter cost in GBP/L as a normal input box."""

    _attr_entity_category = EntityCategory.CONFIG
    _attr_name = "Liter Cost"
    _attr_native_unit_of_measurement = "GBP/L"
    _attr_native_max_value = 1.0
    _attr_native_min_value = 0.00005
    _attr_native_step = 0.00005
    _attr_icon = "mdi:currency-gbp"
    _attr_mode = NumberMode.BOX

    def __init__(
        self,
        config_entry: ConfigEntry,
        initial_value: float | None = None,
    ) -> None:
        """Initialize the Thames Water Liter Cost number entity."""
        self._config_entry = config_entry
        # Handle None value, use default if not provided
        if initial_value is None:
            self._value = DEFAULT_LITER_COST
        else:
            try:
                self._value = float(initial_value)
            except (TypeError, ValueError):
                _LOGGER.debug(
                    "Invalid initial liter_cost value '%s'; using default %s",
                    initial_value,
                    DEFAULT_LITER_COST,
                )
                self._value = DEFAULT_LITER_COST
        self._attr_unique_id = f"{config_entry.entry_id}_liter_cost"

    @property
    def native_value(self) -> float:
        """Return the liter cost value."""
        return self._value

    async def async_set_native_value(self, value: float) -> None:
        """Handle user changes by updating both local value and config options."""
        self._value = value
        new_options = dict(self._config_entry.options)
        new_options["liter_cost"] = value
        self.hass.config_entries.async_update_entry(
            self._config_entry, options=new_options
        )
        self.async_write_ha_state()
