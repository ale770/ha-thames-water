import voluptuous as vol
from homeassistant import config_entries
from homeassistant.core import callback
from homeassistant.data_entry_flow import FlowResult
from typing import Any, Dict, List

from .const import DOMAIN, DEFAULT_LITER_COST


class ThamesWaterConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Thames Water."""

    VERSION = 1

    async def async_step_user(self, user_input=None) -> FlowResult:
        """Handle the initial step."""
        errors = {}
        if user_input is not None:
            errors = self._validate_input(user_input)

            if not errors:
                return self.async_create_entry(title="Thames Water", data=user_input)

        return self.async_show_form(
            step_id="user", data_schema=self._get_data_schema(), errors=errors
        )

    async def async_step_reconfigure(
        self, user_input: Dict[str, Any] | None = None
    ) -> FlowResult:
        """Handle reconfiguration of the integration."""
        errors = {}
        existing_entry = self.hass.config_entries.async_get_entry(
            self.context["entry_id"]
        )
        if existing_entry is None:
            return self.async_abort(reason="Entry not found")
        if user_input is not None:
            errors = self._validate_input(user_input)

            if not errors:
                return self.async_update_reload_and_abort(
                    self._get_reconfigure_entry(),
                    data_updates=user_input,
                )

        return self.async_show_form(
            step_id="reconfigure",
            data_schema=self._get_data_schema(existing_entry.data),
            errors=errors,
        )

    def _validate_input(self, user_input: Dict[str, Any]) -> Dict[str, str]:
        """Validate user input."""
        errors = {}
        liter_cost_str = user_input.get("liter_cost")
        try:
            liter_cost_val = float(liter_cost_str)
            if liter_cost_val < 0.00005 or liter_cost_val > 1.0:
                errors["liter_cost"] = "Value must be between 0.00005 and 1.0"
        except (TypeError, ValueError):
            errors["liter_cost"] = "Not a valid number"

        hours_str = user_input.get("fetch_hours", "")
        try:
            hours = [int(hour) for hour in hours_str.split(",")]
            if any(hour < 0 or hour > 23 for hour in hours):
                errors["fetch_hours"] = "Hours must be between 0 and 23"
        except ValueError:
            errors["fetch_hours"] = "Invalid format. Use comma-separated hours."

        return errors

    def _get_data_schema(self, defaults: Dict[str, Any] = None) -> vol.Schema:
        """Return the data schema with optional defaults."""
        if defaults is None:
            defaults = {}

        return vol.Schema(
            {
                vol.Required(
                    "username", default=defaults.get("username", "email@email.com")
                ): str,
                vol.Required("password", default=defaults.get("password", "")): str,
                vol.Required(
                    "account_number", default=defaults.get("account_number", "")
                ): str,
                vol.Required("meter_id", default=defaults.get("meter_id", "")): str,
                vol.Required(
                    "liter_cost", default=str(defaults.get("liter_cost", DEFAULT_LITER_COST))
                ): str,
                vol.Optional(
                    "fetch_hours", default=defaults.get("fetch_hours", "15,23")
                ): str,
            }
        )
