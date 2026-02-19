"""Config Flow for integration."""

from typing import Any

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.config_entries import ConfigFlowResult

from .const import DEFAULT_LITER_COST, DOMAIN


class ThamesWaterConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Thames Water."""

    VERSION = 1

    async def async_step_user(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle the initial step."""
        if self._async_current_entries():
            return self.async_abort(reason="single_instance_allowed")

        errors = {}
        if user_input is not None:
            errors = self._validate_input(user_input)

            if not errors:
                unique_id = self._build_unique_id(user_input)
                await self.async_set_unique_id(unique_id)
                self._abort_if_unique_id_configured()
                return self.async_create_entry(title="Thames Water", data=user_input)

        return self.async_show_form(
            step_id="user", data_schema=self._get_data_schema(), errors=errors
        )

    async def async_step_reconfigure(
        self, user_input: dict[str, Any] | None = None
    ) -> ConfigFlowResult:
        """Handle reconfiguration of the integration."""
        errors = {}
        entry_id = self.context.get("entry_id")
        if entry_id is None:
            return self.async_abort(reason="no_entry_id")
        existing_entry = self.hass.config_entries.async_get_entry(entry_id)

        if existing_entry is None:
            return self.async_abort(reason="entry_not_found")
        if user_input is not None:
            errors = self._validate_input(user_input)

            if not errors:
                return self.async_update_reload_and_abort(
                    self._get_reconfigure_entry(),
                    data_updates=user_input,
                )

        return self.async_show_form(
            step_id="reconfigure",
            data_schema=self._get_data_schema(dict(existing_entry.data)),
            errors=errors,
        )

    def _validate_input(self, user_input: dict[str, Any]) -> dict[str, str]:
        """Validate user input."""
        errors = {}
        liter_cost_str = user_input.get("liter_cost")
        try:
            if liter_cost_str is None or liter_cost_str == "":
                liter_cost_val = DEFAULT_LITER_COST
            else:
                liter_cost_val = float(liter_cost_str)

            if liter_cost_val < 0.00005 or liter_cost_val > 1.0:
                errors["liter_cost"] = "liter_cost_out_of_range"
        except (TypeError, ValueError):
            errors["liter_cost"] = "invalid_liter_cost"

        hours_str = user_input.get("fetch_hours", "")
        if hours_str is None or str(hours_str).strip() == "":
            return errors
        try:
            hours = [int(hour.strip()) for hour in str(hours_str).split(",")]
            if any(hour < 0 or hour > 23 for hour in hours):
                errors["fetch_hours"] = "fetch_hours_out_of_range"
        except ValueError:
            errors["fetch_hours"] = "invalid_fetch_hours"

        return errors

    @staticmethod
    def _build_unique_id(user_input: dict[str, Any]) -> str:
        """Build a stable unique ID for a Thames Water config entry."""
        account_number = str(user_input.get("account_number", "")).strip()
        meter_id = str(user_input.get("meter_id", "")).strip()
        return f"{account_number}:{meter_id}"

    def _get_data_schema(self, defaults: dict[str, Any] | None = None) -> vol.Schema:
        """Return the data schema with optional defaults."""
        if defaults is None:
            defaults = {}

        return vol.Schema(
            {
                vol.Required(
                    "username",
                    default=defaults.get("username", "email@email.com"),
                ): str,
                vol.Required(
                    "password",
                    default=defaults.get("password", ""),
                ): str,
                vol.Required(
                    "account_number",
                    default=defaults.get("account_number", ""),
                ): str,
                vol.Required(
                    "meter_id",
                    default=defaults.get("meter_id", ""),
                ): str,
                vol.Required(
                    "liter_cost",
                    default=str(defaults.get("liter_cost", DEFAULT_LITER_COST)),
                ): str,
                vol.Optional(
                    "fetch_hours",
                    default=defaults.get("fetch_hours", "15,23"),
                ): str,
            }
        )
