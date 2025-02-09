"""Platform for sensor integration."""

from __future__ import annotations

from datetime import datetime, timedelta, time
import json
import logging

import brotli
import requests

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from homeassistant.components.recorder.models import StatisticData, StatisticMetaData
from homeassistant.components.recorder.statistics import async_add_external_statistics
from homeassistant.components.sensor import SensorEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import CONF_NAME, UnitOfVolume
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.event import async_track_time_change
from homeassistant.util import dt as dt_util

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)
SELENIUM_TIMEOUT = 60


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities
) -> bool:
    """Set up the Thames Water sensor platform."""
    username = entry.data["username"]
    password = entry.data["password"]
    selenium_url = entry.data["selenium_url"]
    account_number = entry.data["account_number"]
    meter_id = entry.data["meter_id"]

    entity_registry = er.async_get(hass)
    unique_id = get_unique_id(meter_id)
    # Check if the entity already exists in the registry
    entity_id = entity_registry.async_get_entity_id("sensor", DOMAIN, unique_id)
    first_run = entity_id is None

    _LOGGER.debug(
        "Configured with username: %s, selenium_url: %s, account_number: %s, meter_id: %s",
        username,
        selenium_url,
        account_number,
        meter_id,
    )

    name = entry.data.get(CONF_NAME, "Thames Water Sensor")

    sensor = ThamesWaterSensor(
        hass,
        name,
        first_run,
        username,
        password,
        account_number,
        meter_id,
        selenium_url,
        unique_id,
    )
    async_add_entities([sensor], update_before_add=True)

    # Schedule the sensor to update every day at 12:00 PM.
    async_track_time_change(
        hass,
        sensor.async_update_callback,
        hour=12,
        minute=0,
        second=0,
    )
    return True


def get_unique_id(meter_id: str) -> str:
    """Return a unique ID for the sensor."""
    return f"water_usage_{meter_id}"


class ThamesWaterSensor(SensorEntity):
    """Thames Water Sensor class."""

    _attr_state_class = "measurement"

    def __init__(
        self,
        hass: HomeAssistant,
        name: str,
        first_run: bool,
        username: str,
        password: str,
        account_number: str,
        meter_id: str,
        selenium_url: str,
        unique_id: str,
    ) -> None:
        """Initialize the sensor."""
        self._hass = hass
        self._name = name
        self._state: float | None = None

        self._username = username
        self._password = password
        self._account_number = account_number
        self._meter_id = meter_id
        self._selenium_url = selenium_url
        self._cookies_dict = None

        self._first_run = first_run
        self._unique_id = unique_id
        self._attr_should_poll = False

    @property
    def unique_id(self) -> str:
        """Return a unique ID for this sensor."""
        return self._unique_id

    @property
    def name(self):
        """Return the name of the sensor."""
        return self._name

    @property
    def state(self) -> float | None:
        """Return the sensor state (latest hourly consumption in Liters)."""
        return self._state

    @property
    def unit_of_measurement(self) -> str:
        """Return the unit of measurement (Liters)."""
        return UnitOfVolume.LITERS

    @callback
    async def async_update_callback(self, ts) -> None:
        """Callback triggered by time change to update the sensor and inject statistics."""
        await self.async_update()
        self.async_write_ha_state()

    async def async_update(self):
        """Fetch data, build hourly statistics, and inject external statistics."""
        # Data is available from at least 3 days ago.
        end_dt = datetime.now() - timedelta(days=3)
        if self._first_run:
            start_dt = end_dt - timedelta(days=30)
        else:
            start_dt = end_dt - timedelta(days=3)

        current_date = start_dt.date()
        end_date = end_dt.date()
        # hist_states holds all hourly data for the entire period.
        hist_states: list[dict] = []

        while current_date <= end_date:
            year = current_date.year
            month = current_date.month
            day = current_date.day

            # Run the blocking Selenium operation in an executor.
            data = await self._hass.async_add_executor_job(
                self._fetch_data_with_selenium,
                year,
                month,
                day,
                self._account_number,
                self._meter_id,
            )

            # Process the returned data; expect a "Lines" list.
            lines = data.get("Lines", [])
            for line in lines:
                time_str = line.get("Label")
                usage = line.get("Usage")
                try:
                    hour, minute = map(int, time_str.split(":"))
                except Exception as err:
                    _LOGGER.error("Error parsing time %s: %s", time_str, err)
                    continue
                naive_datetime = datetime(year, month, day, hour, minute)
                hist_states.append(
                    {
                        "state": usage,  # Usage in Liters per hour
                        "dt": naive_datetime,
                    }
                )
            current_date = current_date + timedelta(days=1)

        _LOGGER.info("Fetched %d historical entries", len(hist_states))

        # Build per-hour statistics from each reading.
        if hist_states:
            sorted_hist = sorted(hist_states, key=lambda x: x["dt"])
            cumulative = 0.0
            stats: list[StatisticData] = []
            for entry in sorted_hist:
                # Truncate the time to the start of the hour.
                hour_ts = entry["dt"].replace(minute=0, second=0, microsecond=0)
                usage = float(entry["state"])
                cumulative += usage
                stats.append(
                    StatisticData(
                        start=dt_util.as_utc(hour_ts),
                        state=usage,
                        sum=cumulative,
                    )
                )
            # Set the sensor's state to the most recent hourly measurement.
            self._state = round(float(sorted_hist[-1]["state"]), 2)

            metadata = StatisticMetaData(
                has_mean=False,
                has_sum=True,
                name="Thames Water Consumption",
                source=DOMAIN,
                statistic_id=f"{DOMAIN}:thameswater_consumption",
                unit_of_measurement=UnitOfVolume.LITERS,
            )
            async_add_external_statistics(self._hass, metadata, stats)
        else:
            self._state = None

        # Clear temporary cookies.
        self._cookies_dict = None

    def _fetch_data_with_selenium(
        self, year: int, month: int, day: int, account_number: str, meter_id: str
    ) -> dict:
        """Fetch data using Selenium in a blocking manner."""
        driver = None
        try:
            if not self._cookies_dict:
                chrome_options = Options()
                chrome_options.add_argument("--headless")  # Run in headless mode
                chrome_options.add_argument("--no-sandbox")
                chrome_options.add_argument("--disable-dev-shm-usage")
                driver = webdriver.Remote(
                    command_executor=self._selenium_url, options=chrome_options
                )

                _LOGGER.debug("Navigating to login page")
                driver.get("https://www.thameswater.co.uk/login")

                _LOGGER.debug("Waiting for the email field")
                WebDriverWait(driver, SELENIUM_TIMEOUT).until(
                    EC.presence_of_element_located((By.ID, "email"))
                )

                _LOGGER.debug("Entering credentials")
                email_element = driver.find_element(By.ID, "email")
                password_element = driver.find_element(By.ID, "password")
                submit_element = driver.find_element(By.ID, "next")
                email_element.send_keys(self._username)
                password_element.send_keys(self._password)
                submit_element.click()

                _LOGGER.debug("Waiting for login to complete")
                WebDriverWait(driver, SELENIUM_TIMEOUT).until(
                    EC.text_to_be_present_in_element(
                        (By.TAG_NAME, "body"), account_number
                    )
                )

                _LOGGER.debug("Navigating to usage page")
                driver.get(
                    f"https://myaccount.thameswater.co.uk/mydashboard/my-meters-usage?contractAccountNumber={account_number}"
                )

                _LOGGER.debug("Waiting for the usage page to load")
                WebDriverWait(driver, SELENIUM_TIMEOUT).until(
                    EC.text_to_be_present_in_element(
                        (By.TAG_NAME, "body"), account_number
                    )
                )

                cookies = driver.get_cookies()
                _LOGGER.debug("Got Cookies!")
                self._cookies_dict = {
                    cookie["name"]: cookie["value"] for cookie in cookies
                }

            _LOGGER.debug("Fetching data for %s/%s/%s", day, month, year)
            url = "https://myaccount.thameswater.co.uk/ajax/waterMeter/getSmartWaterMeterConsumptions"
            params = {
                "meter": meter_id,
                "startDate": day,
                "startMonth": month,
                "startYear": year,
                "endDate": day,
                "endMonth": month,
                "endYear": year,
                "granularity": "H",
                "isForC4C": "false",
            }
            headers = {
                "referer": "https://myaccount.thameswater.co.uk/mydashboard/my-meters-usage",
                "x-requested-with": "XMLHttpRequest",
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/114.0.0.0 Safari/537.36"
                ),
                "Accept": (
                    "text/html,application/xhtml+xml,application/xml;q=0.9,"
                    "image/avif,image/webp,image/apng,*/*;q=0.8,"
                    "application/signed-exchange;v=b3;q=0.9"
                ),
                "Accept-Language": "en-US,en;q=0.9",
                "Accept-Encoding": "gzip, deflate, br",
                "Connection": "keep-alive",
            }
            response = requests.get(
                url, params=params, cookies=self._cookies_dict, headers=headers
            )

            try:
                if response.headers.get("Content-Encoding") == "br":
                    decompressed_data = brotli.decompress(response.content)
                else:
                    decompressed_data = response.content
            except Exception:
                decompressed_data = response.content

            response_text = decompressed_data.decode("utf-8")
            _LOGGER.debug("Got the API response data for %s/%s/%s", day, month, year)

            data = json.loads(response_text)
            if data.get("IsError"):
                _LOGGER.error("Error in response: %s", data)
                return {}
            if not data.get("IsDataAvailable"):
                _LOGGER.warning("No data available in response.")
                return {}
            return data
        except Exception as e:
            _LOGGER.error("Error in _fetch_data_with_selenium: %s", e)
            return {}
        finally:
            if driver is not None:
                driver.quit()
