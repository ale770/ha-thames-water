"""Calculation and refresh tests with mocked Home Assistant imports and UTC helpers."""

import asyncio
import datetime as dt
import importlib
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import MagicMock, Mock, call, patch


def load_coordinator():
    """Load the calculation code without starting or installing Home Assistant."""
    names = (
        "homeassistant",
        "homeassistant.components",
        "homeassistant.components.recorder",
        "homeassistant.components.recorder.models",
        "homeassistant.components.recorder.statistics",
        "homeassistant.config_entries",
        "homeassistant.const",
        "homeassistant.core",
        "homeassistant.helpers",
        "homeassistant.helpers.update_coordinator",
        "homeassistant.util",
    )
    modules = {name: MagicMock() for name in names}
    modules["homeassistant.components.recorder.models"].StatisticData = dict
    modules["homeassistant.components.recorder.models"].StatisticMetaData = dict

    class CoordinatorBase:
        def __init__(self, hass, logger, *, config_entry, **kwargs):
            self.hass = hass
            self.config_entry = config_entry
            self.data = None

        def __class_getitem__(cls, item):
            return cls

    class UpdateFailed(Exception):
        pass

    update_coordinator = modules["homeassistant.helpers.update_coordinator"]
    update_coordinator.DataUpdateCoordinator = CoordinatorBase
    update_coordinator.UpdateFailed = UpdateFailed
    package = ModuleType("_test_thames_water")
    package.__path__ = [
        str(Path(__file__).resolve().parents[1] / "custom_components/thames_water")
    ]
    modules[package.__name__] = package
    with patch.dict(sys.modules, modules):
        return importlib.import_module(f"{package.__name__}.coordinator")


coordinator = load_coordinator()


class CoordinatorCalculationTests(unittest.TestCase):
    def test_daily_totals_include_zero_usage_and_preserve_existing_readings(self):
        day = dt.datetime(2026, 11, 18)
        previous = {"dt": day - dt.timedelta(hours=1), "state": 2.0}
        readings = [previous]
        lines = [
            SimpleNamespace(Label="00:00", Usage=12.5, Read=100.0),
            SimpleNamespace(Label="01:00", Usage=0.0, Read=100.0),
            SimpleNamespace(Label="02:00", Usage=7.5, Read=107.5),
        ]
        with patch.object(
            coordinator.dt_util, "parse_time", side_effect=dt.time.fromisoformat
        ):
            last_read, data = coordinator._process_day_lines(day, lines, readings)
        self.assertEqual((last_read, data.last_read), (107.5, 107.5))
        self.assertEqual(data.date, day.date())
        self.assertEqual((data.total_usage, data.min_usage), (20.0, 0.0))
        expected = [previous] + [
            {"dt": day + dt.timedelta(hours=hour), "state": usage}
            for hour, usage in enumerate((12.5, 0.0, 7.5))
        ]
        self.assertEqual(readings, expected)

    def test_invalid_hour_is_excluded_from_usage_and_statistics(self):
        readings = []
        lines = [SimpleNamespace(Label="invalid", Usage=50.0, Read=100.0)]
        with patch.object(coordinator.dt_util, "parse_time", return_value=None):
            with self.assertLogs(coordinator._LOGGER, level="ERROR"):
                _, data = coordinator._process_day_lines(
                    dt.datetime(2026, 11, 18), lines, readings
                )
        self.assertEqual(readings, [])
        self.assertEqual((data.total_usage, data.min_usage), (0.0, 0.0))

    def test_consumption_and_cost_statistics_continue_previous_sums(self):
        start = dt.datetime(2026, 11, 18, 1, 30)
        readings = [
            {"dt": start, "state": 12.5},
            {"dt": start + dt.timedelta(hours=1), "state": 7.5},
        ]
        for cost, initial, states, sums in (
            (None, 100.0, [12.5, 7.5], [112.5, 120.0]),
            (0.004, 2.0, [0.05, 0.03], [2.05, 2.08]),
        ):
            with self.subTest(liter_cost=cost):
                # These tests cover calculations; HA timezone conversion is mocked.
                with patch.object(
                    coordinator.dt_util,
                    "as_utc",
                    side_effect=lambda value: value.replace(tzinfo=dt.UTC),
                ) as as_utc:
                    stats = coordinator._generate_statistics_from_readings(
                        readings, initial, cost
                    )
                self.assertEqual(
                    as_utc.call_args_list,
                    [call(dt.datetime(2026, 11, 18, hour)) for hour in (1, 2)],
                )
                for index, stat in enumerate(stats):
                    self.assertEqual(
                        stat["start"],
                        dt.datetime(2026, 11, 18, index + 1, tzinfo=dt.UTC),
                    )
                    self.assertAlmostEqual(stat["state"], states[index])
                    self.assertAlmostEqual(stat["sum"], sums[index])
                self.assertEqual(len(stats), 2)

    def test_empty_readings_do_not_create_statistics(self):
        self.assertEqual(coordinator._generate_statistics_from_readings([], 100.0), [])


class CoordinatorRefreshTests(unittest.IsolatedAsyncioTestCase):
    """Exercise refresh decisions with fake recorder/API data and real worker threads."""

    def setUp(self):
        self.day = dt.datetime(2026, 11, 22)
        self.history = {}
        self.entry = SimpleNamespace(
            data={
                "username": "test@example.invalid",
                "password": "test-only",
                "account_number": "123",
                "meter_id": "456",
                "liter_cost": 0.01,
                "no_data_before": self.day.date().isoformat(),
            },
            options={},
        )
        self.subject = coordinator.ThamesWaterCoordinator(
            SimpleNamespace(async_add_executor_job=asyncio.to_thread), self.entry
        )
        self.client = Mock()
        self.client.get_meter_usage.side_effect = self.fetch_day
        self.payloads = {}
        self.worker_calls = []
        self.written = {}
        self.enterContext(patch.object(
            coordinator, "get_instance",
            return_value=SimpleNamespace(async_add_executor_job=asyncio.to_thread),
        ))
        self.enterContext(patch.object(
            coordinator, "get_last_statistics", side_effect=lambda *args: self.history
        ))
        self.factory = self.enterContext(patch.object(
            coordinator, "ThamesWater", side_effect=self.create_client
        ))
        self.enterContext(patch.object(
            coordinator, "async_add_external_statistics", side_effect=self.capture_write
        ))
        # UTC-only test clock: HA's timezone handling and recorder are not exercised.
        self.enterContext(patch.object(coordinator, "dt_util", SimpleNamespace(
            now=lambda: dt.datetime(2026, 11, 25, 12, tzinfo=dt.UTC),
            parse_date=dt.date.fromisoformat,
            parse_time=dt.time.fromisoformat,
            as_utc=lambda value: value.replace(tzinfo=dt.UTC),
            as_local=lambda value: value.replace(tzinfo=dt.UTC),
        )))

    def assert_worker_thread(self):
        with self.assertRaises(RuntimeError):
            asyncio.get_running_loop()

    def create_client(self, *args):
        self.assert_worker_thread()
        self.worker_calls.append("authenticate")
        return self.client

    def fetch_day(self, meter, start, end):
        self.assert_worker_thread()
        self.worker_calls.append(start.date())
        return self.payloads.get(start.date(), self.usage())

    @staticmethod
    def usage(hours=range(24)):
        return SimpleNamespace(
            IsError=False, IsDataAvailable=True,
            Lines=[SimpleNamespace(Label=f"{hour:02}:00", Usage=1.0, Read=500.0 + hour)
                   for hour in hours],
        )

    def capture_write(self, hass, metadata, rows):
        self.written[metadata["statistic_id"]] = rows

    async def test_refresh_uses_executor_and_records_the_actual_latest_hour(self):
        result = await self.subject._async_update_data()
        self.assertEqual(self.worker_calls, ["authenticate", self.day.date()])
        self.factory.assert_called_once_with("test@example.invalid", "test-only", "123")
        self.client.get_meter_usage.assert_called_once_with("456", self.day, self.day)
        self.assertEqual(result.latest_reading, 523.0)
        self.assertEqual(result.latest_day.total_usage, 24.0)
        self.assertEqual(result.last_data_time, self.day.replace(hour=23, tzinfo=dt.UTC))
        self.assertEqual(len(self.written["thames_water:thameswater_consumption"]), 24)

    async def test_resume_excludes_recorded_hours_and_preserves_both_running_totals(self):
        checkpoint = self.day.replace(hour=20, tzinfo=dt.UTC).timestamp()
        self.history = {
            "thames_water:thameswater_consumption": [{"start": checkpoint, "sum": 80.0}],
            "thames_water:thameswater_cost": [{"start": checkpoint, "sum": 3.0}],
        }
        self.entry.options["liter_cost"] = 0.005
        await self.subject._async_update_data()
        water = self.written["thames_water:thameswater_consumption"]
        cost = self.written["thames_water:thameswater_cost"]
        self.assertEqual([row["start"].hour for row in water], [21, 22, 23])
        self.assertEqual([row["sum"] for row in water], [81.0, 82.0, 83.0])
        self.assertEqual([row["start"] for row in cost], [row["start"] for row in water])
        for index, row in enumerate(cost, start=1):
            self.assertAlmostEqual(row["sum"], 3.0 + index * 0.005)
            self.assertEqual(row["state"], 0.005)

    async def test_missing_or_failed_fetch_keeps_previous_values_without_writes(self):
        previous = coordinator.ThamesWaterData(
            coordinator.DayData(self.day.date(), 18.0, 0.0, 480.0),
            480.0, self.day.replace(tzinfo=dt.UTC),
        )
        self.subject.data = previous
        for outcome in (None, self.usage([]), OSError("test connection failure")):
            with self.subTest(outcome=outcome):
                if isinstance(outcome, Exception):
                    self.client.get_meter_usage.side_effect = outcome
                else:
                    self.client.get_meter_usage.side_effect = None
                    self.client.get_meter_usage.return_value = outcome
                with self.assertLogs(coordinator._LOGGER, level="WARNING"):
                    result = await self.subject._async_update_data()
                self.assertEqual(result, previous)
                self.assertEqual(self.written, {})

    async def test_partial_latest_day_waits_for_more_data(self):
        self.entry.data["no_data_before"] = "2026-11-21"
        self.payloads[self.day.date()] = self.usage([0, 8, 16])
        with self.assertLogs(coordinator._LOGGER, level="WARNING"):
            result = await self.subject._async_update_data()
        self.assertEqual(result.latest_day.date, dt.date(2026, 11, 21))
        self.assertEqual(result.last_data_time, dt.datetime(2026, 11, 21, 23, tzinfo=dt.UTC))
        self.assertEqual(len(self.written["thames_water:thameswater_consumption"]), 24)

    async def test_partial_older_day_is_imported_when_a_complete_day_follows(self):
        self.entry.data["no_data_before"] = "2026-11-21"
        self.payloads[dt.date(2026, 11, 21)] = self.usage([0, 8, 16])
        with self.assertLogs(coordinator._LOGGER, level="WARNING"):
            result = await self.subject._async_update_data()
        rows = self.written["thames_water:thameswater_consumption"]
        self.assertEqual([row["start"].hour for row in rows[:3]], [0, 8, 16])
        self.assertEqual(rows[0]["start"].date(), dt.date(2026, 11, 21))
        self.assertEqual(len(rows), 27)
        self.assertEqual(rows[-1]["sum"], 27.0)
        self.assertEqual(result.latest_day.date, self.day.date())

    async def test_authentication_failure_stops_refresh_before_fetching_or_writing(self):
        self.factory.side_effect = OSError("test sign-in failure")
        with self.assertRaisesRegex(coordinator.UpdateFailed, "test sign-in failure"):
            await self.subject._async_update_data()
        self.client.get_meter_usage.assert_not_called()
        self.assertEqual(self.written, {})


if __name__ == "__main__":
    unittest.main()
