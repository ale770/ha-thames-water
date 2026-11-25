"""Focused calculation tests with mocked Home Assistant imports and UTC helpers."""

import datetime as dt
import importlib
from pathlib import Path
import sys
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import MagicMock, call, patch


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

    class CoordinatorBase:
        def __class_getitem__(cls, item):
            return cls

    update_coordinator = modules["homeassistant.helpers.update_coordinator"]
    update_coordinator.DataUpdateCoordinator = CoordinatorBase
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


if __name__ == "__main__":
    unittest.main()
