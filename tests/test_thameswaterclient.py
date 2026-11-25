"""Client parsing tests using synthetic responses, without Home Assistant."""

from copy import deepcopy
import datetime
import importlib.util
from pathlib import Path
import sys
import unittest
from unittest.mock import Mock


# Load the standalone client without importing the integration's HA setup code.
CLIENT_PATH = (
    Path(__file__).resolve().parents[1]
    / "custom_components/thames_water/thameswaterclient.py"
)
SPEC = importlib.util.spec_from_file_location("thameswaterclient", CLIENT_PATH)
client_module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = client_module
SPEC.loader.exec_module(client_module)


class MeterUsageParsingTests(unittest.TestCase):
    def setUp(self):
        self.line = {
            "Label": "00:00",
            "Usage": 12.5,
            "Read": 12345.5,
            "IsEstimated": False,
            "MeterSerialNumberHis": "TEST-METER",
        }
        self.payload = {
            "IsError": False,
            "IsDataAvailable": True,
            "IsConsumptionAvailable": True,
            "TargetUsage": 100.0,
            "AverageUsage": 90.0,
            "ActualUsage": 12.5,
            "MyUsage": "NA",
            "AverageUsagePerPerson": 45.0,
            "IsMO365Customer": False,
            "IsMOPartialCustomer": False,
            "IsMOCompleteCustomer": False,
            "IsExtraMonthConsumptionMessage": False,
            "Lines": [self.line],
        }
        self.client = client_module.ThamesWater.__new__(client_module.ThamesWater)
        self.client.s = Mock()

    def fetch(self, payload):
        self.client.s.get.return_value.json.return_value = deepcopy(payload)
        day = datetime.datetime(2026, 11, 18)
        return self.client.get_meter_usage(123, day, day)

    def test_existing_response_preserves_readings(self):
        result = self.fetch(self.payload)
        self.assertFalse(result.IsError)
        self.assertTrue(result.IsDataAvailable)
        self.assertEqual(result.ActualUsage, 12.5)
        self.assertEqual(result.Lines, [client_module.Line(**self.line)])
        self.assertEqual(result.AlertsValues, {})
        self.client.s.get.return_value.raise_for_status.assert_called_once_with()

    def test_continuous_flow_does_not_prevent_readings(self):
        for value in (True, False, None):
            with self.subTest(continuousFlow=value):
                self.payload["Lines"] = [dict(self.line, continuousFlow=value)]
                result = self.fetch(self.payload)
                self.assertEqual(result.Lines, [client_module.Line(**self.line)])

    def test_additional_reading_fields_are_ignored(self):
        self.payload["Lines"] = [dict(self.line, NewReadingField="unused")]
        result = self.fetch(self.payload)
        self.assertEqual(result.Lines, [client_module.Line(**self.line)])

    def test_additional_response_fields_are_ignored(self):
        self.payload["NewResponseField"] = {"enabled": True}
        self.payload["AlertsValues"] = {"test": False}
        result = self.fetch(self.payload)
        self.assertEqual(result.Lines, [client_module.Line(**self.line)])
        self.assertEqual(result.AlertsValues, {"test": False})

    def test_null_or_empty_lines_are_preserved(self):
        for lines in (None, []):
            with self.subTest(lines=lines):
                self.payload["Lines"] = lines
                self.payload["IsDataAvailable"] = False
                result = self.fetch(self.payload)
                self.assertEqual(result.Lines, lines)
                self.assertFalse(result.IsDataAvailable)

    def test_missing_lines_remain_optional(self):
        del self.payload["Lines"]
        self.assertIsNone(self.fetch(self.payload).Lines)

    def test_missing_required_reading_field_still_fails(self):
        del self.line["Usage"]
        with self.assertRaisesRegex(TypeError, "Usage"):
            self.fetch(self.payload)

    def test_missing_required_response_field_still_fails(self):
        del self.payload["IsError"]
        with self.assertRaisesRegex(TypeError, "IsError"):
            self.fetch(self.payload)


if __name__ == "__main__":
    unittest.main()
