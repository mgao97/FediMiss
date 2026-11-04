import tempfile
import unittest

from fedimiss.config import Config
from fedimiss.store import Store
from fedimiss.time_windows import historical_window, in_window, longitudinal_windows


class TimeWindowTests(unittest.TestCase):
    def test_historical_window_is_not_endpoint_inclusive(self):
        config = Config({"time_config": {"historical": {
            "enabled": True,
            "start": "2024-10-25T00:00:00Z",
            "end": "2024-10-31T00:00:00Z",
        }}})
        window = historical_window(config)
        self.assertTrue(in_window("2024-10-30T23:59:59Z", window["start"], window["end"]))
        self.assertFalse(in_window("2024-10-31T00:00:00Z", window["start"], window["end"]))

    def test_longitudinal_window_requires_start_date(self):
        config = Config({"time_config": {"longitudinal": {
            "enabled": True, "start_date": None, "days": 90, "window_days": 30,
        }}})
        with self.assertRaisesRegex(ValueError, "start_date"):
            longitudinal_windows(config)

    def test_longitudinal_window_is_split_into_three(self):
        config = Config({"time_config": {"longitudinal": {
            "enabled": True, "start_date": "2025-01-01T00:00:00Z",
            "days": 90, "window_days": 30,
        }}})
        windows = longitudinal_windows(config)
        self.assertEqual(3, len(windows))
        self.assertEqual("2025-01-01T00:00:00Z", windows[0]["start"])
        self.assertEqual("2025-01-31T00:00:00Z", windows[1]["start"])
        self.assertEqual("2025-04-01T00:00:00Z", windows[2]["end"])

    def test_store_migration_keeps_fields_and_adds_snapshot_tables(self):
        with tempfile.TemporaryDirectory() as directory:
            store = Store(directory + "/test.sqlite3")
            store.upsert_account("alice@misskey.example", display_name="Alice")
            store.insert_notes([{
                "note_key": "misskey.example#1",
                "note_id": "1",
                "source_instance": "misskey.example",
                "author_account_key": "alice@misskey.example",
                "created_at": "2024-10-30T12:00:00Z",
                "text": "hello",
                "collected_at": "2024-11-01T00:00:00Z",
                "window_id": "historical-20241025-20241031",
                "window_type": "historical",
            }])
            store.insert_account_snapshot(
                "alice#1", "alice@misskey.example", "historical-20241025-20241031",
                "2024-11-01T00:00:00Z", store.get_account("alice@misskey.example"),
            )
            counts = store.counts()
            self.assertEqual(1, counts["notes"])
            self.assertEqual(1, counts["account_snapshots"])
            store.close()


if __name__ == "__main__":
    unittest.main()
