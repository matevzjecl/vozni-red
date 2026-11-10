import csv
import importlib.util
import tempfile
import unittest
from pathlib import Path

MODULE = Path(__file__).resolve().parents[1] / "scripts" / "resolve_stops.py"
spec = importlib.util.spec_from_file_location("resolve_stops", MODULE)
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)


def write_csv(path, fields, rows):
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


class TestStops(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.old = r.HERE
        r.HERE = self.root
        self.addCleanup(lambda: setattr(r, "HERE", self.old))
        self.rows = [dict(key="bus", stop_id="old", type="0", stop_name="Petrovče", lat="46.247293", lon="15.190189")]
        self.stops = [dict(stop_id="new", stop_name="Petrovče", stop_lat="46.247293", stop_lon="15.190189"),
                      dict(stop_id="unselected", stop_name="Petrovče", stop_lat="46.247518", stop_lon="15.189126")]
        self.routes = [dict(route_id="bus_route", route_type="3"), dict(route_id="train_route", route_type="2")]
        self.trips = [dict(trip_id="bus_trip", route_id="bus_route"), dict(trip_id="train_trip", route_id="train_route")]
        self.stop_times = [dict(stop_id="new", trip_id="bus_trip")]
        self.save()

    def save(self):
        write_csv(self.root / "type_mappings.txt", r.FIELDS, self.rows)
        write_csv(self.root / "stops.txt", ("stop_id","stop_name","stop_lat","stop_lon"), self.stops)
        write_csv(self.root / "routes.txt", ("route_id","route_type"), self.routes)
        write_csv(self.root / "trips.txt", ("trip_id","route_id"), self.trips)
        write_csv(self.root / "stop_times.txt", ("stop_id","trip_id"), self.stop_times)

    def resolved_id(self):
        return next(r.read_csv(self.root / "type_mappings.txt"))["stop_id"]

    def test_unique_nearby_bus_recovery(self):
        r.main()
        self.assertEqual(self.resolved_id(), "new")

    def test_unselected_duplicate_never_added(self):
        r.main()
        self.assertEqual([v["stop_id"] for v in r.read_csv(self.root / "type_mappings.txt")], ["new"])

    def test_other_mode_is_not_accepted(self):
        self.stop_times = [dict(stop_id="new", trip_id="train_trip")]
        self.save()
        with self.assertRaises(ValueError):
            r.main()
        self.assertEqual(self.resolved_id(), "old")

    def test_ambiguous_nearby_candidates_are_rejected(self):
        self.stops[1].update(stop_lat="46.247297",stop_lon="15.190190")
        self.stop_times.append(dict(stop_id="unselected",trip_id="bus_trip"))
        self.save()
        with self.assertRaises(ValueError):
            r.main()
        self.assertEqual(self.resolved_id(), "old")

    def test_far_away_candidate_is_not_accepted(self):
        self.stops[0]["stop_lat"] = "46.250"
        self.save()
        with self.assertRaises(ValueError):
            r.main()

    def test_valid_existing_id_remains(self):
        self.rows[0]["stop_id"] = "new"
        self.save()
        r.main()
        self.assertEqual(self.resolved_id(), "new")

    def test_reused_id_with_wrong_location_recovers(self):
        self.stops.append(dict(stop_id="old",stop_name="Petrovče",stop_lat="46.3",stop_lon="15.190189"))
        self.save()
        r.main()
        self.assertEqual(self.resolved_id(), "new")


if __name__ == "__main__":
    unittest.main()
