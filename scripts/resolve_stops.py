#!/usr/bin/env python3
"""Resolve curated GTFS stop IDs without introducing unselected stops."""
import csv
import math
import os
import sys
import tempfile
from collections import defaultdict
from pathlib import Path

HERE = Path(__file__).resolve().parent
FIELDS = ("key", "stop_id", "type", "stop_name", "lat", "lon")
ROUTE_TYPES = {"0": "3", "1": "2"}
EXISTING_TOLERANCE_M = 50
REPLACEMENT_TOLERANCE_M = 30


def read_csv(path):
    with path.open(encoding="utf-8-sig", newline="") as f:
        yield from csv.DictReader(f)


def meters(lat1, lon1, lat2, lon2):
    a, b, c, d = map(math.radians, map(float, (lat1, lon1, lat2, lon2)))
    h = math.sin((c - a) / 2)**2 + math.cos(a)*math.cos(c)*math.sin((d - b)/2)**2
    return 12742000 * math.asin(min(1, math.sqrt(h)))


def distance(row, stop):
    return meters(row["lat"], row["lon"], stop["stop_lat"], stop["stop_lon"])


def same_name(row, stop):
    return " ".join(row["stop_name"].casefold().split()) == " ".join(stop["stop_name"].casefold().split())


def candidate_modes(ids):
    routes = {r["route_id"]: r["route_type"] for r in read_csv(HERE / "routes.txt")}
    trips = {r["trip_id"]: routes.get(r["route_id"]) for r in read_csv(HERE / "trips.txt")}
    modes = defaultdict(set)
    for r in read_csv(HERE / "stop_times.txt"):
        sid = r["stop_id"]
        if sid in ids and trips.get(r["trip_id"]):
            modes[sid].add(trips[r["trip_id"]])
    return modes


def main():
    path = HERE / "type_mappings.txt"
    rows = list(read_csv(path))
    if not rows or not set(FIELDS).issubset(rows[0]):
        raise ValueError("type_mappings.txt must include: " + ", ".join(FIELDS))
    stops = {r["stop_id"]: r for r in read_csv(HERE / "stops.txt")}
    keys, ids = set(), set()
    for row in rows:
        key, sid = row["key"], row["stop_id"]
        if not key or key in keys or not sid or sid in ids or row["type"] not in ROUTE_TYPES:
            raise ValueError("Missing/duplicate key or stop_id, or invalid type: " + str(row))
        if not row["stop_name"] or not all(math.isfinite(float(row[k])) for k in ("lat", "lon")):
            raise ValueError("Invalid stop name or coordinates: " + str(row))
        keys.add(key)
        ids.add(sid)

    valid = {r["stop_id"] for r in rows if r["stop_id"] in stops and same_name(r, stops[r["stop_id"]])
             and distance(r, stops[r["stop_id"]]) <= EXISTING_TOLERANCE_M}
    missing = [r for r in rows if r["stop_id"] not in valid]
    if not missing:
        print(f"All {len(rows)} curated stop IDs are valid.", file=sys.stderr)
        return

    choices = {}
    all_candidates = set()
    for row in missing:
        options = [(sid, distance(row, stop)) for sid, stop in stops.items()
                   if sid not in valid and same_name(row, stop)
                   and distance(row, stop) <= REPLACEMENT_TOLERANCE_M]
        choices[row["key"]] = sorted(options, key=lambda v: v[1])
        all_candidates.update(sid for sid, _ in options)
    modes = candidate_modes(all_candidates) if all_candidates else {}
    replacements, errors = {}, []
    for row in missing:
        candidates = [(sid, d) for sid, d in choices[row["key"]] if ROUTE_TYPES[row["type"]] in modes[sid]]
        if len(candidates) == 1:
            sid, d = candidates[0]
            replacements[row["key"]] = sid
            print(f"Recovered {row['key']}: {row['stop_id']} -> {sid} ({d:.1f}m)", file=sys.stderr)
        else:
            suggestions = ", ".join(f"{sid} ({d:.1f}m, modes={sorted(modes[sid])})" for sid, d in choices[row["key"]])
            errors.append(f"{row['key']} ({row['stop_name']}): {suggestions or 'no nearby candidates'}")
    if len(set(replacements.values())) != len(replacements):
        errors.append("Multiple selected stops would map to the same replacement ID")
    if errors:
        raise ValueError("Cannot safely resolve stop IDs:\n" + "\n".join(errors))

    for row in rows:
        if row["key"] in replacements:
            row["stop_id"] = replacements[row["key"]]
    fd, temp = tempfile.mkstemp(prefix=".mappings-", suffix=".csv", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as out:
            writer = csv.DictWriter(out, fieldnames=FIELDS, lineterminator="\n")
            writer.writeheader()
            writer.writerows({k: row[k] for k in FIELDS} for row in rows)
        os.replace(temp, path)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(1)
