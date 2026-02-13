#!/usr/bin/env python3
import csv
import json
import os
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date as Date, timedelta
from typing import Dict, List, Optional, Set

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

HERE = os.path.dirname(os.path.abspath(__file__))

GTFS_FILES = {
    "agency": "agency.txt",
    "routes": "routes.txt",
    "trips": "trips.txt",
    "stops": "stops.txt",
    "stop_times": "stop_times.txt",
    "calendar": "calendar.txt",
    "calendar_dates": "calendar_dates.txt",
    "type_mappings": "type_mappings.txt",
}

BUS_ROUTE_TYPE = "3"
TRAIN_ROUTE_TYPE = "2"


def detect_encoding(path: str) -> str:
    raw = open(path, "rb").read(8192)
    for enc in ("utf-8-sig", "utf-8", "cp1250", "iso-8859-2"):
        try:
            raw.decode(enc)
            return enc
        except UnicodeDecodeError:
            continue
    return "utf-8-sig"


def open_csv(path: str):
    enc = detect_encoding(path)
    return open(path, "r", encoding=enc, newline="")


def parse_yyyymmdd(s: str) -> Date:
    s = s.strip()
    return Date(int(s[0:4]), int(s[4:6]), int(s[6:8]))


def yyyymmdd(d: Date) -> str:
    return f"{d.year:04d}{d.month:02d}{d.day:02d}"


def weekday_key(d: Date) -> str:
    keys = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
    return keys[d.weekday()]


def time_to_seconds(t: str) -> int:
    t = (t or "").strip()
    if not t:
        return 10**9
    parts = t.split(":")
    try:
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        s = int(parts[2]) if len(parts) > 2 else 0
        return h * 3600 + m * 60 + s
    except Exception:
        return 10**9


@dataclass(frozen=True)
class Trip:
    trip_id: str
    route_id: str
    service_id: str
    trip_headsign: str


@dataclass(frozen=True)
class Route:
    route_id: str
    agency_id: str
    route_short_name: str
    route_long_name: str
    route_type: str


@dataclass
class ServiceDef:
    start: Optional[Date] = None
    end: Optional[Date] = None
    dow: Optional[Dict[str, str]] = None
    adds: Set[Date] = None
    rems: Set[Date] = None

    def __post_init__(self):
        if self.adds is None:
            self.adds = set()
        if self.rems is None:
            self.rems = set()


@dataclass(frozen=True)
class Occ:
    stop_id: str
    station: str
    stop_type: Optional[int]
    seq: int
    arr: str
    dep: str


def load_type_mappings() -> Dict[str, int]:
    p = os.path.join(HERE, GTFS_FILES["type_mappings"])
    if not os.path.exists(p):
        return {}
    out: Dict[str, int] = {}
    with open_csv(p) as f:
        r = csv.DictReader(f)
        for row in r:
            sid = (row.get("stop_id") or "").strip()
            t = (row.get("type") or "").strip()
            if not sid or not t:
                continue
            try:
                out[sid] = int(t)
            except ValueError:
                continue
    return out


def load_stop_names() -> Dict[str, str]:
    p = os.path.join(HERE, GTFS_FILES["stops"])
    out: Dict[str, str] = {}
    with open_csv(p) as f:
        r = csv.DictReader(f)
        for row in r:
            out[row["stop_id"]] = row.get("stop_name", "") or ""
    return out


def load_agencies() -> Dict[str, str]:
    p = os.path.join(HERE, GTFS_FILES["agency"])
    out: Dict[str, str] = {}
    with open_csv(p) as f:
        r = csv.DictReader(f)
        for row in r:
            out[row["agency_id"]] = row.get("agency_name", "") or ""
    return out


def load_routes() -> Dict[str, Route]:
    p = os.path.join(HERE, GTFS_FILES["routes"])
    out: Dict[str, Route] = {}
    with open_csv(p) as f:
        r = csv.DictReader(f)
        for row in r:
            rid = row["route_id"]
            out[rid] = Route(
                route_id=rid,
                agency_id=row.get("agency_id", "") or "",
                route_short_name=row.get("route_short_name", "") or "",
                route_long_name=row.get("route_long_name", "") or "",
                route_type=row.get("route_type", "") or "",
            )
    return out


def load_trips() -> Dict[str, Trip]:
    p = os.path.join(HERE, GTFS_FILES["trips"])
    out: Dict[str, Trip] = {}
    with open_csv(p) as f:
        r = csv.DictReader(f)
        for row in r:
            tid = row["trip_id"]
            out[tid] = Trip(
                trip_id=tid,
                route_id=row.get("route_id", "") or "",
                service_id=row.get("service_id", "") or "",
                trip_headsign=row.get("trip_headsign", "") or "",
            )
    return out


def load_services() -> Dict[str, ServiceDef]:
    services: Dict[str, ServiceDef] = {}

    p = os.path.join(HERE, GTFS_FILES["calendar"])
    with open_csv(p) as f:
        r = csv.DictReader(f)
        for row in r:
            sid = row["service_id"]
            sd = services.get(sid) or ServiceDef()
            sd.start = parse_yyyymmdd(row["start_date"])
            sd.end = parse_yyyymmdd(row["end_date"])
            sd.dow = {
                "monday": row.get("monday", "0"),
                "tuesday": row.get("tuesday", "0"),
                "wednesday": row.get("wednesday", "0"),
                "thursday": row.get("thursday", "0"),
                "friday": row.get("friday", "0"),
                "saturday": row.get("saturday", "0"),
                "sunday": row.get("sunday", "0"),
            }
            services[sid] = sd

    p = os.path.join(HERE, GTFS_FILES["calendar_dates"])
    with open_csv(p) as f:
        r = csv.DictReader(f)
        for row in r:
            sid = row["service_id"]
            d = parse_yyyymmdd(row["date"])
            et = row.get("exception_type", "")
            sd = services.get(sid) or ServiceDef()
            if et == "1":
                sd.adds.add(d)
            elif et == "2":
                sd.rems.add(d)
            services[sid] = sd

    return services


def dates_for_service(sd: ServiceDef) -> List[str]:
    if sd.start is None or sd.end is None or sd.dow is None:
        ds = sorted([d for d in sd.adds if d not in sd.rems])
        return [yyyymmdd(d) for d in ds]

    out: Set[Date] = set()
    d = sd.start
    while d <= sd.end:
        if sd.dow.get(weekday_key(d), "0") == "1":
            out.add(d)
        d += timedelta(days=1)

    out |= sd.adds
    out -= sd.rems
    return [yyyymmdd(d) for d in sorted(out)]


def reduce_station_sequence(occs: List[Occ]) -> List[Occ]:
    occs = sorted(occs, key=lambda o: o.seq)
    reduced: List[Occ] = []
    last_station = None
    for o in occs:
        if o.station == last_station:
            continue
        reduced.append(o)
        last_station = o.station
    return reduced


def expected_route_type(stop_type: int) -> str:
    return BUS_ROUTE_TYPE if stop_type == 0 else TRAIN_ROUTE_TYPE


def main():
    type_map = load_type_mappings()
    cli_stop_ids = [a.strip() for a in sys.argv[1:] if a.strip()]

    if cli_stop_ids:
        stop_ids = cli_stop_ids
    else:
        if not type_map:
            print("No stop_ids provided and type_mappings.txt not found (or empty).", file=sys.stderr)
            sys.exit(2)
        stop_ids = list(type_map.keys())

    if len(stop_ids) < 2:
        print("Need at least 2 stop_ids.", file=sys.stderr)
        sys.exit(2)

    stop_names = load_stop_names()
    missing = [sid for sid in stop_ids if sid not in stop_names]
    if missing:
        print("Unknown stop_id(s) not found in stops.txt:", ", ".join(missing), file=sys.stderr)
        sys.exit(1)

    agencies = load_agencies()
    routes = load_routes()
    trips = load_trips()
    services = load_services()

    allowed_stop_ids = set(stop_ids)
    service_dates_cache: Dict[str, List[str]] = {}

    def get_service_dates(service_id: str) -> List[str]:
        if service_id in service_dates_cache:
            return service_dates_cache[service_id]
        sd = services.get(service_id)
        ds = dates_for_service(sd) if sd else []
        service_dates_cache[service_id] = ds
        return ds

    trip_occs: Dict[str, List[Occ]] = defaultdict(list)

    stop_times_path = os.path.join(HERE, GTFS_FILES["stop_times"])
    with open_csv(stop_times_path) as f:
        r = csv.DictReader(f)
        for row in r:
            sid = (row.get("stop_id") or "").strip()
            if sid not in allowed_stop_ids:
                continue

            trip_id = (row.get("trip_id") or "").strip()
            if not trip_id:
                continue

            seq_raw = (row.get("stop_sequence") or "").strip()
            try:
                seq = int(seq_raw)
            except ValueError:
                continue

            trip_occs[trip_id].append(
                Occ(
                    stop_id=sid,
                    station=stop_names[sid],
                    stop_type=type_map.get(sid),
                    seq=seq,
                    arr=(row.get("arrival_time") or "").strip(),
                    dep=(row.get("departure_time") or "").strip(),
                )
            )

    result: Dict[str, Dict[str, List[dict]]] = {}

    for trip_id, occs in trip_occs.items():
        t = trips.get(trip_id)
        if not t:
            continue

        r = routes.get(t.route_id)
        rt = r.route_type if r else ""

        seq = reduce_station_sequence(occs)
        if len(seq) < 2:
            continue

        svc_dates = get_service_dates(t.service_id)
        if not svc_dates:
            continue

        agency_name = agencies.get(r.agency_id, "") if r else ""
        route_short = r.route_short_name if r else ""
        route_long = r.route_long_name if r else ""

        for i in range(len(seq) - 1):
            o = seq[i]
            d = seq[i + 1]

            if o.stop_type is not None and d.stop_type is not None:
                if o.stop_type != d.stop_type:
                    continue
                if rt and rt != expected_route_type(o.stop_type):
                    continue

            dep = o.dep or o.arr or ""
            arr = d.arr or d.dep or ""

            entry = {
                "from_stop_id": o.stop_id,
                "to_stop_id": d.stop_id,
                "from_stop_type": o.stop_type,
                "to_stop_type": d.stop_type,
                "departure_time": dep,
                "arrival_time": arr,
                "trip_id": t.trip_id,
                "service_id": t.service_id,
                "trip_headsign": t.trip_headsign,
                "route_id": t.route_id,
                "route_type": rt,
                "agency_name": agency_name,
                "route_short_name": route_short,
                "route_long_name": route_long,
                "dates": svc_dates,
            }

            result.setdefault(o.station, {}).setdefault(d.station, []).append(entry)

    for frm in result:
        for to in result[frm]:
            result[frm][to].sort(
                key=lambda e: (
                    time_to_seconds(e.get("departure_time", "")),
                    time_to_seconds(e.get("arrival_time", "")),
                    e.get("trip_id", ""),
                )
            )

    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
