#!/usr/bin/env python3
import json
import html
import os
import sys
import unicodedata
from datetime import date as Date, datetime, timedelta

try:
    from zoneinfo import ZoneInfo
except Exception:
    ZoneInfo = None


def slugify(s: str) -> str:
    s = s.replace("–", "-").replace("—", "-")
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()

    out = []
    for ch in s:
        if ch.isalnum():
            out.append(ch)
        elif ch in (" ", "-", "_"):
            out.append("-")
        else:
            out.append("-")

    slug = "".join(out)
    while "--" in slug:
        slug = slug.replace("--", "-")
    return slug.strip("-") or "route"


def route_filename(frm: str, to: str) -> str:
    return f"{slugify(frm)}-{slugify(to)}.html"


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


def yyyymmdd(d: Date) -> str:
    return f"{d.year:04d}{d.month:02d}{d.day:02d}"


def day_name_sl(d: Date) -> str:
    names = ["pon", "tor", "sre", "čet", "pet", "sob", "ned"]
    return names[d.weekday()]


def dd_mm(d: Date) -> str:
    return f"{d.day:02d}. {d.month:02d}."


def today_sl() -> Date:
    if ZoneInfo is None:
        return datetime.now().date()
    return datetime.now(ZoneInfo("Europe/Ljubljana")).date()


def delete_old_route_pages(out_dir: str):
    for fn in os.listdir(out_dir):
        if not fn.endswith(".html"):
            continue
        if fn in ("index.html", "routes.html"):
            continue
        p = os.path.join(out_dir, fn)
        if os.path.isfile(p):
            os.remove(p)


def resolve_input_path() -> str:
    if len(sys.argv) >= 2 and (sys.argv[1] or "").strip():
        return sys.argv[1].strip()

    envp = (os.environ.get("OUT_JSON") or "").strip()
    if envp:
        return envp

    for cand in ("gtfs_tmp/out.json", "out.json"):
        if os.path.exists(cand):
            return cand

    return "gtfs_tmp/out.json"


def main():
    src_out_json = resolve_input_path()
    if not os.path.exists(src_out_json):
        print(f"Missing out.json: {src_out_json}", file=sys.stderr)
        print("Usage: python scripts/build_pages.py <path-to-out.json>", file=sys.stderr)
        sys.exit(2)

    out_dir = os.getcwd()

    with open(src_out_json, "r", encoding="utf-8") as f:
        data = json.load(f)

    with open(os.path.join(out_dir, "out.json"), "w", encoding="utf-8", newline="\n") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")

    delete_old_route_pages(out_dir)

    routes = []
    for frm, tos in data.items():
        for to, entries in tos.items():
            routes.append((frm, to, entries))
    routes.sort(key=lambda x: (x[0], x[1]))

    base = today_sl()
    days = [base + timedelta(days=i) for i in range(10)]
    day_keys = [yyyymmdd(d) for d in days]
    day_set = set(day_keys)

    for frm, to, entries in routes:
        fn = route_filename(frm, to)

        per_day = {k: {} for k in day_keys}
        for e in entries:
            dep = (e.get("departure_time") or "").strip() or "—"
            arr = (e.get("arrival_time") or "").strip() or "—"
            ag = (e.get("agency_name") or "").strip() or "—"
            key = (dep, arr, ag)

            for dk in (e.get("dates") or []):
                if dk in day_set:
                    per_day[dk][key] = True

        nav = []
        day_blocks = []

        for idx, (d, dk) in enumerate(zip(days, day_keys)):
            rows = list(per_day[dk].keys())
            rows.sort(key=lambda r: (time_to_seconds(r[0]), time_to_seconds(r[1]), r[2]))

            label = f"{day_name_sl(d)} {dd_mm(d)}"
            nav.append(f'<li><a href="#d{html.escape(dk)}">{html.escape(label)}</a></li>')

            tr = []
            for dep, arr, ag in rows:
                tr.append(
                    "<tr>"
                    f"<td>{html.escape(dep)}</td>"
                    f"<td>{html.escape(arr)}</td>"
                    f"<td>{html.escape(ag)}</td>"
                    "</tr>"
                )

            body_rows = "".join(tr) if tr else "<tr><td>—</td><td>—</td><td>—</td></tr>"
            open_attr = " open" if idx == 0 else ""

            day_blocks.append(
                f"""
<details class="day" id="d{html.escape(dk)}"{open_attr}>
  <summary>{html.escape(label)} ({len(rows)})</summary>
  <table border="1" cellpadding="6" cellspacing="0">
    <thead>
      <tr>
        <th>Odhod</th>
        <th>Prihod</th>
        <th>Prevoznik</th>
      </tr>
    </thead>
    <tbody>
      {body_rows}
    </tbody>
  </table>
</details>
""".strip()
            )

        page = f"""<!doctype html>
<html lang="sl">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>{html.escape(frm)} – {html.escape(to)}</title>
  </head>
  <body>
    <p><a href="./">← Nazaj</a></p>
    <h1>{html.escape(frm)} – {html.escape(to)}</h1>

    <p>Izberi dan (naslednjih 10 dni):</p>
    <ul>
      {''.join(nav)}
    </ul>

    {''.join(day_blocks)}
  </body>
</html>
"""
        with open(os.path.join(out_dir, fn), "w", encoding="utf-8", newline="\n") as f:
            f.write(page)

    li = []
    for frm, to, _ in routes:
        fn = route_filename(frm, to)
        li.append(f'<li><a href="./{html.escape(fn)}">{html.escape(frm)} – {html.escape(to)}</a></li>')

    routes_html = f"""<!doctype html>
<html lang="sl">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>Relacije</title>
  </head>
  <body>
    <p><a href="./">← Nazaj</a></p>
    <h1>Relacije</h1>
    <ul>
      {''.join(li) if li else '<li>Ni relacij.</li>'}
    </ul>
  </body>
</html>
"""
    with open(os.path.join(out_dir, "routes.html"), "w", encoding="utf-8", newline="\n") as f:
        f.write(routes_html)

    index_html = f"""<!doctype html>
<html lang="sl">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>Vozni redi</title>
  </head>
  <body>
    <h1>Vozni redi</h1>

    <p><a href="./routes.html">Vse relacije</a></p>
    <p><a href="./out.json">out.json</a></p>

    <ul>
      {''.join(li) if li else '<li>Ni relacij.</li>'}
    </ul>
  </body>
</html>
"""
    with open(os.path.join(out_dir, "index.html"), "w", encoding="utf-8", newline="\n") as f:
        f.write(index_html)


if __name__ == "__main__":
    main()
