#!/usr/bin/env python3
"""
pure-dr-report.py - Visual and text reports of the pg_dr_refresh.py log files.

What it shows
-------------
Reads the weekly pg_dr_refresh_YYYYMMDD.log files and opens a window with these tabs:
  * Overview         - each protection group and its volumes: size, average change per day, change as
                       % of size, peak day, last run, average / maximum replication time, OK / failed runs
  * Sizes            - provisioned size of every DR volume, grouped by protection group
  * Daily change     - data replicated per day (stacked by volume) and change rate as % of size
  * Replication time - time per run for each protection group, and per-volume average / maximum
  * Weekly / Monthly - totals per week or month by protection group and volume: data replicated,
                       average per day, average / maximum replication time, OK / failed runs and
                       the change versus the previous week / month; plus the selected period
                       compared with the previous period of the same length

Periods
-------
Period picker (GUI) / --period (CLI): Last 7 days, Last 30 days, Previous week (Mon-Sun),
Previous month, This month, All, or a custom date range (--from / --to, inclusive).
Comparison periods:  Last N days -> the N days before;  Previous week/month -> the week/month before;
This month -> the same days of last month;  Custom -> the same number of days immediately before.

How the numbers are calculated
------------------------------
* A run is dated by its start time (the DRDATA 'time' field), so a run that crosses midnight
  counts on the day it started.
* "Avg / day" = data replicated in the period / calendar days in the period. Days before the first
  log, and today until its first run, are not counted; any other day without a successful run
  counts as 0. The Overview, the comparisons and the Weekly / Monthly tab all use this rule, so
  weeks, months and partial periods compare fairly.
* "chg" = percent change versus the previous period / week / month. Avg/day chg is the like-for-like
  figure; Data chg compares raw totals (misleading for a period still in progress).
* Replication times are averaged over successful runs. Failed runs are counted but add no data.
  Runs logged with status 'started' (wait_for_replication = false, so nothing was measured) are
  listed as the last status but not counted as OK or failed.
* Weekly / Monthly buckets are full calendar weeks (Mon-Sun) or months that overlap the selected
  period; '*' marks a bucket still in progress.

Data source
-----------
The 'DRDATA {...}' records pg_dr_refresh.py writes after each run (one per protection group and one
per volume). Older logs without DRDATA records are read from their VOLUME / summary lines (no sizes).

Usage
-----
  pure-dr-report.py                          # logs from <script folder>/logs/
  pure-dr-report.py -c pg_dr_config.json     # logs from the config's log_file location
  pure-dr-report.py -l /var/log/pure         # a folder, file(s), or glob(s)
  pure-dr-report.py --period prev-month      # last7 | last30 | prev-week | prev-month | this-month | all
  pure-dr-report.py --from 2026-09-01 --to 2026-09-30   # custom date range (inclusive)
  pure-dr-report.py --days 14 --pgroup PG1   # rolling last N days / one protection group
  pure-dr-report.py --as-of 2026-10-31       # report as if today were that date
  pure-dr-report.py --no-gui                 # text report only (no window)
  pure-dr-report.py --no-gui --summary week  # weekly/monthly section: auto | week | month | both | none
  pure-dr-report.py --export-png DIR         # save the charts (incl. weekly + monthly) as PNG files
  pure-dr-report.py --export-csv FILE        # per-run CSV + FILE_period.csv, FILE_weekly.csv, FILE_monthly.csv

Requirements
------------
Python 3.8+. Text and CSV output need nothing else. The window and PNG export need matplotlib 3.5+
(distro package python3-matplotlib, or pip inside a virtual environment); the window also needs Tk
(Linux: python3-tk / python3-tkinter / python3.11-tkinter / python311-tk; included with the python.org
Windows and macOS installers) and a display (desktop session or ssh -X). See README.md for each distro.
"""
import argparse, collections, csv, datetime, glob, json, os, re, statistics, sys
from pathlib import Path

if sys.version_info < (3, 8):
    sys.exit("pure-dr-report.py needs Python 3.8 or newer (found %d.%d)" % sys.version_info[:2])

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_LOG_GLOB = SCRIPT_DIR / "logs" / "pg_dr_refresh_*.log"
ALL = "All protection groups"


# ============================================================================ helpers
UNITS = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4, "P": 1024**5}

def parse_size(s):
    """Size text -> bytes (binary units): '1.50G' -> 1610612736.0, '2T' -> 2199023255552.0; None if unparseable."""
    m = re.match(r"^\s*([\d.]+)\s*([KMGTP]?)(?:i?B)?\s*$", str(s or ""), re.I)
    return float(m.group(1)) * UNITS[m.group(2).upper()] if m else None

def fmt_bytes(n):
    """Bytes -> human text with binary units: 1610612736 -> '1.50 GiB'; None -> '-'."""
    if n is None:
        return "-"
    for u in ("B", "KiB", "MiB", "GiB", "TiB", "PiB"):
        if abs(n) < 1024 or u == "PiB":
            return f"{n:.0f} {u}" if u == "B" else f"{n:.2f} {u}"
        n /= 1024

def parse_hms(s):
    """'H:MM:SS' -> seconds (legacy log lines); None if unparseable."""
    m = re.match(r"^(\d+):(\d\d):(\d\d)$", str(s or ""))
    return int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3)) if m else None

def fmt_secs(sec):
    """Seconds -> 'H:MM:SS'; None -> '-'."""
    if sec is None:
        return "-"
    sec = int(round(sec))
    return f"{sec // 3600:d}:{sec % 3600 // 60:02d}:{sec % 60:02d}"

def mean(xs):
    """Average of the non-None values; None if there are none."""
    xs = [x for x in xs if x is not None]
    return statistics.mean(xs) if xs else None


# ============================================================================ log discovery
def find_logs(paths, config):
    """Resolve -l paths/globs, else the config's log_file location, else the default folder."""
    pats = []
    if paths:
        for p in paths:
            p = os.path.expanduser(p)
            pats.append(os.path.join(p, "*.log") if os.path.isdir(p) else p)
    elif config:
        base = None
        try:
            text = Path(config).read_text(encoding="utf-8")
            text = re.sub(r'("(?:\\.|[^"\\])*")|//[^\n]*|/\*.*?\*/', lambda m: m.group(1) or "", text, flags=re.S)
            base = json.loads(text).get("log_file")
        except (OSError, ValueError) as e:
            sys.exit(f"Cannot read config {config}: {e}")
        if base:                                    # relative paths are relative to the config file's folder
            base = Path(os.path.expanduser(base))
            if not base.is_absolute():
                base = Path(config).resolve().parent / base
        else:
            base = SCRIPT_DIR / "logs" / "pg_dr_refresh.log"
        pats.append(str(base.with_name(f"{base.stem}_*{base.suffix or '.log'}")))
    else:
        pats.append(str(DEFAULT_LOG_GLOB))
    files = sorted({f for pat in pats for f in glob.glob(pat) if os.path.isfile(f)})
    return files, pats


# ============================================================================ parsing
DR_RE   = re.compile(r"DRDATA (\{.*\})\s*$")
TS_RE   = re.compile(r"^(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d)")
VOL_RE  = re.compile(r"VOLUME (\S+): replicated (\S+) \(written (\S+)\) in (\S+)")
SUM_RE  = re.compile(r"INFO\s+(\S+) -> (\S+): data (\S+), physical written (\S+), time (\S+)")

def split_snap(name):
    """'PG1-DR.dr-20261104-023541.vol-a' -> ('PG1-DR', 'dr-20261104-023541', 'vol-a')"""
    parts = name.split(".")
    return (parts[0], parts[1], ".".join(parts[2:]) or None) if len(parts) >= 2 else (name, None, None)

def load_records(files):
    """Return a de-duplicated list of pgroup and volume records from all log files."""
    recs, legacy, have_drdata = {}, [], set()
    for f in files:
        with open(f, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                m = DR_RE.search(line)
                if m:
                    try:
                        r = json.loads(m.group(1))
                    except ValueError:
                        continue
                    r["time"] = datetime.datetime.fromisoformat(r["time"]) if r.get("time") else None
                    key = (r.get("type"), r.get("run"), r.get("dr_pgroup"), r.get("volume"))
                    recs[key] = r
                    have_drdata.add(r.get("run"))
                    continue
                ts = TS_RE.match(line)
                when = datetime.datetime.strptime(ts.group(1), "%Y-%m-%d %H:%M:%S") if ts else None
                m_vol, m_sum = VOL_RE.search(line), SUM_RE.search(line)
                if m_vol:                                           # legacy per-volume line
                    m = m_vol
                    pg, run, vol = split_snap(m.group(1))
                    legacy.append({"type": "volume", "run": run, "time": when, "dr_pgroup": pg,
                                   "source_pgroup": None, "volume": vol, "status": "ok",
                                   "data_bytes": parse_size(m.group(2)), "written_bytes": parse_size(m.group(3)),
                                   "seconds": parse_hms(m.group(4)), "size_bytes": None})
                elif m_sum:                                         # legacy pgroup summary line
                    m = m_sum
                    pg, run, _ = split_snap(m.group(1))
                    legacy.append({"type": "pgroup", "run": run, "time": when, "dr_pgroup": pg,
                                   "source_pgroup": None, "volume": None, "status": "ok",
                                   "data_bytes": parse_size(m.group(3)), "written_bytes": parse_size(m.group(4)),
                                   "seconds": parse_hms(m.group(5))})
    for r in legacy:                                                # only for runs without DRDATA
        if r["run"] and r["run"] not in have_drdata:
            recs.setdefault((r["type"], r["run"], r["dr_pgroup"], r["volume"]), r)
    return [r for r in recs.values() if r.get("time")]


# ============================================================================ model
class Model:
    """Protection groups -> volumes, with per-run and per-day figures for one set of records.

    pgroups[name] = {"name", "dr_pgroup", "runs": [pgroup records], "volumes": {vol: {...}},
                     plus the metrics from _metrics()}. Protection groups are keyed by their SOURCE
    pgroup name (falls back to the DR pgroup name for old logs that do not record the source)."""
    def __init__(self, records, days=None):
        """records = DRDATA dicts (any period); days = calendar days the records cover, used for
        'average per day' (None = average over the days that have runs, e.g. per-run CSV / charts)."""
        self.days = days
        self.records = sorted(records, key=lambda r: r["time"])
        self.pgroups = collections.OrderedDict()
        dr_to_src = {r["dr_pgroup"]: r["source_pgroup"] for r in self.records if r.get("source_pgroup")}
        for r in self.records:
            name = r.get("source_pgroup") or dr_to_src.get(r["dr_pgroup"]) or r["dr_pgroup"]
            pg = self.pgroups.setdefault(name, {"name": name, "dr_pgroup": r["dr_pgroup"],
                                                "runs": [], "volumes": collections.OrderedDict()})
            if r["type"] == "pgroup":
                pg["runs"].append(r)
            elif r["type"] == "volume" and r.get("volume"):
                v = pg["volumes"].setdefault(r["volume"], {"name": r["volume"], "source": None,
                                                           "size": None, "runs": []})
                v["runs"].append(r)
                v["source"] = r.get("source_volume") or v["source"]
                v["size"] = r.get("size_bytes") or v["size"]
        for pg in self.pgroups.values():
            for v in pg["volumes"].values():
                self._metrics(v)
            self._metrics(pg, volumes=list(pg["volumes"].values()))

    @staticmethod
    def daily(runs):
        """Successful runs -> {date: bytes replicated that day}, sorted by date."""
        d = collections.defaultdict(float)
        for r in runs:
            if r.get("status") == "ok" and r.get("data_bytes") is not None:
                d[r["time"].date()] += r["data_bytes"]
        return dict(sorted(d.items()))

    def _metrics(self, obj, volumes=None):
        """Add size, daily figures, change %, replication times, last run and OK / failed counts to a
            pgroup or volume dict. For a pgroup (volumes given), size = sum of its volume sizes, and if the logs
            have no pgroup-level records the figures are derived from its volumes."""
        runs = obj["runs"]
        ok = [r for r in runs if r.get("status") == "ok"]
        if volumes is not None:                         # pgroup: size = sum of volume sizes
            sizes = [v["size"] for v in volumes if v["size"]]
            obj["size"] = sum(sizes) if sizes else None
            if not ok:                                  # no pgroup records: derive from volumes
                ok = [r for v in volumes for r in v["runs"] if r.get("status") == "ok"]
        obj["daily"] = self.daily(ok)
        total = sum(obj["daily"].values()) if obj["daily"] else None
        # Average per calendar day of the period (days without a successful run count as 0), so the
        # Overview matches the Weekly / Monthly figures; without a day count, average the days with runs.
        obj["avg_daily"] = (total / self.days) if (total is not None and self.days) else mean(obj["daily"].values())
        obj["peak_daily"] = max(obj["daily"].values()) if obj["daily"] else None
        obj["change_pct"] = (obj["avg_daily"] / obj["size"] * 100) if obj["avg_daily"] and obj["size"] else None
        secs = [r.get("seconds") for r in ok if r.get("seconds") is not None]
        obj["avg_secs"], obj["max_secs"] = mean(secs), (max(secs) if secs else None)
        last = runs[-1] if runs else None
        obj["last"] = last
        obj["last_status"] = last.get("status") if last else "-"
        obj["n_ok"], obj["n_fail"] = len([r for r in runs if r.get("status") == "ok"]), \
                                     len([r for r in runs if r.get("status") == "failed"])

    def selected(self, pg_filter):
        """Protection groups matching the filter (source or DR pgroup name); None / ALL = every group."""
        return [pg for n, pg in self.pgroups.items() if pg_filter in (None, ALL, n, pg["dr_pgroup"])]

    def span(self):
        """(first record time, last record time), or (None, None) when empty."""
        if not self.records:
            return None, None
        return self.records[0]["time"], self.records[-1]["time"]


# ============================================================================ periods
D = datetime.timedelta
PERIODS = collections.OrderedDict([
    ("last7", "Last 7 days"), ("last30", "Last 30 days"), ("prev-week", "Previous week (Mon-Sun)"),
    ("prev-month", "Previous month"), ("this-month", "This month"), ("all", "All"),
    ("custom", "Custom date range")])

def period_label(key):
    """Period key -> display label: 'prev-week' -> 'Previous week (Mon-Sun)', 'last14' -> 'Last 14 days'."""
    m = re.fullmatch(r"last(\d+)", key or "")
    return PERIODS.get(key) or (f"Last {m.group(1)} days" if m else key)

def parse_date(s):
    """'YYYY-MM-DD' -> date (argparse type; raises a clear error for bad input)."""
    try:
        return datetime.date.fromisoformat(str(s).strip())
    except ValueError:
        raise argparse.ArgumentTypeError(f"invalid date '{s}' (use YYYY-MM-DD)")

def add_months(d, n):
    """First day of the month n months after d's month (n may be negative)."""
    y, m = divmod(d.month - 1 + n, 12)
    return datetime.date(d.year + y, m + 1, 1)

def month_end(d):
    """Last day of d's month."""
    return add_months(d, 1) - D(days=1)

def resolve_period(key, today, start=None, end=None):
    """-> dict(key, label, start, end, prev_start, prev_end); dates inclusive, None = unbounded."""
    s = e = ps = pe = None
    m = re.fullmatch(r"last(\d+)", key)
    if m:
        n = max(1, int(m.group(1)))
        s, e = today - D(days=n - 1), today
        ps, pe = s - D(days=n), s - D(days=1)
    elif key == "prev-week":
        s = today - D(days=today.weekday() + 7); e = s + D(days=6)
        ps, pe = s - D(days=7), s - D(days=1)
    elif key == "prev-month":
        s = add_months(today, -1); e = month_end(s)
        ps = add_months(s, -1); pe = month_end(ps)
    elif key == "this-month":
        s, e = today.replace(day=1), today
        ps = add_months(s, -1); pe = min(ps + D(days=today.day - 1), month_end(ps))
    elif key == "custom":
        s, e = sorted((start or today, end or today))
        n = (e - s).days + 1
        ps, pe = s - D(days=n), s - D(days=1)
    elif key != "all":
        raise ValueError(f"unknown period {key}")
    return {"key": key, "label": period_label(key), "start": s, "end": e, "prev_start": ps, "prev_end": pe}

def bucket_ranges(kind, s, e):
    """Calendar weeks (Mon-Sun) or months overlapping s..e -> [(start, end)]."""
    out = []
    cur = s - D(days=s.weekday()) if kind == "week" else s.replace(day=1)
    while cur <= e:
        nxt = cur + D(days=7) if kind == "week" else add_months(cur, 1)
        out.append((cur, nxt - D(days=1)))
        cur = nxt
    return out

def iso_week(d):
    """ISO week label, e.g. 2026-11-02 -> '2026-W45' (portable; avoids strftime %G/%V)."""
    y, w, _ = d.isocalendar()
    return f"{y}-W{w:02d}"

def prev_bucket(kind, s):
    """(start, end) of the week / month before the bucket starting on s."""
    return (s - D(days=7), s - D(days=1)) if kind == "week" else (add_months(s, -1), s - D(days=1))


# ============================================================================ period summaries
def pct(a, b):
    """Percent change from b to a; None if either is missing or b is 0."""
    return (a - b) / b * 100 if a is not None and b else None

def fmt_pct(x):
    """Percent change -> '+12%' / '-5%' / '+0%'; None -> '-'."""
    return "-" if x is None else f"{0 if abs(x) < 0.5 else x:+.0f}%"

def _sum(xs):
    """Sum of the non-None values; None if there are none."""
    xs = [x for x in xs if x is not None]
    return sum(xs) if xs else None

def pg_runs(pg):
    """Per-run records of a protection group; derived from its volumes when only volume records exist."""
    if pg["runs"]:
        return pg["runs"]
    by = collections.OrderedDict()
    for v in pg["volumes"].values():
        for r in v["runs"]:
            a = by.setdefault(r.get("run"), {"time": r["time"], "status": "ok", "data_bytes": None, "seconds": None})
            if r.get("status") == "failed":
                a["status"] = "failed"
            if r.get("data_bytes") is not None:
                a["data_bytes"] = (a["data_bytes"] or 0) + r["data_bytes"]
            if r.get("seconds") is not None:
                a["seconds"] = max(a["seconds"] or 0, r["seconds"])
    return list(by.values())

def summarize(runs, days):
    """Totals for one pgroup or volume over a period of `days` calendar days:
    data replicated, average per day, average / maximum replication time, OK and failed run counts.
    Only 'ok' runs count toward data and time; 'started' (unmonitored) runs are ignored."""
    ok = [r for r in runs if r.get("status") == "ok"]
    data = _sum(r.get("data_bytes") for r in ok)
    secs = [r["seconds"] for r in ok if r.get("seconds") is not None]
    return {"data": data, "per_day": data / days if data is not None and days else None,
            "avg_secs": mean(secs), "max_secs": max(secs) if secs else None, "n_ok": len(ok),
            "n_fail": sum(1 for r in runs if r.get("status") == "failed"), "days": days}

def summaries(records, days, pg_filter=None):
    """(pgroup, volume|None) -> summary dict, protection group rows first, then their volumes."""
    out = collections.OrderedDict()
    for pg in Model(records).selected(pg_filter):
        out[(pg["name"], None)] = dict(summarize(pg_runs(pg), days), label=f"{pg['name']} -> {pg['dr_pgroup']}",
                                       dr_pgroup=pg["dr_pgroup"])
        for v in pg["volumes"].values():
            out[(pg["name"], v["name"])] = dict(summarize(v["runs"], days), label=v["name"], dr_pgroup=pg["dr_pgroup"])
    return out

def compare(cur, prev, days):
    """Merge current and previous summaries -> rows with change % (avg/day is the fair comparison)."""
    keys = list(cur) + [k for k in prev if k not in cur]
    order = {}
    for k in keys:
        order.setdefault(k[0], len(order))
    keys.sort(key=lambda k: order[k[0]])                         # keep pgroup blocks together (stable)
    rows = []
    for k in keys:
        p = prev.get(k)
        c = cur.get(k) or dict(summarize([], days), label=p["label"], dr_pgroup=p["dr_pgroup"])
        rows.append({"pg": k[0], "volume": k[1], "label": c["label"], "dr_pgroup": c["dr_pgroup"], "cur": c,
                     "prev": p, "chg": {"data": pct(c["data"], p and p["data"]),
                                        "per_day": pct(c["per_day"], p and p["per_day"]),
                                        "avg_secs": pct(c["avg_secs"], p and p["avg_secs"])}})
    return rows

class Report:
    """All records, sliced by period / week / month."""
    def __init__(self, records, today=None):
        """records = every DRDATA record from the logs; today = report date (--as-of), default today.
        Records dated after 'today' are ignored, so --as-of reproduces a report from that date."""
        self.today = today or datetime.date.today()
        self.records = sorted((r for r in records if r["time"].date() <= self.today), key=lambda r: r["time"])
        self.first = self.records[0]["time"].date() if self.records else None
        self.last = self.records[-1]["time"].date() if self.records else None
        self.all_model = Model(self.records)

    def period(self, key, start=None, end=None):
        """Resolve a period key (custom: start defaults to the first log, end to today)."""
        if key == "custom":
            start = start or self.first or self.today
            end = end or self.today
        return resolve_period(key, self.today, start, end)

    def window(self, s, e):
        """Records whose run date is within s..e (inclusive; None = unbounded)."""
        return [r for r in self.records if (s is None or r["time"].date() >= s)
                and (e is None or r["time"].date() <= e)]

    def days(self, s, e):
        """Calendar days of s..e from the first log up to today (days with no runs count as 0).
        Today only counts once it has a run, so a report before the nightly run is not diluted."""
        if not self.first:
            return 0
        data_end = self.today if self.last >= self.today else self.today - D(days=1)
        lo, hi = max(s or self.first, self.first), min(e or data_end, data_end)
        return max(0, (hi - lo).days + 1)

    def model_for(self, period):
        """Model of the records in the period, with averages per calendar day of the period."""
        return Model(self.window(period["start"], period["end"]), self.days(period["start"], period["end"]))

    def comparison(self, period, pg_filter=None):
        """Rows comparing the period with its previous period, per protection group and volume."""
        s, e, ps, pe = period["start"], period["end"], period["prev_start"], period["prev_end"]
        cur = summaries(self.window(s, e), self.days(s, e), pg_filter)
        prev = summaries(self.window(ps, pe), self.days(ps, pe), pg_filter) if ps else {}
        return compare(cur, prev, self.days(s, e))

    def buckets(self, kind, period, pg_filter=None):
        """Calendar weeks (kind='week', Mon-Sun) or months overlapping the period, each with rows
            comparing it with the previous week / month. Buckets end at today; a bucket containing today
            is flagged 'partial' (in progress)."""
        if not self.first:
            return []
        s = max(period["start"] or self.first, self.first)
        e = min(period["end"] or self.today, self.today)
        out = []
        for bs, be in bucket_ranges(kind, s, e) if s <= e else []:
            pbs, pbe = prev_bucket(kind, bs)
            days = self.days(bs, be)
            cur = summaries(self.window(bs, be), days, pg_filter)
            prev = summaries(self.window(pbs, pbe), self.days(pbs, pbe), pg_filter)
            length = (be - bs).days + 1
            partial = be >= self.today                      # week / month still in progress
            if kind == "week":
                title, short = f"Week {iso_week(bs)}  {bs:%Y-%m-%d} .. {be:%Y-%m-%d}", f"{bs:%b %d}"
            else:
                title, short = f"{bs:%B %Y}  {bs:%Y-%m-%d} .. {be:%Y-%m-%d}", f"{bs:%b %Y}"
            if partial:
                title += f"   * in progress, {days} of {length} days"
                short += "*"
            elif days < length:                             # logs start part-way through this bucket
                title += f"   (logs start {self.first:%Y-%m-%d}; {days} of {length} days)"
            out.append({"start": bs, "end": be, "days": days, "partial": partial, "title": title,
                        "short": short, "rows": compare(cur, prev, days)})
        return out

    def describe(self, period):
        """'Previous month: 2026-10-01 .. 2026-10-31 (31 days)   vs 2026-09-01 .. 2026-09-30'"""
        if not self.first:
            return f"{period['label']}: no data"
        s, e = period["start"] or self.first, period["end"] or self.today
        txt = f"{period['label']}: {s:%Y-%m-%d} .. {e:%Y-%m-%d} ({(e - s).days + 1} days)"
        if period["prev_start"]:
            ps, pe = period["prev_start"], period["prev_end"]
            txt += f"   vs previous {ps:%Y-%m-%d} .. {pe:%Y-%m-%d}"
            if self.days(ps, pe) == 0:
                txt += " (no logs)"
        return txt

def cmp_cells(row):
    """Display strings for one comparison row, in CMP_HEADS order."""
    c, p, d = row["cur"], row["prev"] or {}, row["chg"]
    return (fmt_bytes(c["data"]), fmt_bytes(c["per_day"]), fmt_secs(c["avg_secs"]), fmt_secs(c["max_secs"]),
            f"{c['n_ok']}/{c['n_fail']}", fmt_bytes(p.get("data")), fmt_bytes(p.get("per_day")),
            fmt_pct(d["data"]), fmt_pct(d["per_day"]), fmt_pct(d["avg_secs"]))

CMP_HEADS = ("Data replicated", "Avg / day", "Avg time", "Max time", "OK/Fail", "Prev data", "Prev avg/day",
             "Data chg", "Avg/day chg", "Time chg")


# ============================================================================ text / csv output
def overview_lines(model, pg_filter=None):
    """Text Overview table: one line per protection group and per volume for the period's Model."""
    hdr = f"{'Protection group / volume':<34}{'Size':>12}{'Avg/day':>13}{'%/day':>8}{'Last data':>13}" \
          f"{'Last time':>11}{'Avg time':>10}{'Max time':>10}{'OK/Fail':>9}  Status"
    out = [hdr, "-" * len(hdr)]
    for pg in model.selected(pg_filter):
        def row(label, o):
            """One formatted Overview line."""
            last = o["last"] or {}
            pct_ = f"{o['change_pct']:.2f}" if o["change_pct"] is not None else "-"
            return (f"{label[:33]:<34}{fmt_bytes(o['size']):>12}{fmt_bytes(o['avg_daily']):>13}{pct_:>8}"
                    f"{fmt_bytes(last.get('data_bytes')):>13}{fmt_secs(last.get('seconds')):>11}"
                    f"{fmt_secs(o['avg_secs']):>10}{fmt_secs(o['max_secs']):>10}"
                    f"{str(o['n_ok']) + '/' + str(o['n_fail']):>9}  {o['last_status']}")
        out.append(row(f"{pg['name']} -> {pg['dr_pgroup']}", pg))
        for v in pg["volumes"].values():
            out.append(row(f"  {v['name']}", v))
    if len(out) == 2:
        out.append("(no runs in this period)")
    return out

CMP_WIDTHS = (16, 12, 10, 10, 9, 13, 13, 10, 12, 10)

def cmp_header():
    """Header + rule lines for the text comparison tables."""
    hdr = f"{'Protection group / volume':<34}" + "".join(f"{h:>{w}}" for h, w in zip(CMP_HEADS, CMP_WIDTHS))
    return [hdr, "-" * len(hdr)]

def cmp_lines(rows):
    """Text lines for comparison rows (volumes indented under their protection group)."""
    out = []
    for row in rows:
        label = row["label"] if row["volume"] is None else "  " + row["label"]
        out.append(f"{label[:33]:<34}" + "".join(f"{v:>{w}}" for v, w in zip(cmp_cells(row), CMP_WIDTHS)))
    return out or ["(no data)"]

def summary_kinds(summary, report, period):
    """Which breakdowns the text report prints: 'auto' = weekly for periods under 5 weeks, else monthly."""
    if summary == "auto":
        s = period["start"] or report.first
        e = period["end"] or report.today
        return ["week"] if s and (e - s).days < 35 else ["month"]
    return {"both": ["week", "month"], "none": []}.get(summary, [summary])

def text_report(report, period, pg_filter=None, summary="auto"):
    """Full text report: Overview, comparison with the previous period, then weekly / monthly breakdowns."""
    model = report.model_for(period)
    out = [f"Pure DR replication report   {report.describe(period)}",
           f"Logs: {report.first} .. {report.last}   (as of {report.today})" if report.first else "No DR data found", ""]
    out += overview_lines(model, pg_filter)
    if period["prev_start"]:
        out += ["", f"PERIOD COMPARISON  {report.describe(period)}", "(chg = change vs the previous period; "
                "Avg/day chg is the like-for-like comparison)"]
        out += cmp_header() + cmp_lines(report.comparison(period, pg_filter))
    for kind in summary_kinds(summary, report, period):
        name = "WEEKLY (Mon-Sun)" if kind == "week" else "MONTHLY"
        out += ["", f"{name} SUMMARY   (chg = change vs the previous {kind}; * = {kind} in progress)"]
        out += cmp_header()
        for bk in report.buckets(kind, period, pg_filter):
            out += [f"[{bk['title']}]"] + cmp_lines(bk["rows"])
    return "\n".join(out)

# Per-run CSV columns (one row per DRDATA record; time = run start)
RUN_COLS = ["time", "run", "source_pgroup", "dr_pgroup", "volume", "source_volume", "status",
            "size_bytes", "data_bytes", "written_bytes", "seconds", "time_source", "error"]
SUM_COLS = ["protection_group", "dr_pgroup", "level", "volume", "days", "data_bytes", "data_gib",
            "avg_per_day_bytes", "avg_per_day_gib", "avg_seconds", "avg_time", "max_seconds", "max_time",
            "ok_runs", "failed_runs", "prev_days", "prev_data_bytes", "prev_data_gib", "prev_avg_per_day_bytes",
            "prev_avg_per_day_gib", "prev_avg_seconds", "prev_max_seconds", "prev_ok_runs", "prev_failed_runs",
            "data_change_pct", "avg_per_day_change_pct", "avg_time_change_pct"]

def _csv_row(row):
    """One comparison row -> CSV dict (bytes and GiB, seconds and H:MM:SS, change %)."""
    c, p, d = row["cur"], row["prev"] or {}, row["chg"]
    rnd = lambda x, n=0: "" if x is None else (round(x) if n == 0 else round(x, n))
    gib = lambda x: "" if x is None else round(x / 1024**3, 3)
    return {"protection_group": row["pg"], "dr_pgroup": row["dr_pgroup"],
            "level": "pgroup" if row["volume"] is None else "volume", "volume": row["volume"] or "",
            "days": c["days"], "data_bytes": rnd(c["data"]), "data_gib": gib(c["data"]),
            "avg_per_day_bytes": rnd(c["per_day"]), "avg_per_day_gib": gib(c["per_day"]),
            "avg_seconds": rnd(c["avg_secs"]), "avg_time": fmt_secs(c["avg_secs"]) if c["avg_secs"] is not None else "",
            "max_seconds": rnd(c["max_secs"]), "max_time": fmt_secs(c["max_secs"]) if c["max_secs"] is not None else "",
            "ok_runs": c["n_ok"], "failed_runs": c["n_fail"], "prev_days": p.get("days", ""),
            "prev_data_bytes": rnd(p.get("data")), "prev_data_gib": gib(p.get("data")),
            "prev_avg_per_day_bytes": rnd(p.get("per_day")), "prev_avg_per_day_gib": gib(p.get("per_day")),
            "prev_avg_seconds": rnd(p.get("avg_secs")), "prev_max_seconds": rnd(p.get("max_secs")),
            "prev_ok_runs": p.get("n_ok", ""), "prev_failed_runs": p.get("n_fail", ""),
            "data_change_pct": rnd(d["data"], 1), "avg_per_day_change_pct": rnd(d["per_day"], 1),
            "avg_time_change_pct": rnd(d["avg_secs"], 1)}

def export_csv(report, period, path, pg_filter=None):
    """Per-run CSV for the period + <name>_period.csv, <name>_weekly.csv, <name>_monthly.csv."""
    p = Path(path).expanduser()
    p.parent.mkdir(parents=True, exist_ok=True)                    # create the folder if needed
    base = p.with_suffix("") if p.suffix.lower() == ".csv" else p
    written = []
    keep = lambda r: pg_filter in (None, ALL) or pg_filter in (r.get("source_pgroup"), r.get("dr_pgroup"))
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=RUN_COLS, extrasaction="ignore")
        w.writeheader()
        for r in report.window(period["start"], period["end"]):
            if keep(r):
                w.writerow({**r, "time": r["time"].isoformat(sep=" ")})
    written.append(str(p))

    path_p = f"{base}_period.csv"
    with open(path_p, "w", newline="", encoding="utf-8") as f:
        cols = ["period", "start", "end", "prev_start", "prev_end"] + SUM_COLS
        w = csv.DictWriter(f, fieldnames=cols); w.writeheader()
        s, e = period["start"] or report.first, period["end"] or report.today
        for row in report.comparison(period, pg_filter):
            w.writerow({"period": period["label"], "start": s, "end": e, "prev_start": period["prev_start"] or "",
                        "prev_end": period["prev_end"] or "", **_csv_row(row)})
    written.append(path_p)

    for kind, suffix in (("week", "weekly"), ("month", "monthly")):
        path_b = f"{base}_{suffix}.csv"
        with open(path_b, "w", newline="", encoding="utf-8") as f:
            cols = [kind, f"{kind}_start", f"{kind}_end", "in_progress"] + SUM_COLS
            w = csv.DictWriter(f, fieldnames=cols); w.writeheader()
            for bk in report.buckets(kind, period, pg_filter):
                label = iso_week(bk["start"]) if kind == "week" else f"{bk['start']:%Y-%m}"
                for row in bk["rows"]:
                    w.writerow({kind: label, f"{kind}_start": bk["start"], f"{kind}_end": bk["end"],
                                "in_progress": "yes" if bk["partial"] else "", **_csv_row(row)})
        written.append(path_b)
    return written


# ============================================================================ charts (matplotlib)
def palette(model):
    """Base colour per protection group; volumes get lighter shades of it."""
    from matplotlib import colormaps
    from matplotlib.colors import to_rgb
    base = colormaps["tab10"].colors
    pg_col, vol_col = {}, {}
    for i, (name, pg) in enumerate(model.pgroups.items()):
        c = to_rgb(base[i % len(base)])
        pg_col[name] = c
        n = max(1, len(pg["volumes"]))
        for k, v in enumerate(pg["volumes"]):
            t = 0.55 * k / n                            # blend toward white
            vol_col[(name, v)] = tuple(ch + (1 - ch) * t for ch in c)
    return pg_col, vol_col

def _empty(fig, msg="No data for this selection"):
    """Replace a figure's contents with a centred message (no data for the selection)."""
    ax = fig.add_subplot(111); ax.axis("off")
    ax.text(0.5, 0.5, msg, ha="center", va="center", fontsize=12, color="#666")

def _gib(n):
    """Bytes -> GiB for chart axes (None -> 0)."""
    return (n or 0) / 1024**3

def chart_sizes(model, fig, pg_filter=None):
    """Sizes tab: horizontal bar per DR volume (provisioned size), coloured by protection group."""
    fig.clear()
    pgs = model.selected(pg_filter)
    items = [(pg["name"], v) for pg in pgs for v in pg["volumes"].values() if v["size"]]
    if not items:
        return _empty(fig, "No volume sizes in the logs (sizes are recorded by pg_dr_refresh.py runs)")
    pg_col, _ = palette(model)
    ax = fig.add_subplot(111)
    labels = [f"{v['name']}" for _, v in items][::-1]
    vals = [_gib(v["size"]) for _, v in items][::-1]
    cols = [pg_col[p] for p, _ in items][::-1]
    bars = ax.barh(labels, vals, color=cols)
    for b, (_, v) in zip(bars, items[::-1]):
        ax.text(b.get_width(), b.get_y() + b.get_height() / 2, "  " + fmt_bytes(v["size"]),
                va="center", fontsize=9)
    ax.set_xlabel("Provisioned size (GiB)")
    ax.set_title("DR volume sizes by protection group", loc="left", fontweight="bold")
    ax.set_xlim(0, max(vals) * 1.25)
    from matplotlib.patches import Patch
    ax.legend(handles=[Patch(color=pg_col[pg["name"]], label=f"{pg['name']} ({fmt_bytes(pg['size'])})")
                       for pg in pgs if pg["size"]], loc="lower right", frameon=False)
    ax.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()

def chart_daily(model, fig, pg_filter=None):
    """Daily change tab: top = data replicated per day stacked by volume;
    bottom = average daily change per volume as % of its provisioned size."""
    fig.clear()
    pgs = model.selected(pg_filter)
    vols = [(pg["name"], v) for pg in pgs for v in pg["volumes"].values() if v["daily"]]
    if not vols:
        return _empty(fig)
    _, vol_col = palette(model)
    days = sorted({d for _, v in vols for d in v["daily"]})
    ax1 = fig.add_subplot(211)
    bottom = [0.0] * len(days)
    for p, v in vols:
        vals = [_gib(v["daily"].get(d)) for d in days]
        ax1.bar(days, vals, bottom=bottom, color=vol_col[(p, v["name"])], label=f"{p} / {v['name']}", width=0.8)
        bottom = [a + b for a, b in zip(bottom, vals)]
    ax1.set_ylabel("Data replicated per day (GiB)")
    ax1.set_title("Daily change rate (data replicated per day)", loc="left", fontweight="bold")
    ax1.legend(fontsize=8, frameon=False, ncol=2, loc="upper left", bbox_to_anchor=(1.0, 1.0))
    ax1.spines[["top", "right"]].set_visible(False)
    fig.autofmt_xdate()

    ax2 = fig.add_subplot(212)
    rows = [(f"{v['name']}", v["change_pct"], vol_col[(p, v["name"])], v["avg_daily"]) for p, v in vols]
    rows = [r for r in rows if r[1] is not None][::-1]
    if rows:
        bars = ax2.barh([r[0] for r in rows], [r[1] for r in rows], color=[r[2] for r in rows])
        for b, r in zip(bars, rows):
            ax2.text(b.get_width(), b.get_y() + b.get_height() / 2,
                     f"  {r[1]:.2f}%/day  ({fmt_bytes(r[3])}/day)", va="center", fontsize=9)
        ax2.set_xlim(0, max(r[1] for r in rows) * 1.6)
    else:
        ax2.text(0.5, 0.5, "Change % needs volume sizes", ha="center", transform=ax2.transAxes)
    ax2.set_xlabel("Average daily change (% of provisioned size)")
    ax2.set_title("Average daily change rate by volume", loc="left", fontweight="bold")
    ax2.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()

def chart_repl_time(model, fig, pg_filter=None):
    """Replication time tab: top = time per run per protection group (failed runs marked x);
    bottom = average and maximum replication time per volume."""
    fig.clear()
    pgs = model.selected(pg_filter)
    if not any(pg["runs"] or pg["volumes"] for pg in pgs):
        return _empty(fig)
    pg_col, vol_col = palette(model)
    ax1 = fig.add_subplot(211)
    for pg in pgs:
        runs = pg["runs"] or []
        ok = [r for r in runs if r.get("status") == "ok" and r.get("seconds") is not None]
        bad = [r for r in runs if r.get("status") == "failed"]
        if ok:
            ax1.plot([r["time"] for r in ok], [r["seconds"] / 60 for r in ok], marker="o", ms=4,
                     color=pg_col[pg["name"]], label=f"{pg['name']} -> {pg['dr_pgroup']}")
        if bad:
            ax1.scatter([r["time"] for r in bad], [0] * len(bad), marker="x", s=60, color="#d62728",
                        zorder=5, label="Failed run", clip_on=False)
    ax1.set_ylabel("Minutes")
    ax1.set_title("Replication time per run", loc="left", fontweight="bold")
    handles, labels = ax1.get_legend_handles_labels()
    uniq = dict(zip(labels, handles))                       # one "Failed run" entry
    ax1.legend(uniq.values(), uniq.keys(), fontsize=8, frameon=False, loc="upper left", bbox_to_anchor=(1.0, 1.0))
    ax1.set_ylim(bottom=0)
    ax1.spines[["top", "right"]].set_visible(False)
    fig.autofmt_xdate()

    ax2 = fig.add_subplot(212)
    rows = [(v["name"], v["avg_secs"], v["max_secs"], vol_col[(pg["name"], v["name"])])
            for pg in pgs for v in pg["volumes"].values() if v["avg_secs"] is not None][::-1]
    if rows:
        y = range(len(rows))
        ax2.barh([i + 0.2 for i in y], [r[2] / 60 for r in rows], height=0.4, color="#cccccc", label="Maximum")
        ax2.barh([i - 0.2 for i in y], [r[1] / 60 for r in rows], height=0.4, color=[r[3] for r in rows],
                 label="Average")
        ax2.set_yticks(list(y)); ax2.set_yticklabels([r[0] for r in rows])
        for i, r in zip(y, rows):
            ax2.text(r[2] / 60, i + 0.2, f"  max {fmt_secs(r[2])}", va="center", fontsize=8)
            ax2.text(r[1] / 60, i - 0.2, f"  avg {fmt_secs(r[1])}", va="center", fontsize=8)
        ax2.set_xlim(0, max(r[2] for r in rows) / 60 * 1.3)
        ax2.legend(fontsize=8, frameon=False, loc="lower right")
    else:
        ax2.text(0.5, 0.5, "No per-volume replication times in the logs", ha="center", transform=ax2.transAxes)
    ax2.set_xlabel("Minutes")
    ax2.set_title("Per-volume replication time", loc="left", fontweight="bold")
    ax2.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()

def chart_buckets(report, fig, period, pg_filter=None, kind="week"):
    """Weekly / monthly totals stacked by protection group + avg / max replication time per bucket."""
    fig.clear()
    buckets = report.buckets(kind, period, pg_filter)
    names = [n for n in report.all_model.pgroups if pg_filter in (None, ALL, n)]
    if not buckets or not any(r["cur"]["n_ok"] or r["cur"]["n_fail"] for bk in buckets for r in bk["rows"]):
        return _empty(fig)
    pg_col, _ = palette(report.all_model)
    unit = "week" if kind == "week" else "month"
    pgrow = lambda bk, n: next((r for r in bk["rows"] if r["pg"] == n and r["volume"] is None), None)
    x = list(range(len(buckets)))
    ax1 = fig.add_subplot(211)
    bottom = [0.0] * len(x)
    for n in names:
        vals = [_gib((pgrow(bk, n) or {"cur": {"data": 0}})["cur"]["data"]) for bk in buckets]
        bars = ax1.bar(x, vals, bottom=bottom, color=pg_col[n], width=0.7, label=n)
        for bar, bk in zip(bars, buckets):
            if bk["partial"]:
                bar.set_hatch("//"); bar.set_alpha(0.55)
        bottom = [a + v for a, v in zip(bottom, vals)]
    for i, bk in enumerate(buckets):                         # change of avg/day vs previous bucket
        rows = [pgrow(bk, n) for n in names if pgrow(bk, n)]
        cur = _sum(r["cur"]["per_day"] for r in rows)
        prev = _sum((r["prev"] or {}).get("per_day") for r in rows)
        ch = pct(cur, prev)
        if ch is not None:
            ax1.text(i, bottom[i], f"{ch:+.0f}%", ha="center", va="bottom", fontsize=8,
                     color="#c62828" if ch > 25 else "#333")
    ax1.set_xticks(x); ax1.set_xticklabels([bk["short"] for bk in buckets], fontsize=8,
                                           rotation=45 if len(x) > 8 else 0, ha="right" if len(x) > 8 else "center")
    ax1.set_ylabel(f"Data replicated per {unit} (GiB)")
    ax1.set_title(f"{'Weekly (Mon-Sun)' if kind == 'week' else 'Monthly'} data replicated   "
                  f"(labels: avg/day change vs previous {unit}; hatched = {unit} in progress)",
                  loc="left", fontweight="bold", fontsize=10)
    ax1.legend(fontsize=8, frameon=False, loc="upper left", bbox_to_anchor=(1.0, 1.0))
    ax1.set_ylim(0, max(bottom + [0.001]) * 1.15)
    ax1.spines[["top", "right"]].set_visible(False)

    ax2 = fig.add_subplot(212)
    for n in names:
        rows = [pgrow(bk, n) for bk in buckets]
        avg = [r["cur"]["avg_secs"] / 60 if r and r["cur"]["avg_secs"] is not None else float("nan") for r in rows]
        mx = [r["cur"]["max_secs"] / 60 if r and r["cur"]["max_secs"] is not None else float("nan") for r in rows]
        ax2.plot(x, avg, marker="o", ms=4, color=pg_col[n], label=f"{n} average")
        ax2.plot(x, mx, ls="--", color=pg_col[n], alpha=0.6, label=f"{n} maximum")
    top = ax2.get_ylim()[1]
    for i, bk in enumerate(buckets):
        fails = sum(r["cur"]["n_fail"] for r in bk["rows"] if r["volume"] is None)
        if fails:
            ax2.text(i, top * 0.02, f"{fails} failed", ha="center", va="bottom", fontsize=7, color="#c62828")
    ax2.set_ylim(bottom=0)
    ax2.set_xticks(x); ax2.set_xticklabels([bk["short"] for bk in buckets], fontsize=8,
                                           rotation=45 if len(x) > 8 else 0, ha="right" if len(x) > 8 else "center")
    ax2.set_ylabel("Minutes per run")
    ax2.set_title("Replication time per run (average and maximum)", loc="left", fontweight="bold", fontsize=10)
    ax2.legend(fontsize=8, frameon=False, loc="upper left", bbox_to_anchor=(1.0, 1.0))
    ax2.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()

CHARTS = [("Sizes", chart_sizes), ("Daily change", chart_daily), ("Replication time", chart_repl_time)]

def export_png(report, period, outdir, pg_filter=None):
    """Save the Sizes, Daily change, Replication time, Weekly and Monthly charts for the period as PNGs
    (uses the non-GUI Agg backend, so it works without Tk or a display). Returns the file paths."""
    from matplotlib.figure import Figure
    from matplotlib.backends.backend_agg import FigureCanvasAgg      # no GUI backend needed
    os.makedirs(outdir, exist_ok=True)
    model = report.model_for(period)
    jobs = [(t, lambda fig, fn=fn: fn(model, fig, pg_filter)) for t, fn in CHARTS]
    jobs += [(t, lambda fig, k=k: chart_buckets(report, fig, period, pg_filter, k))
             for t, k in (("Weekly", "week"), ("Monthly", "month"))]
    saved = []
    for title, draw in jobs:
        fig = Figure(figsize=(12, 7.5), dpi=110)
        FigureCanvasAgg(fig)
        draw(fig)
        path = os.path.join(outdir, f"pure-dr-{title.lower().replace(' ', '-')}.png")
        fig.savefig(path)
        saved.append(path)
    return saved


# ============================================================================ GUI (tkinter)
def run_gui(args, files, report, period_key, custom):
    """Open the Tk window: period picker, protection-group filter, Overview tree, chart tabs and the
    Weekly / Monthly tab. All tabs follow the selected period and protection group."""
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox
    import matplotlib; matplotlib.use("TkAgg")
    from matplotlib.figure import Figure
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk

    root = tk.Tk()
    root.title("Pure DR Replication Report")
    root.geometry("1320x860")
    state = {"files": files, "report": report, "model": None, "period": None}

    # ---- row 1: data info + file actions
    top = ttk.Frame(root, padding=(10, 8, 10, 2)); top.pack(fill="x")
    info = ttk.Label(top, font=("TkDefaultFont", 10)); info.pack(side="left")
    ttk.Button(top, text="Export PNGs...", command=lambda: do_export()).pack(side="right", padx=4)
    ttk.Button(top, text="Export CSV...", command=lambda: do_export_csv()).pack(side="right", padx=4)
    ttk.Button(top, text="Reload", command=lambda: reload(state["files"])).pack(side="right", padx=4)
    ttk.Button(top, text="Open logs...", command=lambda: open_logs()).pack(side="right", padx=4)

    # ---- row 2: period picker (+ custom range) and protection group
    bar = ttk.Frame(root, padding=(10, 2, 10, 8)); bar.pack(fill="x")
    ttk.Label(bar, text="Period:").pack(side="left")
    keys = list(PERIODS) if period_key in PERIODS else [period_key] + list(PERIODS)
    key_of = {period_label(k): k for k in keys}
    period_var = tk.StringVar(value=period_label(period_key))
    period_box = ttk.Combobox(bar, textvariable=period_var, values=list(key_of), state="readonly", width=24)
    period_box.pack(side="left", padx=(4, 8))
    custom_box = ttk.Frame(bar)
    c_start = custom[0] or report.first or report.today
    c_end = custom[1] or report.today
    from_var, to_var = tk.StringVar(value=str(c_start)), tk.StringVar(value=str(c_end))
    ttk.Label(custom_box, text="From").pack(side="left")
    from_entry = ttk.Entry(custom_box, textvariable=from_var, width=11); from_entry.pack(side="left", padx=(3, 6))
    ttk.Label(custom_box, text="To").pack(side="left")
    to_entry = ttk.Entry(custom_box, textvariable=to_var, width=11); to_entry.pack(side="left", padx=(3, 6))
    ttk.Button(custom_box, text="Apply", command=lambda: refresh()).pack(side="left")
    ttk.Label(custom_box, text="(YYYY-MM-DD)", foreground="#777").pack(side="left", padx=4)
    period_txt = ttk.Label(bar, foreground="#444"); period_txt.pack(side="left", padx=10)
    pg_var = tk.StringVar(value=args.pgroup or ALL)
    pg_box = ttk.Combobox(bar, textvariable=pg_var, state="readonly", width=26)
    pg_box.pack(side="right", padx=(4, 0))
    ttk.Label(bar, text="Protection group:").pack(side="right")

    nb = ttk.Notebook(root); nb.pack(fill="both", expand=True, padx=8, pady=(0, 8))

    # ---- overview tab (tree)
    ov = ttk.Frame(nb); nb.add(ov, text="Overview")
    cols = ("size", "avg", "pct", "peak", "lastdata", "lasttime", "avgtime", "maxtime", "runs", "status")
    heads = ("Size", "Avg change/day", "%/day", "Peak day", "Last replicated", "Last time",
             "Avg time", "Max time", "OK / Failed", "Last status")
    tree = ttk.Treeview(ov, columns=cols, show="tree headings")
    tree.heading("#0", text="Protection group / volume"); tree.column("#0", width=260)
    for c, h in zip(cols, heads):
        tree.heading(c, text=h); tree.column(c, width=105, anchor="e")
    tree.column("status", anchor="center")
    tree.tag_configure("pg", font=("TkDefaultFont", 10, "bold"))
    tree.tag_configure("failed", foreground="#c62828")
    sb = ttk.Scrollbar(ov, orient="vertical", command=tree.yview); tree.configure(yscrollcommand=sb.set)
    tree.pack(side="left", fill="both", expand=True); sb.pack(side="right", fill="y")

    # ---- chart tabs
    canvases = []
    for title, fn in CHARTS:
        frame = ttk.Frame(nb); nb.add(frame, text=title)
        fig = Figure(figsize=(10, 6.5), dpi=100)
        canvas = FigureCanvasTkAgg(fig, master=frame)
        NavigationToolbar2Tk(canvas, frame).update()
        canvas.get_tk_widget().pack(fill="both", expand=True)
        canvases.append((fn, fig, canvas))

    # ---- weekly / monthly tab: comparison tree (top) + chart (bottom)
    wm = ttk.Frame(nb); nb.add(wm, text="Weekly / Monthly")
    ctl = ttk.Frame(wm, padding=(4, 6)); ctl.pack(fill="x")
    gran_var = tk.StringVar(value="week" if args.summary == "week" else
                            "month" if args.summary == "month" else "auto")
    ttk.Label(ctl, text="Group by:").pack(side="left")
    for val, txt in (("week", "Weekly (Mon-Sun)"), ("month", "Monthly")):
        ttk.Radiobutton(ctl, text=txt, value=val, variable=gran_var,
                        command=lambda: refresh_wm()).pack(side="left", padx=6)
    ttk.Label(ctl, foreground="#555",
              text="chg = change vs the previous week / month (first row: vs the previous period).  "
                   "Avg/day chg is like-for-like.  * = still in progress").pack(side="left", padx=12)
    pane = ttk.PanedWindow(wm, orient="vertical"); pane.pack(fill="both", expand=True)
    tf, cf = ttk.Frame(pane), ttk.Frame(pane)
    pane.add(tf, weight=1); pane.add(cf, weight=1)
    wcols = ("data", "perday", "avgtime", "maxtime", "runs", "pdata", "pperday", "cdata", "cperday", "ctime")
    wtree = ttk.Treeview(tf, columns=wcols, show="tree headings")
    wtree.heading("#0", text="Period / protection group / volume"); wtree.column("#0", width=330)
    for c, h in zip(wcols, CMP_HEADS):
        wtree.heading(c, text=h); wtree.column(c, width=92, anchor="e")
    wtree.tag_configure("hdr", font=("TkDefaultFont", 10, "bold"), background="#eef2f7")
    wtree.tag_configure("pg", font=("TkDefaultFont", 10, "bold"))
    wtree.tag_configure("failed", foreground="#c62828")
    wsb = ttk.Scrollbar(tf, orient="vertical", command=wtree.yview); wtree.configure(yscrollcommand=wsb.set)
    wtree.pack(side="left", fill="both", expand=True); wsb.pack(side="right", fill="y")
    wfig = Figure(figsize=(10, 4.2), dpi=100)
    wcanvas = FigureCanvasTkAgg(wfig, master=cf)
    NavigationToolbar2Tk(wcanvas, cf).update()
    wcanvas.get_tk_widget().pack(fill="both", expand=True)

    def values(o):
        """Overview tree cell values for one pgroup or volume."""
        last = o["last"] or {}
        return (fmt_bytes(o["size"]), fmt_bytes(o["avg_daily"]),
                f"{o['change_pct']:.2f}%" if o["change_pct"] is not None else "-",
                fmt_bytes(o["peak_daily"]), fmt_bytes(last.get("data_bytes")), fmt_secs(last.get("seconds")),
                fmt_secs(o["avg_secs"]), fmt_secs(o["max_secs"]), f"{o['n_ok']} / {o['n_fail']}", o["last_status"])

    def current_period():
        """Period from the picker (shows the From / To boxes for a custom range; keeps the last
            valid period and shows an error if a custom date is invalid)."""
        key = key_of.get(period_var.get(), "all")
        if key == "custom":
            custom_box.pack(side="left", after=period_box)
            try:
                s, e = parse_date(from_var.get()), parse_date(to_var.get())
            except argparse.ArgumentTypeError as ex:
                messagebox.showerror("Custom date range", str(ex))
                return state["period"]
            return state["report"].period("custom", s, e)
        custom_box.pack_forget()
        return state["report"].period(key)

    def kind():
        """Grouping for the Weekly / Monthly tab ('auto' resolves once, from the period length)."""
        g = gran_var.get()
        if g == "auto":
            g = summary_kinds("auto", state["report"], state["period"])[0]
            gran_var.set(g)
        return g

    def add_block(text, rows, open_, tags=("hdr",)):
        """Add one period / week / month to the Weekly / Monthly tree: header row, then pgroups with
            their volumes nested beneath."""
        node = wtree.insert("", "end", text=text, open=open_, tags=tags)
        parents = {}
        for row in rows:
            ftag = ("failed",) if row["cur"]["n_fail"] else ()
            if row["volume"] is None:
                parents[row["pg"]] = wtree.insert(node, "end", text=row["label"], values=cmp_cells(row),
                                                  open=True, tags=("pg",) + ftag)
            else:
                wtree.insert(parents.get(row["pg"], node), "end", text=row["label"], values=cmp_cells(row), tags=ftag)

    def refresh_wm():
        """Rebuild the Weekly / Monthly tab: the selected period vs previous, then each bucket newest first,
            and redraw its chart."""
        rep, per, sel = state["report"], state["period"], pg_var.get()
        k = kind()
        wtree.delete(*wtree.get_children())
        if per["prev_start"]:
            add_block(f"Selected period  ({rep.describe(per)})", rep.comparison(per, sel), True)
        buckets = rep.buckets(k, per, sel)
        for i, bk in enumerate(reversed(buckets)):                  # newest first
            add_block(bk["title"], bk["rows"], i < 2)
        chart_buckets(rep, wfig, per, sel, k); wcanvas.draw_idle()

    def refresh(*_):
        """Re-read the picker and filter, then rebuild every tab."""
        state["period"] = current_period()
        rep, per = state["report"], state["period"]
        m = state["model"] = rep.model_for(per)
        sel = pg_var.get()
        info.config(text=(f"{len(state['files'])} log file(s)   "
                          + (f"logs {rep.first:%Y-%m-%d} .. {rep.last:%Y-%m-%d}   "
                             f"{len(rep.all_model.pgroups)} protection group(s), "
                             f"{sum(len(p['volumes']) for p in rep.all_model.pgroups.values())} volume(s)"
                             if rep.first else "no DR data found")))
        period_txt.config(text=rep.describe(per).split(": ", 1)[-1])
        pg_box["values"] = [ALL] + list(rep.all_model.pgroups)
        if sel not in pg_box["values"]:
            pg_var.set(ALL); sel = ALL
        tree.delete(*tree.get_children())
        for pg in m.selected(sel):
            tags = ("pg",) + (("failed",) if pg["last_status"] == "failed" else ())
            pid = tree.insert("", "end", text=f"{pg['name']}  ->  {pg['dr_pgroup']}", values=values(pg),
                              open=True, tags=tags)
            for v in pg["volumes"].values():
                label = v["name"] + (f"   (from {v['source']})" if v["source"] else "")
                tree.insert(pid, "end", text=label, values=values(v),
                            tags=(("failed",) if v["last_status"] == "failed" else ()))
        if not m.pgroups:
            tree.insert("", "end", text="(no runs in this period)")
        for fn, fig, canvas in canvases:
            fn(m, fig, sel); canvas.draw_idle()
        refresh_wm()

    def reload(new_files):
        """Re-read log files (Reload / Open logs...) and refresh."""
        state["files"] = new_files
        state["report"] = Report(load_records(new_files), args.as_of)
        refresh()

    def open_logs():
        """Pick log files to load instead of the configured folder."""
        picked = filedialog.askopenfilenames(title="Select pg_dr_refresh log files",
                                             filetypes=[("Log files", "*.log"), ("All files", "*")])
        if picked:
            reload(sorted(picked))

    def do_export():
        """Export PNG charts for the current period / protection group to a chosen folder."""
        d = filedialog.askdirectory(title="Folder for PNG charts")
        if d:
            saved = export_png(state["report"], state["period"], d, pg_var.get())
            messagebox.showinfo("Export complete", "Saved:\n" + "\n".join(saved))

    def do_export_csv():
        """Export the per-run CSV plus _period, _weekly and _monthly summaries to a chosen file name."""
        f = filedialog.asksaveasfilename(title="CSV file (per-run data; summaries saved alongside)",
                                         defaultextension=".csv", initialfile="pure-dr-report.csv",
                                         filetypes=[("CSV", "*.csv")])
        if f:
            saved = export_csv(state["report"], state["period"], f, pg_var.get())
            messagebox.showinfo("Export complete", "Saved:\n" + "\n".join(saved))

    period_box.bind("<<ComboboxSelected>>", refresh)
    pg_box.bind("<<ComboboxSelected>>", refresh)
    for ent in (from_entry, to_entry):
        ent.bind("<Return>", refresh)
    refresh()
    root.mainloop()


# ============================================================================ main
def main():
    """Parse arguments, find and read the logs, resolve the period, then export / print / open the window."""
    ap = argparse.ArgumentParser(description="Graphical report of pg_dr_refresh.py log files")
    ap.add_argument("-c", "--config", help="pg_dr_refresh JSON config (uses its log_file location)")
    ap.add_argument("-l", "--logs", nargs="+", help="log file(s), folder(s) or glob(s)")
    ap.add_argument("--period", choices=[k for k in PERIODS if k != "custom"],
                    help="last7 | last30 | prev-week (Mon-Sun) | prev-month | this-month | all (default)")
    ap.add_argument("--from", dest="date_from", type=parse_date, metavar="YYYY-MM-DD",
                    help="custom range start (default: first log)")
    ap.add_argument("--to", dest="date_to", type=parse_date, metavar="YYYY-MM-DD",
                    help="custom range end, inclusive (default: today)")
    ap.add_argument("--days", type=int, help="rolling period: the last N days")
    ap.add_argument("--as-of", type=parse_date, metavar="YYYY-MM-DD",
                    help="treat this date as today (default: today)")
    ap.add_argument("--summary", choices=["auto", "week", "month", "both", "none"], default="auto",
                    help="weekly/monthly section in the text report (auto: weekly for periods under "
                         "5 weeks, else monthly); also the initial Weekly / Monthly tab grouping")
    ap.add_argument("--pgroup", help="only this protection group (GUI: start with it selected)")
    ap.add_argument("--no-gui", action="store_true", help="print a text report instead of opening a window")
    ap.add_argument("--export-png", metavar="DIR", help="save the charts as PNG files to DIR")
    ap.add_argument("--export-csv", metavar="FILE",
                    help="save per-run data to FILE, plus FILE_period.csv, FILE_weekly.csv, FILE_monthly.csv")
    args = ap.parse_args()

    chosen = [bool(args.period), bool(args.date_from or args.date_to), bool(args.days)]
    if sum(chosen) > 1:
        ap.error("use only one of --period, --from/--to, --days")
    if args.days is not None and args.days < 1:
        ap.error("--days must be 1 or more")
    period_key = ("custom" if chosen[1] else f"last{args.days}" if args.days else args.period or "all")

    files, pats = find_logs(args.logs, args.config)
    if not files:
        sys.exit("No log files found in: " + ", ".join(pats))
    report = Report(load_records(files), args.as_of)
    period = report.period(period_key, args.date_from, args.date_to)

    if args.export_csv:
        for p in export_csv(report, period, args.export_csv, args.pgroup):
            print(f"CSV written: {p}")
    if args.export_png:
        try:
            import matplotlib  # noqa: F401
        except ImportError:
            sys.exit("--export-png needs matplotlib 3.5+: install the distro package (e.g. python3-matplotlib) "
                     "or 'pip install matplotlib' inside a virtual environment - see README.md")
        for p in export_png(report, period, args.export_png, args.pgroup):
            print(f"PNG written: {p}")
    if args.no_gui or args.export_png or args.export_csv:
        if args.no_gui:
            print(text_report(report, period, args.pgroup, args.summary))
        return
    try:
        import tkinter  # noqa: F401
        import matplotlib  # noqa: F401
    except ImportError as e:
        print(f"GUI unavailable ({e}). Install matplotlib and Tk (see README.md: Linux packages "
              f"python3-matplotlib + python3-tk / python3-tkinter). Showing text report instead.\n")
        print(text_report(report, period, args.pgroup, args.summary))
        return
    if sys.platform.startswith("linux") and not (os.environ.get("DISPLAY") or os.environ.get("WAYLAND_DISPLAY")):
        print("No display found (DISPLAY is not set). Run from a desktop session or 'ssh -X', or use "
              "--export-png / --export-csv. Showing text report instead.\n")
        print(text_report(report, period, args.pgroup, args.summary))
        return
    try:
        run_gui(args, files, report, period_key, (args.date_from, args.date_to))
    except tkinter.TclError as e:                                  # e.g. display refused the connection
        print(f"Cannot open the window ({e}). Showing text report instead.\n")
        print(text_report(report, period, args.pgroup, args.summary))

if __name__ == "__main__":
    main()
