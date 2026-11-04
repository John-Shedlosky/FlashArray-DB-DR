#!/usr/bin/env python3
"""
pg_dr_refresh.py - FlashArray protection-group DR refresh over SSH (Purity//FA CLI).

What it does
------------
For every protection group (pgroup) listed in the JSON config, in order:

  Phase 1 - prepare and start replication (no waiting between pgroups)
    0. Check every DR (target) volume exists and record its size   purevol list --csv
    1. Snapshot the source pgroup                                  purepgroup snap --apply-retention
    2. Check the snapshot contains every source volume              purevol list --snap --pgrouplist
       (if one is missing, nothing in that pgroup is overwritten)
    3. Overwrite each existing DR volume from its source snapshot   purevol copy --overwrite
    4. Snapshot the DR pgroup and replicate it now                  purepgroup snap --apply-retention --replicate-now

  Phase 2 - monitor every replication together until each one completes or times out
    5. purepgroup list --snap --transfer --csv   (pgroup: started / completed / data transferred)
       purevol list --snap --transfer --csv      (the same per volume)
    6. Write a summary of data replicated and replication time per pgroup and per volume,
       plus one machine-readable 'DRDATA {json}' log line per pgroup and per volume.
       pure-dr-report.py reads those DRDATA lines to build its reports.

Snapshot lifetime follows the retention policy already set on each pgroup on the array
(--apply-retention), so the script never deletes snapshots itself.

Only the SOURCE array is contacted (over SSH). The replication target is named in the config so the
script can check it is connected and label the logs; the script never logs in to it.

Replication progress
--------------------
Completion is detected from the 'Completed' column. Some Purity releases do not report a 'Progress'
column when listing transfers on the replication SOURCE (progress is only known on the target);
in that case the -V display shows "running" with the data transferred so far instead of a % bar.

Logging
-------
Every command, its array output, progress and the summary are written to a weekly log file:
<log_file stem>_<Monday of this week, YYYYMMDD>.log  (e.g. pg_dr_refresh_20261102.log).
The console shows progress and the summary; -V also shows each CLI command and its output.

Configuration
-------------
JSON with optional // and /* */ comments - see pg_dr_config.json for every setting.

Usage
-----
  pg_dr_refresh.py -c pg_dr_config.json            # normal run
  pg_dr_refresh.py -c pg_dr_config.json -V         # verbose: commands, array output, progress bars
  pg_dr_refresh.py -c pg_dr_config.json --dry-run  # log the commands that would run; change nothing
  pg_dr_refresh.py -c pg_dr_config.json --setup    # one-time: create DR volumes, then DR pgroups + target

Exit code: 0 = every pgroup succeeded, 1 = at least one pgroup failed, 2 = config / usage error.

Requirements: Python 3.8+ and an OpenSSH client ('ssh' on the PATH) with key-based login to the
source array. No extra Python packages are needed.
"""
import argparse, csv, datetime, io, json, logging, re, shlex, subprocess, sys, time
from pathlib import Path

if sys.version_info < (3, 8):
    sys.exit("pg_dr_refresh.py needs Python 3.8 or newer (found %d.%d)" % sys.version_info[:2])

# ----------------------------------------------------------------------------- constants
SCRIPT_DIR  = Path(__file__).resolve().parent
DEFAULT_LOG = SCRIPT_DIR / "logs" / "pg_dr_refresh.log"   # becomes logs/pg_dr_refresh_YYYYMMDD.log

# Used for any key missing from the config's "settings" block (see pg_dr_config.json for details).
DEFAULT_SETTINGS = {
    "snapshot_prefix": "dr",            # snapshots are named <pgroup>.<prefix>-YYYYMMDD-HHMMSS
    "wait_for_replication": True,       # monitor until transfer completes (needed for DRDATA / reports)
    "poll_interval_seconds": 30,        # seconds between transfer checks
    "replication_timeout_minutes": 720, # give up on one replication after this long
    "hash_every_percent": 3,            # -V progress bar: one '#' per N% replicated
    "ssh_timeout_seconds": 600,         # maximum run time of a single CLI command
}
# Numeric settings and the smallest value each may have (validated in load_config).
NUMERIC_SETTINGS = {"poll_interval_seconds": 1, "replication_timeout_minutes": 1,
                    "hash_every_percent": 1, "ssh_timeout_seconds": 10}

NAME_RE   = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_:\-]*$")   # Purity object names (pod::vol allowed)
PREFIX_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9\-]*$")     # snapshot suffix: letters, digits, dashes

log  = logging.getLogger("pgdr")        # console + log file
flog = logging.getLogger("pgdr.file")   # log file only (used while drawing the progress bar)


class CLIError(Exception):
    """A Purity CLI command failed (non-zero exit, SSH error or timeout). Fails only the current pgroup."""


def die(msg):
    """Print a config / usage problem to stderr and exit with code 2 (nothing has been changed)."""
    print(msg, file=sys.stderr)
    sys.exit(2)


# ----------------------------------------------------------------------------- logging
def weekly_log_path(base):
    """logs/pg_dr_refresh.log -> logs/pg_dr_refresh_<Monday-of-this-week YYYYMMDD>.log"""
    base = Path(base).expanduser()
    today = datetime.date.today()
    monday = today - datetime.timedelta(days=today.weekday())
    return base.with_name(f"{base.stem}_{monday:%Y%m%d}{base.suffix or '.log'}")


def setup_logging(log_base, verbose, dry_run):
    """Send everything (DEBUG and up) to this week's log file; INFO and up to the console
    (DEBUG too with -V / --dry-run, which shows each CLI command and its output).
    `log` writes to both; `flog` writes to the file only (keeps the -V progress bars clean).
    Returns the log file path."""
    path = weekly_log_path(log_base)
    path.parent.mkdir(parents=True, exist_ok=True)
    fh = logging.FileHandler(path, mode="a", encoding="utf-8")
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)-7s %(message)s"))
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG if (verbose or dry_run) else logging.INFO)
    ch.setFormatter(logging.Formatter("%(message)s"))
    log.setLevel(logging.DEBUG); log.addHandler(fh); log.addHandler(ch)
    flog.setLevel(logging.DEBUG); flog.addHandler(fh); flog.propagate = False
    return path


# ----------------------------------------------------------------------------- ssh / CLI
class FlashArray:
    """The source array, reached with the system 'ssh' client (key-based login, BatchMode)."""
    def __init__(self, host, name, user, key, ssh_timeout, dry_run):
        """host = SSH address; name = label used in logs (replaced by the Purity array name once read);
        user / key = SSH login; ssh_timeout = max seconds per command; dry_run = log commands only."""
        self.host, self.name, self.user, self.key = host, name, user, key
        self.ssh_timeout, self.dry_run = ssh_timeout, dry_run

    def run(self, cmd, quiet=False, check=True):
        """Run one Purity CLI command over SSH and return its stdout.
        The command and the array's output always go to the log file; the console shows them only
        with -V (quiet=True keeps them file-only, used for repeated progress polls).
        Raises CLIError on SSH failure, timeout, or a non-zero exit (unless check=False).
        In dry-run mode the command is only logged and "" is returned."""
        lg = flog if quiet else log
        lg.debug(f"CMD [{self.name}] $ {cmd}")
        if self.dry_run:
            return ""
        ssh = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15"]
        if self.key:
            ssh += ["-i", str(Path(self.key).expanduser())]
        ssh += [f"{self.user}@{self.host}", cmd]
        try:
            r = subprocess.run(ssh, capture_output=True, text=True, timeout=self.ssh_timeout)
        except subprocess.TimeoutExpired:
            raise CLIError(f"[{self.name}] timed out after {self.ssh_timeout}s: {cmd}")
        out, err = r.stdout.rstrip(), r.stderr.strip()
        if out:
            lg.debug(f"OUT [{self.name}]\n" + "\n".join("    " + l for l in out.splitlines()))
        if err:
            lg.debug(f"ERR [{self.name}] {err}")
        if r.returncode != 0 and check:
            raise CLIError(f"[{self.name}] rc={r.returncode} '{cmd}': {err or out}")
        return out


# ----------------------------------------------------------------------------- config
def strip_comments(text):
    """Remove // line comments and /* */ block comments that are outside of JSON strings."""
    out, i, n, in_str = [], 0, len(text), False
    while i < n:
        c = text[i]
        if in_str:
            out.append(c)
            if c == "\\" and i + 1 < n:
                out.append(text[i + 1]); i += 2; continue
            if c == '"':
                in_str = False
        elif c == '"':
            in_str = True; out.append(c)
        elif text.startswith("//", i):
            while i < n and text[i] != "\n":
                i += 1
            continue
        elif text.startswith("/*", i):
            end = text.find("*/", i + 2)
            nl = text.count("\n", i, end if end != -1 else n)
            out.append("\n" * nl)                    # keep line numbers for error messages
            i = n if end == -1 else end + 2
            continue
        else:
            out.append(c)
        i += 1
    return "".join(out)


def resolve_log_base(cfg, config_path):
    """log_file from the config; a relative path is taken relative to the config file's folder
    (so cron jobs work from any working directory). Missing -> <script folder>/logs/pg_dr_refresh.log."""
    base = cfg.get("log_file")
    if not base:
        return DEFAULT_LOG
    base = Path(base).expanduser()
    return base if base.is_absolute() else Path(config_path).resolve().parent / base


def load_config(path):
    """Read and validate the JSON config. Returns (config dict, settings with defaults filled in).
    Exits with a list of every problem found, so all mistakes can be fixed in one pass."""
    try:
        with open(path, encoding="utf-8") as f:
            text = strip_comments(f.read())
    except OSError as e:
        die(f"Cannot read config {path}: {e}")
    try:
        cfg = json.loads(text)
    except json.JSONDecodeError as e:
        die(f"Config {path}: invalid JSON at line {e.lineno} col {e.colno}: {e.msg}")
    settings = {**DEFAULT_SETTINGS, **cfg.get("settings", {})}
    errs = []
    for k in sorted(set(cfg.get("settings", {})) - set(DEFAULT_SETTINGS)):
        errs.append(f"settings.{k} is not a known setting (check the spelling)")
    for k, low in NUMERIC_SETTINGS.items():
        v = settings[k]
        if isinstance(v, bool) or not isinstance(v, (int, float)) or v < low:
            errs.append(f"settings.{k} must be a number >= {low} (got {v!r})")
    if not isinstance(settings["wait_for_replication"], bool):
        errs.append("settings.wait_for_replication must be true or false")
    src, tgt = cfg.get("source_array", {}), cfg.get("target_array", {})
    if not src.get("host"):
        errs.append("source_array.host is required")
    if not tgt.get("name"):
        errs.append("target_array.name is required")
    for k in ("source_retention", "dr_retention"):
        if any(k in pg for pg in cfg.get("protection_groups") or []):
            errs.append(f"'{k}' is no longer supported - set retention on the array (purepgroup retain)")
    if not PREFIX_RE.match(str(settings["snapshot_prefix"])):
        errs.append("settings.snapshot_prefix may contain only letters, digits and dashes")
    pgs = cfg.get("protection_groups") or []
    if not pgs:
        errs.append("protection_groups must contain at least one entry")
    all_targets = set()
    for i, pg in enumerate(pgs):
        tag = f"protection_groups[{i}]"
        for k in ("source_pgroup", "dr_pgroup"):
            if not NAME_RE.match(str(pg.get(k, ""))):
                errs.append(f"{tag}.{k} missing or invalid")
        vols = pg.get("volumes") or []
        if not vols:
            errs.append(f"{tag}.volumes must list at least one {{target, source}} pair")
        for j, v in enumerate(vols):
            s_, t = v.get("source", ""), v.get("target", "")
            if not (NAME_RE.match(s_) and NAME_RE.match(t)):
                errs.append(f"{tag}.volumes[{j}] needs valid 'target' and 'source' names")
            elif s_ == t:
                errs.append(f"{tag}.volumes[{j}] source and target are the same volume ({s_})")
            elif t in all_targets:
                errs.append(f"{tag}.volumes[{j}] target {t} is listed more than once")
            all_targets.add(t)
    if errs:
        die("Config errors:\n  " + "\n  ".join(errs))
    return cfg, settings


def csv_column(out, column):
    """Return the values of one column (case-insensitive header match) from Purity --csv output;
    [] if the output is empty or the column is missing."""
    rows = list(csv.reader(io.StringIO(out)))
    if not rows:
        return []
    hdr = [h.strip().lower() for h in rows[0]]
    if column.lower() not in hdr:
        return []
    i = hdr.index(column.lower())
    return [r[i].strip() for r in rows[1:] if len(r) > i]


def discover_array_name(arr):
    """Read the source array's Purity name (purearray list) so it does not need to be in the config."""
    try:
        names = csv_column(arr.run("purearray list --csv"), "Name")
    except CLIError as e:
        names = []
        log.warning(f"Could not read array name ({e})")
    if names:
        arr.name = names[0]
        log.info(f"Source array Purity name: {arr.name} (ssh host {arr.host})")
    else:
        log.warning(f"Could not determine Purity array name; using host '{arr.host}' in log labels")


def check_target_connected(arr, target):
    """Warn (not fail) if the replication target is not in the source array's connected-array list."""
    for cmd in ("purearray connection list --csv", "purearray list --connect --csv"):  # 6.x, then older
        try:
            names = csv_column(arr.run(cmd), "Name")
        except CLIError:
            continue
        if names:
            if target in names:
                log.info(f"Replication target {target} is connected to {arr.name}")
            else:
                log.warning(f"Target '{target}' not found in connected arrays {names} - "
                            f"check target_array.name matches the Purity array name")
            return
    log.warning("Could not list connected arrays; skipping target check")


# ----------------------------------------------------------------------------- replication progress
UNITS = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4, "P": 1024**5, "E": 1024**6}

def parse_size(s):
    """Purity size text -> bytes (binary units): '1.50G' -> 1610612736.0, '512' -> 512.0, '-' -> None."""
    m = re.match(r"^\s*([\d.]+)\s*([KMGTPE]?)(?:i?B)?\s*$", str(s or ""), re.I)
    return float(m.group(1)) * UNITS[m.group(2).upper()] if m else None

def fmt_size(n):
    """Bytes -> Purity-style text: 1610612736 -> '1.50G'; None -> 'unknown'."""
    if n is None:
        return "unknown"
    for u in ("B", "K", "M", "G", "T", "P"):
        if n < 1024 or u == "P":
            return f"{n:.2f}{u}"
        n /= 1024

def parse_progress(s):
    """Progress column -> percent: '37.62%' -> 37.62, '0.5' -> 50.0 (fraction), '-' or '' -> None."""
    s = str(s or "").strip()
    if not s or s == "-":
        return None
    try:
        return float(s.rstrip("%")) if s.endswith("%") else float(s) * (100 if float(s) <= 1 else 1)
    except ValueError:
        return None

def parse_array_time(s):
    """'2026-11-03 10:00:02 EST' -> datetime (timezone abbreviation ignored); None if '-' or unparseable."""
    s = str(s or "").strip()
    if not s or s == "-":
        return None
    parts = s.split()
    if parts and parts[-1].isalpha():
        parts = parts[:-1]
    try:
        return datetime.datetime.strptime(" ".join(parts), "%Y-%m-%d %H:%M:%S")
    except ValueError:
        return None


def fmt_duration(seconds):
    """Seconds -> 'HH:MM:SS'; None -> 'unknown'."""
    if seconds is None:
        return "unknown"
    seconds = int(round(seconds))
    return f"{seconds // 3600:02d}:{seconds % 3600 // 60:02d}:{seconds % 60:02d}"


def parse_transfer_table(out):
    """Parse `purepgroup|purevol list --snap --transfer --csv` into {name: row}.
    Columns: Name, ..., Started, Completed, Progress, Data Transferred, (Physical) Bytes Written."""
    rows = list(csv.reader(io.StringIO(out)))
    table = {}
    if rows and len(rows[0]) > 1:
        hdr = [h.strip().lower() for h in rows[0]]
        col = lambda word: next((i for i, h in enumerate(hdr) if word in h), None)
        idx = {"name": col("name"), "started": col("started"), "completed": col("completed"),
               "progress": col("progress"), "transferred": col("transferred"), "written": col("written")}
        for r in rows[1:]:
            g = lambda k: r[idx[k]].strip() if idx[k] is not None and idx[k] < len(r) else ""
            if g("name"):
                table[g("name")] = {"progress": parse_progress(g("progress")), "transferred": g("transferred"),
                                    "written": g("written"), "started": g("started"), "completed": g("completed")}
        return table
    for line in out.splitlines():                                  # non-CSV fallback (progress only)
        tok = line.split()
        m = re.search(r"(\d+(?:\.\d+)?)%", line)
        if tok and m:
            table[tok[0]] = {"progress": float(m.group(1)), "transferred": "", "written": "",
                             "started": "", "completed": ""}
    return table


def find_row(table, name):
    """Match a local name or a remote-prefixed name (array:PG.suffix)."""
    if name in table:
        return table[name]
    return next((r for n, r in table.items() if n.endswith(":" + name)), None)


def row_done(row):
    """True once a transfer row is complete: Progress reached 100% or the Completed column has a time.
    (Completed is the reliable signal - newer Purity omits Progress when listed on the source.)"""
    return bool(row) and ((row["progress"] is not None and row["progress"] >= 100)
                          or row["completed"] not in ("", "-"))


def row_duration(row, fallback_seconds=None):
    """Array-reported Started->Completed; falls back to script-measured wall time."""
    st, cp = parse_array_time(row.get("started")), parse_array_time(row.get("completed"))
    if st and cp and cp >= st:
        return (cp - st).total_seconds(), "array"
    return fallback_seconds, "measured"


# ----------------------------------------------------------------------------- workflow
def snap_names(output):
    """First column (snapshot name) of each line of `purevol list --snap` text output."""
    return {l.split()[0].strip('"').split(",")[0] for l in output.splitlines() if l.strip()}


def start_pgroup(arr, pg, target, suffix, run_time, dry_run):
    """Phase 1 for one pgroup: check DR volumes, snapshot, verify, overwrite, snapshot + start replication.
    Returns a 'job' dict that monitor_all() polls; does NOT wait for the transfer.
    Raises CLIError if any step fails; steps after the failure are not run for this pgroup."""
    src_pg, dr_pg, vols = pg["source_pgroup"], pg["dr_pgroup"], pg["volumes"]
    q = shlex.quote
    log.info(f"=== {src_pg} -> {dr_pg} -> {target}  (suffix {suffix}) ===")

    # 0. Pre-check: every DR volume to be overwritten must already exist (also records sizes)
    out = arr.run("purevol list --csv " + " ".join(q(v["target"]) for v in vols))
    sizes = dict(zip(csv_column(out, "Name"), csv_column(out, "Size")))

    # 1. Snapshot the source pgroup; lifetime follows the pgroup's retention policy on the array
    arr.run(f"purepgroup snap --apply-retention --suffix {suffix} {q(src_pg)}")
    src_snap = f"{src_pg}.{suffix}"
    log.info(f"Created snapshot {src_snap}")

    # 2. Verify every source volume is in the snapshot before overwriting anything
    out = arr.run(f"purevol list --snap --pgrouplist {q(src_snap)}")
    if not dry_run:
        present = snap_names(out)
        missing = [v["source"] for v in vols if f"{src_snap}.{v['source']}" not in present]
        if missing:
            raise CLIError(f"{src_snap} does not contain: {', '.join(missing)} - nothing overwritten")

    # 3. Overwrite each existing DR volume from its source volume snapshot
    for v in vols:
        src_vol_snap = f"{src_snap}.{v['source']}"
        arr.run(f"purevol copy --overwrite {q(src_vol_snap)} {q(v['target'])}")
        log.info(f"Overwrote {v['target']} from {src_vol_snap}")

    # 4. Snapshot the DR pgroup and replicate now (retention follows the pgroup's target policy)
    arr.run(f"purepgroup snap --apply-retention --replicate-now --suffix {suffix} {q(dr_pg)}")
    dr_snap = f"{dr_pg}.{suffix}"
    log.info(f"Created snapshot {dr_snap} and started replication to {target} - continuing")

    # Everything monitor_all() / write_summary() need to track this replication
    return {"pg": src_pg, "dr_pg": dr_pg, "suffix": suffix, "dr_snap": dr_snap, "target": target,
            "run_time": run_time, "t0": time.time(), "sizes": sizes,
            "sources": {v["target"]: v["source"] for v in vols},
            "vol_snaps": {f"{dr_snap}.{v['target']}": v["target"] for v in vols},
            "row": None, "vol_rows": {}, "vol_logged": set(), "done": False, "error": None,
            "hashes": 0, "last_p": None, "last_state": None, "estimated": False, "elapsed": None,
            "pg_csv": True, "vol_query": True}


def poll_job(arr, job):
    """One poll of a replication job: pgroup-level row plus per-volume rows. All output -> log file only."""
    q = shlex.quote
    snap = job["dr_snap"]
    cmd = f"purepgroup list --snap --transfer{' --csv' if job['pg_csv'] else ''} {q(snap)}"
    try:
        out = arr.run(cmd, quiet=True)
    except CLIError as e:
        if not job["pg_csv"]:
            raise
        flog.warning(f"{snap}: --csv not accepted ({e}); using text output")
        job["pg_csv"] = False
        return
    row = find_row(parse_transfer_table(out), snap)
    if row:
        job["row"] = row
    if job["vol_query"]:
        vcmd = "purevol list --snap --transfer --csv " + " ".join(q(n) for n in job["vol_snaps"])
        try:
            vtable = parse_transfer_table(arr.run(vcmd, quiet=True))
            for name in job["vol_snaps"]:
                r = find_row(vtable, name)
                if r:
                    job["vol_rows"][name] = r
        except CLIError as e:
            flog.warning(f"{snap}: per-volume transfer listing unavailable ({e}); pgroup-level only")
            job["vol_query"] = False


def log_job_progress(job):
    """Write progress / estimate / per-volume completions to the log file when they change."""
    row, snap = job["row"], job["dr_snap"]
    p = row["progress"] if row else None
    state = (p, row["transferred"]) if row else None
    if row and state != job["last_state"]:                 # log only when something changed
        job["last_state"] = state
        if p is not None:
            job["last_p"] = p
        flog.info(f"PROGRESS {snap}: {f'{p:.2f}%' if p is not None else 'progress not reported'}  "
                  f"data transferred={row['transferred'] or '-'}  physical written={row['written'] or '-'}")
        sent = parse_size(row["transferred"])
        if not job["estimated"] and sent and p is not None and 0 < p < 100:
            flog.info(f"ESTIMATE {snap}: ~{fmt_size(sent / (p / 100))} to be replicated "
                      f"(based on {row['transferred']} at {p:.2f}%)")
            job["estimated"] = True
    for name, vr in job["vol_rows"].items():
        if name not in job["vol_logged"] and row_done(vr):
            dur, src = row_duration(vr, time.time() - job["t0"])
            flog.info(f"VOLUME {name}: replicated {vr['transferred'] or '-'} "
                      f"(written {vr['written'] or '-'}) in {fmt_duration(dur)} [{src}]  "
                      f"started={vr['started'] or '-'} completed={vr['completed'] or '-'}")
            job["vol_logged"].add(name)


class ProgressDisplay:
    """-V display: one bar per replication, one '#' per `step` percent.
    On a terminal the block is redrawn in place; otherwise a line is printed when a bar grows.
    If the array does not report Progress (source-side listing on newer Purity), the line shows
    'running', the elapsed time and the data transferred so far instead of a growing bar."""
    def __init__(self, jobs, step):
        """jobs = replication jobs from start_pgroup(); step = percent per '#'."""
        self.jobs, self.step, self.width = jobs, step, 100 // step
        self.tty, self.drawn = sys.stdout.isatty(), 0
        self.namew = max(len(j["dr_snap"]) for j in jobs)

    def line(self, j):
        """-> (text line for one job, number of '#' drawn)."""
        row = j["row"] or {}
        p = row.get("progress")
        h = self.width if j["done"] and not j["error"] else min(int((p or 0) // self.step), self.width)
        started = row.get("started") not in (None, "", "-")
        status = ("FAILED" if j["error"] else "done" if j["done"]
                  else f"{p:6.2f}%" if p is not None else "running" if started else "waiting")
        data = row.get("transferred") or "-"
        if p is None and not j["done"]:
            data += f"  ({fmt_duration(time.time() - j['t0'])} elapsed)"
        return f"  {j['dr_snap']:<{self.namew}} |{'#' * h}{' ' * (self.width - h)}| {status:>7}  {data}", h

    def update(self):
        """Redraw (terminal) or print changed lines (log/pipe) for every job."""
        lines = [self.line(j) for j in self.jobs]
        if self.tty:
            if self.drawn:
                sys.stdout.write(f"\033[{self.drawn}F")
            for text, _ in lines:
                sys.stdout.write(text + "\033[K\n")
            self.drawn = len(lines)
        else:                                    # not a terminal: print a job's line only when it changes
            for j, (text, h) in zip(self.jobs, lines):
                key = (h, j["done"], bool(j["error"]), (j["row"] or {}).get("transferred"))
                if key != j.get("shown"):
                    sys.stdout.write(text + "\n")
                    j["shown"] = key
        for j, (_, h) in zip(self.jobs, lines):
            j["hashes"] = max(j["hashes"], h)
        sys.stdout.flush()


def monitor_all(arr, jobs, s, verbose):
    """Phase 2: poll every in-flight replication in one loop until all complete or time out."""
    if not jobs:
        return
    log.info(f"Monitoring {len(jobs)} replication(s) to {jobs[0]['target']} "
             f"(poll every {s['poll_interval_seconds']}s)")
    timeout = s["replication_timeout_minutes"] * 60
    disp = ProgressDisplay(jobs, max(1, int(s["hash_every_percent"]))) if verbose else None
    note = flog if verbose else log          # with -V keep the console for the bars; summary prints after
    if disp:
        disp.update()
    while any(not j["done"] for j in jobs):
        for j in jobs:
            if j["done"]:
                continue
            try:
                poll_job(arr, j)
            except CLIError as e:
                j["done"], j["error"] = True, str(e)
                note.error(f"FAILED monitoring {j['dr_snap']}: {e}")
                continue
            log_job_progress(j)
            if row_done(j["row"]):
                j["done"], j["elapsed"] = True, time.time() - j["t0"]
                if j["vol_query"] and not all(row_done(j["vol_rows"].get(n)) for n in j["vol_snaps"]):
                    try:                                            # one last per-volume read
                        poll_job(arr, j); log_job_progress(j)
                    except CLIError as e:
                        flog.warning(f"{j['dr_snap']}: final per-volume read failed ({e})")
                note.info(f"Replication complete: {j['dr_snap']} -> {j['target']}  "
                          f"data replicated={(j['row'] or {}).get('transferred') or '-'}")
            elif time.time() - j["t0"] > timeout:
                last = (f"last progress {j['last_p']:.2f}%" if j["last_p"] is not None else
                        f"last data transferred {(j['row'] or {}).get('transferred') or '-'}")
                j["done"], j["error"] = True, f"not complete after {s['replication_timeout_minutes']} min ({last})"
                note.error(f"FAILED {j['dr_snap']}: {j['error']}")
        if disp:
            disp.update()
        if any(not j["done"] for j in jobs):
            time.sleep(s["poll_interval_seconds"])
    if disp:
        disp.update()
    write_summary(jobs)


def drdata(record):
    """Write one machine-readable 'DRDATA {json}' line (log file only); pure-dr-report.py reads these.
    Fields: type (pgroup|volume), run (snapshot suffix), time (run start, ISO), logged (when written),
    source_pgroup, dr_pgroup, dr_snapshot, target, status (ok|failed|started), error,
    data_bytes, written_bytes, seconds, time_source (array|measured), started, completed,
    and for volumes: volume, source_volume, size_bytes."""
    record.setdefault("logged", datetime.datetime.now().isoformat(timespec="seconds"))
    flog.info("DRDATA " + json.dumps(record, sort_keys=True))


def drdata_base(run, run_time, src_pg, dr_pg, target):
    """Fields shared by every DRDATA record of one pgroup in one run."""
    return {"run": run, "time": run_time, "source_pgroup": src_pg, "dr_pgroup": dr_pg,
            "dr_snapshot": f"{dr_pg}.{run}", "target": target}


def emit_failed_start(pg, run, run_time, target, error):
    """DRDATA for a pgroup that failed in phase 1 (before replication started): the pgroup and each of
    its volumes are recorded as failed for this run, so the reports count the failure at every level."""
    base = drdata_base(run, run_time, pg["source_pgroup"], pg["dr_pgroup"], target)
    drdata({**base, "type": "pgroup", "status": "failed", "error": error})
    for v in pg["volumes"]:
        drdata({**base, "type": "volume", "volume": v["target"], "source_volume": v["source"],
                "status": "failed", "error": error})


def emit_started(j):
    """DRDATA when wait_for_replication is false: replication was started but not monitored, so no
    data or time is known. Reports show the run with status 'started' (not counted as ok or failed)."""
    base = drdata_base(j["suffix"], j["run_time"], j["pg"], j["dr_pg"], j["target"])
    drdata({**base, "type": "pgroup", "status": "started", "error": None})
    for name, vol in j["vol_snaps"].items():
        drdata({**base, "type": "volume", "volume": vol, "source_volume": j["sources"].get(vol),
                "size_bytes": parse_size(j["sizes"].get(vol)), "status": "started", "error": None})


def emit_drdata(j):
    """DRDATA for a monitored replication: one pgroup record plus one record per volume, with the
    data replicated, physical bytes written and replication time reported by the array."""
    row = j["row"] or {}
    dur, src = row_duration(row, j["elapsed"])
    base = drdata_base(j["suffix"], j["run_time"], j["pg"], j["dr_pg"], j["target"])
    drdata({**base, "type": "pgroup", "status": "failed" if j["error"] else "ok", "error": j["error"],
            "data_bytes": parse_size(row.get("transferred")), "written_bytes": parse_size(row.get("written")),
            "seconds": None if j["error"] else dur, "time_source": src,
            "started": row.get("started") or None, "completed": row.get("completed") or None})
    for name, vol in j["vol_snaps"].items():
        vr = j["vol_rows"].get(name) or {}
        vdur, vsrc = row_duration(vr, None) if vr else (None, None)
        drdata({**base, "type": "volume", "volume": vol, "source_volume": j["sources"].get(vol),
                "size_bytes": parse_size(j["sizes"].get(vol)), "status": "failed" if j["error"] else "ok",
                "data_bytes": parse_size(vr.get("transferred")), "written_bytes": parse_size(vr.get("written")),
                "seconds": vdur, "time_source": vsrc,
                "started": vr.get("started") or None, "completed": vr.get("completed") or None})


def write_summary(jobs):
    """Per-pgroup and per-volume change data and replication time (console + log file)."""
    log.info("----- Replication summary -----")
    for j in jobs:
        row = j["row"] or {}
        if j["error"]:
            log.error(f"{j['dr_snap']}: FAILED - {j['error']}")
            continue
        dur, src = row_duration(row, j["elapsed"])
        log.info(f"{j['dr_snap']} -> {j['target']}: data {row.get('transferred') or '-'}, "
                 f"physical written {row.get('written') or '-'}, time {fmt_duration(dur)} [{src}]")
        for name, vol in j["vol_snaps"].items():
            vr = j["vol_rows"].get(name)
            if not vr:
                log.info(f"    {vol:<24} per-volume data not reported")
                continue
            vdur, vsrc = row_duration(vr, None)
            log.info(f"    {vol:<24} data {vr['transferred'] or '-':>10}  written {vr['written'] or '-':>10}  "
                     f"time {fmt_duration(vdur)} [{vsrc}]")
    for j in jobs:
        emit_drdata(j)


def _already(e):
    """True when a CLI error only says the object is already there (safe to ignore during --setup)."""
    return "already" in str(e).lower()


def _msg(e):
    """CLI error text without a trailing period, so it can be followed by a hint."""
    return str(e).rstrip(". ")


def setup(arr, cfg):
    """One-time preparation of the source array. For each protection_groups entry, in this order:
      1. Create every missing DR (target) volume as a copy of its source volume (purevol copy), so it
         has exactly the same size. The copy is metadata-only on FlashArray and the first refresh
         overwrites it from the source pgroup snapshot. Existing DR volumes are left as they are.
      2. Create the DR pgroup with those volumes, or add any missing volumes to an existing DR pgroup.
         (Volumes must exist first: purepgroup create fails with 'Volume does not exist' otherwise.)
      3. Add the replication target to the DR pgroup.
      4. Show the retention of the source and DR pgroups. Retention is never changed.
    Safe to run again: anything that already exists is reported and left unchanged.
    The source volumes and source pgroups are never created or changed.
    Returns the number of protection_groups entries that could not be fully set up."""
    q = shlex.quote
    target, pgs = cfg["target_array"]["name"], cfg["protection_groups"]
    if arr.dry_run:                       # the array is not contacted, so assume nothing exists yet
        log.info("Dry run: the array is not contacted, so every create command is shown")
        vols_now, pgs_now = None, set()
    else:
        try:
            out = arr.run("purevol list --csv")
            vols_now = dict(zip(csv_column(out, "Name"), csv_column(out, "Size")))   # name -> size
            pgs_now = set(csv_column(arr.run("purepgroup list --csv"), "Name"))
        except CLIError as e:
            log.error(f"Setup stopped: could not list the volumes and protection groups ({e})")
            return len(pgs)
    exists = lambda name: vols_now is not None and name in vols_now
    did = lambda done, would: would if arr.dry_run else done          # log wording for --dry-run
    failed, n_vols, n_pgs = [], 0, 0

    for pg in pgs:
        src_pg, dr_pg, vols = pg["source_pgroup"], pg["dr_pgroup"], pg["volumes"]
        log.info(f"=== setup {src_pg} -> {dr_pg} -> {target} ===")
        problems = []
        if vols_now is not None and src_pg not in pgs_now:
            log.warning(f"Source protection group {src_pg} not found. --setup doesn't create it; "
                        f"create it (or fix source_pgroup) before the first refresh")

        # 1. DR volumes first, each the same size as its source volume.
        #    Every missing DR volume needs its source volume; if any source is missing, nothing is
        #    created for this entry, so a typo in the config doesn't leave half a set of DR volumes.
        missing_src = [v["source"] for v in vols
                       if vols_now is not None and not exists(v["target"]) and v["source"] not in vols_now]
        if missing_src:
            log.error(f"Source volume(s) not found: {', '.join(missing_src)}. Nothing created for {dr_pg}; "
                      f"fix the 'source' names in the config or create the volumes")
            failed.append(dr_pg)
            continue
        for v in vols:
            tgt, src = v["target"], v["source"]
            if exists(tgt):
                log.info(f"Volume {tgt} already exists ({vols_now[tgt]}), left as is")
                continue
            try:
                arr.run(f"purevol copy {q(src)} {q(tgt)}")
            except CLIError as e:
                hint = (". A destroyed volume with this name may be waiting to be eradicated: check "
                        "'purevol list --pending', then 'purevol recover' or 'purevol eradicate' it"
                        if _already(e) else "")
                problems.append(f"can't create {tgt}: {_msg(e)}{hint}")
                log.error(problems[-1])
                continue
            n_vols += 1
            if vols_now is not None:
                vols_now[tgt] = vols_now[src]
                log.info(f"Created volume {tgt} ({vols_now[tgt]}) as a copy of {src}")
            else:
                log.info(f"Would create volume {tgt} as a copy of {src}")
        if problems:
            log.error(f"{dr_pg} not created or changed, because its volumes aren't all there")
            failed.append(dr_pg)
            continue

        # 2. DR protection group with the DR volumes
        if dr_pg not in pgs_now:
            try:
                arr.run(f"purepgroup create --vollist {','.join(q(v['target']) for v in vols)} {q(dr_pg)}")
            except CLIError as e:
                hint = (". A destroyed protection group with this name may be waiting to be eradicated: "
                        "check 'purepgroup list --pending'" if _already(e) else "")
                log.error(f"can't create protection group {dr_pg}: {_msg(e)}{hint}")
                failed.append(dr_pg)
                continue
            n_pgs += 1
            pgs_now.add(dr_pg)
            log.info(f"{did('Created', 'Would create')} protection group {dr_pg} with "
                     f"{', '.join(v['target'] for v in vols)}")
        else:
            log.info(f"Protection group {dr_pg} already exists, adding any missing volumes")
            for v in vols:
                try:
                    arr.run(f"purepgroup setattr --addvollist {q(v['target'])} {q(dr_pg)}")
                    log.info(f"Added {v['target']} to {dr_pg}")
                except CLIError as e:
                    if _already(e):
                        log.info(f"{v['target']} is already in {dr_pg}")
                    else:
                        problems.append(f"can't add {v['target']} to {dr_pg}: {_msg(e)}")
                        log.error(problems[-1])

        # 3. Replication target
        try:
            arr.run(f"purepgroup setattr --addtargetlist {q(target)} {q(dr_pg)}")
            log.info(f"{did('Added', 'Would add')} replication target {target} to {dr_pg}")
        except CLIError as e:
            if _already(e):
                log.info(f"{target} is already a target of {dr_pg}")
            else:
                problems.append(f"can't add target {target} to {dr_pg}: {_msg(e)}. Check that "
                                f"'purearray connection list' shows {target} (target_array.name)")
                log.error(problems[-1])

        # 4. Retention (read only)
        show = [n for n in (src_pg, dr_pg) if vols_now is None or n in pgs_now]
        try:
            out = arr.run(f"purepgroup list --retention {' '.join(q(n) for n in show)}")
            if out:
                log.info("Retention:\n" + "\n".join("    " + l for l in out.splitlines()))
                log.info(f"Review the retention above. To change it, e.g.: "
                         f"purepgroup retain --target-all-for 14d {dr_pg}")
        except CLIError as e:
            log.warning(f"Could not read retention ({e})")
        if problems:
            failed.append(dr_pg)

    nxt = ("run --setup again without --dry-run to make these changes" if arr.dry_run else
           "every DR protection group is ready. Next: run with --dry-run, then -V")
    log.info(f"Setup finished: {did('created', 'would create')} {n_vols} volume(s) and {n_pgs} "
             f"protection group(s); "
             + (f"{len(failed)} not ready ({', '.join(failed)}). Fix the errors above and run --setup again"
                if failed else nxt))
    return len(failed)


def main():
    """Parse arguments, load the config, then run phase 1 (start) and phase 2 (monitor) for every pgroup.
    Returns the process exit code: 0 all succeeded, 1 any pgroup failed."""
    ap = argparse.ArgumentParser(description="FlashArray pgroup DR refresh + replication")
    ap.add_argument("-c", "--config", required=True, help="JSON config file")
    ap.add_argument("-V", "--verbose", action="store_true",
                    help="show CLI commands/array output and a replication progress bar")
    ap.add_argument("--dry-run", action="store_true", help="log the commands without running them")
    ap.add_argument("--setup", action="store_true",
                    help="one-time: create the missing DR volumes (as copies of their source volumes), "
                         "then the DR pgroups, and add the replication target (retention is not changed)")
    args = ap.parse_args()

    cfg, s = load_config(args.config)
    log_path = setup_logging(resolve_log_base(cfg, args.config), args.verbose, args.dry_run)
    src = cfg["source_array"]
    arr = FlashArray(src["host"], src["host"], src.get("ssh_user") or "pureuser",
                     src.get("ssh_key") or None, s["ssh_timeout_seconds"], args.dry_run)
    target = cfg["target_array"]["name"]
    started = datetime.datetime.now()
    suffix = f"{s['snapshot_prefix']}-{started:%Y%m%d-%H%M%S}"     # one suffix for every pgroup in this run
    run_time = started.isoformat(timespec="seconds")               # DRDATA 'time' = run start (report date)

    log.info(f"##### pg_dr_refresh start  config={args.config}  source={src['host']}  target={target}  "
             f"{'DRY-RUN ' if args.dry_run else ''}{'SETUP' if args.setup else ''}")
    log.info(f"Log file: {log_path}")
    if not args.dry_run:
        discover_array_name(arr)
        check_target_connected(arr, target)

    if args.setup:
        return 1 if setup(arr, cfg) else 0

    # Phase 1: start every protection group (snapshot, overwrite, replicate-now) without waiting
    failed, jobs = [], []
    for pg in cfg["protection_groups"]:
        try:
            job = start_pgroup(arr, pg, target, suffix, run_time, args.dry_run)
            if not args.dry_run:
                jobs.append(job)
        except CLIError as e:
            log.error(f"FAILED {pg['source_pgroup']} -> {pg['dr_pgroup']}: {e}")
            failed.append(pg["source_pgroup"])
            if not args.dry_run:
                emit_failed_start(pg, suffix, run_time, target, str(e))

    # Phase 2: monitor all replications together and log data / time per pgroup and per volume
    if jobs and (s["wait_for_replication"] or args.verbose):
        monitor_all(arr, jobs, s, args.verbose)
        failed += [j["pg"] for j in jobs if j["error"]]
    elif jobs:
        log.info("wait_for_replication is false - replications left running; check with "
                 "purepgroup list --snap --transfer")
        for j in jobs:
            emit_started(j)

    ok = len(cfg["protection_groups"]) - len(failed)
    log.info(f"##### pg_dr_refresh end  {ok} succeeded, {len(failed)} failed"
             + (f" ({', '.join(failed)})" if failed else ""))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
