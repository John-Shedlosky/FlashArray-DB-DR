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

  Phase 2 - monitor every replication together, ON THE TARGET ARRAY, until each one completes or times out
    5. purepgroup list --snap --transfer --csv <source>:<DR pgroup>   (on the target: started / completed /
                                                  progress / data transferred / physical bytes written)
       purevol list --snap --transfer --csv      (the same per volume, on the target, if it reports it)
    6. Write a summary of data replicated and replication time per pgroup and per volume,
       plus one machine-readable 'DRDATA {json}' log line per pgroup and per volume.
       pure-dr-report.py reads those DRDATA lines to build its reports.

Snapshot lifetime follows the retention policy set on each pgroup on the array (--apply-retention),
so the script never deletes snapshots itself. --setup sets that policy from the config's "retention"
blocks wherever it differs (source and DR pgroups on the source array, and the DR pgroups' target
retention on the target array); retention that isn't in the config is left as is.

Both arrays are reached over SSH. The SOURCE array takes the snapshots, overwrites the DR volumes and
starts replication. The TARGET array (target_array.host, logged in as target_array.ssh_user) is where the
replication statistics and status live, so every replication is monitored there. --setup also logs in to
the target to read its connection key when the arrays aren't connected yet.

The target's host (FQDN) is also used to find its row in `purearray connection list --csv` on the source.
The arrays count as connected only when that row's last field (Status) is "connected".

Replication progress
--------------------
On the target, a replicated pgroup snapshot is named <source Purity name>:<DR pgroup>.<suffix>, for example
flasharray1:PG1-DR.dr-20261105-020000. The script polls
    purepgroup list --snap --transfer --csv flasharray1:PG1-DR
on the target and reads that snapshot's row. Until the snapshot appears on the target the replication is
shown as "waiting". Completion is detected from the 'Completed' column (or Progress reaching 100%).

Logging
-------
Every command, its array output, progress and the summary are written to a weekly log file:
<log_file stem>_<Monday of this week, YYYYMMDD>.log  (e.g. pg_dr_refresh_20261102.log).
The console shows progress and the summary; -V also shows each CLI command and its output.

Configuration
-------------
JSON with optional // and /* */ comments - see pg_dr_config.json for every setting.

Split runs: --snaponly (-S) then --cloneandreplicate (-R)
--------------------------------------------------------
A normal run does steps 0-6 in one go. To keep the time a database spends in backup mode as short as
possible, the work can be split in two:

  -S / --snaponly           Step 1 only: snapshot every source pgroup, back to back, on the source array.
                            Nothing else is checked or contacted (not the target array, not the DR
                            volumes), so the database can leave backup mode as soon as this returns.
                            The snapshots taken are recorded in a small state file (see below).
  -R / --cloneandreplicate  Steps 0 and 2-6, using the snapshots recorded by the last --snaponly run:
                            check the DR volumes, check each recorded snapshot still exists and holds
                            every source volume, overwrite the DR volumes, snapshot + replicate the
                            DR pgroups and monitor them on the target.

-R fails (exit code 2, nothing changed) if --snaponly hasn't recorded any snapshots, or if they have
all been used by an earlier -R already: run -S again first. If a pgroup fails in -R before any of its
DR volumes were overwritten (e.g. a DR volume is missing), it stays unused in the state file and the
next -R retries just that pgroup with the same snapshot.

The DR snapshots made by -R use the same suffix as the -S snapshots (e.g. PG1.dr-20261109-020000 and
PG1-DR.dr-20261109-020000), so names still show the point in time the data is from.

State file: "state_file" in the config, or by default <config name>.state.json next to the config
(pg_dr_config.json -> pg_dr_config.state.json). It is written by the script; don't edit it. The config
itself is never written to, so its comments stay intact.

Usage
-----
  pg_dr_refresh.py -c pg_dr_config.json            # normal run (snapshot, overwrite, replicate)
  pg_dr_refresh.py -c pg_dr_config.json -V         # verbose: commands, array output, progress bars
  pg_dr_refresh.py -c pg_dr_config.json --dry-run  # log the commands that would run; change nothing
  pg_dr_refresh.py -c pg_dr_config.json --setup    # one-time: connect the arrays, create the volumes,
                                                   # then the pgroups + target
  pg_dr_refresh.py -c pg_dr_config.json -S         # snapshots only (database in backup mode)
  pg_dr_refresh.py -c pg_dr_config.json -R         # then: overwrite + replicate from those snapshots

Exit code: 0 = every pgroup succeeded, 1 = at least one pgroup failed, 2 = config / usage error
(including -R with no unused --snaponly snapshots).

Requirements: Python 3.8+ and an OpenSSH client ('ssh' on the PATH) with key-based login to the
source array. No extra Python packages are needed.
"""
import argparse, csv, datetime, io, json, logging, os, re, shlex, subprocess, sys, time
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
SIZE_RE   = re.compile(r"^[1-9][0-9]*[KMGTP]$", re.I)      # --setup volume size: 100G, 2T, 500M ...

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
    """One FlashArray (the source or the replication target), reached with the system 'ssh' client
    (key-based login, BatchMode)."""
    def __init__(self, host, name, user, key, ssh_timeout, dry_run):
        """host = SSH address; name = label used in logs (replaced by the Purity array name once read);
        user / key = SSH login; ssh_timeout = max seconds per command; dry_run = log commands only."""
        self.host, self.name, self.user, self.key = host, name, user, key
        self.ssh_timeout, self.dry_run = ssh_timeout, dry_run
        self.purity_name = None              # set once read with purearray list

    def run(self, cmd, quiet=False, check=True, read_only=False, stdin=None, secret=False):
        """Run one Purity CLI command over SSH and return its stdout.
        The command and the array's output always go to the log file; the console shows them only
        with -V (quiet=True keeps them file-only, used for repeated progress polls).
        Raises CLIError on SSH failure, timeout, or a non-zero exit (unless check=False).
        In dry-run mode the command is only logged and "" is returned, unless read_only=True
        (list commands that change nothing, which --setup --dry-run runs to preview accurately).
        stdin = text sent to the command's prompt (e.g. a connection key), never logged.
        secret = True keeps the command's output out of the log (e.g. a connection key)."""
        lg = flog if quiet else log
        lg.debug(f"CMD [{self.name}] $ {cmd}")
        if self.dry_run and not read_only:
            return ""
        ssh = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15"]
        if self.key:
            ssh += ["-i", str(Path(self.key).expanduser())]
        ssh += [f"{self.user}@{self.host}", cmd]
        try:
            r = subprocess.run(ssh, capture_output=True, text=True, timeout=self.ssh_timeout,
                               input=stdin if stdin is not None else None,
                               stdin=None if stdin is not None else subprocess.DEVNULL)
        except subprocess.TimeoutExpired:
            raise CLIError(f"[{self.name}] timed out after {self.ssh_timeout}s: {cmd}")
        except OSError as e:                                    # e.g. no 'ssh' program on the PATH
            raise CLIError(f"[{self.name}] can't run ssh ({e}); install the OpenSSH client")
        out, err = r.stdout.rstrip(), r.stderr.strip()
        if out and secret:
            lg.debug(f"OUT [{self.name}] (not logged: contains a connection key)")
        elif out:
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


def resolve_state_path(cfg, config_path):
    """Where --snaponly records its snapshots for --cloneandreplicate: state_file from the config
    (relative to the config's folder), else <config stem>.state.json next to the config."""
    base = cfg.get("state_file")
    cdir = Path(config_path).resolve().parent
    if not base:
        return cdir / f"{Path(config_path).stem}.state.json"
    base = Path(base).expanduser()
    return base if base.is_absolute() else cdir / base


def read_state(path):
    """The --snaponly state as a dict, or None if there is no state file.
    Raises ValueError if the file exists but can't be read or parsed."""
    try:
        with open(path, encoding="utf-8") as f:
            st = json.load(f)
    except FileNotFoundError:
        return None
    except (OSError, json.JSONDecodeError) as e:
        raise ValueError(f"can't read the state file {path}: {e}")
    if not isinstance(st, dict) or not isinstance(st.get("snapshots"), list) or not st.get("suffix"):
        raise ValueError(f"the state file {path} isn't one written by pg_dr_refresh.py --snaponly")
    return st


def write_state(path, st):
    """Write the state file atomically (temp file + rename), so an interrupted write never leaves a
    half-written file behind."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f, indent=2)
        f.write("\n")
    os.replace(tmp, path)


def unused_snapshots(st, pgroups=None):
    """State entries -R still has to process: snapshot taken OK and not yet used by a -R run.
    pgroups = source pgroup names still in the config (entries for others are ignored)."""
    return [e for e in st["snapshots"] if e.get("status") == "ok" and not e.get("cloned")
            and (pgroups is None or e.get("source_pgroup") in pgroups)]


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
    if not (tgt.get("host") or tgt.get("name")):
        errs.append("target_array.host is required (the target's management FQDN, e.g. flasharray2.example.com)")
    ct = tgt.get("connect_timeout_seconds", 300)
    if isinstance(ct, bool) or not isinstance(ct, (int, float)) or ct < 10:
        errs.append(f"target_array.connect_timeout_seconds must be a number >= 10 (got {ct!r})")
    if "encrypted" in tgt and not isinstance(tgt["encrypted"], bool):
        errs.append("target_array.encrypted must be true or false")
    if str(tgt.get("replication_transport", "ip")).lower() not in ("ip", "fc"):
        errs.append("target_array.replication_transport must be \"ip\" or \"fc\"")
    for k in ("source_retention", "dr_retention"):
        if any(k in pg for pg in cfg.get("protection_groups") or []):
            errs.append(f"'{k}' is no longer supported - use a \"retention\" block instead "
                        f"(see pg_dr_config.json)")
    validate_retention(cfg.get("retention"), "retention", errs)
    if "state_file" in cfg and not (cfg["state_file"] is None or isinstance(cfg["state_file"], str)):
        errs.append("state_file must be a file path (text) or null")
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
        validate_retention(pg.get("retention"), f"{tag}.retention", errs)
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
            sz = v.get("size")
            if sz is not None and not SIZE_RE.match(str(sz)):
                errs.append(f"{tag}.volumes[{j}].size must be a whole number followed by K, M, G, T or P, "
                            f"e.g. \"100G\" or \"2T\" (got {sz!r})")
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


def discover_array_name(arr, role="Source"):
    """Read an array's Purity name (purearray list) so it does not need to be in the config.
    Raises CLIError if the array can't be reached; returns the name, or None if it wasn't listed."""
    names = csv_column(arr.run("purearray list --csv"), "Name")
    if names:
        arr.name = arr.purity_name = names[0]
        log.info(f"{role} array Purity name: {arr.name} (ssh {arr.user}@{arr.host})")
        return arr.name
    log.warning(f"Could not determine the {role.lower()} array's Purity name; using host '{arr.host}' in log labels")
    return None


CONNECT_POLL_SECONDS = 5      # how often --setup re-reads the connection list while waiting for "connected"


def connection_list(arr):
    """Rows of the source array's connection list as (header, rows), header in lower case.
    Uses purearray connection list --csv (Purity 6.x), or purearray list --connect --csv (older).
    Returns None if neither command could be read. Read-only, so it also runs in --dry-run."""
    for cmd in ("purearray connection list --csv", "purearray list --connect --csv"):
        try:
            out = arr.run(cmd, read_only=True)
        except CLIError:
            continue
        rows = [r for r in csv.reader(io.StringIO(out)) if any(c.strip() for c in r)]
        if not rows:
            return [], []
        hdr = [h.strip().lower() for h in rows[0]]
        if "name" not in hdr:
            continue
        return hdr, [[c.strip() for c in r] for r in rows[1:]]
    return None


def row_status(hdr, row):
    """Connection status of one row: the LAST field (Purity//FA 6.8.3 and later list Status last).
    On older releases, where Status isn't the last column, the Status column is used instead."""
    if "status" in hdr and hdr[-1] != "status" and len(row) == len(hdr):
        return row[hdr.index("status")].lower()
    return row[-1].lower() if row else ""


def target_keys(tcfg):
    """What identifies the target in the connection list: its configured name and host (FQDN or IP),
    plus the host's first DNS label (flasharray2.testdrive.local -> flasharray2), the usual Purity name."""
    host, name = (tcfg.get("host") or "").strip().lower(), (tcfg.get("name") or "").strip().lower()
    keys = {k for k in (host, name) if k}
    if host and not re.match(r"^[0-9.]+$|:", host):
        keys.add(host.split(".")[0])
    return keys


def target_connection(arr, tcfg):
    """The source array's replication connection to the target, from purearray connection list --csv:
    the row that contains the target's host (e.g. flasharray2.testdrive.local in Management Address)
    or its Purity name, ignoring fleet-management rows. Returns
      {"name": Purity name, "status": "connected" | "connecting" | ..., "encrypted": bool or None,
       "type": ..., "line": the row as text}
    or {} if the target isn't listed, or None if the list couldn't be read."""
    got = connection_list(arr)
    if got is None:
        return None
    hdr, rows = got
    keys, ni = target_keys(tcfg), hdr.index("name") if "name" in hdr else 0
    ti = hdr.index("type") if "type" in hdr else None
    best = {}
    for r in rows:
        if not r or not ({c.lower() for c in r} & keys):
            continue                                      # this row isn't the target
        rtype = (r[ti] if ti is not None and len(r) == len(hdr) else "").lower()
        if "fleet" in rtype:
            continue                                      # fleet-management, not replication
        enc = None
        for col in ("encryption", "encrypted"):
            if col in hdr and len(r) == len(hdr):
                v = r[hdr.index(col)].lower()
                enc = v in ("true", "encrypted", "yes") if v not in ("", "-") else None
                break
        hit = {"name": r[ni], "status": row_status(hdr, r), "encrypted": enc, "type": rtype,
               "line": ",".join(r)}
        if not best or "async" in rtype:                  # prefer the async-replication row
            best = hit
    return best


def check_target_connected(arr, tcfg):
    """Refresh runs: warn (not fail) unless the target's connection status is "connected".
    Returns the target's Purity name from the connection list (or the configured name/host)."""
    label = tcfg.get("name") or tcfg.get("host")
    conn = target_connection(arr, tcfg)
    if conn is None:
        log.warning("Could not list connected arrays; skipping target check")
    elif not conn:
        log.warning(f"Target {label} not found in 'purearray connection list' on {arr.name} - check "
                    f"target_array.host/name, or run --setup to connect it")
    elif conn["status"] != "connected":
        log.warning(f"Replication target {conn['name']} is listed but its status is '{conn['status']}', "
                    f"not 'connected'. Replication won't complete until it is connected "
                    f"(see 'purearray connection list --path' on {arr.name})")
        return conn["name"]
    else:
        log.info(f"Replication target {conn['name']} is connected to {arr.name} (status: connected)")
        return conn["name"]
    return label


def wait_until_connected(arr, tcfg, timeout):
    """Re-read purearray connection list --csv until the target's row has status "connected"
    (its last field), for up to `timeout` seconds. Logs each status change.
    Returns the connection dict when connected, else None."""
    deadline, last = time.monotonic() + timeout, None
    while True:
        conn = target_connection(arr, tcfg) or {}
        status = conn.get("status") or "not listed"
        if status != last:
            log.info(f"Connection to {conn.get('name') or tcfg.get('host')}: status '{status}'")
            last = status
        if status == "connected":
            return conn
        left = deadline - time.monotonic()
        if left <= 0:
            return None
        time.sleep(min(CONNECT_POLL_SECONDS, left))


def log_connection_paths(arr):
    """After a failed wait: log 'purearray connection list --path' so the reason is in the log
    (the Details column shows e.g. 'Connection time-out' on Purity 6.8.3+). Read-only."""
    try:
        out = arr.run("purearray connection list --path", read_only=True)
    except CLIError as e:
        log.warning(f"Could not read the connection paths ({_msg(e)})")
        return
    if out:
        log.error("purearray connection list --path:\n" + "\n".join("    " + l for l in out.splitlines()))


def parse_connection_key(out):
    """The key from `purearray list --connection-key [--encrypted]` output:
        Connection Key
        Ae707eda-ca7b-872d-4c11-1d61d12ae8f2
    Header lines are skipped; returns the last field of the first data line, or None."""
    lines = [l.strip() for l in out.splitlines() if l.strip()]
    data = [l for l in lines if "connection key" not in l.lower()]
    if not data:
        return None
    key = data[0].split()[-1].strip('"')
    return key if len(key) >= 16 else None


def connect_arrays(arr, tarr, tcfg):
    """--setup step 1: make sure the source array has a "connected" async-replication connection to the
    target. A connection counts as up only when its row in `purearray connection list --csv` (the row
    containing the target's host, e.g. flasharray2.testdrive.local) has "connected" as its last field.
      * listed and "connected"            -> left as is
      * listed, any other status          -> waits up to connect_timeout_seconds for "connected"
                                             (it isn't reconnected); fails if it doesn't get there
      * not listed                        -> connects:
          1. On the TARGET (over SSH): purearray list --connection-key --encrypted   -> the key
          2. On the SOURCE: purearray connect --encrypted --management-address <target host>
                              --replication-transport ip --type async-replication
             The key is typed in at the command's prompt (sent on stdin), so it never appears in a
             command line, a process list or the log.
          3. Waits up to connect_timeout_seconds for the status to change from "connecting" to "connected".
    With "encrypted": false the key comes from purearray list --connection-key, and the connect command
    uses --connection-key instead of --encrypted (Purity before 6.6.7).
    Returns (connected, name): connected is True when the status is "connected" (or, in --dry-run, would
    be); name is the target's Purity name used in purepgroup setattr --addtargetlist, if it was read."""
    host = tcfg.get("host") or tcfg["name"]
    enc = tcfg.get("encrypted", True)
    transport = str(tcfg.get("replication_transport", "ip")).lower()
    timeout = tcfg.get("connect_timeout_seconds", 300)
    conn = target_connection(arr, tcfg)
    if conn is None:
        log.warning("Could not list the connected arrays; trying to connect anyway")
    elif conn:
        log.info(f"{arr.name} has a connection to {conn['name']} ({host}), status '{conn['status']}'")
        if enc and conn.get("encrypted") is False:
            log.warning(f"The connection to {conn['name']} isn't encrypted. --setup doesn't change an existing "
                        f"connection; see 'purearray connection enable --encrypted' in the Purity docs")
        if conn["status"] == "connected":
            log.info(f"{arr.name} is connected to {conn['name']}, left as is")
            return True, conn["name"]
        if arr.dry_run:
            log.warning(f"Status is '{conn['status']}', not 'connected'. The real run waits up to {timeout}s "
                        f"for 'connected' and stops step 1 if it doesn't get there")
            return True, conn["name"]
        log.info(f"Waiting up to {timeout}s for the status to become 'connected' (the connection isn't remade)")
        up = wait_until_connected(arr, tcfg, timeout)
        if up:
            return True, up["name"]
        log.error(f"The connection to {conn['name']} is still not 'connected' after {timeout}s. Check the "
                  f"replication network between the arrays (purenetwork list on both, and the Details column below), then "
                  f"run --setup again")
        log_connection_paths(arr)
        return False, conn["name"]

    # 1. log in to the target: read its Purity name, then its connection key
    try:
        names = csv_column(tarr.run("purearray list --csv", read_only=True), "Name")
    except CLIError as e:
        log.error(f"Can't log in to the target array {host} as {tarr.user} ({_msg(e)}). Set up SSH key "
                  f"login to the target too (README section 5), or connect the arrays by hand")
        return False, tcfg.get("name") or host
    pname = names[0] if names else (tcfg.get("name") or host.split(".")[0])
    if tcfg.get("name") and tcfg["name"].lower() not in (pname.lower(), host.lower()):
        log.warning(f"{host} reports Purity name '{pname}', but target_array.name is '{tcfg['name']}'; "
                    f"using '{pname}'")
    log.info(f"Logged in to the target {host} as {tarr.user}: Purity name {pname}")
    key_cmd = "purearray list --connection-key" + (" --encrypted" if enc else "")
    connect_cmd = (f"purearray connect{' --encrypted' if enc else ''} --management-address {shlex.quote(host)} "
                   f"--replication-transport {transport} --type async-replication"
                   + ("" if enc else " --connection-key"))
    if arr.dry_run:
        tarr.run(key_cmd)                                 # logged only: the key isn't read in a dry run
        arr.run(connect_cmd + "   (connection key typed in at the prompt)")
        log.info(f"Would read the {'encrypted ' if enc else ''}connection key from {pname}, connect "
                 f"{arr.name} to it ({transport}, async-replication{', encrypted' if enc else ''}), then wait "
                 f"up to {timeout}s for 'purearray connection list --csv' to show it 'connected'")
        return True, pname
    try:
        key = parse_connection_key(tarr.run(key_cmd, secret=True))
    except CLIError as e:
        log.error(f"Can't read the connection key from {pname}: {_msg(e)}"
                  + (". '--encrypted' needs Purity//FA 6.6.7 or later; for older arrays set "
                     "\"encrypted\": false in target_array" if enc else ""))
        return False, pname
    if not key:
        log.error(f"No connection key found in the output of '{key_cmd}' on {pname}")
        return False, pname
    log.info(f"Read the {'encrypted ' if enc else ''}connection key from {pname} (not logged)")

    # 2. connect from the source, answering the key prompt on stdin
    try:
        arr.run(connect_cmd, stdin=key + "\n")
    except CLIError as e:
        log.error(f"Connecting {arr.name} to {pname} failed: {_msg(e)}. Check the management address "
                  f"({host}) and the replication network ports, then run --setup again")
        return False, pname

    # 3. the new connection shows "connecting" first; wait for "connected"
    log.info(f"Waiting up to {timeout}s for {pname} to show 'connected' in purearray connection list")
    up = wait_until_connected(arr, {**tcfg, "name": tcfg.get("name") or pname}, timeout)
    if up:
        log.info(f"Connected {arr.name} to {up['name']} ({transport}, async-replication"
                 f"{', encrypted' if enc else ''}; status: connected)")
        return True, up["name"]
    log.error(f"'{connect_cmd}' ran, but {pname} isn't 'connected' after {timeout}s. Check the replication "
              f"network between the arrays (purenetwork list on both, and the Details column below), then run --setup again; it "
              f"waits for the existing connection instead of connecting again")
    log_connection_paths(arr)
    return False, pname


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


def check_dr_volumes(arr, vols):
    """Step 0: every DR volume to be overwritten must already exist. Returns {DR volume: size}.
    Raises CLIError (from purevol list) if one doesn't exist."""
    out = arr.run("purevol list --csv " + " ".join(shlex.quote(v["target"]) for v in vols))
    return dict(zip(csv_column(out, "Name"), csv_column(out, "Size")))


def snap_source(arr, src_pg, suffix):
    """Step 1: snapshot the source pgroup; lifetime follows the pgroup's retention policy on the array.
    Returns the snapshot name <pgroup>.<suffix>. Raises CLIError if the snapshot fails."""
    arr.run(f"purepgroup snap --apply-retention --suffix {suffix} {shlex.quote(src_pg)}")
    src_snap = f"{src_pg}.{suffix}"
    log.info(f"Created snapshot {src_snap}")
    return src_snap


def verify_snapshot(arr, src_snap, vols, dry_run):
    """Step 2: every source volume must be in the snapshot before anything is overwritten.
    Raises CLIError if the snapshot doesn't exist (any more) or a volume is missing from it."""
    try:
        out = arr.run(f"purevol list --snap --pgrouplist {shlex.quote(src_snap)}")
    except CLIError as e:
        if _missing(e):
            raise CLIError(f"snapshot {src_snap} doesn't exist on {arr.name} (removed by retention, or "
                           f"destroyed?) - nothing overwritten. Run --snaponly again ({e})")
        raise
    if not dry_run:
        present = snap_names(out)
        missing = [v["source"] for v in vols if f"{src_snap}.{v['source']}" not in present]
        if missing:
            raise CLIError(f"{src_snap} does not contain: {', '.join(missing)} - nothing overwritten")


def start_pgroup(arr, pg, target, suffix, run_time, dry_run):
    """Phase 1 for one pgroup (normal run): check DR volumes, snapshot, verify, overwrite, snapshot +
    start replication. Returns a 'job' dict that monitor_all() polls; does NOT wait for the transfer.
    Raises CLIError if any step fails; steps after the failure are not run for this pgroup."""
    src_pg, dr_pg, vols = pg["source_pgroup"], pg["dr_pgroup"], pg["volumes"]
    log.info(f"=== {src_pg} -> {dr_pg} -> {target}  (suffix {suffix}) ===")
    sizes = check_dr_volumes(arr, vols)                       # 0
    src_snap = snap_source(arr, src_pg, suffix)               # 1
    verify_snapshot(arr, src_snap, vols, dry_run)             # 2
    return overwrite_and_replicate(arr, pg, target, suffix, run_time, src_snap, sizes)   # 3 + 4


def clone_pgroup(arr, pg, target, suffix, run_time, dry_run, on_change=None):
    """Phase 1 for one pgroup with --cloneandreplicate: like start_pgroup, but uses the snapshot
    <source pgroup>.<suffix> taken earlier by --snaponly instead of taking a new one.
    on_change() is called just before the first DR volume is overwritten (the point after which the
    pgroup can't simply be retried with the same snapshot)."""
    src_pg, dr_pg, vols = pg["source_pgroup"], pg["dr_pgroup"], pg["volumes"]
    src_snap = f"{src_pg}.{suffix}"
    log.info(f"=== {src_snap} (from --snaponly) -> {dr_pg} -> {target} ===")
    sizes = check_dr_volumes(arr, vols)                       # 0
    verify_snapshot(arr, src_snap, vols, dry_run)             # 2 (also: does it still exist?)
    if on_change:
        on_change()
    return overwrite_and_replicate(arr, pg, target, suffix, run_time, src_snap, sizes)   # 3 + 4


def overwrite_and_replicate(arr, pg, target, suffix, run_time, src_snap, sizes):
    """Steps 3 and 4: overwrite each DR volume from its volume in src_snap, then snapshot the DR pgroup
    and start replication. Returns the 'job' dict monitor_all() polls."""
    dr_pg, vols = pg["dr_pgroup"], pg["volumes"]
    src_pg = pg["source_pgroup"]
    q = shlex.quote

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
    # On the target the DR pgroup is listed as <source Purity name>:<DR pgroup>
    remote_pg = f"{arr.purity_name}:{dr_pg}" if arr.purity_name else None
    return {"pg": src_pg, "dr_pg": dr_pg, "suffix": suffix, "dr_snap": dr_snap, "target": target,
            "remote_pg": remote_pg, "seen_on_target": False,
            "run_time": run_time, "t0": time.time(), "sizes": sizes,
            "sources": {v["target"]: v["source"] for v in vols},
            "vol_snaps": {f"{dr_snap}.{v['target']}": v["target"] for v in vols},
            "row": None, "vol_rows": {}, "vol_logged": set(), "done": False, "error": None,
            "hashes": 0, "last_p": None, "last_state": None, "estimated": False, "elapsed": None,
            "pg_csv": True, "vol_query": True}


def _missing(e):
    """True when a CLI error says the object doesn't exist (yet), e.g. a snapshot not on the target yet."""
    t = str(e).lower()
    return "does not exist" in t or "not exist" in t or "no such" in t or "not found" in t


def poll_job(tarr, job):
    """One poll of a replication job ON THE TARGET ARRAY: the pgroup snapshot's transfer row, then
    (once that row exists) the per-volume rows. All output goes to the log file only.
      pgroup:  purepgroup list --snap --transfer --csv <source>:<DR pgroup>
               -> row <source>:<DR pgroup>.<suffix>
      volumes: purevol list --snap --transfer --csv <source>:<DR pgroup>.<suffix>.<volume> ...
    Until the snapshot reaches the target it isn't listed (or the pgroup doesn't exist there yet, on the
    first replication); that counts as 'waiting', not as an error."""
    q = shlex.quote
    snap = job["dr_snap"]
    arg = f" {q(job['remote_pg'])}" if job["remote_pg"] else ""      # no source name: list every pgroup
    cmd = f"purepgroup list --snap --transfer{' --csv' if job['pg_csv'] else ''}{arg}"
    try:
        out = tarr.run(cmd, quiet=True)
    except CLIError as e:
        if _missing(e):
            if not job.get("noted_missing"):
                flog.info(f"WAITING {snap}: {job['remote_pg'] or snap} not on {tarr.name} yet ({e})")
                job["noted_missing"] = True
            return
        if not job["pg_csv"]:
            raise
        flog.warning(f"{snap}: --csv not accepted on {tarr.name} ({e}); using text output")
        job["pg_csv"] = False
        return
    row = find_row(parse_transfer_table(out), snap)
    if not row:                                           # snapshot not on the target yet
        if not job.get("noted_missing"):
            flog.info(f"WAITING {snap}: not listed on {tarr.name} yet")
            job["noted_missing"] = True
        return
    job["row"] = row
    if not job["seen_on_target"]:
        job["seen_on_target"] = True
        flog.info(f"ARRIVED {snap}: listed on {tarr.name} (started={row['started'] or '-'})")
    if job["vol_query"]:
        prefix = f"{job['remote_pg'].split(':')[0]}:" if job["remote_pg"] else ""
        vcmd = "purevol list --snap --transfer --csv " + " ".join(q(prefix + n) for n in job["vol_snaps"])
        try:
            vtable = parse_transfer_table(tarr.run(vcmd, quiet=True))
            for name in job["vol_snaps"]:
                r = find_row(vtable, name)
                if r:
                    job["vol_rows"][name] = r
        except CLIError as e:
            if _missing(e) and not row_done(row):
                return                                    # volume snapshots not all there yet; try again
            flog.warning(f"{snap}: per-volume transfer listing unavailable on {tarr.name} ({e}); "
                         f"pgroup-level only")
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


def monitor_all(tarr, jobs, s, verbose):
    """Phase 2: poll every in-flight replication ON THE TARGET ARRAY (tarr) in one loop until all
    complete or time out."""
    if not jobs:
        return
    log.info(f"Monitoring {len(jobs)} replication(s) on the target {tarr.host} as {tarr.user} "
             f"with purepgroup list --snap --transfer --csv (poll every {s['poll_interval_seconds']}s)")
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
                poll_job(tarr, j)
            except CLIError as e:
                j["done"], j["error"] = True, str(e)
                note.error(f"FAILED monitoring {j['dr_snap']}: {e}")
                continue
            log_job_progress(j)
            if row_done(j["row"]):
                j["done"], j["elapsed"] = True, time.time() - j["t0"]
                if j["vol_query"] and not all(row_done(j["vol_rows"].get(n)) for n in j["vol_snaps"]):
                    try:                                            # one last per-volume read
                        poll_job(tarr, j); log_job_progress(j)
                    except CLIError as e:
                        flog.warning(f"{j['dr_snap']}: final per-volume read failed ({e})")
                note.info(f"Replication complete: {j['dr_snap']} -> {j['target']}  "
                          f"data replicated={(j['row'] or {}).get('transferred') or '-'}")
            elif time.time() - j["t0"] > timeout:
                last = (f"it never appeared on {tarr.name} - check that the arrays are connected and the "
                        f"pgroup is allowed on the target" if not j["seen_on_target"] else
                        f"last progress {j['last_p']:.2f}%" if j["last_p"] is not None else
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


# ----------------------------------------------------------------------------- retention (--setup)
# Config kinds -> (which pgroup of the entry, which retention of that pgroup)
RET_KINDS = {"source": ("source_pgroup", "local"),    # source pgroup snapshots, on the source array
             "dr":     ("dr_pgroup", "local"),        # DR pgroup snapshots, on the source array
             "target": ("dr_pgroup", "target")}       # replicated DR pgroup snapshots, on the target array
RET_FIELDS = ("all_for", "per_day", "days")
RET_FLAGS = {"local":  {"all_for": "--all-for", "per_day": "--per-day", "days": "--days"},
             "target": {"all_for": "--target-all-for", "per_day": "--target-per-day", "days": "--target-days"}}
DUR_CFG_RE = re.compile(r"^\s*(\d+)\s*([smhdw]?)\s*$", re.I)
DUR_TOKEN_RE = re.compile(r"(\d+(?:\.\d+)?)\s*(weeks?|w|days?|d|hours?|hrs?|h|minutes?|mins?|m|seconds?|secs?|s)?",
                          re.I)
DUR_SECONDS = {"w": 604800, "d": 86400, "h": 3600, "m": 60, "s": 1, "": 1}


def parse_duration(v, strict=False):
    """Retention period -> seconds, or None if empty, '-' or not understood.
    Config values (strict): "1d", "12h", "30m", "2w", "90s" or a whole number of seconds.
    Array output is read more loosely: "1d", "1d 12h", "36 hours", "86400"."""
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return int(v) if v >= 0 and float(v).is_integer() else None
    s = str(v or "").strip()
    if not s or s == "-":
        return None
    if strict:
        m = DUR_CFG_RE.match(s)
        return int(m.group(1)) * DUR_SECONDS[m.group(2).lower()] if m else None
    total, pos = 0.0, 0
    for m in DUR_TOKEN_RE.finditer(s):
        if s[pos:m.start()].strip(" ,"):
            return None                                   # something that isn't a duration
        unit = (m.group(2) or "").lower()
        total += float(m.group(1)) * DUR_SECONDS[unit[:1] if unit else ""]
        pos = m.end()
    return int(total) if pos and not s[pos:].strip(" ,") else None


def parse_count(v):
    """Per Day / Days column -> int, or None if '-', empty or not a number."""
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return None


def duration_arg(seconds):
    """Seconds -> the shortest exact Purity duration: 86400 -> '1d', 129600 -> '36h', 90 -> '90s'."""
    for unit, n in (("d", 86400), ("h", 3600), ("m", 60)):
        if seconds and seconds % n == 0:
            return f"{seconds // n}{unit}"
    return f"{seconds}s"


def fmt_retention(r):
    """{'all_for': 86400, 'per_day': 4, 'days': 7} -> 'all for 1d, then 4 per day for 7 days'
    (only the fields present)."""
    if not r:
        return "not reported"
    parts = []
    if r.get("all_for") is not None:
        parts.append(f"all for {duration_arg(r['all_for'])}")
    if r.get("per_day") is not None or r.get("days") is not None:
        pd = r.get("per_day")
        d = r.get("days")
        parts.append(f"then {pd if pd is not None else '?'} per day for {d if d is not None else '?'} days")
    return ", ".join(parts) or "not reported"


def validate_retention(block, tag, errs):
    """Check one "retention" block (global or per protection group); append problems to errs."""
    if block is None:
        return
    if not isinstance(block, dict):
        errs.append(f"{tag} must be an object like {{\"target\": {{\"days\": 30}}}}")
        return
    for kind in sorted(set(block) - set(RET_KINDS)):
        errs.append(f"{tag}.{kind} is not known - use source, dr or target")
    for kind in RET_KINDS:
        r = block.get(kind)
        if r is None:
            continue
        if not isinstance(r, dict):
            errs.append(f"{tag}.{kind} must be an object with all_for, per_day and/or days (or null)")
            continue
        for k in sorted(set(r) - set(RET_FIELDS)):
            errs.append(f"{tag}.{kind}.{k} is not known - use all_for, per_day or days")
        if "all_for" in r and parse_duration(r["all_for"], strict=True) is None:
            errs.append(f"{tag}.{kind}.all_for must be a period like \"1d\", \"12h\", \"30m\" or \"2w\" "
                        f"(or a whole number of seconds); got {r['all_for']!r}")
        for k in ("per_day", "days"):
            if k in r and (isinstance(r[k], bool) or not isinstance(r[k], int) or r[k] < 0):
                errs.append(f"{tag}.{kind}.{k} must be a whole number >= 0 (got {r[k]!r})")


def wanted_retention(cfg, pg):
    """The retention the config asks for, per kind (source / dr / target), with the entry's own
    "retention" overriding the global one field by field. A kind set to null (in the entry, or
    globally without an entry value) isn't managed. Values are normalized (all_for in seconds)."""
    glob, own = cfg.get("retention") or {}, pg.get("retention") or {}
    out = {}
    for kind in RET_KINDS:
        if kind in own and own[kind] is None:
            continue
        r = {**(glob.get(kind) or {}), **(own.get(kind) or {})}
        if r:
            out[kind] = {k: (parse_duration(r[k], strict=True) if k == "all_for" else r[k])
                         for k in RET_FIELDS if k in r}
    return out


def _table_rows(out):
    """Rows of `purepgroup list --retention` output as lists of cells, header first.
    Reads --csv output, or the text table (columns found from the header's positions)."""
    lines = [l for l in out.splitlines() if l.strip()]
    if not lines:
        return []
    if "," in lines[0]:
        return [[c.strip() for c in r] for r in csv.reader(io.StringIO("\n".join(lines)))]
    starts = [m.start() for m in re.finditer(r"\S+(?: \S+)*", lines[0])]
    cut = lambda l: [l[a:b].strip() for a, b in zip(starts, starts[1:] + [None])]
    return [cut(l) for l in lines]


def read_retention(arr, pgroup):
    """A pgroup's current retention: {"local": {...} or None, "target": {...} or None, "target_array": name}.
    From `purepgroup list --retention --csv <pgroup>` (the text output if --csv isn't accepted).
    The row for the source array (its Purity name; the first row if the name isn't known) is the local
    retention; the first other row is the target retention. Read-only, so it also runs in --dry-run.
    Raises CLIError if the retention can't be read."""
    q = shlex.quote(pgroup)
    try:
        out = arr.run(f"purepgroup list --retention --csv {q}", read_only=True)
    except CLIError as e:
        if _missing(e):
            raise
        out = arr.run(f"purepgroup list --retention {q}", read_only=True)
    rows = _table_rows(out)
    if not rows:
        raise CLIError(f"[{arr.name}] no retention listed for {pgroup}")
    hdr = [h.lower() for h in rows[0]]
    col = lambda *names: next((hdr.index(n) for n in names if n in hdr), None)
    ni, ai = col("name"), col("array")
    fi, pi, di = col("all for", "all_for"), col("per day", "per_day"), col("days")
    if fi is None and pi is None and di is None:
        raise CLIError(f"[{arr.name}] unexpected retention listing for {pgroup}: {rows[0]}")
    g = lambda r, i: r[i] if i is not None and i < len(r) else ""
    found, name = [], ""
    for r in rows[1:]:
        name = g(r, ni) or name                       # text output leaves Name blank on later rows
        if ni is not None and name and name != pgroup:
            continue
        found.append({"array": g(r, ai), "all_for": parse_duration(g(r, fi)),
                      "per_day": parse_count(g(r, pi)), "days": parse_count(g(r, di))})
    if not found:
        raise CLIError(f"[{arr.name}] no retention listed for {pgroup}")
    me = (arr.purity_name or "").lower()
    local = next((r for r in found if me and r["array"].lower() == me), found[0])
    target = next((r for r in found if r is not local), None)
    strip = lambda r: {k: r[k] for k in RET_FIELDS} if r else None
    return {"local": strip(local), "target": strip(target), "target_array": target["array"] if target else None}


def set_retention(arr, pg, pgroup, kind, scope, want, have, problem, did, new_in_dry_run, target_name):
    """--setup step 4 for one kind of retention (source / dr / target) of one pgroup: compare the
    config (want) with the array (have, None if unknown) and run purepgroup retain if they differ.
    Returns True if it changed (or, in --dry-run, would change) the retention."""
    where = {"source": f"{pgroup} on {arr.name}", "dr": f"{pgroup} on {arr.name}",
             "target": f"{pgroup} on the target {target_name}"}[kind]
    diffs = [k for k in RET_FIELDS if k in want and (have is None or have.get(k) != want[k])]
    if not diffs:
        log.info(f"Retention of {where} matches the config ({fmt_retention(want)}), left as is")
        return False
    show = lambda k, v: "?" if v is None else duration_arg(v) if k == "all_for" else str(v)
    change = ", ".join(f"{k.replace('_', ' ')} {show(k, (have or {}).get(k))} -> {show(k, want[k])}"
                       for k in diffs)
    flags = RET_FLAGS[scope]
    cmd = ("purepgroup retain " + " ".join(f"{flags[k]} {duration_arg(want[k]) if k == 'all_for' else want[k]}"
                                           for k in RET_FIELDS if k in want) + f" {shlex.quote(pgroup)}")
    try:
        arr.run(cmd)
    except CLIError as e:
        problem(pg, f"can't set the retention of {where} ({fmt_retention(want)}): {_msg(e)}")
        return False
    if new_in_dry_run:
        log.info(f"Would set the retention of {where} (new protection group) to {fmt_retention(want)}")
    else:
        log.info(f"{did('Changed', 'Would change')} the retention of {where}: {change}")
    return True


def setup(arr, cfg, tarr=None):
    """One-time build of everything in the config, in this order:
      1. Array connection - makes sure 'purearray connection list --csv' shows the target as "connected",
         connecting the arrays first if needed (see connect_arrays: reads the connection key from the
         target over SSH).
      2. Volumes (on the source array) - all of them before any protection group:
         * a missing source volume is created with its 'size' from the config
           (purevol create --size <size> <source>)
         * a missing DR volume is created the same size as its source volume:
           purevol create --size <size> <target> when the source volume is also created by this run,
           otherwise purevol copy <source> <target> (an exact-size copy of the existing source volume)
      3. Protection groups - each missing source and DR pgroup is created (purepgroup create <pgroup>),
         then its volumes are added (purepgroup setattr --addvollist v1,v2,... <pgroup>). For an existing
         pgroup, only the volumes not already in it are added. Nothing is ever removed.
         The replication target is then added to each DR pgroup (purepgroup setattr --addtargetlist);
         skipped, and reported, if step 1 couldn't connect the arrays.
      4. Retention - for each pgroup, the retention the config's "retention" blocks ask for is compared
         with `purepgroup list --retention` and, where it differs, set with purepgroup retain:
           source -> source pgroup snapshots on the source array  (--all-for / --per-day / --days)
           dr     -> DR pgroup snapshots on the source array      (--all-for / --per-day / --days)
           target -> DR pgroup snapshots on the target array      (--target-all-for / -per-day / -days)
         Retention that isn't in the config is shown and left as is.
    Safe to run again: anything that already exists (or already matches) is reported and left unchanged.
    With --dry-run the array is read (list commands only) so the preview shows exactly what would be
    created; nothing is changed.
    Returns the number of protection_groups entries that are not fully set up (all of them if the
    arrays couldn't be connected)."""
    q = shlex.quote
    tcfg, pgs = cfg["target_array"], cfg["protection_groups"]
    log.info("=== setup step 1 of 4: connect the arrays ===")
    linked, target = connect_arrays(arr, tarr, tcfg)  # connected?, the target's Purity name
    did = lambda done, would: would if arr.dry_run else done            # log wording for --dry-run
    known = True
    try:                                                                # what is on the array now
        out = arr.run("purevol list --csv", read_only=True)
        vols_now = dict(zip(csv_column(out, "Name"), csv_column(out, "Size")))      # name -> size
        pgs_now = set(csv_column(arr.run("purepgroup list --csv", read_only=True), "Name"))
    except CLIError as e:
        if not arr.dry_run:
            log.error(f"Setup stopped: could not list the volumes and protection groups ({e})")
            return len(pgs)
        log.warning(f"Dry run: could not read the array ({e}). Volumes with a 'size' are shown as created; "
                    f"volumes without one are assumed to exist")
        known, vols_now, pgs_now = False, {}, set()
    pgs_before = set(pgs_now)
    created = set()                                  # volumes created by this run
    label = lambda pg: f"{pg['source_pgroup']} -> {pg['dr_pgroup']}"
    problems = {label(pg): [] for pg in pgs}
    n_vols = n_pgs = 0

    def problem(pg, msg):
        problems[label(pg)].append(msg)
        log.error(msg)

    def make_volume(pg, cmd, name, size, how):
        """Run one volume-create command; record the new volume. Returns True if it was created."""
        nonlocal n_vols
        try:
            arr.run(cmd)
        except CLIError as e:
            hint = (". A destroyed volume with this name may be waiting to be eradicated: check "
                    "'purevol list --pending', then 'purevol recover' or 'purevol eradicate' it"
                    if _already(e) else "")
            problem(pg, f"can't create volume {name}: {_msg(e)}{hint}")
            return False
        vols_now[name] = size
        created.add(name)
        n_vols += 1
        log.info(f"{did('Created', 'Would create')} volume {name} ({size}){how}")
        return True

    def add_volumes(pg, pgroup, names, new):
        """Add volumes to a pgroup: one --addvollist for a new (empty) pgroup; one volume at a time for
        an existing pgroup, so volumes that are already members don't stop the others."""
        if not names:
            return
        if new:
            try:
                arr.run(f"purepgroup setattr --addvollist {','.join(q(n) for n in names)} {q(pgroup)}")
                log.info(f"{did('Added', 'Would add')} {', '.join(names)} to {pgroup}")
            except CLIError as e:
                problem(pg, f"can't add volumes to {pgroup}: {_msg(e)}")
            return
        for n in names:
            try:
                arr.run(f"purepgroup setattr --addvollist {q(n)} {q(pgroup)}")
                log.info(f"{did('Added', 'Would add')} {n} to {pgroup}"
                         + (" (skipped by the array if it's already a member)" if arr.dry_run else ""))
            except CLIError as e:
                if _already(e):
                    log.info(f"{n} is already in {pgroup}")
                else:
                    problem(pg, f"can't add {n} to {pgroup}: {_msg(e)}")

    # ---- 2. every volume, source and DR, before any protection group
    log.info("=== setup step 2 of 4: volumes ===")
    for pg in pgs:
        for v in pg["volumes"]:
            src, tgt = v["source"], v["target"]
            size = str(v["size"]).upper() if v.get("size") else None
            # source volume
            if src in vols_now:
                log.info(f"Volume {src} already exists ({vols_now[src] or 'size not read'}), left as is")
            elif not known and not size:
                vols_now[src] = None
                log.info(f"Volume {src} has no 'size' in the config, so it's assumed to exist")
            elif not size:
                problem(pg, f"can't create volume {src}: it doesn't exist and has no 'size' in the config "
                            f"(add e.g. \"size\": \"100G\" to its entry)")
            else:
                make_volume(pg, f"purevol create --size {size} {q(src)}", src, size, "")
            # DR volume, the same size as its source
            if tgt in vols_now:
                log.info(f"Volume {tgt} already exists ({vols_now[tgt] or 'size not read'}), left as is")
            elif src not in vols_now:
                problem(pg, f"can't create volume {tgt}: its source volume {src} doesn't exist")
            elif src in created:
                make_volume(pg, f"purevol create --size {size} {q(tgt)}", tgt, size, f", the same size as {src}")
            else:
                src_size = vols_now[src]
                if size and src_size and parse_size(size) != parse_size(src_size):
                    log.warning(f"'size' {size} for {src} doesn't match the existing volume ({src_size}); "
                                f"{tgt} is made the same size as {src}")
                make_volume(pg, f"purevol copy {q(src)} {q(tgt)}", tgt, src_size or f"same size as {src}",
                            f" as a copy of {src}")

    # ---- 3. protection groups: create, then add their volumes; DR pgroups also get the target
    log.info("=== setup step 3 of 4: protection groups ===")
    for pg in pgs:
        vols = pg["volumes"]
        for pgroup, members, is_dr in ((pg["source_pgroup"], [v["source"] for v in vols], False),
                                       (pg["dr_pgroup"], [v["target"] for v in vols], True)):
            new = pgroup not in pgs_now
            if new:
                try:
                    arr.run(f"purepgroup create {q(pgroup)}")
                except CLIError as e:
                    hint = (". A destroyed protection group with this name may be waiting to be eradicated: "
                            "check 'purepgroup list --pending'" if _already(e) else "")
                    problem(pg, f"can't create protection group {pgroup}: {_msg(e)}{hint}")
                    continue
                pgs_now.add(pgroup)
                n_pgs += 1
                log.info(f"{did('Created', 'Would create')} protection group {pgroup}")
            else:
                log.info(f"Protection group {pgroup} already exists, adding any missing volumes")
            add_volumes(pg, pgroup, [m for m in members if m in vols_now], new)
            if is_dr and not linked:
                problem(pg, f"replication target {target} not added to {pgroup}: the arrays aren't "
                            f"'connected' (see step 1)")
            elif is_dr:
                try:
                    arr.run(f"purepgroup setattr --addtargetlist {q(target)} {q(pgroup)}")
                    log.info(f"{did('Added', 'Would add')} replication target {target} to {pgroup}")
                except CLIError as e:
                    if _already(e):
                        log.info(f"{target} is already a target of {pgroup}")
                    else:
                        problem(pg, f"can't add target {target} to {pgroup}: {_msg(e)}. Check that "
                                    f"'purearray connection list' shows {target} (target_array.name)")

    # ---- 4. retention: set what the config's "retention" blocks ask for; show the rest
    log.info("=== setup step 4 of 4: retention ===")
    n_ret = 0
    for pg in pgs:
        want = wanted_retention(cfg, pg)
        for pgroup in dict.fromkeys((pg["source_pgroup"], pg["dr_pgroup"])):
            if pgroup not in pgs_now:
                continue                              # couldn't be created; already reported
            todo = [(kind, scope) for kind, (key, scope) in RET_KINDS.items()
                    if pg[key] == pgroup and kind in want]
            have, new_in_dry_run = None, arr.dry_run and pgroup not in pgs_before
            if not new_in_dry_run:
                try:
                    have = read_retention(arr, pgroup)
                except CLIError as e:
                    log.warning(f"Could not read the retention of {pgroup} ({_msg(e)})"
                                + ("; setting it from the config anyway" if todo else ""))
            if not todo:
                if have:
                    log.info(f"Retention of {pgroup} (not in the config, left as is): "
                             f"on {arr.name}: {fmt_retention(have['local'])}"
                             + (f"; on {have['target_array']}: {fmt_retention(have['target'])}"
                                if have["target"] else ""))
                continue
            for kind, scope in todo:
                if set_retention(arr, pg, pgroup, kind, scope, want[kind],
                                 (have or {}).get(scope), problem, did, new_in_dry_run,
                                 (have or {}).get("target_array") or target):
                    n_ret += 1

    failed = [k for k, v in problems.items() if v]
    nxt = ("run --setup again without --dry-run to make these changes" if arr.dry_run else
           "everything in the config is ready. Next: run with --dry-run, then -V")
    log.info(f"Setup finished: {did('created', 'would create')} {n_vols} volume(s) and {n_pgs} "
             f"protection group(s), {did('changed', 'would change')} {n_ret} retention setting(s); "
             + (f"{len(failed)} not ready ({'; '.join(failed)}). Fix the errors above and run --setup again"
                if failed else nxt))
    return len(failed)


def now_iso():
    return datetime.datetime.now().isoformat(timespec="seconds")


def snap_only(arr, cfg, config_path, target, suffix, run_time, state_path, dry_run):
    """--snaponly: step 1 for every pgroup, back to back, and nothing else (no target login, no DR
    volume checks), so it returns as fast as possible. Records the snapshots in the state file for
    --cloneandreplicate. Returns the exit code: 0 all snapshots taken, 1 any failed (or the state file
    couldn't be written)."""
    pgs = cfg["protection_groups"]
    try:
        prev = read_state(state_path)
    except ValueError as e:
        log.warning(f"{e}; it will be replaced")
        prev = None
    if prev and unused_snapshots(prev):
        log.warning(f"Replacing the --snaponly record of {prev.get('created')}: "
                    f"{', '.join(e['snapshot'] for e in unused_snapshots(prev))} never went through "
                    f"--cloneandreplicate. Those snapshots stay on the array until their retention expires")
    log.info(f"=== --snaponly: snapshotting {len(pgs)} protection group(s), suffix {suffix} ===")
    t0 = time.monotonic()
    entries, failed = [], []
    for pg in pgs:
        src_pg = pg["source_pgroup"]
        e = {"source_pgroup": src_pg, "dr_pgroup": pg["dr_pgroup"], "snapshot": f"{src_pg}.{suffix}",
             "status": "ok", "error": None, "taken": None, "cloned": None}
        try:
            snap_source(arr, src_pg, suffix)
            e["taken"] = now_iso()
        except CLIError as ex:
            e["status"], e["error"] = "failed", str(ex)
            log.error(f"FAILED snapshot of {src_pg}: {ex}")
            failed.append(src_pg)
            if not dry_run:                       # DRDATA: the run failed for this pgroup
                emit_failed_start(pg, suffix, run_time, target, str(ex))
        entries.append(e)
    secs = time.monotonic() - t0
    ok = len(pgs) - len(failed)

    st = {"_note": "Written by pg_dr_refresh.py --snaponly and read by --cloneandreplicate. Don't edit.",
          "created": run_time, "suffix": suffix, "run_time": run_time,
          "source_host": cfg["source_array"]["host"], "config": str(Path(config_path).resolve()),
          "status": "pending" if ok else "failed", "snapshots": entries}
    if dry_run:
        log.info(f"Dry run: would record {ok} snapshot(s) in {state_path}")
    else:
        try:
            write_state(state_path, st)
            log.info(f"Recorded {ok} snapshot(s) for --cloneandreplicate in {state_path}")
        except OSError as ex:
            log.error(f"The snapshots were taken but could NOT be recorded in {state_path} ({ex}), so "
                      f"--cloneandreplicate can't use them. Fix the folder permissions or set state_file "
                      f"in the config, then run --snaponly again")
            return 1

    log.info(f"Snapshots finished in {secs:.1f}s: {ok} of {len(pgs)} taken"
             + (f"; FAILED: {', '.join(failed)}" if failed else ""))
    if ok:
        log.info(f"The databases of the snapshotted protection groups can leave backup mode now. "
                 f"Next: pg_dr_refresh.py -c {config_path} --cloneandreplicate")
    if failed:
        log.error(f"No usable snapshot of {', '.join(failed)}: run --snaponly again for a new one "
                  f"(it snapshots every protection group again)")
    log.info(f"##### pg_dr_refresh end (--snaponly)  {ok} snapshot(s) taken, {len(failed)} failed")
    return 1 if failed else 0


def load_pending_state(state_path, config_path, src_host, pgroups):
    """--cloneandreplicate: the --snaponly state with at least one unused snapshot of a pgroup in the
    config (pgroups = source pgroup names), or None after logging why there is nothing to do (the
    caller then exits with code 2, nothing changed)."""
    try:
        st = read_state(state_path)
    except ValueError as e:
        log.error(f"--cloneandreplicate: {e}. Run --snaponly first. Nothing was changed")
        return None
    if st is None:
        log.error(f"--cloneandreplicate uses the snapshots recorded by --snaponly, but there is no record "
                  f"({state_path} doesn't exist). Run 'pg_dr_refresh.py -c {config_path} --snaponly' "
                  f"first. Nothing was changed")
        return None
    if not unused_snapshots(st, pgroups):
        used = [e["cloned"] for e in st["snapshots"] if e.get("cloned")]
        why = (f"they were already used by --cloneandreplicate (at {max(used)})" if used
               else "none of its snapshots succeeded")
        log.error(f"--cloneandreplicate: nothing to do for the --snaponly run of {st.get('created')} "
                  f"(suffix {st['suffix']}): {why}. Run --snaponly again first. Nothing was changed")
        return None
    if st.get("source_host") and st["source_host"].lower() != str(src_host).lower():
        log.error(f"--cloneandreplicate: {state_path} was written for source array {st['source_host']}, "
                  f"but this config's source_array.host is {src_host}. Run --snaponly with this config "
                  f"first. Nothing was changed")
        return None
    created = None
    try:
        created = datetime.datetime.fromisoformat(st.get("created") or "")
    except ValueError:
        pass
    age = f" ({fmt_duration((datetime.datetime.now() - created).total_seconds())} ago)" if created else ""
    log.info(f"Using the --snaponly snapshots of {st.get('created')}{age}, suffix {st['suffix']}: "
             f"{', '.join(e['snapshot'] for e in unused_snapshots(st, pgroups))}")
    return st


def clone_phase1(arr, cfg, st, state_path, target, suffix, run_time, dry_run, failed, jobs, skipped):
    """--cloneandreplicate phase 1: for each pgroup in the config, overwrite its DR volumes from the
    snapshot --snaponly recorded and start replication. Appends to failed / jobs / skipped.
    A pgroup is marked used ("cloned") in the state file just before its first DR volume is
    overwritten; one that fails before that point stays unused, so the next -R retries it."""
    by_pg = {e["source_pgroup"]: e for e in st["snapshots"]}
    in_cfg = {pg["source_pgroup"] for pg in cfg["protection_groups"]}

    def save():
        try:
            try:
                now_on_disk = read_state(state_path)
            except ValueError:
                now_on_disk = None
            if now_on_disk and now_on_disk.get("suffix") != st["suffix"]:
                log.warning(f"{state_path} now holds a newer --snaponly run ({now_on_disk['suffix']}); "
                            f"left as is, not updated for {st['suffix']}")
                return
            st["status"] = "pending" if unused_snapshots(st, in_cfg) else "used"
            write_state(state_path, st)
        except OSError as ex:
            log.warning(f"Could not update {state_path} ({ex}); a later --cloneandreplicate may "
                        f"process these protection groups again")

    for pg in cfg["protection_groups"]:
        src_pg = pg["source_pgroup"]
        e = by_pg.get(src_pg)
        if e is None:
            msg = (f"no snapshot of {src_pg} was recorded by --snaponly at {st.get('created')} "
                   f"(added to the config since?) - run --snaponly again")
            log.error(f"FAILED {src_pg} -> {pg['dr_pgroup']}: {msg}")
            failed.append(src_pg)
            if not dry_run:
                emit_failed_start(pg, suffix, run_time, target, msg)
            continue
        if e.get("status") != "ok":
            log.error(f"FAILED {src_pg} -> {pg['dr_pgroup']}: its --snaponly snapshot failed "
                      f"({e.get('error')}) - run --snaponly again")
            failed.append(src_pg)                  # DRDATA was already written by --snaponly
            continue
        if e.get("cloned"):
            log.info(f"{e['snapshot']}: already used by --cloneandreplicate at {e['cloned']}, skipped")
            skipped.append(src_pg)
            continue
        if e.get("dr_pgroup") and e["dr_pgroup"] != pg["dr_pgroup"]:
            log.warning(f"{src_pg}: dr_pgroup was {e['dr_pgroup']} when --snaponly ran, now "
                        f"{pg['dr_pgroup']}; using {pg['dr_pgroup']} from the config")

        def mark(e=e):
            e["cloned"] = now_iso()
            save()

        try:
            job = clone_pgroup(arr, pg, target, suffix, run_time, dry_run, on_change=None if dry_run else mark)
            if not dry_run:
                jobs.append(job)
        except CLIError as ex:
            log.error(f"FAILED {src_pg} -> {pg['dr_pgroup']}: {ex}")
            if not dry_run and not e.get("cloned"):
                log.info(f"{e['snapshot']} wasn't used (nothing overwritten); the next "
                         f"--cloneandreplicate retries it")
            failed.append(src_pg)
            if not dry_run:
                emit_failed_start(pg, suffix, run_time, target, str(ex))

    extra = sorted(set(by_pg) - in_cfg)
    if extra:
        log.warning(f"--snaponly also recorded {', '.join(extra)}, which is no longer in the config; ignored")
    if not dry_run:
        save()


def main():
    """Parse arguments, load the config, then run phase 1 (start) and phase 2 (monitor) for every pgroup.
    With --snaponly only the source snapshots are taken (and recorded); with --cloneandreplicate the
    recorded snapshots are used instead of new ones.
    Returns the process exit code: 0 all succeeded, 1 any pgroup failed, 2 usage error."""
    ap = argparse.ArgumentParser(description="FlashArray pgroup DR refresh + replication")
    ap.add_argument("-c", "--config", required=True, help="JSON config file")
    ap.add_argument("-V", "--verbose", action="store_true",
                    help="show CLI commands/array output and a replication progress bar")
    ap.add_argument("--dry-run", action="store_true", help="log the commands without running them")
    mode = ap.add_mutually_exclusive_group()
    mode.add_argument("--setup", action="store_true",
                      help="one-time: connect the arrays (reads the target's connection key over SSH), "
                           "create every missing volume in the config (source and DR), then the "
                           "source and DR pgroups, add the volumes to them, and add the replication target "
                           "to the DR pgroups, and set the retention from the config's \"retention\" "
                           "blocks where it differs. With --dry-run, reads the array and shows what "
                           "would be created or changed")
    mode.add_argument("-S", "--snaponly", action="store_true",
                      help="only snapshot the source pgroups, as fast as possible, and record the "
                           "snapshots in the state file; the databases can leave backup mode afterwards. "
                           "Follow with --cloneandreplicate")
    mode.add_argument("-R", "--cloneandreplicate", action="store_true",
                      help="overwrite the DR volumes from the snapshots recorded by the last --snaponly, "
                           "then snapshot + replicate the DR pgroups and monitor them. Fails (exit 2, "
                           "nothing changed) if --snaponly hasn't recorded unused snapshots")
    args = ap.parse_args()

    cfg, s = load_config(args.config)
    log_path = setup_logging(resolve_log_base(cfg, args.config), args.verbose, args.dry_run)
    state_path = resolve_state_path(cfg, args.config)
    src = cfg["source_array"]
    arr = FlashArray(src["host"], src["host"], src.get("ssh_user") or "pureuser",
                     src.get("ssh_key") or None, s["ssh_timeout_seconds"], args.dry_run)
    tcfg = cfg["target_array"]
    target = tcfg.get("name") or tcfg["host"]          # replaced by the Purity name once it is read
    started = datetime.datetime.now()
    suffix = f"{s['snapshot_prefix']}-{started:%Y%m%d-%H%M%S}"     # one suffix for every pgroup in this run
    run_time = started.isoformat(timespec="seconds")               # DRDATA 'time' = run start (report date)

    mode_label = ("SETUP" if args.setup else "SNAPONLY" if args.snaponly
                  else "CLONEANDREPLICATE" if args.cloneandreplicate else "")
    log.info(f"##### pg_dr_refresh start  config={args.config}  source={src['host']}  target={target}  "
             f"{'DRY-RUN ' if args.dry_run else ''}{mode_label}")
    log.info(f"Log file: {log_path}")
    # The target array: replication is monitored there; --setup also reads its connection key there.
    # Login = target_array.ssh_user / ssh_key (each defaults to the source_array value if left out).
    thost = tcfg.get("host") or tcfg["name"]
    tarr = FlashArray(thost, thost, tcfg.get("ssh_user") or src.get("ssh_user") or "pureuser",
                      tcfg.get("ssh_key", src.get("ssh_key")) or None, s["ssh_timeout_seconds"], args.dry_run)
    log.info(f"Source array: {src['host']} as {arr.user}.  Target array: {thost} as {tarr.user}")

    # --snaponly: snapshots only, straight away - no name lookup, no target check or login
    if args.snaponly:
        return snap_only(arr, cfg, args.config, target, suffix, run_time, state_path, args.dry_run)

    # --cloneandreplicate: use the --snaponly snapshots; stop before touching anything if there are none
    st = None
    if args.cloneandreplicate:
        st = load_pending_state(state_path, args.config, src["host"],
                                {pg["source_pgroup"] for pg in cfg["protection_groups"]})
        if st is None:
            return 2
        suffix, run_time = st["suffix"], st.get("run_time") or st.get("created") or run_time

    monitor = s["wait_for_replication"] or args.verbose
    if not args.dry_run:
        try:
            discover_array_name(arr, "Source")
        except CLIError as e:
            log.warning(f"Could not read the source array's name ({e})")
        if not args.setup:                       # --setup checks (and makes) the connection itself
            target = check_target_connected(arr, tcfg)
        if not args.setup and monitor:           # monitoring needs the target: check the login first
            try:
                discover_array_name(tarr, "Target")
            except CLIError as e:
                log.error(f"Can't log in to the target array {thost} as {tarr.user} ({_msg(e)}). Replication "
                          f"is monitored on the target, so nothing was changed. Fix the SSH login "
                          f"(README section 5, step 4; target_array.ssh_user / ssh_key), or set "
                          f"wait_for_replication to false to run without monitoring")
                return 1

    if args.setup:
        return 1 if setup(arr, cfg, tarr) else 0

    # Phase 1: start every protection group (snapshot, overwrite, replicate-now) without waiting
    failed, jobs, skipped = [], [], []
    if st is not None:
        clone_phase1(arr, cfg, st, state_path, target, suffix, run_time, args.dry_run, failed, jobs, skipped)
    else:
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

    # Phase 2: monitor all replications together ON THE TARGET and log data / time per pgroup and volume
    if args.dry_run and monitor:
        src_label = arr.purity_name or "<source Purity name>"
        for pg in cfg["protection_groups"]:
            tarr.run(f"purepgroup list --snap --transfer --csv {src_label}:{pg['dr_pgroup']}"
                     f"   (on the target, until {src_label}:{pg['dr_pgroup']}.{suffix} is complete)")
    if jobs and monitor:
        monitor_all(tarr, jobs, s, args.verbose)
        failed += [j["pg"] for j in jobs if j["error"]]
    elif jobs:
        log.info(f"wait_for_replication is false - replications left running; check on the target {thost} "
                 f"with purepgroup list --snap --transfer {arr.purity_name or '<source>'}:<DR pgroup>")
        for j in jobs:
            emit_started(j)

    ok = len(cfg["protection_groups"]) - len(failed) - len(skipped)
    log.info(f"##### pg_dr_refresh end  {ok} succeeded, {len(failed)} failed"
             + (f" ({', '.join(failed)})" if failed else "")
             + (f", {len(skipped)} already done earlier ({', '.join(skipped)})" if skipped else ""))
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
