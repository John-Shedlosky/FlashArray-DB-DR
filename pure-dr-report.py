#!/usr/bin/env python3
"""
pure-dr-report.py - Visual and text reports of the pg_dr_refresh.py log files.

What it shows
-------------
Reads the weekly pg_dr_refresh_YYYYMMDD.log files and opens a window with these tabs:
  * Overview         - each protection group and its volumes: size, space used on the target, average
                       change per day, change as % of size, peak day, last run, average / maximum
                       replication time, OK / failed runs
  * Sizes            - provisioned size of every DR volume, grouped by protection group
  * Daily change     - data replicated per day (stacked by volume) and change rate as % of size
  * Replication time - time per run for each protection group, and per-volume average / maximum
  * Target space     - space each protection group's snapshots use on the target array, per run, and the
                       latest value (from purepgroup list --snap --space <pgroup> --total on the target);
                       the space each new snapshot added, as an estimated growth rate per day and per week
  * Weekly / Monthly - totals per week or month by protection group and volume: data replicated,
                       average per day, average / maximum replication time, OK / failed runs and
                       the change versus the previous week / month; plus the selected period
                       compared with the previous period of the same length

--cli prints the same information in the terminal only: the Overview table plus bar charts drawn with
'#' (or --bar-char '*') with the numbers beside every bar - DR volume sizes, space used on the target
per protection group (latest and per day), daily change stacked by
protection group, change rate as % of size, replication time per day and per volume (average, then
'*' up to the maximum), this period vs the previous period, and weekly / monthly totals. Each chart
has its own scale ("1 char = ..."). Periods over 45 days show one row per week in the per-day charts.
--cli needs no window, display or matplotlib.

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
* Space on target = the (total) row of `purepgroup list --snap --space <source>:<DR pgroup> --total`,
  read on the target after each monitored run: the space all of that protection group's snapshots use
  there. It is a point-in-time value, so the report shows the latest measurement in the period (and, per
  week / month, the last one in that week / month), never a sum. "Change" = latest minus the first
  measurement in the period. Runs from before pg_dr_refresh.py recorded it show '-'.
* Space growth on target (estimated) - how fast that space grows:
    increment = (total) after this run - (total) after the previous successful run of the same pgroup
                (the space the new snapshot added on the target, net of any older snapshots that
                expired in the meantime)
    interval  = time between the two runs' snapshots (run start times)
    per hour  = increment / interval hours;  est. per day = per hour x 24;  est. per week = per hour x 168
  So a snapshot that added 250 MiB one hour after the previous one -> 6 GiB/day, 42 GiB/week.
  "Last" = the latest snapshot in the period; "Avg" = all increments in the period / all their hours
  (the same x24 / x168), which smooths out single runs. The first measured run of a pgroup has no
  increment. A negative value means older snapshots expired (retention) and freed more than was added.

Data source
-----------
The 'DRDATA {...}' records pg_dr_refresh.py writes after each run (one per protection group and one
per volume; the space on the target is in the protection group record, field target_space_bytes).
Older logs without DRDATA records are read from their VOLUME / summary lines (no sizes).

Usage
-----
  pure-dr-report.py                          # logs from <script folder>/logs/
  pure-dr-report.py -c pg_dr_config.json     # logs from the config's log_file location
  pure-dr-report.py -l /var/log/pure         # a folder, file(s), or glob(s)
  pure-dr-report.py --remote                 # e.g. on Windows: read the logs on the Linux server over SSH;
                                             # asks for the hostname, username and password
  pure-dr-report.py --remote -l /var/log/pure   # with --remote, -l and -c are paths on that server
  pure-dr-report.py --period prev-month      # last7 | last30 | prev-week | prev-month | this-month | all
  pure-dr-report.py --from 2026-09-01 --to 2026-09-30   # custom date range (inclusive)
  pure-dr-report.py --days 14 --pgroup PG1   # rolling last N days / one protection group
  pure-dr-report.py --as-of 2026-10-31       # report as if today were that date
  pure-dr-report.py --cli                    # terminal report with '#' bar charts and numbers (no window)
  pure-dr-report.py --cli --bar-char '*' --width 140   # '*' bars, fixed width (default: terminal width)
  pure-dr-report.py --no-gui                 # text report only - tables, no charts (no window)
  pure-dr-report.py --no-gui --summary week  # weekly/monthly section: auto | week | month | both | none
  pure-dr-report.py --export-png DIR         # save the charts (incl. weekly + monthly) as PNG files
  pure-dr-report.py --export-csv FILE        # per-run CSV + FILE_period.csv, FILE_weekly.csv, FILE_monthly.csv

Requirements
------------
Python 3.8+. --cli, --no-gui and CSV output need nothing else. The window and PNG export need matplotlib 3.5+
(distro package python3-matplotlib, or pip inside a virtual environment); the window also needs Tk
(Linux: python3-tk / python3-tkinter / python3.11-tkinter / python311-tk; included with the python.org
Windows and macOS installers) and a display (desktop session or ssh -X). See README.md for each distro.
--remote also needs paramiko: py -m pip install paramiko (Windows) - see README.md.
"""
import argparse, base64, collections, csv, datetime, fnmatch, getpass, glob, hashlib, io, json, os
import posixpath, re, shlex, stat, statistics, sys
from pathlib import Path, PurePosixPath

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

def fmt_delta(n):
    """Signed byte change: 536870912 -> '+512.00 MiB', -1073741824 -> '-1.00 GiB'; None -> '-'."""
    if n is None:
        return "-"
    return ("+" if n >= 0 else "-") + fmt_bytes(abs(n))

def fmt_hours(h):
    """Hours -> '45 s' / '12 min' (under 1 hour), '1.0 h' (under 72 hours) or '3.5 d'; None -> '-'."""
    if h is None:
        return "-"
    if h < 1 / 60:
        return f"{h * 3600:.0f} s"
    if h < 1:
        return f"{h * 60:.0f} min"
    return f"{h:.1f} h" if h < 72 else f"{h / 24:.1f} d"

SHORT_INTERVAL_HOURS = 0.25         # increments over less time than this are flagged as unreliable estimates

def fmt_signed(n):
    """Bytes with a sign for growth figures: '+6.00 GiB', '-512.00 MiB'; None -> '-'."""
    return "-" if n is None else fmt_delta(n)

def annotate_space_growth(records):
    """Add the estimated space growth on the target to each pgroup record (in place):
      target_space_growth_bytes   = target_space_bytes - the previous successful run's target_space_bytes
                                    (same DR pgroup): the space the new snapshot added on the target
      target_space_interval_hours = hours between the two runs (run start = snapshot time)
      target_space_est_hourly_bytes / _daily_bytes / _weekly_bytes = growth / hours x 1, x 24, x 168
      target_space_prev_snapshots = the previous run's snapshot count (to spot expired snapshots)
    Only 'ok' runs with a space measurement are used, at both ends; a failed run in between is skipped,
    so its time is included in the next run's interval and the rate stays correct."""
    keys = ("target_space_growth_bytes", "target_space_interval_hours", "target_space_est_hourly_bytes",
            "target_space_est_daily_bytes", "target_space_est_weekly_bytes", "target_space_prev_snapshots")
    prev = {}
    for r in sorted((r for r in records if r.get("type") == "pgroup"), key=lambda r: r["time"]):
        for k in keys:
            r[k] = None
        if r.get("status") != "ok" or r.get("target_space_bytes") is None:
            continue
        p = prev.get(r["dr_pgroup"])
        prev[r["dr_pgroup"]] = r
        if not p:
            continue
        hours = (r["time"] - p["time"]).total_seconds() / 3600
        if hours <= 0:
            continue
        inc = r["target_space_bytes"] - p["target_space_bytes"]
        r.update({"target_space_growth_bytes": inc, "target_space_interval_hours": hours,
                  "target_space_est_hourly_bytes": inc / hours, "target_space_est_daily_bytes": inc / hours * 24,
                  "target_space_est_weekly_bytes": inc / hours * 168,
                  "target_space_prev_snapshots": p.get("target_space_snapshots")})

def growth_rate(recs):
    """Period growth from annotated pgroup records -> (total increment, total hours, per day, per week,
    number of increments); per day = total increment / total hours x 24, per week x 168."""
    g = [r for r in recs if r.get("target_space_growth_bytes") is not None]
    inc = _sum(r["target_space_growth_bytes"] for r in g)
    hrs = _sum(r["target_space_interval_hours"] for r in g)
    if inc is None or not hrs:
        return inc, hrs, None, None, len(g)
    return inc, hrs, inc / hrs * 24, inc / hrs * 168, len(g)

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
def config_log_file(text):
    """The log_file setting from the text of a pg_dr_refresh JSON config (comments allowed); None if unset."""
    text = re.sub(r'("(?:\\.|[^"\\])*")|//[^\n]*|/\*.*?\*/', lambda m: m.group(1) or "", text, flags=re.S)
    return json.loads(text).get("log_file")

def weekly_glob(base):
    """log_file -> the weekly files pg_dr_refresh.py writes: .../pg_dr_refresh.log -> .../pg_dr_refresh_*.log"""
    return str(base.with_name(f"{base.stem}_*{base.suffix or '.log'}"))

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
            base = config_log_file(Path(config).read_text(encoding="utf-8"))
        except (OSError, ValueError) as e:
            sys.exit(f"Cannot read config {config}: {e}")
        if base:                                    # relative paths are relative to the config file's folder
            base = Path(os.path.expanduser(base))
            if not base.is_absolute():
                base = Path(config).resolve().parent / base
        else:
            base = SCRIPT_DIR / "logs" / "pg_dr_refresh.log"
        pats.append(weekly_glob(base))
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

def load_records(files, opener=None):
    """Return a de-duplicated list of pgroup and volume records from all log files.
    opener(path) gives one file as a text stream (RemoteLogs.open for --remote); default: open it here."""
    recs, legacy, have_drdata = {}, [], set()
    for f in files:
        with (opener(f) if opener else open(f, encoding="utf-8", errors="replace")) as fh:
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


# ============================================================================ remote logs (--remote)
REMOTE_LOGS = "/var/log/pure/pg_dr_refresh_*.log"        # default: the log_file of the sample config
KNOWN_HOSTS = Path(os.path.expanduser("~")) / ".ssh" / "known_hosts"   # Windows: C:\Users\<you>\.ssh\known_hosts


class RemoteError(Exception):
    """A --remote problem, with a message for the user."""


class Cancelled(Exception):
    """The password dialog was cancelled."""


def ask(prompt, default=""):
    """One answer typed in the console. The prompt goes to stderr, so it is seen even when the report
    itself is redirected (--cli > report.txt)."""
    sys.stderr.write(prompt + (f" [{default}]" if default else "") + ": ")
    sys.stderr.flush()
    return input().strip() or default


def ask_yes_no(question):
    while True:
        a = ask(question + " (yes/no)").lower()
        if a in ("yes", "y", "no", "n"):
            return a.startswith("y")


def split_host(text):
    """'server' -> ('server', 22); 'server:2222' or '[fe80::1]:2222' -> that port."""
    m = re.fullmatch(r"\[([^\]]+)\](?::(\d+))?|([^:\s]+):(\d+)", text)
    host, port = ((m.group(1), m.group(2)) if m.group(1) else (m.group(3), m.group(4))) if m else (text, None)
    if port is not None and not 0 < int(port) < 65536:
        raise RemoteError(f"invalid port in '{text}'")
    return host, int(port or 22)


def default_user():
    try:
        return getpass.getuser()
    except Exception:
        return ""


def import_paramiko():
    """paramiko (SSH for Python), or an install hint."""
    try:
        import paramiko
    except ImportError:
        raise RemoteError("--remote needs the paramiko package. Install it with:\n"
                          "    py -m pip install paramiko         (Windows)\n"
                          "    python3 -m pip install paramiko    (Linux, in a virtual environment)") from None
    import logging
    logging.getLogger("paramiko").setLevel(logging.CRITICAL)        # errors are reported by this script
    return paramiko


def fingerprint(key):
    """A host key's SHA256 fingerprint, as ssh and ssh-keygen -l show it."""
    return "SHA256:" + base64.b64encode(hashlib.sha256(key.asbytes()).digest()).decode().rstrip("=")


def save_host_key(hostname, key):
    """Add an accepted host key to known_hosts (the file Windows' own ssh.exe uses too)."""
    try:
        KNOWN_HOSTS.parent.mkdir(parents=True, exist_ok=True)
        lead = ""
        if KNOWN_HOSTS.exists() and KNOWN_HOSTS.stat().st_size:
            with open(KNOWN_HOSTS, "rb") as f:
                f.seek(-1, os.SEEK_END)
                lead = "" if f.read(1) == b"\n" else "\n"
        with open(KNOWN_HOSTS, "a", encoding="ascii", newline="\n") as f:
            f.write(f"{lead}{hostname} {key.get_name()} {key.get_base64()}\n")
        return f"Added {hostname} to {KNOWN_HOSTS}"
    except OSError as e:
        return f"Warning: couldn't save the host key to {KNOWN_HOSTS} ({e}); you'll be asked again next time"


class RemoteLogs:
    """The pg_dr_refresh logs on a Linux server, read over SSH (SFTP) after a username + password login.
    SSH keys and agents are never used. Each file is read into memory: nothing is copied to this
    computer. The password isn't kept: if the connection drops, Reload asks for it again.
    ask_password / confirm / tell prompt in the console; the window replaces them with dialog boxes."""

    def __init__(self, host, port, user):
        self.pm = import_paramiko()
        self.host, self.port, self.user = host, port, user
        self.client = self.sftp = None
        self.home, self.patterns = "/", []
        self.ask_password = lambda: getpass.getpass(f"Password for {self.label}: ", stream=sys.stderr)
        self.confirm = ask_yes_no
        self.tell = lambda msg: print(msg, file=sys.stderr)

    @property
    def label(self):
        return f"{self.user}@{self.host}" + (f":{self.port}" if self.port != 22 else "")

    def connect(self, tries=3):
        """Log in, asking for the password on each try. Raises RemoteError if it can't."""
        pm, where = self.pm, f"{self.host}:{self.port}"
        for attempt in range(1, tries + 1):
            password = self.ask_password()
            if password is None:
                raise Cancelled()
            client = pm.SSHClient()
            try:
                client.load_system_host_keys()                     # ~/.ssh/known_hosts, if there is one
            except Exception as e:
                self.tell(f"Warning: couldn't read {KNOWN_HOSTS} ({e})")
            client.set_missing_host_key_policy(self._ask_host_key())
            try:
                client.connect(self.host, port=self.port, username=self.user, password=password,
                               allow_agent=False, look_for_keys=False,           # password only, no SSH keys
                               timeout=20, banner_timeout=30, auth_timeout=30)
            except pm.AuthenticationException as e:
                client.close()
                allowed = getattr(e, "allowed_types", None)              # set if the method isn't allowed
                if allowed is not None and not {"password", "keyboard-interactive"} & set(allowed):
                    raise RemoteError(f"{self.host} doesn't allow password logins (it allows: "
                                      f"{', '.join(allowed) or 'none'}). Its administrator can allow them "
                                      f"with 'PasswordAuthentication yes' in /etc/ssh/sshd_config") from None
                self.tell(f"Wrong username or password for {self.label}" + (" - try again" if attempt < tries else ""))
                continue
            except pm.BadHostKeyException as e:
                client.close()
                raise RemoteError(
                    f"the host key of {self.host} has CHANGED (it is now {fingerprint(e.key)}). Someone may be "
                    f"intercepting the connection, or the server was reinstalled, so the password wasn't sent. "
                    f"If the server's administrator confirms the new key, remove the old one with "
                    f"'ssh-keygen -R {self.host}' (or delete its line in {KNOWN_HOSTS}) and connect again") from None
            except (RemoteError, Cancelled):                     # host key not accepted
                client.close()
                raise
            except pm.SSHException as e:
                client.close()
                raise RemoteError(f"SSH to {where} failed: {e}") from None
            except OSError as e:                                 # unknown name, refused, unreachable, timeout
                client.close()
                raise RemoteError(f"can't connect to {where}: {e.strerror or str(e) or type(e).__name__}. "
                                  f"Check the hostname, and that this computer can reach the server on "
                                  f"port {self.port}") from None
            finally:
                password = None                                  # not kept
            try:
                client.get_transport().set_keepalive(30)         # keeps the connection open while the window is
                self.sftp = client.open_sftp()
                self.home = self.sftp.normalize(".")
            except (pm.SSHException, OSError, EOFError) as e:
                client.close()
                raise RemoteError(f"logged in to {self.label}, but it doesn't allow SFTP ({e or type(e).__name__}). "
                                  f"Its administrator can enable 'Subsystem sftp' in /etc/ssh/sshd_config") from None
            self.client = client
            return
        raise RemoteError(f"login to {self.label} failed {tries} times")

    def _ask_host_key(self):
        """paramiko policy for a server that isn't in known_hosts: show its fingerprint, as ssh does, and
        ask. The password is only sent after the key is accepted."""
        remote = self

        class AskUser(self.pm.MissingHostKeyPolicy):
            def missing_host_key(self, client, hostname, key):
                kt = key.get_name()
                kt = "ed25519" if "ed25519" in kt else "ecdsa" if "ecdsa" in kt else "rsa"
                if not remote.confirm(
                        f"The authenticity of host '{hostname}' can't be established.\n"
                        f"{key.get_name()} key fingerprint is {fingerprint(key)}.\n"
                        f"(On the server, 'ssh-keygen -lf /etc/ssh/ssh_host_{kt}_key.pub' shows its fingerprint.)\n"
                        f"Are you sure you want to continue connecting"):
                    raise RemoteError(f"the host key of {hostname} wasn't accepted, so nothing was sent to it")
                client.get_host_keys().add(hostname, key.get_name(), key)
                remote.tell(save_host_key(hostname, key))
        return AskUser()

    def ensure_connected(self):
        """Log in again (asks for the password) if the connection dropped, e.g. the server restarted."""
        t = self.client.get_transport() if self.client else None
        if t is None or not t.is_active():
            self.close()
            self.connect()

    def close(self):
        if self.client:
            self.client.close()
        self.client = self.sftp = None

    # ---- paths on the server: always '/' paths, also when this script runs on Windows
    def path(self, p):
        """A path on the server. ~ and relative paths start in the login's home folder."""
        if re.match(r"[A-Za-z]:[\\/]", p) or "\\" in p:
            raise RemoteError(f"'{p}' is a Windows path. With --remote, -l and -c are paths on the server, "
                              f"e.g. -l /var/log/pure. (Git Bash changes /var/... to C:/Program Files/Git/var/...: "
                              f"use PowerShell or Command Prompt, or set MSYS_NO_PATHCONV=1.)")
        if p == "~" or p.startswith("~/"):
            p = self.home + p[1:]
        return posixpath.normpath(posixpath.join(self.home, p))

    def kind(self, p, attr=None):
        """'file', 'dir' or None (missing, or no access), following symbolic links."""
        try:
            if attr is None or stat.S_ISLNK(attr.st_mode or 0):
                attr = self.sftp.stat(p)
        except OSError:
            return None
        m = attr.st_mode or 0
        return "file" if stat.S_ISREG(m) else "dir" if stat.S_ISDIR(m) else None

    def glob(self, pattern):
        """The files matching a path on the server, with * ? [...] wildcards in the file name."""
        folder, name = posixpath.split(pattern)
        if glob.has_magic(folder):
            raise RemoteError(f"{pattern}: on the server, wildcards work in the file name only")
        if not glob.has_magic(name):
            return [pattern] if self.kind(pattern) == "file" else []
        try:
            entries = self.sftp.listdir_attr(folder)
        except OSError as e:
            raise RemoteError(f"can't list {folder} on {self.host}: {e.strerror or e}") from None
        return [posixpath.join(folder, e.filename) for e in entries
                if fnmatch.fnmatchcase(e.filename, name) and not e.filename.startswith(".")
                and self.kind(posixpath.join(folder, e.filename), e) == "file"]

    def find(self, paths, config):
        """The log files on the server: -l folders / files / globs; else the log_file of the config at -c
        (a path on the server); else /var/log/pure/pg_dr_refresh_*.log. Returns (files, patterns)."""
        if paths:
            pats = []
            for p in paths:
                p = self.path(p)
                pats.append(posixpath.join(p, "*.log") if self.kind(p) == "dir" else p)
        elif config:
            cfg = self.path(config)
            try:
                base = config_log_file(self.read(cfg)) or "logs/pg_dr_refresh.log"   # pg_dr_refresh.py's default
            except (OSError, ValueError) as e:
                raise RemoteError(f"can't read the config {cfg} on {self.host}: "
                                  f"{getattr(e, 'strerror', None) or e}") from None
            if not base.startswith(("/", "~")):                      # relative: from the config's folder
                base = posixpath.join(posixpath.dirname(cfg), base)
            pats = [weekly_glob(PurePosixPath(self.path(base)))]
        else:
            pats = [REMOTE_LOGS]
        return self.search(pats)

    def search(self, pats):
        """Search the patterns (again for Reload, so a new week's log file is included)."""
        files = sorted({f for pat in pats for f in self.glob(pat)})
        self.patterns = pats
        return files, pats

    def read(self, p):
        """A file's text, read into memory over SFTP. Nothing is written to this computer."""
        with self.sftp.open(p, "rb") as fh:
            try:
                fh.prefetch(max_concurrent_requests=64)          # paramiko 3.3+ (64 = OpenSSH's own limit)
            except TypeError:
                fh.prefetch()                                    # older paramiko
            return fh.read().decode("utf-8", errors="replace")

    def open(self, p):
        """load_records' opener: one log file as a text stream."""
        try:
            return io.StringIO(self.read(p))
        except OSError as e:
            raise RemoteError(f"can't read {p} on {self.host}: {e.strerror or e}") from None

    def load(self, files):
        return load_records(files, self.open)


def open_remote():
    """--remote: ask for the server, username and password in the console, then log in (exits on failure)."""
    if not (sys.stdin and sys.stdin is sys.__stdin__ and sys.stdin.isatty()):
        sys.exit("--remote asks for a password, so run it in a Command Prompt or PowerShell window "
                 "(not in IDLE, with pythonw, or with its input redirected)")
    try:
        import_paramiko()                                    # first, so no password is typed in vain
        host = ""
        while not host:
            host = ask("Hostname of the Linux server with the logs")
        host, port = split_host(host)
        remote = RemoteLogs(host, port, ask("Username", default_user()))
        remote.connect()
        return remote
    except (KeyboardInterrupt, EOFError):
        print(file=sys.stderr)
        sys.exit("Cancelled")
    except RemoteError as e:
        sys.exit(f"--remote: {e}")


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
        # Space on the target (pgroup records only): a point-in-time value per run, so keep the series
        # and the latest measurement; volumes have none.
        sp = [r for r in runs if r.get("target_space_bytes") is not None]
        obj["space_series"] = [(r["time"], r["target_space_bytes"]) for r in sp]
        obj["space"] = sp[-1]["target_space_bytes"] if sp else None
        obj["space_snaps"] = sp[-1].get("target_space_snapshots") if sp else None
        obj["space_time"] = sp[-1]["time"] if sp else None
        obj["space_first"] = sp[0]["target_space_bytes"] if sp else None
        obj["space_change"] = (obj["space"] - obj["space_first"]) if len(sp) > 1 else None
        obj["space_max"] = max(r["target_space_bytes"] for r in sp) if sp else None
        # Estimated space growth on the target (see annotate_space_growth): latest snapshot + period average
        gr = [r for r in runs if r.get("target_space_growth_bytes") is not None]
        obj["growth_series"] = [(r["time"], r["target_space_est_daily_bytes"]) for r in gr]
        obj["growth_last"] = gr[-1] if gr else None
        (obj["growth_inc"], obj["growth_hours"], obj["growth_day"], obj["growth_week"],
         obj["growth_n"]) = growth_rate(gr)
        obj["target_pgroup"] = next((r.get("target_pgroup") for r in reversed(runs) if r.get("target_pgroup")), None)
        obj["target"] = next((r.get("target") for r in reversed(runs) if r.get("target")), None)
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
    sp = [r for r in runs if r.get("target_space_bytes") is not None]      # space on target: latest value
    last_sp = max(sp, key=lambda r: r["time"]) if sp else {}
    return {"data": data, "per_day": data / days if data is not None and days else None,
            "avg_secs": mean(secs), "max_secs": max(secs) if secs else None, "n_ok": len(ok),
            "n_fail": sum(1 for r in runs if r.get("status") == "failed"), "days": days,
            "space": last_sp.get("target_space_bytes"), "space_snaps": last_sp.get("target_space_snapshots"),
            "space_time": last_sp.get("time"),
            **dict(zip(("growth", "growth_hours", "growth_day", "growth_week"), growth_rate(runs)[:4]))}

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
                                        "avg_secs": pct(c["avg_secs"], p and p["avg_secs"]),
                                        "space": pct(c["space"], p and p.get("space")),
                                        "growth_day": pct(c.get("growth_day"), p and p.get("growth_day"))}})
    return rows

class Report:
    """All records, sliced by period / week / month."""
    def __init__(self, records, today=None):
        """records = every DRDATA record from the logs; today = report date (--as-of), default today.
        Records dated after 'today' are ignored, so --as-of reproduces a report from that date."""
        self.today = today or datetime.date.today()
        self.records = sorted((r for r in records if r["time"].date() <= self.today), key=lambda r: r["time"])
        annotate_space_growth(self.records)      # over every record, so a period's first run has its baseline
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

    def date_range(self, s, e):
        """(first, last) calendar date of s..e that the logs cover: from the first log up to today,
        where today only counts once it has a run (a report before the nightly run is not diluted).
        (None, None) if nothing in s..e is covered."""
        if not self.first:
            return None, None
        data_end = self.today if self.last >= self.today else self.today - D(days=1)
        lo, hi = max(s or self.first, self.first), min(e or data_end, data_end)
        return (lo, hi) if lo <= hi else (None, None)

    def days(self, s, e):
        """Calendar days of s..e covered by the logs (see date_range); days with no runs count as 0."""
        lo, hi = self.date_range(s, e)
        return (hi - lo).days + 1 if lo else 0

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
    hdr = f"{'Protection group / volume':<34}{'Size':>12}{'Tgt space':>13}{'Tgt est/day':>13}{'Tgt est/wk':>13}" \
          f"{'Avg/day':>13}{'%/day':>8}{'Last data':>13}{'Last time':>11}{'Avg time':>10}{'Max time':>10}" \
          f"{'OK/Fail':>9}  Status"
    out = [hdr, "-" * len(hdr)]
    for pg in model.selected(pg_filter):
        def row(label, o):
            """One formatted Overview line."""
            last = o["last"] or {}
            pct_ = f"{o['change_pct']:.2f}" if o["change_pct"] is not None else "-"
            is_pg = "volumes" in o                                              # space: per pgroup only
            gl = (o["growth_last"] or {}) if is_pg else {}
            space = fmt_bytes(o["space"]) if is_pg else ""
            gday = fmt_signed(gl.get("target_space_est_daily_bytes")) if is_pg else ""
            gweek = fmt_signed(gl.get("target_space_est_weekly_bytes")) if is_pg else ""
            return (f"{label[:33]:<34}{fmt_bytes(o['size']):>12}{space:>13}{gday:>13}{gweek:>13}"
                    f"{fmt_bytes(o['avg_daily']):>13}{pct_:>8}"
                    f"{fmt_bytes(last.get('data_bytes')):>13}{fmt_secs(last.get('seconds')):>11}"
                    f"{fmt_secs(o['avg_secs']):>10}{fmt_secs(o['max_secs']):>10}"
                    f"{str(o['n_ok']) + '/' + str(o['n_fail']):>9}  {o['last_status']}")
        out.append(row(f"{pg['name']} -> {pg['dr_pgroup']}", pg))
        for v in pg["volumes"].values():
            out.append(row(f"  {v['name']}", v))
    if len(out) == 2:
        out.append("(no runs in this period)")
    return out

def space_lines(model, pg_filter=None):
    """Text table of the space each protection group uses on the target: the latest (total) of
    purepgroup list --snap --space <pgroup> --total in the period, its bytes, snapshot count and time,
    plus the first measurement in the period, the change and the maximum."""
    hdr = (f"{'Protection group':<26}{'On target':<34}{'Latest':>12}{'Bytes':>17}{'Snaps':>7}  "
           f"{'Measured':<17}{'Period start':>13}{'Change':>13}{'Max':>12}")
    out = [hdr, "-" * len(hdr)]
    for pg in model.selected(pg_filter):
        where = (pg["target_pgroup"] or pg["dr_pgroup"]) + (f" on {pg['target']}" if pg["target"] else "")
        if pg["space"] is None:
            out.append(f"{(pg['name'] + ' -> ' + pg['dr_pgroup'])[:25]:<26}{where[:33]:<34}{'-':>12}"
                       f"   (not recorded in this period)")
            continue
        snaps = "-" if pg["space_snaps"] is None else str(pg["space_snaps"])
        out.append(f"{(pg['name'] + ' -> ' + pg['dr_pgroup'])[:25]:<26}{where[:33]:<34}{fmt_bytes(pg['space']):>12}"
                   f"{pg['space']:>17,.0f}{snaps:>7}  {pg['space_time']:%Y-%m-%d %H:%M}"
                   f"{fmt_bytes(pg['space_first']):>13}{fmt_delta(pg['space_change']):>13}{fmt_bytes(pg['space_max']):>12}")
    if len(out) == 2:
        out.append("(no runs in this period)")
    return out

def growth_lines(model, pg_filter=None):
    """Text table of the estimated space growth on the target per protection group: the latest snapshot's
    increment, the hours since the previous run, and that rate per hour, x24 per day and x168 per week;
    then the period average (all increments / all their hours, x24 and x168)."""
    hdr = (f"{'Protection group':<18}{'Last snapshot':<18}{'Increment':>13}{'Interval':>10}{'Per hour':>13}"
           f"{'Est/day':>13}{'Est/week':>13}  |{'Period growth':>15}{'Over':>9}{'Avg/day':>13}{'Avg/week':>13}  Note")
    out = [hdr, "-" * len(hdr)]
    for pg in model.selected(pg_filter):
        g = pg["growth_last"]
        if not g:
            why = ("one measurement so far - the rate needs two runs" if pg["space"] is not None
                   else "no space measurements in this period")
            out.append(f"{pg['name'][:17]:<18}{'-':<18}{'-':>13}   ({why})")
            continue
        notes = []
        if g["target_space_interval_hours"] < SHORT_INTERVAL_HOURS:
            notes.append(f"interval under {SHORT_INTERVAL_HOURS * 60:.0f} min: x24 / x168 estimate unreliable")
        ps, ns = g.get("target_space_prev_snapshots"), g.get("target_space_snapshots")
        if ps is not None and ns is not None and ns <= ps:          # each run adds one snapshot
            n = ps + 1 - ns
            notes.append(f"net: {n} older snapshot{'s' if n != 1 else ''} expired ({ps}->{ns})")
        note = "; ".join(notes)
        out.append(f"{pg['name'][:17]:<18}{g['time']:%Y-%m-%d %H:%M}  {fmt_signed(g['target_space_growth_bytes']):>13}"
                   f"{fmt_hours(g['target_space_interval_hours']):>10}{fmt_signed(g['target_space_est_hourly_bytes']):>13}"
                   f"{fmt_signed(g['target_space_est_daily_bytes']):>13}{fmt_signed(g['target_space_est_weekly_bytes']):>13}  |"
                   f"{fmt_signed(pg['growth_inc']):>15}{fmt_hours(pg['growth_hours']):>9}"
                   f"{fmt_signed(pg['growth_day']):>13}{fmt_signed(pg['growth_week']):>13}  {note}".rstrip())
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
    out += ["", "SPACE ON TARGET  (space all snapshots of each DR protection group use on the target: the (total) row of",
            "                  purepgroup list --snap --space <source>:<DR pgroup> --total, read after each run)"]
    out += space_lines(model, pg_filter)
    out += ["", "SPACE GROWTH ON TARGET - estimated  (increment = (total) now - (total) after the previous run;",
            "                  per hour = increment / interval;  Est/day = per hour x 24;  Est/week = per hour x 168;",
            "                  Avg = all increments in the period / all their hours, x 24 and x 168)"]
    out += growth_lines(model, pg_filter)
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

# ============================================================================ CLI charts (--cli)
# Text versions of the window's charts: horizontal bars drawn with '#' / '*' and the numbers printed
# beside each bar. Every section has its own scale, shown in its heading as "1 char = ...".
SERIES_CHARS = "#*=+o%@~"           # stacked bars: one character per protection group

def series_chars(first):
    """Fill characters for stacked bars: the chosen --bar-char first, then the others in SERIES_CHARS."""
    return first + "".join(c for c in SERIES_CHARS if c != first)

def other_char(ch):
    """Second bar character (previous period / up-to-maximum): '*' normally, '#' when --bar-char is '*'."""
    return "*" if ch != "*" else "#"

def term_width(width=None):
    """Output width: --width, else the terminal width (120 when not a terminal); at least 60."""
    if width:
        return max(60, width)
    import shutil
    return max(60, shutil.get_terminal_size((120, 24)).columns)

def hbar(value, vmax, width, ch):
    """Bar of `ch` scaled so vmax fills `width` characters; any value above 0 gets at least one."""
    if not value or not vmax or value <= 0:
        return ""
    return ch * min(width, max(1, int(round(value / vmax * width))))

def hstack(values, vmax, width, chars):
    """Stacked bar: one segment per value, each drawn with its own character (cumulative rounding,
    so the total length matches hbar of the sum)."""
    out, cum, drawn = "", 0.0, 0
    if not vmax:
        return out
    for v, c in zip(values, chars):
        if not v or v <= 0:
            continue
        cum += v
        n = max(1, int(round(cum / vmax * width)) - drawn)
        out += c * n
        drawn += n
    return out[:width]

def hrange(avg, mx, vmax, width, ch, ext):
    """Average / maximum bar: `ch` up to the average, then `ext` up to the maximum."""
    a, m = hbar(avg, vmax, width, ch), hbar(mx, vmax, width, ch)
    return a + ext * max(0, len(m) - len(a))

CLI_DAILY_MAX_DAYS = 45             # longer periods: the per-day charts show one row per week instead

def cli_slots(lo, hi):
    """Rows for the per-day charts: one per day for periods up to CLI_DAILY_MAX_DAYS days, else one per
    calendar week (Mon-Sun, clipped to lo..hi). -> (unit name, [(label, first day, last day, n days)])."""
    if not lo:
        return "day", []
    if (hi - lo).days + 1 <= CLI_DAILY_MAX_DAYS:
        return "day", [(f"{d:%Y-%m-%d %a}", d, d, 1) for d in (lo + D(days=i) for i in range((hi - lo).days + 1))]
    out = []
    for ws, we in bucket_ranges("week", lo, hi):
        a, b = max(ws, lo), min(we, hi)
        out.append((f"wk {a:%Y-%m-%d}", a, b, (b - a).days + 1))
    return "week", out

def cli_section(title, rows, W):
    """Render one chart section.
    title: function(bar_width) -> heading text (so the heading can show the scale); a second line
           after "\n" is printed as a legend;
    rows:  ("text", line) for a sub-heading, or (label, tag, bar_function(bar_width), numbers)."""
    bars = [r for r in rows if len(r) == 4]
    if not bars:
        return []
    lw = min(32, max(max(len(r[0]) for r in bars), 8))
    tw = max(len(r[1]) for r in bars)
    nw = max(len(r[3]) for r in bars)
    bw = max(20, W - (2 + lw + 1 + (tw + 1 if tw else 0) + 2 + 1) - nw)    # bar width (min 20)
    head = title(bw).split("\n")                       # heading, optionally with a legend line
    out = ["", *head, "-" * min(W, max(len(h) for h in head))]
    for r in rows:
        if len(r) != 4:
            out.append(r[1])
            continue
        label, tag, fn, text = r
        tagtxt = f"{tag:>{tw}} " if tw else ""
        out.append(f"  {label[:lw]:<{lw}} {tagtxt}|{fn(bw):<{bw}}| {text}".rstrip())
    return out

def cli_report(report, period, pg_filter=None, summary="auto", width=None, ch="#"):
    """--cli: the text report drawn as ASCII bar charts with the numbers beside every bar:
    Overview table, DR volume sizes, daily change (stacked by protection group), change rate as % of
    size, replication time per day and per volume (average + maximum), the period compared with the
    previous period, and weekly / monthly totals with the change versus the previous week / month."""
    W = term_width(width)
    ext = other_char(ch)
    chars = series_chars(ch)
    model = report.model_for(period)
    pgs = model.selected(pg_filter)
    pg_char = {n: chars[i % len(chars)] for i, n in enumerate(report.all_model.pgroups)}
    legend = "   ".join(f"{pg_char[pg['name']]} = {pg['name']}" for pg in pgs)
    out = [f"Pure DR replication report   {report.describe(period)}",
           f"Logs: {report.first} .. {report.last}   (as of {report.today})" if report.first else "No DR data found",
           "", "OVERVIEW", "-" * 8]
    out += overview_lines(model, pg_filter)
    if not pgs:
        return "\n".join(out)

    # ---- DR volume sizes
    rows = []
    for pg in pgs:
        vols = [v for v in pg["volumes"].values() if v["size"]]
        if vols:
            rows.append(("text", f"{pg['name']} -> {pg['dr_pgroup']}   total {fmt_bytes(pg['size'])}"))
            rows += [(v["name"], "", lambda bw, v=v: hbar(v["size"], vmax, bw, ch), fmt_bytes(v["size"]))
                     for v in vols]
    vmax = max([v["size"] for pg in pgs for v in pg["volumes"].values() if v["size"]] or [0])
    out += cli_section(lambda bw: f"DR VOLUME SIZES   (1 {ch} = {fmt_bytes(vmax / bw)})", rows, W)

    # rows of the per-day charts: one per day (one per week for periods over CLI_DAILY_MAX_DAYS days)
    lo, hi = report.date_range(period["start"], period["end"])
    unit, slots = cli_slots(lo, hi)
    in_slot = lambda d, sl: sl[1] <= d <= sl[2]

    # ---- space used on the target by each protection group's snapshots: latest, then per day / week
    spgs = [pg for pg in pgs if pg["space"] is not None]
    smax = max([pg["space_max"] for pg in spgs] or [0])
    rows = [(pg["name"], "", lambda bw, pg=pg: hbar(pg["space"], smax, bw, ch),
             f"{fmt_bytes(pg['space'])}  {pg['space']:,.0f} bytes  {pg['space_snaps'] if pg['space_snaps'] is not None else '-'} "
             f"snapshots  {pg['target_pgroup'] or pg['dr_pgroup']}  {pg['space_time']:%Y-%m-%d %H:%M}"
             + (f"  change {fmt_delta(pg['space_change'])}" if pg["space_change"] is not None else ""))
            for pg in spgs]
    rows += [(pg["name"], "", lambda bw: "", "not recorded in this period") for pg in pgs if pg["space"] is None]
    if spgs:
        out += cli_section(lambda bw: f"SPACE ON TARGET - latest, all snapshots of each protection group   "
                                      f"(1 {ch} = {fmt_bytes(smax / bw)})", rows, W)

    # ---- estimated growth of that space: latest snapshot (now) and period average (avg), per day
    gpgs = [pg for pg in pgs if pg["growth_last"]]
    gmax = max([x for pg in gpgs for x in (pg["growth_last"]["target_space_est_daily_bytes"], pg["growth_day"])
                if x and x > 0] or [0])
    rows = []
    for pg in gpgs:
        g = pg["growth_last"]
        rows.append((pg["name"], "last", lambda bw, x=g["target_space_est_daily_bytes"]: hbar(x, gmax, bw, ch),
                     f"{fmt_signed(g['target_space_est_daily_bytes'])}/day  {fmt_signed(g['target_space_est_weekly_bytes'])}/week"
                     f"   (snapshot {g['time']:%Y-%m-%d %H:%M}: {fmt_signed(g['target_space_growth_bytes'])} in "
                     f"{fmt_hours(g['target_space_interval_hours'])})"
                     + ("  short interval: estimate unreliable"
                        if g["target_space_interval_hours"] < SHORT_INTERVAL_HOURS else "")))
        rows.append(("", "avg", lambda bw, x=pg["growth_day"]: hbar(x, gmax, bw, ext),
                     f"{fmt_signed(pg['growth_day'])}/day  {fmt_signed(pg['growth_week'])}/week"
                     f"   (period: {fmt_signed(pg['growth_inc'])} over {fmt_hours(pg['growth_hours'])}, "
                     f"{pg['growth_n']} snapshot{'s' if pg['growth_n'] != 1 else ''})"))
    if gpgs:
        out += cli_section(lambda bw: f"SPACE GROWTH ON TARGET - estimated per day   (per hour = increment / hours since "
                                      f"the previous run; x24 per day, x168 per week;  1 char = {fmt_bytes(gmax / bw)})\n"
                                      f"  {ch} last = latest snapshot    {ext} avg = period average", rows, W)

    # ---- space on the target per day (or week): the last measurement in each row, change vs the row before
    first_sp = min((t.date() for pg in spgs for t, _ in pg["space_series"]), default=None)
    sp_slots = [sl for sl in slots if first_sp and sl[2] >= first_sp]      # start at the first measurement
    if spgs and sp_slots:
        rows = []
        for pg in spgs:
            rows.append(("text", f"{pg['name']} -> {pg['target_pgroup'] or pg['dr_pgroup']}   latest "
                                 f"{fmt_bytes(pg['space'])}, maximum {fmt_bytes(pg['space_max'])}"))
            prev = None
            for sl in sp_slots:
                vals = [b for t, b in pg["space_series"] if in_slot(t.date(), sl)]
                if not vals:
                    rows.append((sl[0], "", lambda bw: "", "-"))
                    continue
                v = vals[-1]
                rows.append((sl[0], "", lambda bw, v=v: hbar(v, smax, bw, ch),
                             f"{fmt_bytes(v)}" + (f"  {fmt_delta(v - prev)}" if prev is not None else "")))
                prev = v
        out += cli_section(lambda bw: f"SPACE ON TARGET PER {unit.upper()} - last measurement, change vs the "
                                      f"{unit} before   (1 {ch} = {fmt_bytes(smax / bw)})", rows, W)

    # ---- daily change, stacked by protection group (every calendar day, so missed days show;
    #      periods over CLI_DAILY_MAX_DAYS days show one row per week = average per day of that week)
    fails_on = collections.Counter(r["time"].date() for pg in pgs for r in pg_runs(pg) if r.get("status") == "failed")
    if slots:
        vals = {sl: [sum(b for d, b in pg["daily"].items() if in_slot(d, sl)) / sl[3] for pg in pgs] for sl in slots}
        dmax = max(sum(v) for v in vals.values())
        cs = [pg_char[pg["name"]] for pg in pgs]
        rows = []
        for sl in slots:
            tot = sum(vals[sl])
            nf = sum(n for d, n in fails_on.items() if in_slot(d, sl))
            txt = (fmt_bytes(tot) + ("/day" if unit == "week" else "")) if tot else "no successful run"
            txt += f"   {nf} failed" if nf else ""
            rows.append((sl[0], "", lambda bw, sl=sl: hstack(vals[sl], dmax, bw, cs), txt))
        ndays = sum(sl[3] for sl in slots)
        total = sum(v * sl[3] for sl in slots for v in vals[sl])
        rows.append(("text", f"  Average {fmt_bytes(total / ndays)}/day over {ndays} days; total {fmt_bytes(total)}"))
        what = "data replicated per day" if unit == "day" else "average per day, one row per week"
        if dmax:
            out += cli_section(lambda bw: f"DAILY CHANGE - {what}   (1 char = {fmt_bytes(dmax / bw)};  {legend})",
                               rows, W)

    # ---- average daily change rate as % of size
    rows = []
    for pg in pgs:
        vols = [v for v in pg["volumes"].values() if v["change_pct"] is not None]
        if vols:
            rows.append(("text", f"{pg['name']} -> {pg['dr_pgroup']}   group: "
                                 + (f"{pg['change_pct']:.2f}%/day  ({fmt_bytes(pg['avg_daily'])}/day)"
                                    if pg["change_pct"] is not None else "-")))
            rows += [(v["name"], "", lambda bw, v=v: hbar(v["change_pct"], pmax, bw, ch),
                      f"{v['change_pct']:.2f}%/day  ({fmt_bytes(v['avg_daily'])}/day)") for v in vols]
    pmax = max([v["change_pct"] for pg in pgs for v in pg["volumes"].values() if v["change_pct"] is not None] or [0])
    out += cli_section(lambda bw: f"AVERAGE DAILY CHANGE RATE - % of volume size per day   (1 {ch} = {pmax / bw:.3f}%)",
                       rows, W)

    # ---- replication time per day per protection group (average and maximum)
    rows, tmax = [], 0
    for pg in pgs:
        by_day = collections.defaultdict(list)
        nfail = collections.Counter()
        for r in pg_runs(pg):
            if r.get("status") == "ok" and r.get("seconds") is not None:
                by_day[r["time"].date()].append(r["seconds"])
            elif r.get("status") == "failed":
                nfail[r["time"].date()] += 1
        if not by_day and not nfail:
            continue
        rows.append(("text", f"{pg['name']} -> {pg['dr_pgroup']}   average {fmt_secs(pg['avg_secs'])}, "
                             f"maximum {fmt_secs(pg['max_secs'])}"))
        for sl in slots:
            secs = [x for d, xs in by_day.items() if in_slot(d, sl) for x in xs]
            nf = sum(n for d, n in nfail.items() if in_slot(d, sl))
            a, m = mean(secs), (max(secs) if secs else None)
            tmax = max(tmax, m or 0)
            txt = (f"avg {fmt_secs(a)}  max {fmt_secs(m)}  {len(secs)} run{'s' if len(secs) != 1 else ''}"
                   if secs else "no successful run")
            txt += f"   {nf} failed" if nf else ""
            rows.append((sl[0], "", lambda bw, a=a, m=m: hrange(a, m, tmax, bw, ch, ext), txt))
    out += cli_section(lambda bw: f"REPLICATION TIME PER {unit.upper()}   ({ch} = average, {ext} = up to maximum;"
                                  f"  1 char = {fmt_secs(tmax / bw)})", rows, W)

    # ---- replication time per volume (average and maximum)
    rows = []
    for pg in pgs:
        vols = [v for v in pg["volumes"].values() if v["avg_secs"] is not None]
        if vols:
            rows.append(("text", f"{pg['name']} -> {pg['dr_pgroup']}"))
            rows += [(v["name"], "", lambda bw, v=v: hrange(v["avg_secs"], v["max_secs"], vmx, bw, ch, ext),
                      f"avg {fmt_secs(v['avg_secs'])}  max {fmt_secs(v['max_secs'])}") for v in vols]
    vmx = max([v["max_secs"] for pg in pgs for v in pg["volumes"].values() if v["max_secs"] is not None] or [0])
    out += cli_section(lambda bw: f"REPLICATION TIME PER VOLUME   ({ch} = average, {ext} = up to maximum;"
                                  f"  1 char = {fmt_secs(vmx / bw)})", rows, W)

    # ---- this period vs the previous period (average per day, like-for-like)
    if period["prev_start"]:
        cmp_rows = report.comparison(period, pg_filter)
        cmax = max([x for r in cmp_rows for x in (r["cur"]["per_day"], (r["prev"] or {}).get("per_day")) if x]
                   or [0])
        rows = []
        for r in cmp_rows:
            c, pv = r["cur"], r["prev"] or {}
            label = r["label"] if r["volume"] is None else "  " + r["label"]
            rows.append((label, "now", lambda bw, x=c["per_day"]: hbar(x, cmax, bw, ch),
                         f"{fmt_bytes(c['per_day'])}/day  {fmt_pct(r['chg']['per_day'])}  "
                         f"(total {fmt_bytes(c['data'])}, {c['n_ok']}/{c['n_fail']} ok/failed)"))
            rows.append(("", "prev", lambda bw, x=pv.get("per_day"): hbar(x, cmax, bw, ext),
                         f"{fmt_bytes(pv.get('per_day'))}/day  (total {fmt_bytes(pv.get('data'))})"))
        if cmax:
            ps, pe = period["prev_start"], period["prev_end"]
            cs_, ce_ = period["start"] or report.first, period["end"] or report.today
            out += cli_section(lambda bw: f"PERIOD COMPARISON - average data replicated per day   "
                                          f"(1 char = {fmt_bytes(cmax / bw)})\n"
                                          f"  {ch} now  = this period {cs_:%Y-%m-%d} .. {ce_:%Y-%m-%d}    "
                                          f"{ext} prev = previous {ps:%Y-%m-%d} .. {pe:%Y-%m-%d}", rows, W)

    # ---- weekly / monthly: bar = average per day (fair for partial weeks / months), numbers = totals
    for kind in summary_kinds(summary, report, period):
        buckets = report.buckets(kind, period, pg_filter)
        if not buckets:
            continue
        unit = "week" if kind == "week" else "month"
        name = "WEEKLY (Mon-Sun)" if kind == "week" else "MONTHLY"
        lab = (lambda bk: f"{iso_week(bk['start'])} {bk['start']:%b %d}") if kind == "week" else \
              (lambda bk: f"{bk['start']:%b %Y}")
        busy = lambda bk: "  (in progress)" if bk["partial"] else ""
        grow = lambda bk, n: next((r for r in bk["rows"] if r["pg"] == n and r["volume"] is None), None)
        cur = lambda bk, n, k: ((grow(bk, n) or {}).get("cur") or {}).get(k)
        names = [pg["name"] for pg in pgs]

        if len(names) > 1:                                          # all groups, stacked
            vals = {id(bk): [cur(bk, n, "per_day") or 0 for n in names] for bk in buckets}
            smax = max([sum(v) for v in vals.values()] or [0])
            rows = []
            for bk in buckets:
                tot = sum(vals[id(bk)])
                prev = _sum(((grow(bk, n) or {}).get("prev") or {}).get("per_day") for n in names)
                rows.append((lab(bk), "", lambda bw, bk=bk: hstack(vals[id(bk)], smax, bw, [pg_char[n] for n in names]),
                             f"{fmt_bytes(tot)}/day  {fmt_pct(pct(tot, prev)):>5}  total "
                             f"{fmt_bytes(_sum(cur(bk, n, 'data') for n in names))}{busy(bk)}"))
            if smax:
                out += cli_section(lambda bw: f"{name} - all protection groups, average per day   "
                                              f"(1 char = {fmt_bytes(smax / bw)};  {legend})", rows, W)

        wmax = max([cur(bk, n, "per_day") or 0 for bk in buckets for n in names] or [0])
        rows = [("text", f"  avg/day, change vs previous {unit}, total, replication time avg/max, ok/failed runs, "
                         f"space on target at the end of the {unit} (estimated growth per day)")]
        for n in names:
            pg = next(pg for pg in pgs if pg["name"] == n)
            rows.append(("text", f"{n} -> {pg['dr_pgroup']}"))
            for bk in buckets:
                r = grow(bk, n)
                if not r:
                    rows.append((lab(bk), "", lambda bw: "", "no runs" + busy(bk)))
                    continue
                c = r["cur"]
                rows.append((lab(bk), "", lambda bw, x=c["per_day"]: hbar(x, wmax, bw, ch),
                             f"{fmt_bytes(c['per_day'])}/day {fmt_pct(r['chg']['per_day']):>5}  "
                             f"tot {fmt_bytes(c['data'])}  {fmt_secs(c['avg_secs'])}/{fmt_secs(c['max_secs'])}  "
                             f"{c['n_ok']}/{c['n_fail']}"
                             + (f"  space {fmt_bytes(c['space'])}" if c.get("space") is not None else "")
                             + (f" ({fmt_signed(c['growth_day'])}/day)" if c.get("growth_day") is not None else "")
                             + busy(bk)))
        if wmax:
            out += cli_section(lambda bw: f"{name} - by protection group, average per day   "
                                          f"(1 {ch} = {fmt_bytes(wmax / bw)})", rows, W)
    return "\n".join(out)


# Per-run CSV columns (one row per DRDATA record; time = run start)
RUN_COLS = ["time", "run", "source_pgroup", "dr_pgroup", "volume", "source_volume", "status",
            "size_bytes", "data_bytes", "written_bytes", "seconds", "time_source",
            "target_pgroup", "target_space_bytes", "target_space_snapshots", "target_space_growth_bytes",
            "target_space_interval_hours", "target_space_est_hourly_bytes", "target_space_est_daily_bytes",
            "target_space_est_weekly_bytes", "error"]
SUM_COLS = ["protection_group", "dr_pgroup", "level", "volume", "days", "data_bytes", "data_gib",
            "avg_per_day_bytes", "avg_per_day_gib", "avg_seconds", "avg_time", "max_seconds", "max_time",
            "ok_runs", "failed_runs", "prev_days", "prev_data_bytes", "prev_data_gib", "prev_avg_per_day_bytes",
            "prev_avg_per_day_gib", "prev_avg_seconds", "prev_max_seconds", "prev_ok_runs", "prev_failed_runs",
            "data_change_pct", "avg_per_day_change_pct", "avg_time_change_pct",
            "target_space_bytes", "target_space_gib", "target_space_snapshots", "target_space_measured",
            "prev_target_space_bytes", "target_space_change_bytes", "target_space_change_pct",
            "target_space_growth_bytes", "target_space_growth_hours", "target_space_est_daily_bytes",
            "target_space_est_weekly_bytes", "prev_target_space_est_daily_bytes", "target_space_est_daily_change_pct"]

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
            "avg_time_change_pct": rnd(d["avg_secs"], 1),
            "target_space_bytes": rnd(c.get("space")), "target_space_gib": gib(c.get("space")),
            "target_space_snapshots": "" if c.get("space_snaps") is None else c["space_snaps"],
            "target_space_measured": c["space_time"].isoformat(sep=" ") if c.get("space_time") else "",
            "prev_target_space_bytes": rnd(p.get("space")),
            "target_space_change_bytes": rnd(c["space"] - p["space"]) if c.get("space") is not None
                                         and p.get("space") is not None else "",
            "target_space_change_pct": rnd(d.get("space"), 1),
            "target_space_growth_bytes": rnd(c.get("growth")), "target_space_growth_hours": rnd(c.get("growth_hours"), 2),
            "target_space_est_daily_bytes": rnd(c.get("growth_day")),
            "target_space_est_weekly_bytes": rnd(c.get("growth_week")),
            "prev_target_space_est_daily_bytes": rnd(p.get("growth_day")),
            "target_space_est_daily_change_pct": rnd(d.get("growth_day"), 1)}

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
                rnd = lambda k, n=0: "" if r.get(k) is None else (round(r[k]) if n == 0 else round(r[k], n))
                w.writerow({**r, "time": r["time"].isoformat(sep=" "),
                            **{k: rnd(k) for k in ("target_space_growth_bytes", "target_space_est_hourly_bytes",
                                                   "target_space_est_daily_bytes", "target_space_est_weekly_bytes")},
                            "target_space_interval_hours": rnd("target_space_interval_hours", 4)})
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

def chart_space(model, fig, pg_filter=None):
    """Target space tab: top = space each protection group's snapshots use on the target, per run;
    middle = estimated growth per day of that space for each snapshot (increment / hours x 24), with the
    period average dashed; bottom = latest total per protection group with its estimated daily / weekly growth."""
    fig.clear()
    pgs = [pg for pg in model.selected(pg_filter) if pg["space_series"]]
    if not pgs:
        return _empty(fig, "No space-on-target measurements in this period\n(recorded by pg_dr_refresh.py "
                           "after monitoring: purepgroup list --snap --space <pgroup> --total)")
    pg_col, _ = palette(model)
    ax1 = fig.add_subplot(311)
    for pg in pgs:
        ax1.plot([t for t, _ in pg["space_series"]], [_gib(b) for _, b in pg["space_series"]], marker="o", ms=3,
                 color=pg_col[pg["name"]], label=f"{pg['name']} -> {pg['target_pgroup'] or pg['dr_pgroup']}")
    ax1.set_ylabel("Total (GiB)")
    ax1.set_title("Space used on the target by each protection group's snapshots (per run)",
                  loc="left", fontweight="bold", fontsize=10)
    ax1.legend(fontsize=8, frameon=False, loc="upper left", bbox_to_anchor=(1.0, 1.0))
    ax1.set_ylim(bottom=0)
    ax1.spines[["top", "right"]].set_visible(False)

    ax2 = fig.add_subplot(312, sharex=ax1)
    for pg in pgs:
        if pg["growth_series"]:
            ax2.plot([t for t, _ in pg["growth_series"]], [(b or 0) / 1024**3 for _, b in pg["growth_series"]],
                     marker="o", ms=3, color=pg_col[pg["name"]], label=f"{pg['name']} per snapshot")
        if pg["growth_day"] is not None:
            ax2.axhline(pg["growth_day"] / 1024**3,
                        ls="--", lw=1, color=pg_col[pg["name"]], alpha=0.7,
                        label=f"{pg['name']} average {fmt_signed(pg['growth_day'])}/day")
    ax2.axhline(0, color="#999", lw=0.6)
    ax2.set_ylabel("GiB per day (est.)")
    ax2.set_title("Estimated space growth on the target per day   (each snapshot: increment / hours since the "
                  "previous run x 24)", loc="left", fontweight="bold", fontsize=10)
    if any(pg["growth_series"] for pg in pgs):
        ax2.legend(fontsize=8, frameon=False, loc="upper left", bbox_to_anchor=(1.0, 1.0))
    else:
        ax2.text(0.5, 0.5, "Needs two measured runs per protection group", ha="center", transform=ax2.transAxes)
    ax2.spines[["top", "right"]].set_visible(False)
    for lbl in ax1.get_xticklabels():
        lbl.set_visible(False)
    for lbl in ax2.get_xticklabels():
        lbl.set_rotation(20); lbl.set_ha("right"); lbl.set_fontsize(8)

    ax3 = fig.add_subplot(313)
    rows = pgs[::-1]
    bars = ax3.barh([pg["name"] for pg in rows], [_gib(pg["space"]) for pg in rows],
                    color=[pg_col[pg["name"]] for pg in rows])
    for b, pg in zip(bars, rows):
        g = pg["growth_last"] or {}
        txt = f"  {fmt_bytes(pg['space'])} ({pg['space']:,.0f} bytes"
        txt += f", {pg['space_snaps']} snapshots)" if pg["space_snaps"] is not None else ")"
        if g:
            txt += (f"   est. {fmt_signed(g['target_space_est_daily_bytes'])}/day, "
                    f"{fmt_signed(g['target_space_est_weekly_bytes'])}/week (last snapshot)")
        if pg["growth_day"] is not None:
            txt += f";  avg {fmt_signed(pg['growth_day'])}/day, {fmt_signed(pg['growth_week'])}/week"
        ax3.text(b.get_width(), b.get_y() + b.get_height() / 2, txt, va="center", fontsize=8)
    ax3.set_xlim(0, max(_gib(pg["space"]) for pg in rows) * 3.2)
    ax3.set_xlabel("GiB")
    last = max(pg["space_time"] for pg in pgs)
    ax3.set_title(f"Latest space on target and estimated growth (as of {last:%Y-%m-%d %H:%M})",
                  loc="left", fontweight="bold", fontsize=10)
    ax3.spines[["top", "right"]].set_visible(False)
    fig.tight_layout()

CHARTS = [("Sizes", chart_sizes), ("Daily change", chart_daily), ("Replication time", chart_repl_time),
          ("Target space", chart_space)]

def export_png(report, period, outdir, pg_filter=None):
    """Save the Sizes, Daily change, Replication time, Target space, Weekly and Monthly charts as PNGs
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
def run_gui(args, files, report, period_key, custom, remote=None):
    """Open the Tk window: period picker, protection-group filter, Overview tree, chart tabs and the
    Weekly / Monthly tab. All tabs follow the selected period and protection group.
    remote = the logged-in RemoteLogs (--remote): Reload and Open logs... then read from that server."""
    import tkinter as tk
    from tkinter import ttk, filedialog, messagebox
    import matplotlib; matplotlib.use("TkAgg")
    from matplotlib.figure import Figure
    from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk

    root = tk.Tk()
    root.title("Pure DR Replication Report" + (f" - {remote.label}" if remote else ""))
    if remote:                              # if Reload must log in again: dialog boxes, not the console
        from tkinter import simpledialog
        remote.ask_password = lambda: simpledialog.askstring(
            "Log in again", f"The connection to {remote.host} was closed.\nPassword for {remote.label}:",
            show="*", parent=root)
        remote.confirm = lambda msg: messagebox.askyesno("Unknown host key", msg + "?", parent=root)
        remote.tell = lambda msg: messagebox.showinfo("Remote logs", msg, parent=root)
    root.geometry("1420x860")
    state = {"files": files, "report": report, "model": None, "period": None}

    # ---- row 1: data info + file actions
    top = ttk.Frame(root, padding=(10, 8, 10, 2)); top.pack(fill="x")
    info = ttk.Label(top, font=("TkDefaultFont", 10)); info.pack(side="left")
    ttk.Button(top, text="Export PNGs...", command=lambda: do_export()).pack(side="right", padx=4)
    ttk.Button(top, text="Export CSV...", command=lambda: do_export_csv()).pack(side="right", padx=4)
    ttk.Button(top, text="Reload", command=lambda: reload()).pack(side="right", padx=4)
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
    cols = ("size", "space", "gday", "gweek", "avg", "pct", "peak", "lastdata", "lasttime", "avgtime", "maxtime",
            "runs", "status")
    heads = ("Size", "Space on target", "Target est./day", "Target est./week", "Avg change/day", "%/day",
             "Peak day", "Last replicated", "Last time", "Avg time", "Max time", "OK / Failed", "Last status")
    tree = ttk.Treeview(ov, columns=cols, show="tree headings")
    tree.heading("#0", text="Protection group / volume"); tree.column("#0", width=240)
    for c, h in zip(cols, heads):
        tree.heading(c, text=h); tree.column(c, width=90, minwidth=60, anchor="e")
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
        is_pg = "volumes" in o                                              # per protection group only
        gl = (o["growth_last"] or {}) if is_pg else {}
        space = fmt_bytes(o["space"]) if is_pg else ""
        gday = fmt_signed(gl.get("target_space_est_daily_bytes")) if is_pg else ""
        gweek = fmt_signed(gl.get("target_space_est_weekly_bytes")) if is_pg else ""
        return (fmt_bytes(o["size"]), space, gday, gweek, fmt_bytes(o["avg_daily"]),
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
        info.config(text=((f"{remote.label}:  " if remote else "") + f"{len(state['files'])} log file(s)   "
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

    def reload(new_files=None):
        """Reload: read the logs again (--remote: search the server again, so a new week's file is
        included). Open logs...: read the files picked."""
        try:
            if remote:
                remote.ensure_connected()
                if new_files is None:
                    new_files, pats = remote.search(remote.patterns)
                    if not new_files:
                        raise RemoteError(f"no log files found in {', '.join(pats)}")
                root.config(cursor="watch"); root.update_idletasks()
                try:
                    records = remote.load(new_files)
                finally:
                    root.config(cursor="")
            else:
                new_files = state["files"] if new_files is None else new_files
                records = load_records(new_files)
        except Cancelled:
            return
        except RemoteError as e:
            messagebox.showerror("Remote logs", f"{e}\n\nStill showing the logs read before.", parent=root)
            return
        state["files"] = new_files
        state["report"] = Report(records, args.as_of)
        refresh()

    def open_logs():
        """Pick log files to load instead of the configured folder (--remote: a path on the server)."""
        if remote:
            from tkinter import simpledialog
            p = simpledialog.askstring("Open logs", f"Log folder, file or glob on {remote.label}:",
                                       initialvalue=" ".join(remote.patterns), parent=root)
            if not p or not p.strip():
                return
            try:
                remote.ensure_connected()
                found, pats = remote.find(shlex.split(p), None)
                if not found:
                    raise RemoteError(f"no log files found in {', '.join(pats)}")
            except Cancelled:
                return
            except (RemoteError, ValueError) as e:
                messagebox.showerror("Remote logs", str(e), parent=root)
                return
            reload(found)
            return
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
    ap.add_argument("--remote", action="store_true",
                    help="read the logs on a Linux server over SSH (SFTP), e.g. from a Windows PC. Asks for "
                         "the hostname, username and password (not shown as you type); no SSH keys needed. "
                         "-l and -c are then paths on that server (default: /var/log/pure/pg_dr_refresh_*.log). "
                         "Needs paramiko: py -m pip install paramiko")
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
    ap.add_argument("--no-gui", action="store_true", help="print a text report (tables) instead of opening a window")
    ap.add_argument("--cli", action="store_true",
                    help="print the report in the terminal only: tables plus bar charts drawn with '#' / '*' "
                         "and the numbers beside each bar (no window, no matplotlib needed)")
    ap.add_argument("--bar-char", default="#", metavar="C",
                    help="character for --cli bars (default '#'; '*' also common). Previous-period and "
                         "up-to-maximum bars use '*' (or '#' when C is '*')")
    ap.add_argument("--width", type=int, metavar="N",
                    help="--cli output width in characters (default: terminal width, 120 when piped)")
    ap.add_argument("--export-png", metavar="DIR", help="save the charts as PNG files to DIR")
    ap.add_argument("--export-csv", metavar="FILE",
                    help="save per-run data to FILE, plus FILE_period.csv, FILE_weekly.csv, FILE_monthly.csv")
    args = ap.parse_args()
    if len(args.bar_char) != 1 or args.bar_char.isspace():
        ap.error("--bar-char must be one visible character, e.g. '#' or '*'")

    chosen = [bool(args.period), bool(args.date_from or args.date_to), bool(args.days)]
    if sum(chosen) > 1:
        ap.error("use only one of --period, --from/--to, --days")
    if args.days is not None and args.days < 1:
        ap.error("--days must be 1 or more")
    period_key = ("custom" if chosen[1] else f"last{args.days}" if args.days else args.period or "all")

    remote = open_remote() if args.remote else None
    try:
        show_report(args, period_key, remote)
    finally:
        if remote:
            remote.close()


def show_report(args, period_key, remote=None):
    """Find and read the logs (on this computer, or on the --remote server), then export / print / open
    the window."""
    if remote:
        try:
            files, pats = remote.find(args.logs, args.config)
            if not files:
                raise RemoteError(f"no log files found on {remote.label} in: {', '.join(pats)}. "
                                  f"Give the log folder on the server with -l, e.g. -l /var/log/pure")
            print(f"Reading {len(files)} log file(s) from {remote.label}...", file=sys.stderr)
            records = remote.load(files)
        except RemoteError as e:
            sys.exit(f"--remote: {e}")
    else:
        files, pats = find_logs(args.logs, args.config)
        if not files:
            sys.exit("No log files found in: " + ", ".join(pats))
        records = load_records(files)
    report = Report(records, args.as_of)
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
    if args.cli:
        print(cli_report(report, period, args.pgroup, args.summary, args.width, args.bar_char))
        return
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
        run_gui(args, files, report, period_key, (args.date_from, args.date_to), remote)
    except tkinter.TclError as e:                                  # e.g. display refused the connection
        print(f"Cannot open the window ({e}). Showing text report instead.\n")
        print(text_report(report, period, args.pgroup, args.summary))

if __name__ == "__main__":
    try:
        main()
        sys.stdout.flush()               # flush here so a closed pipe is caught below
    except BrokenPipeError:              # output piped to head / less that closed early - exit quietly
        os.dup2(os.open(os.devnull, os.O_WRONLY), sys.stdout.fileno())
        sys.exit(0)
