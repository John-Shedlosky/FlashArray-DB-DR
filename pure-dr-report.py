#!/usr/bin/env python3
"""
pure-dr-report.py - Replication and space report for the DR protection groups, read directly from the
replication TARGET array (for example flasharray2).

Where the data comes from
-------------------------
Everything the report needs is on the target array, so the script logs in there over SSH (the
target_array settings of pg_dr_config.json) and runs:

  1. purepgroup list --snap --transfer --csv
       One row per protection group snapshot on the target, with the columns
       Name, Source, Created, Started, Completed, Progress, Data Transferred, Physical Bytes Written.
  2. purepgroup list --snap --space --total --csv <source>:<DR pgroup>      (once per protection group)
       One row per snapshot of that group with the space it uses on the target, then the (total) row:
       the space all of the group's snapshots use together, including the space they share.

The transfer rows are grouped by their Source column (for example flasharray1:PG1-DR) and matched to the
protection groups in the config (dr_pgroup, or source_pgroup). Each snapshot's transfer row is then
matched to its space row by snapshot name.

What it shows
-------------
* Protection groups: per group, the replication time (last / median / maximum), data transferred and
  physical bytes written per day, the reduction on the target, and the space its snapshots use now.
* Retention estimate: the space each group would need on the target for longer retention periods.
* Per group: every snapshot on the target, with created time, replication time, progress, data
  transferred, physical bytes written, reduction, throughput and the snapshot's own space.
Sizes are shown in binary units (1 GiB = 1024^3 bytes) instead of bytes.

How the numbers are calculated
------------------------------
* Repl time = Completed - Started. Snapshots that haven't completed show 'running' (with their
  progress) or 'queued', and aren't used in averages or per-day rates.
* Throughput = Data Transferred / Repl time.
* Reduction = Data Transferred / Physical Bytes Written. Data Transferred is the change sent to the
  target. The target reduces it further, mostly by deduplication against data it already holds, and
  Physical Bytes Written is what it actually wrote: the real change rate on the target.
* Per day = the sum for the completed snapshots after the oldest one / the days from the oldest to the
  newest completed snapshot. Each snapshot's transfer is the change since the snapshot replicated before
  it. The oldest snapshot's transfer covers time before that window, or it was the first full copy,
  so it's left out of the rates.
* Snap space = the snapshot's own row in the --space listing: the space that only that snapshot holds
  (what deleting it would free). Space that snapshots share isn't charged to any one of them, so the rows
  add up to less than the group's (total). The report shows the difference as "shared".
* Space now = the group's (total) row. If the array doesn't return a (total) row, the sum of the
  snapshot rows is used and marked as a minimum ("at least").
* Retention estimate for N days = space now + physical bytes written per day x (N - the days the current
  snapshots span). It assumes the change rate stays the same. Snaps = N days / the usual time between
  snapshots.

Usage
-----
  pure-dr-report.py                               # uses pg_dr_config.json next to this script
  pure-dr-report.py -c /opt/pure-dr/pg_dr_config.json
  pure-dr-report.py --pgroup PG1                  # only this protection group (PG1, PG1-DR or flasharray1:PG1-DR)
  pure-dr-report.py --retention 30,13w,6m,1y      # retention periods to estimate (days, or d / w / m / y)
  pure-dr-report.py --csv ~/reports               # also write the report as CSV files (snapshots + summary)
  pure-dr-report.py --save ~/reports/2026-11-06   # also save the array's raw output in that folder ...
  pure-dr-report.py --input ~/reports/2026-11-06  # ... and report from it later, without the array
  pure-dr-report.py --password                    # password login instead of an SSH key (needs paramiko)
  pure-dr-report.py --all                         # also replicated groups on the target that aren't in the config
  pure-dr-report.py --host flasharray2.example.com --user jjones   # without a config: every replicated group
  pure-dr-report.py -V                            # show every command and the array's output (stderr)

Requirements
------------
Python 3.8+ and nothing else, plus the OpenSSH client ('ssh', included in Windows 10 and 11, macOS and
Linux). SSH key login to the target array is tried first, as pg_dr_refresh.py uses. If the array
refuses it (for example on a Windows computer without the key), the script asks for the password
instead - this needs paramiko (py -m pip install paramiko). --password asks for it right away, and
with no config or --host the script asks for the array's name too. The array user only needs to list
protection group snapshots.

Exit code: 0 = report written, 1 = the array or the input couldn't be read or nothing matched,
2 = usage or config error.
"""
import argparse, base64, csv, datetime, getpass, hashlib, io, json, os, re, shlex, statistics, subprocess, sys
from dataclasses import dataclass, field
from pathlib import Path

if sys.version_info < (3, 8):
    sys.exit("pure-dr-report.py needs Python 3.8 or newer (found %d.%d)" % sys.version_info[:2])

SCRIPT_DIR = Path(__file__).resolve().parent
DEFAULT_CONFIG = SCRIPT_DIR / "pg_dr_config.json"
KNOWN_HOSTS = Path("~/.ssh/known_hosts").expanduser()

TRANSFER_CMD = "purepgroup list --snap --transfer --csv"
SPACE_CMD = "purepgroup list --snap --space --total --csv {pgroup}"     # once per protection group
SPACE_ALL_CMD = "purepgroup list --snap --space --csv"                   # fallback: rows only, no (total)
TRANSFER_FILE, SPACE_FILE, CAPTURE_FILE = "transfer.csv", "space.csv", "capture.json"
DEFAULT_RETENTION = "7,30,90,365"
SHORT_SPAN_HOURS = 1.0     # per-day rates from snapshots this close together are flagged as rough


class ReportError(Exception):
    """A problem to report to the user (exit code 1)."""


class UsageError(Exception):
    """A config or command-line problem (exit code 2)."""


def warn(msg):
    print(f"Warning: {msg}", file=sys.stderr)


# ============================================================================ sizes, times, formatting
UNITS = {"": 1, "K": 1024, "M": 1024**2, "G": 1024**3, "T": 1024**4, "P": 1024**5, "E": 1024**6}

def parse_size(s):
    """Size from the array -> bytes: '6657199308' -> 6657199308.0. Purity units are accepted too
    ('1.50G' -> 1610612736.0). '-', '' or anything else -> None."""
    m = re.match(r"^\s*([\d.]+(?:[eE][+-]?\d+)?)\s*([KMGTPE]?)(?:i?B)?\s*$", str(s or ""), re.I)
    if not m:
        return None
    try:
        return float(m.group(1)) * UNITS[m.group(2).upper()]
    except ValueError:
        return None


def parse_progress(s):
    """Progress column -> percent: '100.00%' -> 100.0; a bare fraction '0.5' -> 50.0; '-' -> None."""
    t = str(s or "").strip()
    if not t or t == "-":
        return None
    try:
        if t.endswith("%"):
            return float(t[:-1])
        v = float(t)
        return v * 100 if v <= 1 else v
    except ValueError:
        return None


# Timezone abbreviations the array may print. Times are only subtracted from each other, so the zone
# matters only when it changes between two times (daylight saving).
TZ_HOURS = {"UTC": 0, "GMT": 0, "Z": 0, "EST": -5, "EDT": -4, "CST": -6, "CDT": -5, "MST": -7, "MDT": -6,
            "PST": -8, "PDT": -7, "AKST": -9, "AKDT": -8, "HST": -10, "WET": 0, "WEST": 1, "BST": 1,
            "CET": 1, "CEST": 2, "EET": 2, "EEST": 3, "SGT": 8, "HKT": 8, "JST": 9, "KST": 9,
            "AWST": 8, "ACST": 9.5, "ACDT": 10.5, "AEST": 10, "AEDT": 11, "NZST": 12, "NZDT": 13}

def parse_time(s):
    """Array time -> (datetime for arithmetic, time as shown, timezone text).
    '2026-11-05 02:00:12 EST' -> (2026-11-05 07:00:12, '2026-11-05 02:00:12', 'EST'): known zone
    abbreviations and +hh:mm offsets are converted to UTC, so intervals across a daylight-saving change
    are right. ISO 8601 and epoch seconds / milliseconds are accepted too. '-' or '' -> (None, '', '')."""
    t = str(s or "").strip().strip('"')
    if not t or t == "-":
        return None, "", ""
    if re.fullmatch(r"\d{9,13}(\.\d+)?", t):
        v = float(t)
        v = v / 1000 if v > 1e11 else v
        dt = datetime.datetime.fromtimestamp(v, datetime.timezone.utc).replace(tzinfo=None)
        return dt, dt.strftime("%Y-%m-%d %H:%M:%S"), "UTC"
    m = re.fullmatch(r"(\d{4}-\d{2}-\d{2})[ T](\d{1,2}:\d{2}(?::\d{2})?)(?:\.\d+)?\s*(.*)", t)
    if not m:
        return None, t, ""
    date, clock, zone = m.group(1), m.group(2), m.group(3).strip()
    try:
        dt = datetime.datetime.strptime(f"{date} {clock}", "%Y-%m-%d %H:%M:%S" if clock.count(":") == 2
                                        else "%Y-%m-%d %H:%M")
    except ValueError:
        return None, t, ""
    off = None
    zm = re.fullmatch(r"(?:UTC|GMT)?\s*([+-])(\d{1,2}):?(\d{2})?", zone, re.I)
    if zm:
        off = (1 if zm.group(1) == "+" else -1) * (int(zm.group(2)) + int(zm.group(3) or 0) / 60)
    elif zone:
        off = TZ_HOURS.get(zone.upper())
    if off is not None:
        dt -= datetime.timedelta(hours=off)
    return dt, f"{date} {clock}", zone


def fmt_bytes(n):
    """1610612736 -> '1.50 GiB'; None -> '-'."""
    if n is None:
        return "-"
    sign, n = ("-" if n < 0 else ""), abs(n)
    for u in ("B", "KiB", "MiB", "GiB", "TiB", "PiB", "EiB"):
        if n < 1024 or u == "EiB":
            return sign + (f"{n:.0f} {u}" if u == "B" else f"{n:.2f} {u}")
        n /= 1024


def fmt_secs(sec):
    """Seconds -> 'H:MM:SS' (with days if needed); None -> '-'."""
    if sec is None:
        return "-"
    sec = int(round(sec))
    d, rem = divmod(sec, 86400)
    return (f"{d}d " if d else "") + f"{rem // 3600}:{rem % 3600 // 60:02d}:{rem % 60:02d}"


def fmt_days(days):
    """Span or interval in days -> '12 min', '6.0 h' or '3.5 d'; None -> '-'."""
    if days is None:
        return "-"
    hours = days * 24
    if hours < 1:
        return f"{hours * 60:.0f} min"
    return f"{hours:.1f} h" if hours < 48 else f"{days:.1f} d"


def fmt_rate(bytes_per_s):
    return "-" if bytes_per_s is None else f"{bytes_per_s / 1024**2:.1f} MiB/s"


def fmt_ratio(r):
    return "-" if r is None else f"{r:.1f} to 1"


def fmt_pct(p):
    return "-" if p is None else f"{p:.2f}%"


def total(values):
    """Sum of the values that aren't None; None if all are None."""
    vals = [v for v in values if v is not None]
    return sum(vals) if vals else None


# ============================================================================ config
def strip_comments(text):
    """Remove // and /* */ comments that are outside JSON strings (the config allows comments)."""
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
            out.append("\n" * text.count("\n", i, end if end != -1 else n))
            i = n if end == -1 else end + 2
            continue
        else:
            out.append(c)
        i += 1
    return "".join(out)


@dataclass
class Config:
    path: object = None
    host: str = None              # target array SSH address
    name: str = None              # target array Purity name, if set
    user: str = None
    key: str = None
    source_name: str = None       # source array Purity name, if set (else any source array matches)
    timeout: int = 600
    pgroups: list = field(default_factory=list)     # [(source_pgroup, dr_pgroup), ...] in config order


def load_config(path):
    """The target array login and the protection groups from a pg_dr_refresh JSON config."""
    try:
        text = strip_comments(Path(path).read_text(encoding="utf-8"))
    except OSError as e:
        raise UsageError(f"can't read the config {path}: {e}")
    try:
        cfg = json.loads(text)
    except json.JSONDecodeError as e:
        raise UsageError(f"config {path}: invalid JSON at line {e.lineno} column {e.colno}: {e.msg}")
    src, tgt = cfg.get("source_array") or {}, cfg.get("target_array") or {}
    c = Config(path=Path(path), host=tgt.get("host"), name=tgt.get("name"),
               user=tgt.get("ssh_user", src.get("ssh_user")),      # same fallbacks as pg_dr_refresh.py
               key=tgt.get("ssh_key", src.get("ssh_key")), source_name=src.get("name"))
    t = (cfg.get("settings") or {}).get("ssh_timeout_seconds")
    if isinstance(t, (int, float)) and not isinstance(t, bool) and t >= 10:
        c.timeout = int(t)
    for i, pg in enumerate(cfg.get("protection_groups") or [], 1):
        if not isinstance(pg, dict) or not (pg.get("dr_pgroup") or pg.get("source_pgroup")):
            raise UsageError(f"config {path}: protection_groups entry {i} has no dr_pgroup or source_pgroup")
        c.pgroups.append((pg.get("source_pgroup"), pg.get("dr_pgroup")))
    return c


# ============================================================================ reading the target array
class ArrayError(Exception):
    """A command couldn't be run on the array, or returned an error."""


class LoginError(ArrayError):
    """SSH key login was refused (a password login may still work)."""


class OpenSSHArray:
    """The target array through the system 'ssh' client with key login (BatchMode), as pg_dr_refresh.py
    logs in. Each command is one SSH session."""

    def __init__(self, host, user, key, timeout, verbose):
        self.host, self.user, self.timeout, self.verbose = host, user, timeout, verbose
        self.key = None
        if key:
            path = Path(key).expanduser()
            if path.exists():
                self.key = str(path)
            else:
                print(f"SSH key {path} isn't on this computer: trying ssh's default keys", file=sys.stderr)

    @property
    def label(self):
        return f"{self.user}@{self.host}" if self.user else self.host

    def run(self, cmd):
        argv = ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15"]
        if self.key:
            argv += ["-i", str(Path(self.key).expanduser())]
        argv += [self.label, cmd]
        if self.verbose:
            print(f"$ ssh {self.label} {cmd}", file=sys.stderr)
        try:
            r = subprocess.run(argv, capture_output=True, text=True, encoding="utf-8", errors="replace",
                               timeout=self.timeout, stdin=subprocess.DEVNULL)
        except subprocess.TimeoutExpired:
            raise ArrayError(f"'{cmd}' didn't finish within {self.timeout}s")
        except OSError as e:
            raise ArrayError(f"can't run ssh ({e}). Install the OpenSSH client, or use --password (paramiko)")
        if self.verbose:
            print(r.stdout.rstrip() + ("\n" + r.stderr.rstrip() if r.stderr.strip() else ""), file=sys.stderr)
        if r.returncode == 255:
            err = r.stderr.strip() or "ssh failed"
            hint = (f"The array's host key isn't known yet: log in once with 'ssh {self.label}' and accept it"
                    if "host key verification failed" in err.lower() else
                    "Check target_array.ssh_user / ssh_key in the config, or use --password")
            if re.search(r"permission denied|no more authentication methods|too many authentication failures|"
                         r"no supported authentication methods", err, re.I):
                raise LoginError(f"SSH key login to {self.label} was refused ({err}). Check target_array.ssh_user / "
                                 f"ssh_key in the config, or log in with a password: --password (needs paramiko)")
            raise ArrayError(f"can't log in to {self.label} with SSH: {err}. {hint}")
        if r.returncode != 0:
            raise ArrayError(f"'{cmd}' failed on {self.host}: {(r.stderr or r.stdout).strip() or f'exit code {r.returncode}'}")
        return r.stdout

    def close(self):
        pass


def ask_yes_no(question):
    while True:
        sys.stderr.write(question + " (yes/no): ")
        sys.stderr.flush()
        a = input().strip().lower()
        if a in ("yes", "y", "no", "n"):
            return a.startswith("y")


class PasswordArray:
    """The target array through paramiko with a password login (--password). One connection is used for
    every command. The password is asked for in the terminal and isn't stored. Host keys are checked
    against ~/.ssh/known_hosts; an unknown key is shown for you to accept, then saved there."""

    def __init__(self, host, user, timeout, verbose):
        try:
            import paramiko
        except ImportError:
            raise ArrayError("--password needs the paramiko package: py -m pip install paramiko (Windows) or "
                             "python3 -m pip install paramiko") from None
        import logging
        logging.getLogger("paramiko").setLevel(logging.CRITICAL)
        if not sys.stdin.isatty():
            raise ArrayError("--password asks for the password, so run the script in a terminal "
                             "(PowerShell, Command Prompt or a shell), not IDLE or pythonw")
        self.pm, self.host, self.user, self.timeout, self.verbose = paramiko, host, user, timeout, verbose
        self.client = paramiko.SSHClient()
        try:
            self.client.load_system_host_keys()
        except Exception as e:
            warn(f"couldn't read {KNOWN_HOSTS} ({e})")
        self.client.set_missing_host_key_policy(self._ask_policy())
        for attempt in range(1, 4):
            pw = getpass.getpass(f"Password for {user}@{host}: ", stream=sys.stderr)
            try:
                self.client.connect(host, username=user, password=pw, allow_agent=False, look_for_keys=False,
                                    timeout=20, banner_timeout=30, auth_timeout=30)
                return
            except paramiko.AuthenticationException:
                print(f"Wrong username or password for {user}@{host}" + (" - try again" if attempt < 3 else ""),
                      file=sys.stderr)
            except paramiko.BadHostKeyException:
                raise ArrayError(f"the host key of {host} doesn't match the one in {KNOWN_HOSTS}. If the array "
                                 f"was replaced or re-keyed, remove its line with: ssh-keygen -R {host}") from None
            except (paramiko.SSHException, OSError) as e:
                raise ArrayError(f"can't connect to {host}: {e}") from None
        raise ArrayError(f"login to {user}@{host} failed 3 times")

    def _ask_policy(self):
        pm = self.pm

        class Ask(pm.MissingHostKeyPolicy):
            def missing_host_key(self, client, hostname, key):
                fp = "SHA256:" + base64.b64encode(hashlib.sha256(key.asbytes()).digest()).decode().rstrip("=")
                print(f"The authenticity of host '{hostname}' can't be established.\n"
                      f"{key.get_name()} key fingerprint is {fp}.", file=sys.stderr)
                if not ask_yes_no("Are you sure you want to continue connecting"):
                    raise pm.SSHException("host key not accepted")
                client.get_host_keys().add(hostname, key.get_name(), key)
                try:
                    KNOWN_HOSTS.parent.mkdir(parents=True, exist_ok=True)
                    with open(KNOWN_HOSTS, "a", encoding="ascii", newline="\n") as f:
                        f.write(f"{hostname} {key.get_name()} {key.get_base64()}\n")
                    print(f"Added {hostname} to {KNOWN_HOSTS}", file=sys.stderr)
                except OSError as e:
                    warn(f"couldn't save the host key to {KNOWN_HOSTS} ({e})")
        return Ask()

    def run(self, cmd):
        if self.verbose:
            print(f"$ [{self.user}@{self.host}] {cmd}", file=sys.stderr)
        try:
            _, stdout, stderr = self.client.exec_command(cmd, timeout=self.timeout)
            out = stdout.read().decode("utf-8", "replace")
            err = stderr.read().decode("utf-8", "replace")
            rc = stdout.channel.recv_exit_status()
        except Exception as e:
            raise ArrayError(f"'{cmd}' failed on {self.host}: {e}") from None
        if self.verbose:
            print(out.rstrip() + ("\n" + err.rstrip() if err.strip() else ""), file=sys.stderr)
        if rc != 0:
            raise ArrayError(f"'{cmd}' failed on {self.host}: {(err or out).strip() or f'exit code {rc}'}")
        return out

    def close(self):
        self.client.close()


# ============================================================================ parsing the CSV output
def csv_rows(text):
    """Non-empty CSV rows of a command's output, cells stripped of spaces and quotes."""
    rows = []
    for r in csv.reader(io.StringIO(text or "")):
        cells = [c.strip().strip('"') for c in r]
        if any(cells):
            rows.append(cells)
    return rows


def norm(header):
    """'Physical Bytes Written' / 'physical_bytes_written' -> 'physical bytes written'."""
    return re.sub(r"[\s_]+", " ", header.strip().lower())


TRANSFER_ORDER = ["name", "source", "created", "started", "completed", "progress", "transferred", "written"]

def transfer_columns(header):
    """Column index of each field, found by header name (works with --csv and --raw titles)."""
    idx = {}
    for i, h in enumerate(norm(c) for c in header):
        if h in ("name", "source", "created", "started", "completed", "progress"):
            idx.setdefault(h, i)
        elif "logical" in h:
            idx.setdefault("logical", i)          # older Purity: 'Logical Data Transferred' (not used)
        elif "written" in h or "physical" in h:
            idx.setdefault("written", i)
        elif "transferred" in h:
            idx.setdefault("transferred", i)
    return idx


def parse_transfer(text):
    """`purepgroup list --snap --transfer --csv` -> list of {field: text}. Without a title row
    (--notitle), the documented column order is assumed."""
    out, idx = [], None
    for r in csv_rows(text):
        if norm(r[0]) == "name":
            idx = transfer_columns(r)
            continue
        if idx is None:
            idx = {k: i for i, k in enumerate(TRANSFER_ORDER)}
        if "name" not in idx:
            raise ReportError("the transfer listing has no Name column")
        get = lambda k: r[idx[k]] if k in idx and idx[k] < len(r) else ""
        if get("name") and get("name").lower() != "(total)":
            out.append({k: get(k) for k in TRANSFER_ORDER})
    return out


def parse_space(text):
    """`purepgroup list --snap --space [--total] --csv [pgroup]` output -> list of blocks, one per
    title row (several outputs can be concatenated, as --save stores them):
        {"rows": {snapshot name: bytes}, "total": bytes of the (total) row or None,
         "column": title of the column used, "columns": every size column, "method": Method column value}
    The space column is the one titled Snapshots, else Total, else the last column with sizes."""
    blocks, cur = [], None
    for r in csv_rows(text):
        if norm(r[0]) == "name":
            cur = {"header": r, "raw": []}
            blocks.append(cur)
        elif cur is None:                                    # no title row
            cur = {"header": ["Name", "Snapshots"], "raw": [r]}
            blocks.append(cur)
        else:
            cur["raw"].append(r)
    result = []
    for b in blocks:
        hdr = [norm(h) for h in b["header"]]
        method_i = next((i for i, h in enumerate(hdr) if h == "method"), None)
        sized = [i for i in range(1, len(hdr)) if i != method_i
                 and any(i < len(r) and parse_size(r[i]) is not None for r in b["raw"])]
        pick = (next((i for i in sized if "snapshot" in hdr[i]), None)
                or next((i for i in sized if "total" in hdr[i]), None)
                or (sized[-1] if sized else None))
        blk = {"rows": {}, "total": None, "column": b["header"][pick] if pick is not None else None,
               "columns": [b["header"][i] for i in sized], "method": None}
        for r in b["raw"]:
            v = parse_size(r[pick]) if pick is not None and pick < len(r) else None
            if r[0].lower() == "(total)":
                blk["total"] = v
            else:
                blk["rows"][r[0]] = v
            if method_i is not None and method_i < len(r) and r[method_i] and r[0].lower() != "(total)":
                blk["method"] = r[method_i]
        result.append(blk)
    return result


# ============================================================================ the model
@dataclass
class Snap:
    name: str
    suffix: str
    created: object = None
    created_txt: str = ""
    started: object = None
    started_txt: str = ""
    completed: object = None
    completed_txt: str = ""
    progress: object = None
    transferred: object = None
    written: object = None
    space: object = None
    status: str = ""            # complete | running | queued | no transfer row
    repl_s: object = None       # Completed - Started
    lag_s: object = None        # Completed - Created (snapshot taken -> safe on the target)
    interval_s: object = None   # Created - the previous snapshot's Created
    rate: object = None         # bytes/s
    reduction: object = None
    note: str = ""


@dataclass
class Group:
    name: str                   # name on the target, e.g. flasharray1:PG1-DR
    source_pgroup: str = None
    dr_pgroup: str = None
    order: int = 10**6          # position in the config; unmatched groups (--all) come last
    snaps: list = field(default_factory=list)
    space_rows: dict = field(default_factory=dict)
    total_space: object = None
    space_column: str = None
    space_method: str = None

    @property
    def label(self):
        if self.source_pgroup and self.dr_pgroup and self.source_pgroup != self.dr_pgroup:
            return f"{self.source_pgroup} -> {self.dr_pgroup}"
        return self.dr_pgroup or self.source_pgroup or self.name

    @property
    def short(self):
        """The pgroup's own name: 'flasharray1:PG1-DR' -> 'PG1-DR'."""
        return self.name.rsplit(":", 1)[-1]

    @property
    def array(self):
        """The source array part: 'flasharray1:PG1-DR' -> 'flasharray1' (None for a local group)."""
        return self.name.rsplit(":", 1)[0].rstrip(":") if ":" in self.name else None


def make_snap(row, source, zones):
    name = row["name"]
    s = Snap(name=name, suffix=name[len(source) + 1:] if name.startswith(source + ".") else name)
    for f in ("created", "started", "completed"):
        dt, shown, zone = parse_time(row[f])
        setattr(s, f, dt)
        setattr(s, f + "_txt", shown)
        if zone:
            zones.add(zone)
    s.progress = parse_progress(row["progress"])
    s.transferred, s.written = parse_size(row["transferred"]), parse_size(row["written"])
    if s.completed is not None or (s.progress is not None and s.progress >= 100 and s.started is not None):
        s.status = "complete"
        if s.progress is None:
            s.progress = 100.0
    elif s.started is not None:
        s.status = "running"
    else:
        s.status = "queued"
    if s.started is not None and s.completed is not None:
        s.repl_s = max(0.0, (s.completed - s.started).total_seconds())
    if s.created is not None and s.completed is not None:
        s.lag_s = max(0.0, (s.completed - s.created).total_seconds())
    if s.repl_s and s.transferred is not None:
        s.rate = s.transferred / s.repl_s
    if s.transferred is not None and s.written:
        s.reduction = s.transferred / s.written
    return s


def build_groups(rows, cfg, include_all, only):
    """Group the transfer rows by Source and match them to the config's protection groups.
    Returns (groups to report, config groups not found, replicated groups not in the config)."""
    zones, groups = set(), {}
    for r in rows:
        src = r["source"] or r["name"].rsplit(".", 1)[0]
        g = groups.setdefault(src, Group(src))
        g.snaps.append(make_snap(r, src, zones))
    missing = []
    for i, (spg, dpg) in enumerate(cfg.pgroups if cfg else []):
        found = False
        for want in (dpg, spg):                   # the DR group is the one replicated; else the source group
            for g in groups.values():
                if (want and g.short == want and g.order == 10**6
                        and (not cfg.source_name or g.array == cfg.source_name)):
                    g.source_pgroup, g.dr_pgroup, g.order, found = spg, dpg, i, True
            if found:
                break
        if not found:
            missing.append((spg, dpg))
    replicated = lambda g: any(s.started or s.completed or s.transferred is not None for s in g.snaps)
    extra = sorted(g.name for g in groups.values() if g.order == 10**6 and replicated(g))
    keep = [g for g in groups.values()
            if g.order < 10**6 or ((include_all or not cfg or not cfg.pgroups) and replicated(g))]
    if only:
        wanted = {w.lower() for w in only}
        keep = [g for g in keep if {g.name.lower(), g.short.lower(), (g.source_pgroup or "").lower(),
                                    (g.dr_pgroup or "").lower()} & wanted]
        missing = [m for m in missing if {(m[0] or "").lower(), (m[1] or "").lower()} & wanted]
    keep.sort(key=lambda g: (g.order, g.name))
    return keep, missing, extra, zones


def attach_space(groups, blocks):
    """Put each space row on its snapshot (by name) and each block's (total) on its group. A (total) is
    used only when every row of its block belongs to one group (per-group listing)."""
    by_name = {g.name: g for g in groups}
    notes = []
    for b in blocks:
        owners = set()
        for snap, v in b["rows"].items():
            g = by_name.get(snap.rsplit(".", 1)[0])
            if g:
                g.space_rows.setdefault(snap, v)
                owners.add(g.name)
                if b["method"]:
                    g.space_method = b["method"]
        if b["total"] is not None and len(owners) == 1:
            g = by_name[owners.pop()]
            if g.total_space is None:
                g.total_space, g.space_column = b["total"], b["column"]
        if len(b["columns"]) > 1:
            notes.append(f"the space listing has several size columns ({', '.join(b['columns'])}); "
                         f"'{b['column']}' was used")
    for g in groups:
        names = {s.name for s in g.snaps}
        for s in g.snaps:
            s.space = g.space_rows.get(s.name)
        for snap in sorted(set(g.space_rows) - names):          # in the space list only
            g.snaps.append(Snap(name=snap, suffix=snap[len(g.name) + 1:], space=g.space_rows[snap],
                                status="no transfer row"))
    return sorted(set(notes))


def analyse(g, retention):
    """Order the snapshots, add intervals and notes, and return the group's summary figures."""
    g.snaps.sort(key=lambda s: (s.created is None, s.created or datetime.datetime.min, s.name))
    prev = None
    for s in g.snaps:
        if s.created is not None:
            if prev is not None:
                s.interval_s = (s.created - prev.created).total_seconds()
            prev = s
    done = [s for s in g.snaps if s.status == "complete"]
    window = [s for s in done if s.created is not None]
    st = {"count": len(g.snaps), "complete": len(done),
          "running": sum(s.status == "running" for s in g.snaps),
          "queued": sum(s.status == "queued" for s in g.snaps),
          "oldest": window[0] if window else None, "newest": window[-1] if window else None}
    span = (window[-1].created - window[0].created).total_seconds() / 86400 if len(window) > 1 else None
    gaps = [(b.created - a.created).total_seconds() / 86400 for a, b in zip(window, window[1:])]
    st["span_days"] = span
    st["interval_days"] = statistics.median(gaps) if gaps else None
    st["short_span"] = span is not None and span * 24 < SHORT_SPAN_HOURS
    times = [s for s in done if s.repl_s is not None]
    st["repl_last"] = window[-1].repl_s if window else None
    st["repl_median"] = statistics.median(s.repl_s for s in times) if times else None
    st["repl_avg"] = statistics.mean(s.repl_s for s in times) if times else None
    mx = max(times, key=lambda s: s.repl_s, default=None)
    st["repl_max"], st["repl_max_snap"] = (mx.repl_s, mx.suffix) if mx else (None, None)
    pairs = [(s.transferred, s.repl_s) for s in done if s.transferred is not None and s.repl_s]
    st["throughput"] = sum(a for a, _ in pairs) / sum(b for _, b in pairs) if pairs else None
    st["xfer_total"] = total(s.transferred for s in done)
    st["written_total"] = total(s.written for s in done)
    after = window[1:]
    st["rate_snaps"] = len(after)
    xa, wa = total(s.transferred for s in after), total(s.written for s in after)
    st["xfer_day"] = xa / span if span and xa is not None else None
    st["written_day"] = wa / span if span and wa is not None else None
    red = [(s.transferred, s.written) for s in done if s.transferred is not None and s.written]
    st["reduction"] = sum(a for a, _ in red) / sum(b for _, b in red) if red else None
    rows_sum = total(s.space for s in g.snaps)
    st["space_rows"] = rows_sum
    st["space_from_total"] = g.total_space is not None
    st["space_now"] = g.total_space if g.total_space is not None else rows_sum
    st["space_shared"] = (g.total_space - rows_sum) if g.total_space is not None and rows_sum is not None else None
    if st["written_day"] is not None:
        st["rate"], st["rate_source"] = st["written_day"], "physical bytes written"
    elif st["xfer_day"] is not None:
        st["rate"], st["rate_source"] = st["xfer_day"], "data transferred (no Physical Bytes Written column)"
    else:
        st["rate"], st["rate_source"] = None, None
    st["projections"] = []
    if st["rate"] is not None and st["space_now"] is not None and span:
        for label, days in retention:
            snaps = days / st["interval_days"] if st["interval_days"] else None
            st["projections"].append((label, days, max(0.0, st["space_now"] + st["rate"] * (days - span)), snaps))
    # notes shown in the snapshot table
    if len(window) > 1:
        window[0].note = "oldest"
    elif window:
        window[0].note = "only one"
    for s in g.snaps:
        extra = []
        if s.status in ("running", "queued", "no transfer row"):
            extra.append(s.status)
        if s.space is None and g.space_rows and s.status != "no transfer row":
            extra.append("no space row")
        s.note = ", ".join(([s.note] if s.note else []) + extra)
    return st


# ============================================================================ text report
def table(headers, rows, right=(), footer=None):
    """Plain-text table: titles, a dashed line, the rows (and an optional total row under a line)."""
    allrows = rows + ([footer] if footer else [])
    w = [max([len(h)] + [len(r[i]) for r in allrows]) for i, h in enumerate(headers)]
    line = lambda cells: "  ".join(c.rjust(w[i]) if i in right else c.ljust(w[i])
                                   for i, c in enumerate(cells)).rstrip()
    out = [line(headers), line(["-" * x for x in w])] + [line(r) for r in rows]
    if footer:
        out += [line(["-" * x for x in w]), line(footer)]
    return out


def space_text(st):
    if st["space_now"] is None:
        return "-"
    return ("" if st["space_from_total"] else "at least ") + fmt_bytes(st["space_now"])


def text_report(ctx, groups, stats, missing, extra, notes, retention):
    T = ctx["target"]
    L = [f"DR replication report - target array {T}" + (f" ({ctx['host']})" if ctx.get("host") and ctx["host"] != T else ""),
         f"Read {ctx['read_at']}" + (f" from the files in {ctx['input']}" if ctx.get("input") else "") + " - output of:"]
    L += [f"  {c}" for c in ctx["commands"]]
    zones = "/".join(sorted(ctx["zones"])) if ctx["zones"] else "as listed"
    L += [f"Sizes in binary units (1 GiB = 1024^3 bytes). Times as the array shows them ({zones}).", ""]

    # ---- overview
    L.append("PROTECTION GROUPS")
    rows = []
    for g in groups:
        st = stats[g.name]
        rows.append([g.label, g.name, str(st["count"]), fmt_secs(st["repl_last"]), fmt_secs(st["repl_median"]),
                     fmt_secs(st["repl_max"]), fmt_bytes(st["xfer_day"]), fmt_bytes(st["written_day"]),
                     fmt_ratio(st["reduction"]), space_text(st)])
    foot = None
    if len(groups) > 1:
        sums = lambda k: total(stats[g.name][k] for g in groups)
        allx = total(stats[g.name]["xfer_total"] for g in groups)
        allw = total(stats[g.name]["written_total"] for g in groups)
        sp = sums("space_now")
        foot = ["All groups", "", str(sum(stats[g.name]["count"] for g in groups)), "", "", "",
                fmt_bytes(sums("xfer_day")), fmt_bytes(sums("written_day")),
                fmt_ratio(allx / allw if allx is not None and allw else None),
                ("" if all(stats[g.name]["space_from_total"] for g in groups) else "at least ") + fmt_bytes(sp)]
    L += table(["Protection group", f"On {T}", "Snaps", "Last repl", "Median", "Max",
                "Transferred/day", "Written/day", "Reduction", "Space now"],
               rows, right={2, 3, 4, 5, 6, 7, 8, 9}, footer=foot)
    L.append("")

    # ---- retention estimate
    L.append(f"RETENTION ESTIMATE - space on {T} if snapshots are kept this long, at the current change rate")
    labels = [lab for lab, _ in retention]
    rows, est_totals = [], [[] for _ in retention]
    for g in groups:
        st = stats[g.name]
        now = (f"{space_text(st)} ({st['count']} snaps, {fmt_days(st['span_days'])})"
               if st["space_now"] is not None else "-")
        cells = [g.label, now, fmt_bytes(st["rate"])]
        proj = {p[0]: p for p in st["projections"]}
        for i, lab in enumerate(labels):
            p = proj.get(lab)
            cells.append(f"{fmt_bytes(p[2])}" + (f" ({p[3]:.0f} snaps)" if p and p[3] else "") if p else "-")
            est_totals[i].append(p[2] if p else None)
        rows.append(cells)
    foot = None
    if len(groups) > 1:
        foot = ["All groups", ("" if all(stats[g.name]["space_from_total"] for g in groups) else "at least ")
                + fmt_bytes(total(stats[g.name]["space_now"] for g in groups)),
                fmt_bytes(total(stats[g.name]["rate"] for g in groups))]
        foot += [fmt_bytes(total(v)) if all(x is not None for x in v) else "-" for v in est_totals]
    L += table(["Protection group", "Now", "Change/day"] + labels, rows,
               right=set(range(1, 3 + len(labels))), footer=foot)
    if any(stats[g.name]["short_span"] for g in groups):
        L.append(f"  Groups whose snapshots span under {SHORT_SPAN_HOURS:.0f} h: per-day figures and estimates "
                 f"are a rough extrapolation.")
    L.append("")

    # ---- per group
    for g in groups:
        st = stats[g.name]
        L.append("=" * 100)
        L.append(f"{g.label}   ({g.name} on {T})")
        oldest, newest = st["oldest"], st["newest"]
        state = ", ".join(f"{st[k]} {k}" for k in ("complete", "running", "queued") if st[k])
        if oldest:
            L.append(f"  Snapshots        {st['count']} ({state}), completed ones created {oldest.created_txt} to "
                     f"{newest.created_txt}" + (f", one every {fmt_days(st['interval_days'])}"
                                                if st["interval_days"] else ""))
        else:
            L.append(f"  Snapshots        {st['count']} ({state or 'none complete'})")
        L.append(f"  Replication      last {fmt_secs(st['repl_last'])}   median {fmt_secs(st['repl_median'])}   "
                 f"average {fmt_secs(st['repl_avg'])}   max {fmt_secs(st['repl_max'])}"
                 + (f" ({st['repl_max_snap']})" if st["repl_max_snap"] else "")
                 + f"   throughput {fmt_rate(st['throughput'])}")
        if st["span_days"]:
            L.append(f"  Per day          transferred {fmt_bytes(st['xfer_day'])}   physical bytes written "
                     f"{fmt_bytes(st['written_day'])}   reduction on the target {fmt_ratio(st['reduction'])}"
                     f"   ({st['rate_snaps']} snapshots over {fmt_days(st['span_days'])})")
        else:
            L.append("  Per day          - (needs two or more completed snapshots)")
        if st["space_from_total"]:
            sp = f"  Space on target  {fmt_bytes(st['space_now'])} (total)"
            if st["space_rows"] is not None:
                sp += (f" = {fmt_bytes(st['space_rows'])} in the snapshot rows + "
                       f"{fmt_bytes(st['space_shared'])} shared between snapshots")
            L.append(sp)
        elif st["space_now"] is not None:
            L.append(f"  Space on target  at least {fmt_bytes(st['space_now'])} (sum of the snapshot rows; "
                     f"no (total) row, so space the snapshots share isn't included)")
        else:
            L.append("  Space on target  - (no space rows for this group)")
        if g.space_method:
            L.append(f"  Space method     {g.space_method} (from the Method column of the space listing)")
        L.append("")
        rows = []
        for s in g.snaps:
            prog = fmt_pct(s.progress) if s.status != "no transfer row" else "-"
            repl = fmt_secs(s.repl_s) if s.status == "complete" else (s.status if s.status != "no transfer row" else "-")
            rows.append([s.suffix, s.created_txt or "-", repl, prog, fmt_bytes(s.transferred), fmt_bytes(s.written),
                         fmt_ratio(s.reduction), fmt_rate(s.rate), fmt_bytes(s.space), s.note])
        L += ["  " + x for x in table(["Snapshot", "Created", "Repl time", "Progress", "Transferred",
                                       "Phys written", "Reduction", "Throughput", "Snap space", "Note"],
                                      rows, right={2, 3, 4, 5, 6, 7, 8})]
        L.append("")

    # ---- notes and legend
    info = []
    for spg, dpg in missing:
        info.append(f"{(spg + ' -> ' + dpg) if spg and dpg else (dpg or spg)}: no snapshots of "
                    f"<source array>:{dpg or spg} in '{TRANSFER_CMD}' on {T}")
    if extra:
        info.append(f"Replicated groups on {T} that aren't in the config: {', '.join(extra)}" +
                    ("" if ctx.get("all") else " (add --all to include them)"))
    info += notes + ctx.get("warnings", [])
    if info:
        L.append("NOTES")
        L += [f"  * {x}" for x in info]
        L.append("")
    L += ["HOW TO READ THIS REPORT",
          "  Repl time     Completed - Started of the snapshot's transfer ('running' / 'queued' until it completes).",
          "  Transferred   Data Transferred: the change sent to the target since the previous replicated snapshot.",
          "  Phys written  Physical Bytes Written: what the target wrote after reducing that data further",
          "                (mostly deduplication against data it already holds) - the real change on the target.",
          "  Reduction     Transferred / Phys written. Throughput = Transferred / Repl time.",
          "  Snap space    The snapshot's row in the space listing: space only that snapshot holds (freed if it",
          "                is deleted). Space the snapshots share is only in the group's (total), shown as 'shared'.",
          "  Per day       Sum for the completed snapshots after the oldest / days from the oldest to the newest.",
          "                The oldest is left out: its transfer covers time before that window (or was the full copy).",
          "  Estimate      Space now + physical bytes written per day x (retention days - days the snapshots span",
          "                now). Snaps = retention days / the usual time between snapshots."]
    return "\n".join(L) + "\n"


# ============================================================================ CSV files
def write_csv(folder, ctx, groups, stats, retention):
    folder = Path(folder).expanduser()
    folder.mkdir(parents=True, exist_ok=True)
    stamp = ctx["read_dt"].strftime("%Y%m%d-%H%M%S")
    base = f"pure-dr-report_{re.sub(r'[^A-Za-z0-9._-]', '_', ctx['target'])}_{stamp}"
    snap_path, sum_path = folder / f"{base}_snapshots.csv", folder / f"{base}_summary.csv"
    num = lambda v, nd=0: "" if v is None else (f"{v:.{nd}f}" if nd else f"{v:.0f}")
    with open(snap_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["protection_group", "source_pgroup", "dr_pgroup", "target_pgroup", "snapshot", "suffix",
                    "status", "created", "started", "completed", "replication_seconds", "replication_time",
                    "created_to_completed_seconds", "progress_pct", "data_transferred_bytes", "data_transferred",
                    "physical_bytes_written_bytes", "physical_bytes_written", "reduction_ratio",
                    "throughput_mib_per_s", "hours_since_previous_snapshot", "snapshot_space_bytes",
                    "snapshot_space", "note"])
        for g in groups:
            for s in g.snaps:
                w.writerow([g.label, g.source_pgroup or "", g.dr_pgroup or "", g.name, s.name, s.suffix, s.status,
                            s.created_txt, s.started_txt, s.completed_txt, num(s.repl_s),
                            fmt_secs(s.repl_s) if s.repl_s is not None else "", num(s.lag_s), num(s.progress, 2),
                            num(s.transferred), fmt_bytes(s.transferred) if s.transferred is not None else "",
                            num(s.written), fmt_bytes(s.written) if s.written is not None else "",
                            num(s.reduction, 2), num(s.rate / 1024**2 if s.rate is not None else None, 2),
                            num(s.interval_s / 3600 if s.interval_s is not None else None, 2),
                            num(s.space), fmt_bytes(s.space) if s.space is not None else "", s.note])
    with open(sum_path, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        labels = [lab for lab, _ in retention]
        w.writerow(["protection_group", "source_pgroup", "dr_pgroup", "target_pgroup", "target_array", "read_at",
                    "snapshots", "complete", "running", "queued", "oldest_created", "newest_created", "span_days",
                    "days_between_snapshots", "repl_last_seconds", "repl_median_seconds", "repl_avg_seconds",
                    "repl_max_seconds", "throughput_mib_per_s", "data_transferred_total_bytes",
                    "physical_written_total_bytes", "data_transferred_per_day_bytes",
                    "physical_written_per_day_bytes", "physical_written_per_day", "reduction_ratio",
                    "space_now_bytes", "space_now", "space_now_from", "space_snapshot_rows_bytes",
                    "space_shared_bytes", "space_method", "change_rate_from"]
                   + [f"estimate_{re.sub(r'[^0-9A-Za-z]+', '', lab)}_bytes" for lab in labels]
                   + [f"estimate_{re.sub(r'[^0-9A-Za-z]+', '', lab)}" for lab in labels])
        for g in groups:
            st = stats[g.name]
            proj = {p[0]: p[2] for p in st["projections"]}
            w.writerow([g.label, g.source_pgroup or "", g.dr_pgroup or "", g.name, ctx["target"], ctx["read_at"],
                        st["count"], st["complete"], st["running"], st["queued"],
                        st["oldest"].created_txt if st["oldest"] else "", st["newest"].created_txt if st["newest"] else "",
                        num(st["span_days"], 3), num(st["interval_days"], 3), num(st["repl_last"]),
                        num(st["repl_median"]), num(st["repl_avg"]), num(st["repl_max"]),
                        num(st["throughput"] / 1024**2 if st["throughput"] is not None else None, 2),
                        num(st["xfer_total"]), num(st["written_total"]), num(st["xfer_day"]), num(st["written_day"]),
                        fmt_bytes(st["written_day"]) if st["written_day"] is not None else "", num(st["reduction"], 2),
                        num(st["space_now"]), space_text(st) if st["space_now"] is not None else "",
                        "(total) row" if st["space_from_total"] else ("snapshot rows (minimum)" if st["space_now"] is not None else ""),
                        num(st["space_rows"]), num(st["space_shared"]), g.space_method or "", st["rate_source"] or ""]
                       + [num(proj.get(lab)) for lab in labels]
                       + [fmt_bytes(proj[lab]) if lab in proj else "" for lab in labels])
    return snap_path, sum_path


# ============================================================================ main
def parse_retention(text):
    """'7,30,13w,6m,1y' -> [('7 d', 7.0), ('30 d', 30.0), ('13 w', 91.0), ('6 m', 182.5), ('1 y', 365.0)]."""
    out = []
    for part in str(text).split(","):
        p = part.strip().lower()
        if not p:
            continue
        m = re.fullmatch(r"(\d+(?:\.\d+)?)\s*([dwmy]?)", p)
        if not m or float(m.group(1)) <= 0:
            raise UsageError(f"--retention: '{part.strip()}' isn't a period (use days, or a number with d, w, m or y)")
        n, unit = float(m.group(1)), m.group(2) or "d"
        out.append((f"{m.group(1)} {unit}", n * {"d": 1, "w": 7, "m": 365 / 12, "y": 365}[unit]))
    if not out:
        raise UsageError("--retention needs at least one period")
    return out


def prompt(question, default=None):
    """Ask in the terminal; Enter gives the default."""
    sys.stderr.write(question + (f" [{default}]" if default else "") + ": ")
    sys.stderr.flush()
    return input().strip() or default


def password_possible():
    """A password login can be offered: the script runs in a terminal and paramiko is installed."""
    if not sys.stdin.isatty():
        return False
    try:
        import paramiko  # noqa: F401
        return True
    except ImportError:
        return False


def read_target(args, cfg):
    """Run the commands on the target array -> (transfer text, space text, commands run, warnings, host).
    SSH key login is tried first, as pg_dr_refresh.py logs in. If the array refuses it and the script
    runs in a terminal with paramiko installed, it asks for the password instead (--password: right away)."""
    host = args.host or (cfg.host if cfg else None)
    if not host:
        if not sys.stdin.isatty():
            raise UsageError("no target array: set target_array.host in the config, or use --host")
        host = prompt("Target array (hostname or IP address)")
        if not host:
            raise UsageError("no target array given")
    user = args.user or (cfg.user if cfg else None)
    key = args.key or (cfg.key if cfg else None)
    timeout = args.timeout or (cfg.timeout if cfg else 600)

    def password_login():
        return PasswordArray(host, user or prompt("Username", getpass.getuser()), timeout, args.verbose)

    arr = password_login() if args.password else OpenSSHArray(host, user, key, timeout, args.verbose)
    warnings, commands = [], [TRANSFER_CMD]
    try:
        try:
            transfer = arr.run(TRANSFER_CMD)
        except LoginError as e:
            if args.password or not password_possible():
                raise
            print(f"{e}\nLogging in with a password instead.", file=sys.stderr)
            arr = password_login()
            transfer = arr.run(TRANSFER_CMD)
        groups, _, _, _ = build_groups(parse_transfer(transfer), cfg, args.all, args.pgroup)
        parts, failed = [], []
        for g in groups:
            cmd = SPACE_CMD.format(pgroup=shlex.quote(g.name))
            try:
                parts.append(arr.run(cmd))
            except ArrayError as e:
                failed.append(g.name)
                warnings.append(f"{e}")
        commands.append(SPACE_CMD.format(pgroup="<protection group>"))
        if failed:
            try:
                parts.append(arr.run(SPACE_ALL_CMD))
                commands.append(SPACE_ALL_CMD)
                warnings.append(f"for {', '.join(failed)} the snapshot rows come from '{SPACE_ALL_CMD}', which has "
                                f"no (total) per group, so 'Space now' is the sum of the rows (a minimum)")
            except ArrayError as e:
                warnings.append(f"{e}")
    finally:
        arr.close()
    return transfer, "\n".join(p.strip("\r\n") for p in parts if p.strip()) + "\n", commands, warnings, host


def main(argv=None):
    ap = argparse.ArgumentParser(
        prog="pure-dr-report.py", formatter_class=argparse.RawDescriptionHelpFormatter,
        description="Replication and space report for the DR protection groups, read from the target array.",
        epilog=__doc__[__doc__.index("Usage\n-----"):__doc__.index("Requirements\n")])
    ap.add_argument("-c", "--config", help=f"pg_dr_refresh JSON config (default: {DEFAULT_CONFIG.name} next to this script)")
    ap.add_argument("--host", help="target array to read (default: target_array.host from the config)")
    ap.add_argument("--user", help="SSH user on the target (default: target_array.ssh_user)")
    ap.add_argument("--key", help="SSH private key (default: target_array.ssh_key)")
    ap.add_argument("--password", action="store_true", help="log in with a password instead of an SSH key (needs paramiko)")
    ap.add_argument("--pgroup", nargs="+", metavar="NAME", help="only these protection groups (source_pgroup, dr_pgroup or name on the target)")
    ap.add_argument("--all", action="store_true", help="also report replicated groups on the target that aren't in the config")
    ap.add_argument("--retention", default=DEFAULT_RETENTION, metavar="LIST",
                    help="retention periods to estimate space for: days, or a number with d, w, m or y (default: %(default)s)")
    ap.add_argument("--csv", metavar="DIR", help="also write the report as two CSV files (snapshots, summary) in DIR")
    ap.add_argument("--save", metavar="DIR", help=f"also save the array's raw output in DIR ({TRANSFER_FILE}, {SPACE_FILE})")
    ap.add_argument("--input", metavar="DIR", help="report from output saved with --save, instead of reading the array")
    ap.add_argument("--timeout", type=int, metavar="SEC", help="max seconds per command (default: settings.ssh_timeout_seconds, else 600)")
    ap.add_argument("-V", "--verbose", action="store_true", help="show every command and the array's output (on stderr)")
    args = ap.parse_args(argv)
    try:
        retention = parse_retention(args.retention)
        cfg_path = Path(args.config).expanduser() if args.config else (DEFAULT_CONFIG if DEFAULT_CONFIG.exists() else None)
        cfg = load_config(cfg_path) if cfg_path else None
        if args.input and args.save:
            raise UsageError("--save copies what is read from the array, so it can't be used with --input")
        if args.input:
            folder = Path(args.input).expanduser()
            try:
                transfer = (folder / TRANSFER_FILE).read_text(encoding="utf-8")
            except OSError as e:
                raise ReportError(f"can't read {folder / TRANSFER_FILE}: {e}")
            space = (folder / SPACE_FILE).read_text(encoding="utf-8") if (folder / SPACE_FILE).exists() else ""
            cap = {}
            if (folder / CAPTURE_FILE).exists():
                try:
                    cap = json.loads((folder / CAPTURE_FILE).read_text(encoding="utf-8"))
                except ValueError:
                    cap = {}
            read_dt = (datetime.datetime.fromisoformat(cap["read_at"]) if cap.get("read_at") else
                       datetime.datetime.fromtimestamp((folder / TRANSFER_FILE).stat().st_mtime))
            host = cap.get("host") or args.host or (cfg.host if cfg else None)
            commands = cap.get("commands") or [TRANSFER_CMD, SPACE_CMD.format(pgroup="<protection group>")]
            warnings = cap.get("warnings") or []
            if not space:
                warnings.append(f"{folder / SPACE_FILE} not found: no space figures")
        else:
            read_dt = datetime.datetime.now()
            transfer, space, commands, warnings, host = read_target(args, cfg)
            if args.save:
                folder = Path(args.save).expanduser()
                folder.mkdir(parents=True, exist_ok=True)
                (folder / TRANSFER_FILE).write_text(transfer, encoding="utf-8")
                (folder / SPACE_FILE).write_text(space, encoding="utf-8")
                (folder / CAPTURE_FILE).write_text(json.dumps(
                    {"host": host, "target": (cfg.name if cfg and cfg.name else None), "read_at":
                     read_dt.isoformat(timespec="seconds"), "commands": commands, "warnings": warnings}, indent=2),
                    encoding="utf-8")
                print(f"Saved the array's output in {folder}", file=sys.stderr)
        target = (cfg.name if cfg and cfg.name and not args.host else None) or (host.split(".")[0] if host and
                  not re.fullmatch(r"[\d.]+", host) else host) or "target array"
        rows = parse_transfer(transfer)
        if not rows:
            raise ReportError(f"'{TRANSFER_CMD}' returned no snapshots on {target}")
        groups, missing, extra, zones = build_groups(rows, cfg, args.all, args.pgroup)
        if not groups:
            seen = sorted({r["source"] or r["name"].rsplit(".", 1)[0] for r in rows})
            if args.pgroup:
                raise ReportError(f"--pgroup {' '.join(args.pgroup)} matched none of the groups reported on {target}. "
                                  f"Groups listed there: {', '.join(seen) or 'none'}"
                                  + ("" if args.all or not (cfg and cfg.pgroups) else
                                     " (groups that aren't in the config also need --all)"))
            raise ReportError(f"none of the protection groups in the config ({', '.join(d or s for s, d in cfg.pgroups)}) "
                              f"have snapshots on {target}. Groups listed there: {', '.join(seen) or 'none'}"
                              if cfg and cfg.pgroups else f"no replicated snapshots on {target}")
        notes = attach_space(groups, parse_space(space))
        stats = {g.name: analyse(g, retention) for g in groups}
        ctx = {"target": target, "host": host, "read_dt": read_dt, "read_at": read_dt.strftime("%Y-%m-%d %H:%M:%S"),
               "input": str(Path(args.input).expanduser()) if args.input else None, "commands": commands,
               "zones": zones, "warnings": warnings, "all": args.all}
        sys.stdout.write(text_report(ctx, groups, stats, missing, extra, notes, retention))
        if args.csv:
            for p in write_csv(args.csv, ctx, groups, stats, retention):
                print(f"Wrote {p}", file=sys.stderr)
        return 0
    except UsageError as e:
        print(f"pure-dr-report.py: {e}", file=sys.stderr)
        return 2
    except (ReportError, ArrayError) as e:
        print(f"pure-dr-report.py: {e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Cancelled", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
