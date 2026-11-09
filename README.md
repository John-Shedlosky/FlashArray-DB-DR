# Pure DR Refresh and Reporting

Two Python scripts that refresh DR copies of FlashArray volumes, replicate them to a second array, and report on how much data changes and how long replication takes.

| File | Purpose |
|---|---|
| `pg_dr_refresh.py` | Snapshots each source protection group, overwrites the matching DR volumes from that snapshot, snapshots the DR protection group, replicates it to the target array, and logs data replicated and replication time per protection group and per volume. |
| `pure-dr-report.py` | Reads the refresh logs and shows sizes, daily change, replication time, and weekly or monthly totals with comparisons to the previous period. Opens a window, prints terminal bar charts (`--cli`) or a text report, or exports CSV and PNG files. It can also run on a Windows computer and read the logs on the Linux server over SSH (`--remote`). |
| `pg_dr_config.json` | The configuration used by both scripts. Every setting is explained in comments inside the file. |
| `test_env_setup_commands.txt` | Purity CLI commands that build a matching test environment (volumes, protection groups, schedules, retention) on two arrays. |

## Contents

1. [How it works](#1-how-it-works)
2. [Requirements](#2-requirements)
3. [Install Python and the libraries on Linux](#3-install-python-and-the-libraries-on-linux)
4. [Download and install the scripts](#4-download-and-install-the-scripts)
5. [Set up SSH key login to the arrays](#5-set-up-ssh-key-login-to-the-arrays)
6. [Prepare the arrays](#6-prepare-the-arrays)
7. [Configure](#7-configure)
8. [Run the refresh](#8-run-the-refresh)
9. [Schedule the refresh](#9-schedule-the-refresh)
10. [Logs](#10-logs)
11. [Reports](#11-reports)
12. [Troubleshooting](#12-troubleshooting)

## 1. How it works

The refresh script logs in to both arrays over SSH and runs Purity//FA CLI commands:

* **Source array** (`source_array.host`, as `source_array.ssh_user`): takes the snapshots, overwrites the DR volumes and starts replication (steps 0–4 below).
* **Target array** (`target_array.host`, as `target_array.ssh_user`): monitors replication, then reads how much space each protection group's snapshots use there. The replication status and statistics are on the target, not the source. `--setup` also reads the target's connection key here when the arrays aren't connected yet (section 6).

For each protection group in the config, in order:

| Step | What happens | Purity command |
|---|---|---|
| 0 | Check every DR volume exists and record its size (`--setup` creates the volumes and protection groups, see section 6) | `purevol list --csv` |
| 1 | Snapshot the source protection group | `purepgroup snap --apply-retention --suffix <suffix> PG1` |
| 2 | Check the snapshot contains every source volume. If one is missing, nothing in that group is overwritten. | `purevol list --snap --pgrouplist PG1.<suffix>` |
| 3 | Overwrite each DR volume from its source volume snapshot | `purevol copy --overwrite PG1.<suffix>.vol-data-01 vol-data-01-dr` |
| 4 | Snapshot the DR protection group and start replication | `purepgroup snap --apply-retention --replicate-now --suffix <suffix> PG1-DR` |

To keep databases in backup mode for as short a time as possible, step 1 can be run on its own with `-S` and steps 0 and 2–6 later with `-C` (section 8, "Split runs").

The script starts every group's replication before it waits for any of them. It then monitors all replications together **on the target array** until each one completes or times out. On the target, a replicated group is named after the source array, `flasharray1:PG1-DR`, and its snapshot is `flasharray1:PG1-DR.<suffix>`. On every poll the script runs these on flasharray2.testdrive.local:

| Step | What happens | Purity command (on the target) |
|---|---|---|
| 5 | Read the DR group snapshot's Started, Completed, Progress, Data Transferred and Physical Bytes Written | `purepgroup list --snap --transfer --csv flasharray1:PG1-DR` |
| 5 | The same for each volume in it, if the target reports it | `purevol list --snap --transfer --csv flasharray1:PG1-DR.<suffix>.vol-data-01-dr ...` |

When monitoring is done, the script reads the space each replicated protection group uses on the target, once per group:

| Step | What happens | Purity command (on the target) |
|---|---|---|
| 6 | Read the space all snapshots of the group use on the target. The last row is `(total),<bytes>`. | `purepgroup list --snap --space flasharray1:PG1-DR --total --csv` (and `flasharray1:PG2-DR`) |

The `(total)` value is logged with the summary (for example `space on flasharray2: 6.20G (6657199308 bytes) used by 2 snapshots of flasharray1:PG1-DR`), and recorded in the group's DRDATA line as `target_space_bytes` for the reports. It's read for failed or timed-out groups too, if the group exists on the target. If it can't be read, or the output has no `(total)` row, the script logs a warning. The replication result and exit code don't change. If the `(total)` row has more than one number, the column named `Total` is used, otherwise `Snapshots`, otherwise the first number. The log file records the raw `(total)` row and the column used. If `--csv` isn't accepted, the script reads the text output instead. With `wait_for_replication: false` there's no monitoring, so the space isn't read.

Until the snapshot reaches the target, it isn't listed and the replication shows as `waiting`. On the first replication of a group, the group may not exist on the target yet. That's also treated as waiting, until `replication_timeout_minutes` runs out. A replication is complete when its Completed column has a time. The source Purity name (`flasharray1`) is read with `purearray list` at startup.

Snapshots are named `<pgroup>.<prefix>-YYYYMMDD-HHMMSS` (for example `PG1.dr-20261104-020000`). Every group in one run uses the same suffix.

Snapshot lifetime follows the retention policy already set on each protection group (`--apply-retention`), so the script never deletes snapshots.

After monitoring and the space reads, the script writes a summary and one `DRDATA {json}` line per protection group and per volume to the log. The report script reads these lines.

## 2. Requirements

| Item | Needed for | Version |
|---|---|---|
| Python | Both scripts | 3.8 or newer |
| OpenSSH client (`ssh`) | `pg_dr_refresh.py` | Any current version |
| git, or curl / wget | Downloading the scripts from GitHub | Any current version |
| SSH key login to the source array | `pg_dr_refresh.py` | See section 5 |
| SSH key login to the target array | `pg_dr_refresh.py` (replication monitoring) and `--setup` | See section 5 |
| Purity//FA 6.6.7 or newer on both arrays | `--setup` encrypted connection (`"encrypted": true`) | Set `"encrypted": false` for older releases |
| matplotlib | `pure-dr-report.py` window and `--export-png` | 3.5 or newer |
| Tk (tkinter) | `pure-dr-report.py` window | Shipped with your distro's Python |
| A display (desktop session or `ssh -X`) | `pure-dr-report.py` window | |
| paramiko | `pure-dr-report.py --remote` (reading the logs over SSH, for example from Windows) | 2.2 or newer |
| SSH password login and SFTP on the server with the logs | `pure-dr-report.py --remote` | See section 11 |

`pg_dr_refresh.py` uses only the Python standard library. `pure-dr-report.py` needs nothing extra for `--cli`, `--no-gui` and `--export-csv`. `--remote` also needs paramiko.

Older distros ship a default `python3` that is too old: RHEL / Rocky / Alma 8 and SLES 15 come with Python 3.6. On those, install a newer Python alongside it as shown below. Don't replace or upgrade the system `python3`, because the OS tools depend on it.

## 3. Install Python and the libraries on Linux

Pick the section for your distro. Each one installs Python, the OpenSSH client, Tk and matplotlib. If you only run the refresh script on a server, you can skip Tk and matplotlib there.

### RHEL / Rocky Linux / AlmaLinux 9

```bash
sudo dnf install -y python3 python3-pip python3-tkinter openssh-clients
python3 --version          # 3.9 or newer
```

matplotlib comes from EPEL, or use a virtual environment (see [Virtual environment](#virtual-environment-any-distro)).

Rocky Linux / AlmaLinux 9:

```bash
sudo dnf config-manager --set-enabled crb
sudo dnf install -y epel-release
sudo dnf install -y python3-matplotlib
```

RHEL 9:

```bash
sudo subscription-manager repos --enable codeready-builder-for-rhel-9-$(arch)-rpms
sudo dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm
sudo dnf install -y python3-matplotlib
```

### RHEL / Rocky Linux / AlmaLinux 8

The default `python3` is 3.6, so install Python 3.11 from AppStream:

```bash
sudo dnf install -y python3.11 python3.11-pip python3.11-tkinter openssh-clients
python3.11 --version       # 3.11.x
```

Install matplotlib in a virtual environment built with `python3.11` (see [Virtual environment](#virtual-environment-any-distro)). Then run the scripts with that environment's Python or with `python3.11`, never with plain `python3`.

### Ubuntu 20.04+ / Debian 11+

```bash
sudo apt update
sudo apt install -y python3 python3-venv python3-pip python3-tk python3-matplotlib openssh-client
python3 --version          # 3.8 or newer
python3 -c "import matplotlib; print(matplotlib.__version__)"   # must be 3.5 or newer
```

On Ubuntu 20.04 (matplotlib 3.1) and Debian 11 (matplotlib 3.3), the distro matplotlib is too old for the report window. Use a virtual environment for matplotlib instead.

Debian 12 and Ubuntu 23.04+ block `pip install` and `pip install --user` outside a virtual environment ("externally-managed-environment"). Use the `apt` package or a virtual environment.

### Fedora

```bash
sudo dnf install -y python3 python3-tkinter python3-matplotlib openssh-clients
```

### SUSE Linux Enterprise / openSUSE Leap 15

The default `python3` is 3.6, so install Python 3.11:

```bash
sudo zypper install -y python311 python311-pip python311-tk openssh-clients
python3.11 --version
```

Install matplotlib in a virtual environment built with `python3.11`. On SLES you may first need to enable the Python 3 module: `sudo SUSEConnect -p sle-module-python3/15.<SP>/x86_64`.

### Virtual environment (any distro)

A virtual environment keeps matplotlib separate from the system Python and works on every distro above. It uses the Tk package you installed with the system packages above.

```bash
# Use python3.11 instead of python3 on RHEL 8 / SLES 15
python3 -m venv /opt/pure-dr/venv
/opt/pure-dr/venv/bin/python -m pip install --upgrade pip
/opt/pure-dr/venv/bin/python -m pip install "matplotlib>=3.5"
```

Then run the scripts with `/opt/pure-dr/venv/bin/python`, for example:

```bash
/opt/pure-dr/venv/bin/python /opt/pure-dr/pure-dr-report.py -c /opt/pure-dr/pg_dr_config.json
```

If the server has no internet access, download the wheels on another machine with `pip download "matplotlib>=3.5" -d wheels` (same Python version and CPU type), copy the folder across, and run `pip install --no-index --find-links wheels matplotlib`.

### Check the installation

```bash
python3 --version                                            # 3.8+
python3 -c "import tkinter; tkinter.Tk().destroy(); print('Tk OK')"     # needs a display
python3 -c "import matplotlib; print('matplotlib', matplotlib.__version__)"
ssh -V
```

Replace `python3` with `python3.11` or your virtual environment's Python where that applies.

## 4. Download and install the scripts

The files are in the GitHub repository [John-Shedlosky/FlashArray-DB-DR](https://github.com/John-Shedlosky/FlashArray-DB-DR). These steps install them in `/opt/pure-dr`, with logs in `/var/log/pure`. Run them as the account that will run the refresh.

Create the folders first and give them to that account:

```bash
sudo mkdir -p /opt/pure-dr /var/log/pure
sudo chown "$USER" /opt/pure-dr /var/log/pure
```

### Option A: git clone (recommended)

Install git if needed:

* RHEL, Rocky, Alma or Fedora: `sudo dnf install -y git`
* Ubuntu or Debian: `sudo apt install -y git`
* SLES or openSUSE: `sudo zypper install -y git`

Then clone the repository into the folder:

```bash
git clone https://github.com/John-Shedlosky/FlashArray-DB-DR.git /opt/pure-dr
chmod 755 /opt/pure-dr/*.py
ls /opt/pure-dr
```

To update later:

```bash
cd /opt/pure-dr && git pull
```

`git pull` keeps your edits to `pg_dr_config.json` unless that file also changed in the repository. If it did, run `git stash && git pull && git stash pop`.

### Option B: download without git (curl or wget)

This downloads the current default branch as an archive and unpacks it straight into `/opt/pure-dr`:

```bash
curl -fsSL https://github.com/John-Shedlosky/FlashArray-DB-DR/archive/HEAD.tar.gz | tar -xz --strip-components=1 -C /opt/pure-dr
# or, with wget:
wget -qO- https://github.com/John-Shedlosky/FlashArray-DB-DR/archive/HEAD.tar.gz | tar -xz --strip-components=1 -C /opt/pure-dr
chmod 755 /opt/pure-dr/*.py
```

To download single files instead:

```bash
cd /opt/pure-dr
for f in pg_dr_refresh.py pure-dr-report.py pg_dr_config.json README.md test_env_setup_commands.txt; do
  curl -fsSLO "https://raw.githubusercontent.com/John-Shedlosky/FlashArray-DB-DR/HEAD/$f"
done
chmod 755 *.py
```

These commands assume the files are at the top level of the repository. If they are in a subfolder, add it to the raw URL, for example `.../HEAD/scripts/$f`. With the archive, copy the files out of that subfolder after unpacking.

### If the repository is private

The commands above work without logging in only if the repository is public. For a private repository, use one of these:

* HTTPS with a personal access token: run the `git clone` above. When asked, enter your GitHub username, and paste a token (from GitHub Settings > Developer settings > Personal access tokens, with read access to the repository) as the password. GitHub doesn't accept account passwords here.
* SSH key: add your public key to GitHub (Settings > SSH and GPG keys), then run:

  ```bash
  git clone git@github.com:John-Shedlosky/FlashArray-DB-DR.git /opt/pure-dr
  ```

* GitHub CLI: run `gh auth login`, then `gh repo clone John-Shedlosky/FlashArray-DB-DR /opt/pure-dr`.
* curl with a token:

  ```bash
  curl -fsSL -H "Authorization: Bearer $GITHUB_TOKEN" https://api.github.com/repos/John-Shedlosky/FlashArray-DB-DR/tarball | tar -xz --strip-components=1 -C /opt/pure-dr
  ```

### Check the files

```bash
ls -l /opt/pure-dr
python3 /opt/pure-dr/pg_dr_refresh.py --help
python3 /opt/pure-dr/pure-dr-report.py --help
```

You should see `pg_dr_refresh.py`, `pure-dr-report.py`, `pg_dr_config.json`, `README.md` and `test_env_setup_commands.txt`.

Both scripts start with `#!/usr/bin/env python3`. If that is older than 3.8 on your system, run them with an explicit interpreter, for example `python3.11 /opt/pure-dr/pg_dr_refresh.py ...`.

## 5. Set up SSH key login to the arrays

The refresh script uses non-interactive SSH (`BatchMode=yes`), so password prompts and unknown host keys make it fail. Set up the key once, as the account that will run the script.

1. Create a key with no passphrase, at the path in `source_array.ssh_key`:

   ```bash
   ssh-keygen -t rsa -f ~/.ssh/id_rsa_pure -N ""
   chmod 600 ~/.ssh/id_rsa_pure
   cat ~/.ssh/id_rsa_pure.pub
   ```

2. Log in to the source array as the Purity user named in `source_array.ssh_user` and add the public key. The key is set for the user you are logged in as. Paste the whole `ssh-rsa AAAA...` line at the prompt:

   ```bash
   ssh jjones@flasharray1.testdrive.local
   pureadmin setattr --publickey
   ```

3. Accept the array's host key and test the login. This must not prompt for a password:

   ```bash
   ssh -i ~/.ssh/id_rsa_pure jjones@flasharray1.testdrive.local "purearray list"
   ```

4. **Set up the same login on the target array.** Every refresh run monitors replication there. Add the public key for the user in `target_array.ssh_user` (`jjones` in the sample config; if not set, `source_array.ssh_user` is used). Then test it, again without a password prompt:

   ```bash
   ssh jjones@flasharray2.testdrive.local
   pureadmin setattr --publickey
   exit
   ssh -i ~/.ssh/id_rsa_pure jjones@flasharray2.testdrive.local "purepgroup list --snap --transfer --csv flasharray1:PG1-DR"
   ```

   The last command is the one the script runs while monitoring. It fails with "does not exist" until the first replication of `PG1-DR` reaches the target, and that's fine at this stage. The login must work without a prompt.

   For `--setup`, if the arrays aren't connected yet, also test that this user can read the connection key:

   ```bash
   ssh -i ~/.ssh/id_rsa_pure jjones@flasharray2.testdrive.local "purearray list --connection-key --encrypted"
   ```

   This prints the target's connection key, so don't save its output.

   The target user can be a different account from the source user: set `target_array.ssh_user` and `target_array.ssh_key` in the config.

Use a local Purity account. Keys can't be set for directory (LDAP / AD) users. On the source, the account needs rights to create snapshots, copy volumes and replicate, and for `--setup` to create volumes and protection groups and to connect arrays. On the target, it needs to list protection group snapshots and, for `--setup`, the connection key. The storage_admin or array_admin role provides most of these. Connecting arrays may need array_admin, so test the commands above before the real run. If you use an alternate controller address or VIP, accept its host key too.

## 6. Prepare the arrays

Everything in the config can be created by `--setup` (below) or by hand: the replication connection between the arrays, the source and DR volumes, the source and DR protection groups, and the target on each DR protection group. `test_env_setup_commands.txt` has every command, including how to:

* connect the arrays
* create matching test volumes and protection groups
* set schedules and retention
* allow the protection groups on the target

The essentials are:

* Connect the arrays (`--setup` step 1 does this for you): on the target, run `purearray list --connection-key --encrypted`. On the source, run `purearray connect --encrypted --management-address <target> --replication-transport ip --type async-replication` and paste the key at the prompt.
* Each DR protection group (`PG1-DR`) contains the DR volumes and has the target array in its target list.
* Retention on each protection group is set the way you want, because the script follows it: `purepgroup retain ...`.

### Connect the arrays and create the volumes and protection groups with `--setup`

`--setup` connects the arrays, then builds everything in `protection_groups` on the source array. It creates all the volumes first, then the protection groups, then adds the volumes to them:

1. **Connect the arrays:** the arrays count as connected only when the source's `purearray connection list --csv` has a row containing the target's host (`flasharray2.testdrive.local`, in its Management Address column) whose last field, Status, is `connected`. Fleet-management rows are ignored. `--setup` handles three cases:

   | Target row in `purearray connection list --csv` | What `--setup` does |
   |---|---|
   | Last field is `connected` | Leaves the connection as it is |
   | Listed, but not `connected` yet (for example `connecting`) | Waits up to `connect_timeout_seconds` for `connected`, re-reading the list every 5 seconds. It doesn't connect again. |
   | Not listed | Connects the arrays (below), then waits the same way |

   To connect, `--setup` logs in to both arrays over SSH:
   1. On the target (`target_array.host`), it reads the encrypted connection key:

      ```bash
      purearray list --connection-key --encrypted
      ```

   2. On the source, it connects to the target and types the key in at the command's connection-key prompt:

      ```bash
      purearray connect --encrypted --management-address flasharray2.testdrive.local --replication-transport ip --type async-replication
      ```

   3. A new connection is listed as `connecting` first. `--setup` re-reads `purearray connection list --csv` until the target's row ends in `connected`. If that doesn't happen within `connect_timeout_seconds` (default 300), step 1 fails and `purearray connection list --path` is written to the log. On Purity//FA 6.8.3 and later, its Details column shows the reason, for example a connection time-out.

   The key is sent on the SSH session's input, the same as typing it at the prompt. So it never appears on a command line, in the process list, on the console or in the log. `purearray connect` takes the key at this prompt, not as an argument. Since Purity//FA 6.6.7, its `--connection-key` option is deprecated and no longer used. For a target older than 6.6.7, set `"encrypted": false` in `target_array`. `--setup` then uses `purearray list --connection-key` and `purearray connect --management-address ... --connection-key`, with the key typed in at the prompt in the same way.

   An existing connection is never changed, even if it isn't encrypted (a warning is printed). If the connection fails, the volumes and protection groups are still created, but the target isn't added to the DR protection groups and the run exits with code 1. Fix the error and run `--setup` again.
2. **Volumes:** all of them are created before any protection group.
   * A missing source volume is created with the `size` from its config entry: `purevol create --size 100G vol-data-01`.
   * A missing DR volume is made the same size as its source volume. If `--setup` also created the source volume, it runs `purevol create --size 100G vol-data-01-dr`. If the source volume already existed, it copies it (`purevol copy vol-data-01 vol-data-01-dr`) so the sizes match exactly. The copy is metadata-only, and the first refresh overwrites it.
   * Volumes that already exist are left as they are.
3. **Protection groups:** each missing source and DR protection group is created, then its volumes from the config are added:

   ```bash
   purepgroup create PG1
   purepgroup setattr --addvollist vol-data-01,vol-data-02,vol-log-01 PG1
   purepgroup create PG1-DR
   purepgroup setattr --addvollist vol-data-01-dr,vol-data-02-dr,vol-log-01-dr PG1-DR
   ```

   For a protection group that already exists, only the volumes that aren't in it yet are added. Nothing is ever removed.
   Then the replication target is added to each DR protection group, by its Purity name from the connection list: `purepgroup setattr --addtargetlist flasharray2 PG1-DR`. Source protection groups get no target.
4. **Retention:** prints the retention of each protection group so you can review it. Retention isn't changed.

The `size` of each volume pair goes in the config:

```json
{ "target": "vol-data-01-dr", "source": "vol-data-01", "size": "100G" }
```

* Use a whole number followed by `K`, `M`, `G`, `T` or `P`, for example `"100G"` or `"2T"`.
* `size` is only needed for source volumes that don't exist yet. If the source volume already exists, `size` is ignored, and a warning is printed if it doesn't match.

Preview the commands first, then run it:

```bash
python3 pg_dr_refresh.py -c pg_dr_config.json --setup --dry-run
python3 pg_dr_refresh.py -c pg_dr_config.json --setup
```

* **Dry run:** reads the source array with list commands only (`purearray connection list`, `purevol list`, `purepgroup list`), so the preview shows exactly what would be created. Nothing is changed. If the arrays aren't connected, it also runs `purearray list` on the target to test the SSH login. It doesn't read the connection key or connect the arrays, and it only shows the commands it would run. If the source array can't be reached, volumes with a `size` are shown as created and the others are assumed to exist.
* **Check the names first:** a misspelled `source` name that has a `size` creates a new, empty volume with that name and adds it to the source protection group. Review the dry run before the real run.
* **Schedules and retention:** `--setup` doesn't set them. On new protection groups, set them with `purepgroup schedule` and `purepgroup retain` (examples in step 5 of `test_env_setup_commands.txt`), because the refresh keeps snapshots for as long as the retention says.
* **Safe to repeat:** anything that already exists is reported and left unchanged, so you can run `--setup` again after you add an entry or fix an error.
* **Exit code:** 0 means everything in the config is ready. 1 means at least one entry isn't, and the errors above the last line say why.

## 7. Configure

Edit `pg_dr_config.json`. It is JSON with `//` comments, and each setting is explained in the file. The main ones:

| Setting | Meaning |
|---|---|
| `source_array.host` | Management FQDN, alias or VIP the script SSHes to |
| `source_array.ssh_user`, `ssh_key` | Purity user and private key from section 5 (`ssh_key: null` uses ssh-agent / `~/.ssh` defaults) |
| `target_array.host` | Management FQDN, alias or VIP of the target, for example `flasharray2.testdrive.local`. Every run SSHes here to monitor replication (`purepgroup list --snap --transfer --csv`) and finds the target's row in `purearray connection list --csv` on the source by this value. `--setup` also reads the connection key here and passes the host to `purearray connect --management-address`. |
| `target_array.name` | Optional. Purity name of the target (for example `flasharray2`). If left out, it's read from the connection list or from the target. Purity names can't contain dots, so never put the FQDN here. |
| `target_array.connect_timeout_seconds` | `--setup` only. How long to wait for the status to change from `connecting` to `connected` (default 300, minimum 10) |
| `target_array.ssh_user` | Purity user for SSH logins to the target, used by every run (monitoring) and by `--setup`. `jjones` in the sample config. Change it if the target uses a different account. If the line is removed, `source_array.ssh_user` is used. |
| `target_array.ssh_key` | Private key for the target login (`null` uses ssh-agent / `~/.ssh` defaults). If the line is removed, `source_array.ssh_key` is used. |
| `target_array.encrypted` | `--setup` only. `true` (default) makes an encrypted connection (`purearray connect --encrypted`, Purity//FA 6.6.7+). `false` makes an unencrypted one, for older releases. |
| `target_array.replication_transport` | `--setup` only. `"ip"` (default) or `"fc"` |
| `log_file` | Base log path. A relative path is relative to the config file's folder. |
| `settings.wait_for_replication` | `true` waits for every transfer so data and time are logged. This is needed for the reports. |
| `settings.poll_interval_seconds` | Seconds between transfer checks (minimum 1) |
| `settings.replication_timeout_minutes` | Give up on one replication after this many minutes (minimum 1) |
| `protection_groups[]` | Each entry: `source_pgroup`, `dr_pgroup`, and `volumes` as `{target, source, size}` entries |
| `volumes[].size` | Optional. The size `--setup` uses to create a missing source volume, for example `"100G"`. The DR volume gets the same size. |

The script checks the whole config before it touches the array. It lists every problem it finds, for example a misspelled setting, a missing name or a duplicate target volume, and exits with code 2.

## 8. Run the refresh

Always start with a dry run. It prints every command that would run and changes nothing:

```bash
cd /opt/pure-dr
python3 pg_dr_refresh.py -c pg_dr_config.json --dry-run
```

Then run it for real. `-V` also shows each CLI command, the array output, and a progress bar for each replication:

```bash
python3 pg_dr_refresh.py -c pg_dr_config.json -V
python3 pg_dr_refresh.py -c pg_dr_config.json
```

| Option | Effect |
|---|---|
| `-c FILE` | Config file (required) |
| `-V`, `--verbose` | Show commands, array output and progress bars, and wait for replication even if `wait_for_replication` is `false` |
| `--dry-run` | Log the commands only. Nothing is changed and no DRDATA is written. |
| `--setup` | One-time: connect the arrays if they aren't connected yet (reads the target's connection key over SSH), create every missing volume in the config, then the source and DR protection groups with their volumes, and add the target to the DR groups. Retention isn't changed. Add `--dry-run` to preview. See section 6. |
| `-S`, `--snaponly` | Step 1 only: snapshot every source protection group, back to back, and record the snapshots in the state file. Nothing else is contacted, so it returns in seconds. See "Split runs" below. |
| `-C`, `--cloneandreplicate` | Steps 0 and 2–6 using the snapshots recorded by the last `--snaponly`. Fails with exit code 2, changing nothing, if there are none (or they were all used already). |

`--setup`, `-S` and `-C` can't be combined. With none of them, the script runs steps 0–6 in one go, as before.

Exit codes:

| Code | Meaning |
|---|---|
| 0 | Every protection group succeeded |
| 1 | At least one protection group failed (see the log) |
| 2 | Config or usage error (nothing was changed), including `-C` with no unused `--snaponly` snapshots |

### Split runs: `--snaponly` then `--cloneandreplicate`

Use these when a database has to be in backup mode while its snapshot is taken. A normal run keeps the database waiting through the DR volume checks and the target login. `-S` only takes the snapshots:

```bash
# 1. put the databases in backup mode
python3 pg_dr_refresh.py -c pg_dr_config.json -S
# 2. take the databases out of backup mode (as soon as -S returns)
python3 pg_dr_refresh.py -c pg_dr_config.json -C
```

`-S` runs one `purepgroup snap --apply-retention --suffix <suffix> <pgroup>` per protection group and nothing else. It doesn't read the array name, check the target or check the DR volumes, so each extra SSH login is avoided. It writes the snapshot names to the **state file**: `state_file` in the config, or `<config name>.state.json` next to the config by default (`pg_dr_config.state.json`). The config itself is never written to, so its comments stay intact. Don't edit the state file.

`-C` reads the state file, then for each protection group:

1. checks the DR volumes exist (step 0)
2. checks the recorded snapshot still exists and holds every source volume (step 2). If retention removed it, nothing is overwritten.
3. overwrites the DR volumes, snapshots the DR group and starts replication (steps 3–4), then monitors on the target as usual

The DR snapshots use the same suffix as the `-S` snapshots (`PG1.dr-20261109-020000` → `PG1-DR.dr-20261109-020000`), so their names show the point in time the data is from. DRDATA records are dated by the `-S` time.

The state file tracks each protection group separately:

| Situation | What `-C` does |
|---|---|
| `-S` was never run | Fails, exit 2, nothing changed |
| Every recorded snapshot was already used by an earlier `-C` | Fails, exit 2, nothing changed. Run `-S` again. |
| A group's snapshot failed in `-S` | That group fails; the others continue |
| A group fails in `-C` before any DR volume is overwritten (for example a DR volume is missing) | That group stays unused. The next `-C` retries just that group, with the same snapshot. |
| A group fails after its DR volumes were overwritten | It's marked used. Run `-S` again for a new snapshot. |
| A group was added to the config after `-S` | That group fails: it has no snapshot. Run `-S` again. |
| `-S` runs twice without a `-C` | The second run replaces the record (with a warning). The older snapshots stay on the array until their retention expires. |

`--dry-run` works with both. It never writes the state file; `-C --dry-run` still needs a state file to preview from.

A failure in one protection group doesn't stop the others.

Replication is monitored on the target array, where Purity reports each transfer's Progress, Data Transferred and Physical Bytes Written. If the target can't be logged in to at startup, the run stops before changing anything (exit code 1), because the results couldn't be recorded. With `wait_for_replication: false` the target isn't needed, and replications are left running.

## 9. Schedule the refresh

Run the refresh from cron as the account that has the SSH key. Use full paths, because cron starts in the home directory with a minimal `PATH`.

```bash
crontab -e
```

```cron
# DR refresh every day at 02:00 (use python3.11 or /opt/pure-dr/venv/bin/python where needed)
0 2 * * * /usr/bin/python3 /opt/pure-dr/pg_dr_refresh.py -c /opt/pure-dr/pg_dr_config.json >> /var/log/pure/pg_dr_refresh_cron.log 2>&1
```

For split runs, call `-S` from the database backup job, between entering and leaving backup mode, then start `-C` after it (it can run in the background, for example `nohup python3 pg_dr_refresh.py -c ... -C >> /var/log/pure/pg_dr_refresh_cron.log 2>&1 &`).

Make sure one run finishes before the next starts. Watch the replication times in the report, and keep `replication_timeout_minutes` below the gap between runs.

## 10. Logs

Every run appends to a weekly file named after the Monday of the current week:

```
/var/log/pure/pg_dr_refresh_20261102.log
/var/log/pure/pg_dr_refresh_20261109.log
```

Each file contains:

* every command and the array's output
* replication progress
* per-volume completions
* the space each protection group uses on the target: the `(total)` row of `purepgroup list --snap --space ... --total --csv`, in bytes and Purity units, with the snapshot count
* the summary
* the `DRDATA` lines

The protection group DRDATA line has these space fields:

| Field | Value |
|---|---|
| `target_pgroup` | The group's name on the target, for example `flasharray1:PG1-DR` |
| `target_space_bytes` | The `(total)` bytes: the space all snapshots of the group use on the target. `null` if not read. |
| `target_space_snapshots` | The number of snapshot rows listed above the `(total)` row |
| `target_space_column` | The CSV column the value came from, for example `Snapshots` |

DRDATA records are dated by the run's start time, so a run that crosses midnight counts on the day it started. Each record has a status:

| Status | Meaning |
|---|---|
| `ok` | Replication completed. Data, physical bytes written and time are recorded. |
| `failed` | The group failed, either before replication started (the group and each of its volumes are recorded as failed) or during monitoring |
| `started` | `wait_for_replication` was `false`, so replication was started but not measured |

Old log files aren't deleted automatically. Keep as many weeks as you want to report on, or rotate them with your usual tooling.

## 11. Reports

```bash
python3 pure-dr-report.py -c pg_dr_config.json          # logs from the config's log_file location
python3 pure-dr-report.py -l /var/log/pure               # or a folder, files or globs
```

### Run the report from Windows (`--remote`)

With `--remote`, the report runs on a Windows computer and reads the logs on the Linux server over SSH. You don't copy the logs or create SSH keys. The script asks for the hostname, username and password:

* The password isn't shown as you type it, and it isn't saved.
* The password is sent only after the server's host key has been checked.
* The logs are read into memory. Nothing is copied to the Windows computer.

Set up the Windows computer once:

1. Install Python 3.8 or newer from [python.org](https://www.python.org/downloads/windows/). The installer includes Tk and the `py` launcher.
2. In PowerShell or Command Prompt, install the libraries:

   ```powershell
   py -m pip install matplotlib paramiko
   ```

3. Copy `pure-dr-report.py` to a folder, for example `C:\pure-dr`. No other file is needed on Windows.

Run it from PowerShell or Command Prompt. The first connection to a server looks like this:

```text
PS C:\pure-dr> py pure-dr-report.py --remote
Hostname of the Linux server with the logs: linuxhost
Username [administrator]: jjones
Password for jjones@linuxhost:
The authenticity of host 'linuxhost' can't be established.
ssh-ed25519 key fingerprint is SHA256:Q6/a8BE82YNyuoW+jWmVHkt2mfHHhUd+nW2JvbREKB8.
(On the server, 'ssh-keygen -lf /etc/ssh/ssh_host_ed25519_key.pub' shows its fingerprint.)
Are you sure you want to continue connecting (yes/no): yes
Added linuxhost to C:\Users\administrator\.ssh\known_hosts
Reading 7 log file(s) from jjones@linuxhost...
```

* **Hostname:** a name or IP address. Add `:port` if SSH doesn't use port 22, for example `linuxhost:2222`.
* **Username:** press Enter to use your Windows user name.
* **Password:** you get 3 tries.
* **Host key:** asked only the first time you connect to a server. Before you type `yes`, check that the fingerprint matches the one the server shows. The key is then saved in `C:\Users\<you>\.ssh\known_hosts`, the same file Windows' own `ssh` uses. If the key changes later, the script doesn't connect (see section 12).

Which logs are read:

| Option | With `--remote` |
|---|---|
| None | `/var/log/pure/pg_dr_refresh_*.log` on the server (the `log_file` of the sample config) |
| `-l PATH ...` | Folders (their `*.log` files), files or globs on the server, for example `-l /var/log/pure` or `-l "/var/log/pure/pg_dr_refresh_202610*.log"`. Wildcards work in the file name only. `~` and relative paths start in your home folder on the server. |
| `-c PATH` | A `pg_dr_config.json` on the server. Its `log_file` is used. |

All other options work as usual:

```powershell
py pure-dr-report.py --remote                                       # the window
py pure-dr-report.py --remote -c /opt/pure-dr/pg_dr_config.json     # logs from the config's log_file on the server
py pure-dr-report.py --remote --period last30 --cli                 # terminal charts in PowerShell
py pure-dr-report.py --remote --export-csv C:\Reports\dr.csv        # CSV files saved on this computer
```

In the window:

* The title bar shows the server and the user, for example `jjones@linuxhost`.
* **Reload** searches the server again, so a new week's log file is included.
* **Open logs...** asks for a folder, file or glob on the server.
* The connection stays open while the window is open. If it closes, for example because the server restarted, **Reload** asks for the password again.
* **Export PNGs...** and **Export CSV...** save to the Windows computer.

What the server needs:

* SSH password login. Password or keyboard-interactive login must be allowed in `/etc/ssh/sshd_config`. Many servers allow it by default, but some (cloud images in particular) turn it off. Ask the server's administrator if the script reports that password logins aren't allowed.
* SFTP (`Subsystem sftp` in `/etc/ssh/sshd_config`). It is on by default in RHEL, Ubuntu, Debian and SUSE.
* Read access to the log folder and files for the account you log in with.

Notes:

* Run the script from PowerShell or Command Prompt. `--remote` refuses to start in IDLE or with `pythonw`, where the password can't be hidden.
* In Git Bash, `/var/log/pure` is changed to `C:/Program Files/Git/var/log/pure` before the script sees it. Use PowerShell or Command Prompt, or run `export MSYS_NO_PATHCONV=1` first.
* `--remote` also works from Linux or macOS. It still asks for a password there; it doesn't use SSH keys.

### Window

The window has a period picker, a protection group filter, and these tabs:

* **Overview:** each protection group and its volumes, with:
  * size
  * space on target: the latest space the group's snapshots use on the target (protection groups only)
  * target est./day and est./week: how fast that space is growing, from the latest snapshot (see "Space growth on the target")
  * average change per day, and that change as % of size
  * peak day and last run
  * average and maximum replication time
  * OK and failed runs
* **Sizes:** provisioned size of each DR volume
* **Daily change:** data replicated per day, stacked by volume, and change as % of size
* **Replication time:** time per run per group, and per-volume average and maximum
* **Target space:**
  * the space each protection group's snapshots use on the target, per run
  * the estimated growth per day of each new snapshot, with the period average shown dashed
  * the latest total per group, with its bytes, snapshot count, and estimated growth per day and per week
* **Weekly / Monthly:** totals per week or month by protection group and volume, compared with the previous week or month, and the selected period compared with the previous period

The window needs Tk, matplotlib and a display. On a server without a desktop, connect with `ssh -X user@server`, or use `--cli` or the text, CSV and PNG options below. If no display is found, the script prints the text report instead.

### Periods

| Period | Option | Compared with |
|---|---|---|
| Last 7 days | `--period last7` | The 7 days before |
| Last 30 days | `--period last30` | The 30 days before |
| Last N days | `--days N` | The N days before |
| Previous week (Mon–Sun) | `--period prev-week` | The week before it |
| Previous month | `--period prev-month` | The month before it |
| This month | `--period this-month` | The same days of last month |
| All (default) | `--period all` | Nothing |
| Custom date range | `--from YYYY-MM-DD --to YYYY-MM-DD` | The same number of days immediately before |

`--as-of YYYY-MM-DD` runs any report as if that date were today. `--pgroup NAME` limits everything to one protection group (source or DR name).

### Terminal charts (`--cli`)

`--cli` prints the whole report in the terminal, with no window, display or matplotlib needed. Bar charts are drawn with `#` (or `*`), and the numbers are printed next to every bar.

```bash
python3 pure-dr-report.py -c pg_dr_config.json --cli
python3 pure-dr-report.py -c pg_dr_config.json --cli --period prev-month
python3 pure-dr-report.py -c pg_dr_config.json --cli --period last30 --summary both --pgroup PG1
python3 pure-dr-report.py -c pg_dr_config.json --cli --bar-char '*' --width 140 | less -S
```

It prints these sections, in order:

| Section | Bars show | Numbers beside each bar |
|---|---|---|
| Overview | (table) | Size, space on target, its estimated growth per day and per week, average per day, % per day, last run, average and maximum time, OK and failed runs, status |
| DR volume sizes | Provisioned size per volume, grouped by protection group | Size and protection group total |
| Space on target | Latest space used on the target per protection group | Size, bytes, snapshots, target group name, when it was measured, change in the period |
| Space growth on target | Estimated growth per day: `#` last = latest snapshot, `*` avg = period average | Per day and per week, the latest snapshot's increment and interval, and the period's total growth and time |
| Space on target per day | The last measurement each day (each week for periods over 45 days), per protection group, from the first measurement | Size and change vs the day before |
| Daily change | Data replicated per day, stacked by protection group (`#` = first group, `*` = second, then `=`, `+` and so on) | Total per day, failed runs, and the period's average and total |
| Average daily change rate | Change per day as % of each volume's size | %/day and GiB/day, per volume and per group |
| Replication time per day | `#` up to the average, then `*` up to the maximum, per protection group | Average, maximum, runs and failed runs |
| Replication time per volume | `#` up to the average, then `*` up to the maximum | Average and maximum |
| Period comparison | `now` (this period) above `prev` (previous period), average per day | Average per day, change %, total, OK and failed runs |
| Weekly / Monthly | All groups stacked, then each group, average per day | Average per day, change vs previous week or month, total, average and maximum time, OK and failed runs, and space on target at the end of the week or month with its estimated growth per day. "(in progress)" marks the current week or month. |

Example (previous week, `--width 100`):

```text
DAILY CHANGE - data replicated per day   (1 char = 2.96 GiB;  # = PG1   * = PG2)
--------------------------------------------------------------------------------
  2026-10-26 Mon |######################################********************************| 206.96 GiB
  2026-10-27 Tue |################################*****************************         | 179.63 GiB
  2026-10-28 Wed |####################################**************************        | 183.81 GiB
  2026-10-29 Thu |###################################***************************        | 183.53 GiB
  2026-10-30 Fri |####################################***************************       | 184.95 GiB
  2026-10-31 Sat |############**********                                                | 64.66 GiB
  2026-11-01 Sun |#############********                                                 | 63.13 GiB
  Average 152.38 GiB/day over 7 days; total 1.04 TiB

REPLICATION TIME PER VOLUME   (# = average, * = up to maximum;  1 char = 0:00:04)
---------------------------------------------------------------------------------
PG1 -> PG1-DR
  vol-data-01-dr |#########################***********************        | avg 0:01:42  max 0:03:18
  vol-data-02-dr |#################*********                              | avg 0:01:11  max 0:01:47
  vol-log-01-dr  |#############################**********************     | avg 0:01:58  max 0:03:31
PG2 -> PG2-DR
  sql-db-dr      |##############################**************************| avg 0:02:02  max 0:03:51
  sql-log-dr     |###########################**********************       | avg 0:01:50  max 0:03:21
```

Options for `--cli`:

* **Scale:** each chart has its own scale, shown in its heading as `1 char = ...`. Bars in one chart can be compared with each other, but not with bars in another chart.
* **`--bar-char C`:** the bar character, `#` by default. Previous-period and up-to-maximum bars use `*`, or `#` when `C` is `*`.
* **`--width N`:** sets the output width. By default the script uses the terminal width, or 120 characters when the output goes to a file or pipe. The Overview table is a fixed 167 characters wide, so use `less -S` or a wide terminal to read it in full.
* **Long periods:** for periods over 45 days, such as `--period all`, the daily-change and replication-time charts show one row per week (average per day) instead of one per day.
* **Other options:** `--period`, `--from` / `--to`, `--days`, `--as-of`, `--pgroup` and `--summary` work the same as for the other report modes.
* **Saving the output:** use `--cli > report.txt` to save it, or pipe it to `mail`.

### Text, CSV and PNG output

```bash
python3 pure-dr-report.py -c pg_dr_config.json --period prev-month --no-gui
python3 pure-dr-report.py -c pg_dr_config.json --period last30 --no-gui --summary both
python3 pure-dr-report.py -c pg_dr_config.json --from 2026-09-01 --to 2026-09-30 --export-csv ~/reports/sept.csv
python3 pure-dr-report.py -c pg_dr_config.json --period prev-month --export-png ~/reports/oct
```

* `--no-gui` prints tables only, with no bar charts:
  * the Overview
  * a **Space on target** table: the latest space per protection group, in bytes and GiB, with snapshot count, time measured, first value in the period, change and maximum
  * a **Space growth on target** table: for the latest snapshot, its increment, interval, per hour, est./day and est./week; then the period's total growth, hours, avg/day and avg/week
  * the comparison with the previous period
  * a weekly or monthly breakdown `--summary` takes `auto` (weekly for periods under 5 weeks, otherwise monthly), `week`, `month`, `both` or `none`.
* `--export-csv FILE` writes four files:
  * `FILE`: per-run data, one row per protection group or volume per run, including errors. Protection group rows also have:
    * `target_pgroup`, `target_space_bytes`, `target_space_snapshots`
    * `target_space_growth_bytes`, `target_space_interval_hours`
    * `target_space_est_hourly_bytes`, `target_space_est_daily_bytes`, `target_space_est_weekly_bytes`
  * `FILE_period.csv`: the period compared with the previous period
  * `FILE_weekly.csv` and `FILE_monthly.csv`: the breakdowns
  * The summaries include the latest space in the period, week or month (`target_space_bytes`, `target_space_gib`, `target_space_snapshots`, `target_space_measured`) and its change (`prev_target_space_bytes`, `target_space_change_bytes`, `target_space_change_pct`).
  * They also include the estimated growth over the same time: `target_space_growth_bytes`, `target_space_growth_hours`, `target_space_est_daily_bytes`, `target_space_est_weekly_bytes`, `prev_target_space_est_daily_bytes` and `target_space_est_daily_change_pct`.
  * Sizes are given in bytes and GiB, times in seconds and H:MM:SS, changes in %.
* `--export-png DIR` saves the Sizes, Daily change, Replication time, Target space (`pure-dr-target-space.png`), Weekly and Monthly charts. It doesn't need Tk or a display.

### How the numbers are calculated

* **Avg / day:** data replicated in the period divided by the calendar days in the period.
  * Days before the first log aren't counted, and neither is today until its first run.
  * Any other day without a successful run counts as zero.
  * Every tab uses this rule, so weeks, months and partial periods compare fairly.
* **Avg/day chg:** the like-for-like comparison. **Data chg** compares raw totals and looks large for a period still in progress.
* **Replication times:** averaged over successful runs. Failed runs are counted but add no data. `started` runs appear as the last status but aren't counted as OK or failed.
* **Space growth on the target (estimated):** how fast the space on the target grows, worked out for each snapshot:
  * **Increment:** the `(total)` after this run minus the `(total)` after the previous successful run of the same group. This is the space the new snapshot added on the target. On the target, data shared between snapshots is reported in the newest snapshot, so its own row shows the whole image, not the change. The difference in the `(total)` is used instead.
  * **Interval:** the time between the two runs' start times, which is when the snapshots were taken.
  * **Per hour** = increment ÷ interval in hours. **Est./day** = per hour × 24. **Est./week** = per hour × 168.
  * For example, a run 1 hour after the previous one that added 250 MiB shows 250 MiB per hour, 5.86 GiB per day and 41.02 GiB per week.
  * **Last** is the latest snapshot in the period. **Avg** is all increments in the period divided by all their hours, × 24 and × 168. It smooths out single runs. Weekly and monthly rows use the same rule for that week or month.
  * A failed run in between is skipped, so its time counts toward the next run's interval and the rate stays correct.
  * The first measured run of a group has no increment.
  * The increment is net. If older snapshots expire under the retention policy, the space they free is subtracted. The text report notes this, for example `net: 1 older snapshot expired (14->14)`. A negative value means more was freed than added.
  * Intervals under 15 minutes are flagged, because × 24 and × 168 inflate them.
* **Space on target:** a point-in-time value, never added up. The report shows the latest measurement in the period, or the last one in each week or month. "Change" is the latest value minus the first one in the period. Runs from before the script recorded it show `-`.
* **Weekly / Monthly:** whole calendar weeks (Mon–Sun) or months that overlap the period. `*` marks a week or month still in progress. A bucket that starts before the first log is labelled "logs start".

## 12. Troubleshooting

| Symptom | Fix |
|---|---|
| `Permission denied (publickey)` or `Host key verification failed` | Repeat section 5 as the account that runs the script. Check `ssh_user` and `ssh_key` in the config. |
| `needs Python 3.8 or newer` | Run with `python3.11` or a virtual environment (section 3). |
| `Config errors: ...` | Fix each listed item in `pg_dr_config.json`. Nothing was changed on the array. |
| `Target x not found in 'purearray connection list'` | Set `target_array.host` to the target's address exactly as the Management Address column shows it, or run `--setup` to connect the arrays. |
| `Replication target x is listed but its status is 'connecting'` | The arrays are listed but not connected, so replication won't complete. Check `purearray connection list --path` on the source and the replication network (`purenetwork list` on both arrays). |
| `... does not contain: vol-x - nothing overwritten` | The source volume isn't a member of the source protection group. Add it, or fix the config. |
| `Volume does not exist` at step 0 | Run `--setup` to create the missing DR volumes (section 6), or create them by hand. |
| `--setup`: `doesn't exist and has no 'size'` | Add `"size": "100G"` (or the right size) to that volume's entry, or create the source volume by hand. |
| `--setup`: `'size' ... doesn't match the existing volume` | Warning only. The DR volume was made the same size as the real source volume. Fix or remove `size` in the config. |
| `--setup`: `can't create ...: ... already exists` | A destroyed volume or protection group with that name is waiting to be eradicated. Check `purevol list --pending` or `purepgroup list --pending`, then recover or eradicate it. |
| `--setup`: `can't add target ...` | Check that `purearray connection list` on the source shows the target as `connected`, and that `target_array.name` (if set) matches its Name column. |
| `--setup`: `still not 'connected' after 300s` | The connection exists but stays `connecting`. Read the `purearray connection list --path` output in the log (the Details column), fix the replication network, then run `--setup` again. It waits for the existing connection. It doesn't connect again. |
| `--setup`: `Can't log in to the target array` | Do step 4 of section 5 for `target_array.ssh_user`, and check `target_array.host`. Or connect the arrays by hand (section 6) and run `--setup` again. |
| `--setup`: `Can't read the connection key ... Invalid option '--encrypted'` | The target runs Purity//FA older than 6.6.7. Set `"encrypted": false` in `target_array`, or upgrade the arrays. |
| `--setup`: `Connecting flasharray1 to flasharray2 failed` | Check that the source can reach `target_array.host` on the management network and that the replication ports are up (`purenetwork list` on both arrays). Then run `--setup` again. |
| `--setup`: `reports Purity name 'x', but target_array.name is 'y'` | `--setup` uses the reported name. Fix or remove `target_array.name`. Connections and protection group targets use the Purity name. |
| `--setup`: `not added to PG1-DR: the arrays aren't 'connected'` | Step 1 failed. Fix the error printed under `setup step 1 of 4`, then run `--setup` again. |
| Replication `not complete after N min` | On the target, run `purepgroup list --snap --transfer flasharray1:PG1-DR`. If the message says `it never appeared on flasharray2`, check that the arrays are `connected` and the group is allowed on the target (`purepgroup list flasharray1:PG1-DR`). Raise `replication_timeout_minutes` if needed. |
| `Can't log in to the target array ... Replication is monitored on the target, so nothing was changed` | Do step 4 of section 5 for `target_array.ssh_user` and `ssh_key`. Or set `wait_for_replication` to `false` to run without monitoring (no data or time is logged for the reports). |
| `space not read on flasharray2` or `no (total) row` | On the target, run `purepgroup list --snap --space flasharray1:PG1-DR --total --csv`. The group must exist there and the output must end with a `(total)` row. The replication result isn't affected. |
| The report shows `-` for space on target | That period has no runs with `target_space_bytes`, for example logs from before this version or runs with `wait_for_replication: false` |
| `-V` shows `waiting` for a long time | The snapshot hasn't reached the target yet. Check the replication connection status on the source (`purearray connection list --csv`) and the transfer on the target. |
| `-V` shows `running` but no % | The target didn't report a Progress value for this transfer. Completion is still detected from the Completed column. |
| `GUI unavailable (No module named 'tkinter')` | Install the Tk package for your distro (section 3). |
| `No display found` | Use a desktop session or `ssh -X`, or use `--cli`, `--no-gui`, `--export-csv` or `--export-png`. |
| `--cli` bars or lines wrap | Widen the terminal, set `--width` to your terminal width, or pipe to `less -S`. |
| `git clone` asks for a password, or `curl` returns 404 | The repository is private. Use a token, an SSH key or `gh` (section 4). |
| `--remote needs the paramiko package` | Run `py -m pip install paramiko` (Windows) or `python3 -m pip install paramiko` (Linux, in a virtual environment). |
| `--remote asks for a password, so run it in a Command Prompt or PowerShell window` | Start the script from PowerShell or Command Prompt, not IDLE, `pythonw` or a double-clicked `.pyw` file. |
| `--remote: can't connect to host:22` | Check the hostname, and that the Windows computer can reach the server on port 22 (`Test-NetConnection host -Port 22` in PowerShell). Add `:port` to the hostname if SSH uses another port. |
| `Wrong username or password` | Type the Linux username and password. After 3 tries the script stops. |
| `doesn't allow password logins` | The server allows SSH key logins only. Ask its administrator to allow password logins (`PasswordAuthentication yes`), or run the report on the server itself. |
| `the host key of x has CHANGED` | The password wasn't sent. Don't connect until the server's administrator confirms the new key. Then run `ssh-keygen -R x` in PowerShell, or delete the line for the server in `C:\Users\<you>\.ssh\known_hosts`, and connect again. |
| `logged in ..., but it doesn't allow SFTP` | Ask the server's administrator to enable `Subsystem sftp` in `/etc/ssh/sshd_config`. |
| `can't list /var/log/pure on x: Permission denied` | The account you logged in with can't read the logs. Log in as the account that runs `pg_dr_refresh.py`, or ask the administrator for read access to the folder (for example through a group). |
| `no log files found on ...` | Give the log folder on the server with `-l`, for example `-l /var/log/pure`, or the config with `-c`. |
| `... is a Windows path` | Git Bash changed the path. Use PowerShell or Command Prompt, or run `export MSYS_NO_PATHCONV=1` first. |
| `No module named 'matplotlib'` or matplotlib older than 3.5 | Install it as in section 3. `--cli`, `--no-gui` and `--export-csv` work without it. |
| `externally-managed-environment` from pip | Use the distro package or a virtual environment (section 3). |
| Report shows no data | Check the log path (`-c` uses `log_file`; `-l` takes a folder or files) and that `wait_for_replication` is `true`. |
