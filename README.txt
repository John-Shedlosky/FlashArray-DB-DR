Pure DR Refresh and Reporting
Two Python scripts that refresh DR copies of FlashArray volumes, replicate them to a second array, and report on how much data changes and how long replication takes.

Test Drive Cheat Sheet

sudo dnf install -y python3 python3-pip python3-tkinter openssh-clients git

python3 --version 

sudo mkdir -p /opt/pure-dr /var/log/pure

sudo chown "$USER" /opt/pure-dr /var/log/pure

git clone https://github.com/John-Shedlosky/FlashArray-DB-DR.git /opt/pure-dr

sudo chmod 755 /opt/pure-dr/*.py

ls /opt/pure-dr

 ssh-keygen -t rsa -f ~/.ssh/id_rsa_pure -N ""

 chmod 600 ~/.ssh/id_rsa_pure

 cat ~/.ssh/id_rsa_pure.pub

[copy and paste into flasharray1 and flasharray2]
 ssh jjones@flasharray1

 pureadmin setattr --publickey

 ssh jjones@flasharray2

 pureadmin setattr --publickey

ssh -i ~/.ssh/id_rsa_pure jjones@flasharray1
ssh -i ~/.ssh/id_rsa_pure jjones@flasharray2

cd /opt/pure-dr/

python3 pg_dr_refresh.py -c pg_dr_config.json --setup












File
	Purpose
	pg_dr_refresh.py
	Snapshots each source protection group, overwrites the matching DR volumes from that snapshot, snapshots the DR protection group, replicates it to the target array, and logs data replicated and replication time per protection group and per volume.
	pure-dr-report.py
	Reads the refresh logs and shows sizes, daily change, replication time, and weekly or monthly totals with comparisons to the previous period. Opens a window, prints terminal bar charts (--cli) or a text report, or exports CSV and PNG files.
	pg_dr_config.json
	The configuration used by both scripts. Every setting is explained in comments inside the file.
	test_env_setup_commands.txt
	Purity CLI commands that build a matching test environment (volumes, protection groups, schedules, retention) on two arrays.
	Contents
1. How it works
2. Requirements
3. Install Python and the libraries on Linux
4. Download and install the scripts
5. Set up SSH key login to the source array
6. Prepare the arrays
7. Configure
8. Run the refresh
9. Schedule the refresh
10. Logs
11. Reports
12. Troubleshooting
1. How it works
The refresh script logs in to the source array over SSH and runs Purity//FA CLI commands. It never logs in to the target array.


For each protection group in the config, in order:


Step
	What happens
	Purity command
	0
	Check every DR volume exists and record its size
	purevol list --csv
	1
	Snapshot the source protection group
	purepgroup snap --apply-retention --suffix <suffix> PG1
	2
	Check the snapshot contains every source volume. If one is missing, nothing in that group is overwritten.
	purevol list --snap --pgrouplist PG1.<suffix>
	3
	Overwrite each DR volume from its source volume snapshot
	purevol copy --overwrite PG1.<suffix>.vol-data-01 vol-data-01-dr
	4
	Snapshot the DR protection group and start replication
	purepgroup snap --apply-retention --replicate-now --suffix <suffix> PG1-DR
	

The script starts every group's replication before it waits for any of them. It then monitors all replications together until each one completes or times out:


* purepgroup list --snap --transfer --csv for each DR group snapshot
* purevol list --snap --transfer --csv for each volume in it


Snapshots are named <pgroup>.<prefix>-YYYYMMDD-HHMMSS (for example PG1.dr-20261104-020000). Every group in one run uses the same suffix.


Snapshot lifetime follows the retention policy already set on each protection group (--apply-retention), so the script never deletes snapshots.


After monitoring, the script writes a summary and one DRDATA {json} line per protection group and per volume to the log. The report script reads these lines.
2. Requirements
Item
	Needed for
	Version
	Python
	Both scripts
	3.8 or newer
	OpenSSH client (ssh)
	pg_dr_refresh.py
	Any current version
	git, or curl / wget
	Downloading the scripts from GitHub
	Any current version
	SSH key login to the source array
	pg_dr_refresh.py
	See section 5
	matplotlib
	pure-dr-report.py window and --export-png
	3.5 or newer
	Tk (tkinter)
	pure-dr-report.py window
	Shipped with your distro's Python
	A display (desktop session or ssh -X)
	pure-dr-report.py window
	

	

pg_dr_refresh.py uses only the Python standard library. pure-dr-report.py needs nothing extra for --cli, --no-gui and --export-csv.


Older distros ship a default python3 that is too old: RHEL / Rocky / Alma 8 and SLES 15 come with Python 3.6. On those, install a newer Python alongside it as shown below. Don't replace or upgrade the system python3, because the OS tools depend on it.
3. Install Python and the libraries on Linux
Pick the section for your distro. Each one installs Python, the OpenSSH client, Tk and matplotlib. If you only run the refresh script on a server, you can skip Tk and matplotlib there.
RHEL / Rocky Linux / AlmaLinux 9
sudo dnf install -y python3 python3-pip python3-tkinter openssh-clients
python3 --version          # 3.9 or newer

matplotlib comes from EPEL, or use a virtual environment (see Virtual environment).


Rocky Linux / AlmaLinux 9:


sudo dnf config-manager --set-enabled crb
sudo dnf install -y epel-release
sudo dnf install -y python3-matplotlib

RHEL 9:


sudo subscription-manager repos --enable codeready-builder-for-rhel-9-$(arch)-rpms
sudo dnf install -y https://dl.fedoraproject.org/pub/epel/epel-release-latest-9.noarch.rpm
sudo dnf install -y python3-matplotlib
RHEL / Rocky Linux / AlmaLinux 8
The default python3 is 3.6, so install Python 3.11 from AppStream:


sudo dnf install -y python3.11 python3.11-pip python3.11-tkinter openssh-clients
python3.11 --version       # 3.11.x

Install matplotlib in a virtual environment built with python3.11 (see Virtual environment). Then run the scripts with that environment's Python or with python3.11, never with plain python3.
Ubuntu 20.04+ / Debian 11+
sudo apt update
sudo apt install -y python3 python3-venv python3-pip python3-tk python3-matplotlib openssh-client
python3 --version          # 3.8 or newer
python3 -c "import matplotlib; print(matplotlib.__version__)"   # must be 3.5 or newer

On Ubuntu 20.04 (matplotlib 3.1) and Debian 11 (matplotlib 3.3), the distro matplotlib is too old for the report window. Use a virtual environment for matplotlib instead.


Debian 12 and Ubuntu 23.04+ block pip install and pip install --user outside a virtual environment ("externally-managed-environment"). Use the apt package or a virtual environment.
Fedora
sudo dnf install -y python3 python3-tkinter python3-matplotlib openssh-clients
SUSE Linux Enterprise / openSUSE Leap 15
The default python3 is 3.6, so install Python 3.11:


sudo zypper install -y python311 python311-pip python311-tk openssh-clients
python3.11 --version

Install matplotlib in a virtual environment built with python3.11. On SLES you may first need to enable the Python 3 module: sudo SUSEConnect -p sle-module-python3/15.<SP>/x86_64.
Virtual environment (any distro)
A virtual environment keeps matplotlib separate from the system Python and works on every distro above. It uses the Tk package you installed with the system packages above.


# Use python3.11 instead of python3 on RHEL 8 / SLES 15
python3 -m venv /opt/pure-dr/venv
/opt/pure-dr/venv/bin/python -m pip install --upgrade pip
/opt/pure-dr/venv/bin/python -m pip install "matplotlib>=3.5"

Then run the scripts with /opt/pure-dr/venv/bin/python, for example:


/opt/pure-dr/venv/bin/python /opt/pure-dr/pure-dr-report.py -c /opt/pure-dr/pg_dr_config.json

If the server has no internet access, download the wheels on another machine with pip download "matplotlib>=3.5" -d wheels (same Python version and CPU type), copy the folder across, and run pip install --no-index --find-links wheels matplotlib.
Check the installation
python3 --version                                            # 3.8+
python3 -c "import tkinter; tkinter.Tk().destroy(); print('Tk OK')"     # needs a display
python3 -c "import matplotlib; print('matplotlib', matplotlib.__version__)"
ssh -V

Replace python3 with python3.11 or your virtual environment's Python where that applies.
4. Download and install the scripts
The files are in the GitHub repository John-Shedlosky/FlashArray-DB-DR. These steps install them in /opt/pure-dr, with logs in /var/log/pure. Run them as the account that will run the refresh.


Create the folders first and give them to that account:


sudo mkdir -p /opt/pure-dr /var/log/pure
sudo chown "$USER" /opt/pure-dr /var/log/pure
Option A: git clone (recommended)
Install git if needed:


* RHEL, Rocky, Alma or Fedora: sudo dnf install -y git
* Ubuntu or Debian: sudo apt install -y git
* SLES or openSUSE: sudo zypper install -y git


Then clone the repository into the folder:


git clone https://github.com/John-Shedlosky/FlashArray-DB-DR.git /opt/pure-dr
chmod 755 /opt/pure-dr/*.py
ls /opt/pure-dr

To update later:


cd /opt/pure-dr && git pull

git pull keeps your edits to pg_dr_config.json unless that file also changed in the repository. If it did, run git stash && git pull && git stash pop.
Option B: download without git (curl or wget)
This downloads the current default branch as an archive and unpacks it straight into /opt/pure-dr:


curl -fsSL https://github.com/John-Shedlosky/FlashArray-DB-DR/archive/HEAD.tar.gz | tar -xz --strip-components=1 -C /opt/pure-dr
# or, with wget:
wget -qO- https://github.com/John-Shedlosky/FlashArray-DB-DR/archive/HEAD.tar.gz | tar -xz --strip-components=1 -C /opt/pure-dr
chmod 755 /opt/pure-dr/*.py

To download single files instead:


cd /opt/pure-dr
for f in pg_dr_refresh.py pure-dr-report.py pg_dr_config.json README.md test_env_setup_commands.txt; do
  curl -fsSLO "https://raw.githubusercontent.com/John-Shedlosky/FlashArray-DB-DR/HEAD/$f"
done
chmod 755 *.py

These commands assume the files are at the top level of the repository. If they are in a subfolder, add it to the raw URL, for example .../HEAD/scripts/$f. With the archive, copy the files out of that subfolder after unpacking.
If the repository is private
The commands above work without logging in only if the repository is public. For a private repository, use one of these:


* HTTPS with a personal access token: run the git clone above. When asked, enter your GitHub username, and paste a token (from GitHub Settings > Developer settings > Personal access tokens, with read access to the repository) as the password. GitHub doesn't accept account passwords here.


* SSH key: add your public key to GitHub (Settings > SSH and GPG keys), then run:


git clone git@github.com:John-Shedlosky/FlashArray-DB-DR.git /opt/pure-dr

* GitHub CLI: run gh auth login, then gh repo clone John-Shedlosky/FlashArray-DB-DR /opt/pure-dr.


* curl with a token:


curl -fsSL -H "Authorization: Bearer $GITHUB_TOKEN" https://api.github.com/repos/John-Shedlosky/FlashArray-DB-DR/tarball | tar -xz --strip-components=1 -C /opt/pure-dr
Check the files
ls -l /opt/pure-dr
python3 /opt/pure-dr/pg_dr_refresh.py --help
python3 /opt/pure-dr/pure-dr-report.py --help

You should see pg_dr_refresh.py, pure-dr-report.py, pg_dr_config.json, README.md and test_env_setup_commands.txt.


Both scripts start with #!/usr/bin/env python3. If that is older than 3.8 on your system, run them with an explicit interpreter, for example python3.11 /opt/pure-dr/pg_dr_refresh.py ....
5. Set up SSH key login to the source array
The refresh script uses non-interactive SSH (BatchMode=yes), so password prompts and unknown host keys make it fail. Set up the key once, as the account that will run the script.


1. Create a key with no passphrase, at the path in source_array.ssh_key:


ssh-keygen -t rsa -b 4096 -f ~/.ssh/id_rsa_pure -N ""
chmod 600 ~/.ssh/id_rsa_pure
cat ~/.ssh/id_rsa_pure.pub

2. Log in to the source array as the Purity user named in source_array.ssh_user and add the public key. The key is set for the user you are logged in as. Paste the whole ssh-rsa AAAA... line at the prompt:


ssh pureuser@flasharray1
pureadmin setattr --publickey

3. Accept the array's host key and test the login. This must not prompt for a password:


ssh -i ~/.ssh/id_rsa_pure pureuser@flasharray1 "purearray list"

Use a local Purity account. Keys can't be set for directory (LDAP / AD) users. The account needs rights to create snapshots, copy volumes and replicate, which the storage_admin or array_admin role provides. If you use an alternate controller address or VIP, accept its host key too.
6. Prepare the arrays
The arrays must already be connected for asynchronous replication, and the DR volumes must already exist. test_env_setup_commands.txt has every command, including how to:


* connect the arrays
* create matching test volumes and protection groups
* set schedules and retention
* allow the protection groups on the target


The essentials are:


* Connect the arrays: on the target, run purearray list --connection-key. On the source, run purearray connect --management-address <target> --type async-replication --connection-key.
* Each DR protection group (PG1-DR) contains the DR volumes and has the target array in its target list.
* Retention on each protection group is set the way you want, because the script follows it: purepgroup retain ....


--setup can create the DR protection groups and add the target for you. It does not change retention; it prints the current retention so you can review it:


python3 pg_dr_refresh.py -c pg_dr_config.json --setup
7. Configure
Edit pg_dr_config.json. It is JSON with // comments, and each setting is explained in the file. The main ones:


Setting
	Meaning
	source_array.host
	Management FQDN, alias or VIP the script SSHes to
	source_array.ssh_user, ssh_key
	Purity user and private key from section 5 (ssh_key: null uses ssh-agent / ~/.ssh defaults)
	target_array.name
	Purity name of the replication target, exactly as listed in purearray connection list
	log_file
	Base log path. A relative path is relative to the config file's folder.
	settings.wait_for_replication
	true waits for every transfer so data and time are logged. This is needed for the reports.
	settings.poll_interval_seconds
	Seconds between transfer checks (minimum 1)
	settings.replication_timeout_minutes
	Give up on one replication after this many minutes (minimum 1)
	protection_groups[]
	Each entry: source_pgroup, dr_pgroup, and volumes as {target, source} pairs
	

The script checks the whole config before it touches the array. It lists every problem it finds, for example a misspelled setting, a missing name or a duplicate target volume, and exits with code 2.
8. Run the refresh
Always start with a dry run. It prints every command that would run and changes nothing:


cd /opt/pure-dr
python3 pg_dr_refresh.py -c pg_dr_config.json --dry-run

Then run it for real. -V also shows each CLI command, the array output, and a progress bar for each replication:


python3 pg_dr_refresh.py -c pg_dr_config.json -V
python3 pg_dr_refresh.py -c pg_dr_config.json

Option
	Effect
	-c FILE
	Config file (required)
	-V, --verbose
	Show commands, array output and progress bars, and wait for replication even if wait_for_replication is false
	--dry-run
	Log the commands only. Nothing is changed and no DRDATA is written.
	--setup
	One-time: create DR protection groups and add the target. Retention isn't changed.
	

Exit codes:


Code
	Meaning
	0
	Every protection group succeeded
	1
	At least one protection group failed (see the log)
	2
	Config or usage error (nothing was changed)
	

A failure in one protection group doesn't stop the others.


On some Purity releases, listing transfers on the replication source doesn't report a Progress column, because progress is only known on the target. The script detects completion from the Completed column, so this doesn't affect results. The -V line then shows running with the data transferred so far instead of a growing bar.
9. Schedule the refresh
Run the refresh from cron as the account that has the SSH key. Use full paths, because cron starts in the home directory with a minimal PATH.


crontab -e

# DR refresh every day at 02:00 (use python3.11 or /opt/pure-dr/venv/bin/python where needed)
0 2 * * * /usr/bin/python3 /opt/pure-dr/pg_dr_refresh.py -c /opt/pure-dr/pg_dr_config.json >> /var/log/pure/pg_dr_refresh_cron.log 2>&1

Make sure one run finishes before the next starts. Watch the replication times in the report, and keep replication_timeout_minutes below the gap between runs.
10. Logs
Every run appends to a weekly file named after the Monday of the current week:


/var/log/pure/pg_dr_refresh_20261102.log
/var/log/pure/pg_dr_refresh_20261109.log

Each file contains:


* every command and the array's output
* replication progress
* per-volume completions
* the summary
* the DRDATA lines


DRDATA records are dated by the run's start time, so a run that crosses midnight counts on the day it started. Each record has a status:


Status
	Meaning
	ok
	Replication completed. Data, physical bytes written and time are recorded.
	failed
	The group failed, either before replication started (the group and each of its volumes are recorded as failed) or during monitoring
	started
	wait_for_replication was false, so replication was started but not measured
	

Old log files aren't deleted automatically. Keep as many weeks as you want to report on, or rotate them with your usual tooling.
11. Reports
python3 pure-dr-report.py -c pg_dr_config.json          # logs from the config's log_file location
python3 pure-dr-report.py -l /var/log/pure               # or a folder, files or globs
Window
The window has a period picker, a protection group filter, and these tabs:


* Overview: each protection group and its volumes, with:
   * size
   * average change per day, and that change as % of size
   * peak day and last run
   * average and maximum replication time
   * OK and failed runs
* Sizes: provisioned size of each DR volume
* Daily change: data replicated per day, stacked by volume, and change as % of size
* Replication time: time per run per group, and per-volume average and maximum
* Weekly / Monthly: totals per week or month by protection group and volume, compared with the previous week or month, and the selected period compared with the previous period


The window needs Tk, matplotlib and a display. On a server without a desktop, connect with ssh -X user@server, or use --cli or the text, CSV and PNG options below. If no display is found, the script prints the text report instead.
Periods
Period
	Option
	Compared with
	Last 7 days
	--period last7
	The 7 days before
	Last 30 days
	--period last30
	The 30 days before
	Last N days
	--days N
	The N days before
	Previous week (Mon–Sun)
	--period prev-week
	The week before it
	Previous month
	--period prev-month
	The month before it
	This month
	--period this-month
	The same days of last month
	All (default)
	--period all
	Nothing
	Custom date range
	--from YYYY-MM-DD --to YYYY-MM-DD
	The same number of days immediately before
	

--as-of YYYY-MM-DD runs any report as if that date were today. --pgroup NAME limits everything to one protection group (source or DR name).
Terminal charts (--cli)
--cli prints the whole report in the terminal, with no window, display or matplotlib needed. Bar charts are drawn with # (or *), and the numbers are printed next to every bar.


python3 pure-dr-report.py -c pg_dr_config.json --cli
python3 pure-dr-report.py -c pg_dr_config.json --cli --period prev-month
python3 pure-dr-report.py -c pg_dr_config.json --cli --period last30 --summary both --pgroup PG1
python3 pure-dr-report.py -c pg_dr_config.json --cli --bar-char '*' --width 140 | less -S

It prints these sections, in order:


Section
	Bars show
	Numbers beside each bar
	Overview
	(table)
	Size, average per day, % per day, last run, average and maximum time, OK and failed runs, status
	DR volume sizes
	Provisioned size per volume, grouped by protection group
	Size and protection group total
	Daily change
	Data replicated per day, stacked by protection group (# = first group, * = second, then =, + and so on)
	Total per day, failed runs, and the period's average and total
	Average daily change rate
	Change per day as % of each volume's size
	%/day and GiB/day, per volume and per group
	Replication time per day
	# up to the average, then * up to the maximum, per protection group
	Average, maximum, runs and failed runs
	Replication time per volume
	# up to the average, then * up to the maximum
	Average and maximum
	Period comparison
	now (this period) above prev (previous period), average per day
	Average per day, change %, total, OK and failed runs
	Weekly / Monthly
	All groups stacked, then each group, average per day
	Average per day, change vs previous week or month, total, average and maximum time, OK and failed runs. "(in progress)" marks the current week or month.
	

Example (previous week, --width 100):


DAILY CHANGE - data replicated per day   (1 char = 2.96 GiB;  # = PG1   * = PG2)
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

Options for --cli:


* Scale: each chart has its own scale, shown in its heading as 1 char = .... Bars in one chart can be compared with each other, but not with bars in another chart.
* --bar-char C: the bar character, # by default. Previous-period and up-to-maximum bars use *, or # when C is *.
* --width N: sets the output width. By default the script uses the terminal width, or 120 characters when the output goes to a file or pipe. The Overview table is a fixed 128 characters wide, so use less -S or a wide terminal to read it in full.
* Long periods: for periods over 45 days, such as --period all, the daily-change and replication-time charts show one row per week (average per day) instead of one per day.
* Other options: --period, --from / --to, --days, --as-of, --pgroup and --summary work the same as for the other report modes.
* Saving the output: use --cli > report.txt to save it, or pipe it to mail.
Text, CSV and PNG output
python3 pure-dr-report.py -c pg_dr_config.json --period prev-month --no-gui
python3 pure-dr-report.py -c pg_dr_config.json --period last30 --no-gui --summary both
python3 pure-dr-report.py -c pg_dr_config.json --from 2026-09-01 --to 2026-09-30 --export-csv ~/reports/sept.csv
python3 pure-dr-report.py -c pg_dr_config.json --period prev-month --export-png ~/reports/oct

* --no-gui prints tables only, with no bar charts: the Overview, the comparison with the previous period, and a weekly or monthly breakdown. --summary takes auto (weekly for periods under 5 weeks, otherwise monthly), week, month, both or none.
* --export-csv FILE writes four files:
   * FILE: per-run data, one row per protection group or volume per run, including errors
   * FILE_period.csv: the period compared with the previous period
   * FILE_weekly.csv and FILE_monthly.csv: the breakdowns
   * Sizes are given in bytes and GiB, times in seconds and H:MM:SS, changes in %.
* --export-png DIR saves the Sizes, Daily change, Replication time, Weekly and Monthly charts. It doesn't need Tk or a display.
How the numbers are calculated
* Avg / day: data replicated in the period divided by the calendar days in the period.
   * Days before the first log aren't counted, and neither is today until its first run.
   * Any other day without a successful run counts as zero.
   * Every tab uses this rule, so weeks, months and partial periods compare fairly.
* Avg/day chg: the like-for-like comparison. Data chg compares raw totals and looks large for a period still in progress.
* Replication times: averaged over successful runs. Failed runs are counted but add no data. started runs appear as the last status but aren't counted as OK or failed.
* Weekly / Monthly: whole calendar weeks (Mon–Sun) or months that overlap the period. * marks a week or month still in progress. A bucket that starts before the first log is labelled "logs start".
12. Troubleshooting
Symptom
	Fix
	Permission denied (publickey) or Host key verification failed
	Repeat section 5 as the account that runs the script. Check ssh_user and ssh_key in the config.
	needs Python 3.8 or newer
	Run with python3.11 or a virtual environment (section 3).
	Config errors: ...
	Fix each listed item in pg_dr_config.json. Nothing was changed on the array.
	Target 'x' not found in connected arrays
	Set target_array.name to the name shown by purearray connection list on the source.
	... does not contain: vol-x - nothing overwritten
	The source volume isn't a member of the source protection group. Add it, or fix the config.
	Volume does not exist at step 0
	Create the DR volume first (see test_env_setup_commands.txt).
	Replication not complete after N min
	Check purepgroup list --snap --transfer on the source and target, and check that the group is allowed on the target. Raise replication_timeout_minutes if needed.
	-V shows running but no %
	Expected on Purity releases that don't report Progress on the source. Completion is still detected.
	GUI unavailable (No module named 'tkinter')
	Install the Tk package for your distro (section 3).
	No display found
	Use a desktop session or ssh -X, or use --cli, --no-gui, --export-csv or --export-png.
	--cli bars or lines wrap
	Widen the terminal, set --width to your terminal width, or pipe to less -S.
	git clone asks for a password, or curl returns 404
	The repository is private. Use a token, an SSH key or gh (section 4).
	No module named 'matplotlib' or matplotlib older than 3.5
	Install it as in section 3. --cli, --no-gui and --export-csv work without it.
	externally-managed-environment from pip
	Use the distro package or a virtual environment (section 3).
	Report shows no data
	Check the log path (-c uses log_file; -l takes a folder or files) and that wait_for_replication is true.
	

Co-authored with Glean
