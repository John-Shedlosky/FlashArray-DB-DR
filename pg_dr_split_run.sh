#!/bin/bash
# =============================================================================
# pg_dr_split_run.sh - DR refresh with the databases in backup mode for as short
# a time as possible:
#
#   1. put the databases in backup mode            (begin_backup, below)
#   2. snapshot the source protection groups       pg_dr_refresh.py -S
#   3. take the databases out of backup mode       (end_backup, below)
#   4. overwrite the DR volumes and replicate      pg_dr_refresh.py -R
#
# Backup mode is always ended, even if step 2 fails or the script is stopped.
# Run it from a systemd timer or cron (README section 9), as the account that
# has the SSH key for the arrays.
#
# Exit code: the worst of the steps - 0 all OK, 1 something failed (see the log),
# 2 config / usage error.
# =============================================================================
set -u

PYTHON=/usr/bin/python3                     # or python3.11, or /opt/pure-dr/venv/bin/python
SCRIPT=/opt/pure-dr/pg_dr_refresh.py
CONFIG=/opt/pure-dr/pg_dr_config.json

# ---- Fill these in: the commands that put your databases in and out of backup mode.
# They run as the account that runs this script, so that account needs the database
# rights (or an ssh / sudo to the account that has them). Return non-zero on failure.
# Example (Oracle, archivelog mode):
#   echo "ALTER DATABASE BEGIN BACKUP;" | sqlplus -s / as sysdba
#   echo "ALTER DATABASE END BACKUP;"   | sqlplus -s / as sysdba
begin_backup() {
    return 0
}
end_backup() {
    return 0
}
# ---------------------------------------------------------------------------------

log() { echo "$(date '+%Y-%m-%d %H:%M:%S') pg_dr_split_run: $*"; }

IN_BACKUP=0
cleanup() {                                 # never leave the databases in backup mode
    if [ "$IN_BACKUP" -eq 1 ]; then
        IN_BACKUP=0
        log "ending backup mode (script stopped early)"
        end_backup || log "ERROR: ending backup mode failed - check the databases now"
    fi
}
trap cleanup EXIT
trap 'exit 130' INT TERM

log "putting the databases in backup mode"
if ! begin_backup; then
    log "ERROR: couldn't put the databases in backup mode; nothing was done"
    exit 1
fi
IN_BACKUP=1

"$PYTHON" "$SCRIPT" -c "$CONFIG" -S
SNAP_RC=$?

log "taking the databases out of backup mode"
IN_BACKUP=0
END_RC=0
if ! end_backup; then
    log "ERROR: ending backup mode failed - check the databases now"
    END_RC=1
fi

if [ "$SNAP_RC" -eq 2 ]; then
    log "ERROR: -S stopped with a config / usage error; -R not run"
    exit 2
fi

"$PYTHON" "$SCRIPT" -c "$CONFIG" -R      # uses whichever snapshots -S took (fails, exit 2, if none)
REP_RC=$?

RC=$SNAP_RC
[ "$REP_RC" -gt "$RC" ] && RC=$REP_RC
[ "$END_RC" -gt "$RC" ] && RC=$END_RC
log "finished: -S exit $SNAP_RC, -R exit $REP_RC, overall exit $RC"
exit "$RC"
