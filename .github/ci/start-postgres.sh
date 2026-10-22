#!/usr/bin/env bash
set -euxo pipefail
# UTF8 preserves the multibyte data used by the tests.
initdb -D "$HOME/pgdata" --locale=C.UTF-8 -E UTF8 --auth=trust
# Backends inherit TWO_TASK from the postmaster; the FDW sets NLS_LANG.
# The regression server uses dbserver '', so export before pg_ctl.
export TWO_TASK="//localhost:1521/$ORACLE_SERVICE"
pg_ctl -D "$HOME/pgdata" -l "$HOME/pgdata.log" -o "-p $PGPORT -k $PGHOST" start
pg_isready -q
# Check runtime dependencies before starting the regression suite.
psql -X -v ON_ERROR_STOP=1 -d postgres -c "LOAD 'oracle_fdw';"
