#!/usr/bin/env bash
set -euxo pipefail
# The container's oracle OS user authenticates locally as SYSDBA.
# Create SCOTT in the selected PDB of this job's fresh Oracle instance.
# SQL*Plus must explicitly turn SQL errors into a failing exit status.
{
  echo 'WHENEVER SQLERROR EXIT SQL.SQLCODE'
  echo "ALTER SESSION SET CONTAINER = $ORACLE_SERVICE;"
  cat .github/ci/oracle_setup.sql
} | docker exec -i "$ORACLE_CONTAINER_ID" sqlplus -s / as sysdba
# Check SCOTT login and the views used by EXPLAIN VERBOSE.
docker exec -i "$ORACLE_CONTAINER_ID" sqlplus -s "scott/tiger@localhost/$ORACLE_SERVICE" <<'SQL'
WHENEVER SQLERROR EXIT SQL.SQLCODE
SELECT COUNT(*) FROM v$sql;
SELECT COUNT(*) FROM v$sql_plan;
EXIT
SQL
