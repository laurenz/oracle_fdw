-- Create SCOTT and grant regression-test privileges in the selected PDB.
-- V$SQL and V$SQL_PLAN are required by EXPLAIN VERBOSE.

CREATE USER scott IDENTIFIED BY tiger
   DEFAULT TABLESPACE users
   QUOTA UNLIMITED ON users;

GRANT CREATE SESSION, CREATE TABLE, CREATE VIEW, CREATE MATERIALIZED VIEW TO scott;
GRANT SELECT ON sys.v_$sql TO scott;
GRANT SELECT ON sys.v_$sql_plan TO scott;

EXIT
