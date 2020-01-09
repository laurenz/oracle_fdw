CREATE FUNCTION oracle_fdw_handler() RETURNS fdw_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

COMMENT ON FUNCTION oracle_fdw_handler()
IS 'Oracle foreign data wrapper handler';

CREATE FUNCTION oracle_fdw_validator(text[], oid) RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

COMMENT ON FUNCTION oracle_fdw_validator(text[], oid)
IS 'Oracle foreign data wrapper options validator';

CREATE FUNCTION oracle_close_connections() RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

COMMENT ON FUNCTION oracle_close_connections()
IS 'closes all open Oracle connections';

CREATE FUNCTION oracle_diag(name DEFAULT NULL) RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE CALLED ON NULL INPUT;

COMMENT ON FUNCTION oracle_diag(name)
IS 'shows the version of oracle_fdw, PostgreSQL, Oracle client and Oracle server';

CREATE FUNCTION oracle_execute(server name, statement text) RETURNS void
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

COMMENT ON FUNCTION oracle_execute(name, text)
IS 'executes an arbitrary SQL statement with no results on the Oracle server';

CREATE FOREIGN DATA WRAPPER oracle_fdw
  HANDLER oracle_fdw_handler
  VALIDATOR oracle_fdw_validator;

COMMENT ON FOREIGN DATA WRAPPER oracle_fdw
IS 'Oracle foreign data wrapper';
