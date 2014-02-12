CREATE FUNCTION oracle_diag(name DEFAULT NULL) RETURNS text
AS 'MODULE_PATHNAME'
LANGUAGE C STABLE CALLED ON NULL INPUT;

COMMENT ON FUNCTION oracle_diag(name)
IS 'shows the version of oracle_fdw, Oracle client and Oracle server';
