MODULE_big = oracle_fdw
OBJS = oracle_fdw.o oracle_utils.o

EXTENSION = oracle_fdw
DATA = oracle_fdw--1.0.sql

# Oracle's shared library is oci.dll on Windows and libclntsh elsewhere
ORACLE_SHLIB=$(if $(findstring win32,$(PORTNAME)),oci,clntsh)

# add include and library paths for both Instant Client and regular Client
PG_CPPFLAGS = -I$(ORACLE_HOME)/sdk/include -I$(ORACLE_HOME)/oci/include -I$(ORACLE_HOME)/rdbms/public
SHLIB_LINK = -L$(ORACLE_HOME) -L$(ORACLE_HOME)/bin -L$(ORACLE_HOME)/lib -l$(ORACLE_SHLIB)

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
