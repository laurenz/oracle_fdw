MODULE_big = oracle_fdw
OBJS = oracle_fdw.o oracle_utils.o oracle_gis.o
EXTENSION = oracle_fdw
DATA = oracle_fdw--1.1.sql oracle_fdw--1.0--1.1.sql
DOCS = README.oracle_fdw
REGRESS = oracle_fdw oracle_gis oracle_import oracle_fdw_join

# add include and library paths for both Instant Client and regular Client
PG_CPPFLAGS = -I$(ORACLE_HOME)/sdk/include -I$(ORACLE_HOME)/oci/include -I$(ORACLE_HOME)/rdbms/public -I/usr/include/oracle/12.2/client -I/usr/include/oracle/12.2/client64 -I/usr/include/oracle/12.1/client -I/usr/include/oracle/12.1/client64 -I/usr/include/oracle/11.2/client -I/usr/include/oracle/11.2/client64 -I/usr/include/oracle/11.1/client -I/usr/include/oracle/11.1/client64 -I/usr/include/oracle/10.2.0.5/client -I/usr/include/oracle/10.2.0.5/client64 -I/usr/include/oracle/10.2.0.4/client -I/usr/include/oracle/10.2.0.4/client64 -I/usr/include/oracle/10.2.0.3/client -I/usr/include/oracle/10.2.0.3/client64
SHLIB_LINK = -L$(ORACLE_HOME) -L$(ORACLE_HOME)/bin -L$(ORACLE_HOME)/lib -l$(ORACLE_SHLIB) -L/usr/lib/oracle/12.2/client/lib -L/usr/lib/oracle/12.2/client64/lib -L/usr/lib/oracle/12.1/client/lib -L/usr/lib/oracle/12.1/client64/lib -L/usr/lib/oracle/11.2/client/lib -L/usr/lib/oracle/11.2/client64/lib -L/usr/lib/oracle/11.1/client/lib -L/usr/lib/oracle/11.1/client64/lib -L/usr/lib/oracle/10.2.0.5/client/lib -L/usr/lib/oracle/10.2.0.5/client64/lib -L/usr/lib/oracle/10.2.0.4/client/lib -L/usr/lib/oracle/10.2.0.4/client64/lib -L/usr/lib/oracle/10.2.0.3/client/lib -L/usr/lib/oracle/10.2.0.3/client64/lib

ifdef NO_PGXS
subdir = contrib/oracle_fdw
top_builddir = ../..
include $(top_builddir)/src/Makefile.global
include $(top_srcdir)/contrib/contrib-global.mk
else
PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)
endif

# Oracle's shared library is oci.dll on Windows and libclntsh elsewhere
ifeq ($(PORTNAME),win32)
ORACLE_SHLIB=oci
else
ifeq ($(PORTNAME),cygwin)
ORACLE_SHLIB=oci
else
ORACLE_SHLIB=clntsh
endif
endif
