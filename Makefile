MODULE_big = oracle_fdw
OBJS = oracle_fdw.o oracle_utils.o oracle_gis.o
EXTENSION = oracle_fdw
DATA = oracle_fdw--1.2.sql oracle_fdw--1.0--1.1.sql oracle_fdw--1.1--1.2.sql
DOCS = README.oracle_fdw
REGRESS = oracle_fdw oracle_gis oracle_import oracle_join

# add include and library paths for both Instant Client and regular Client
PG_CPPFLAGS = -I"$(ORACLE_HOME)/sdk/include" -I"$(ORACLE_HOME)/oci/include" -I"$(ORACLE_HOME)/rdbms/public" -I"$(ORACLE_HOME)/" -I/usr/include/oracle/21/client64 -I/usr/include/oracle/19.14/client64 -I/usr/include/oracle/19.12/client64 -I/usr/include/oracle/19.12/client -I/usr/include/oracle/19.11/client64 -I/usr/include/oracle/19.11/client -I/usr/include/oracle/19.10/client64 -I/usr/include/oracle/19.10/client -I/usr/include/oracle/19.9/client -I/usr/include/oracle/19.9/client64 -I/usr/include/oracle/19.8/client -I/usr/include/oracle/19.8/client64 -I/usr/include/oracle/19.6/client -I/usr/include/oracle/19.6/client64 -I/usr/include/oracle/19.3/client -I/usr/include/oracle/19.3/client64 -I/usr/include/oracle/18.5/client -I/usr/include/oracle/18.5/client64 -I/usr/include/oracle/18.3/client -I/usr/include/oracle/18.3/client64 -I/usr/include/oracle/12.2/client -I/usr/include/oracle/12.2/client64 -I/usr/include/oracle/12.1/client -I/usr/include/oracle/12.1/client64 -I/usr/include/oracle/11.2/client -I/usr/include/oracle/11.2/client64
SHLIB_LINK = -L"$(ORACLE_HOME)/" -L"$(ORACLE_HOME)/bin" -L"$(ORACLE_HOME)/lib" -L"$(ORACLE_HOME)/lib/amd64" -l$(ORACLE_SHLIB) -L/usr/lib/oracle/21/client64/lib -L/usr/lib/oracle/19.14/client64/lib -L/usr/lib/oracle/19.12/client64/lib -L/usr/lib/oracle/19.12/client/lib -L/usr/lib/oracle/19.11/client64/lib -L/usr/lib/oracle/19.11/client/lib -L/usr/lib/oracle/19.10/client64/lib -L/usr/lib/oracle/19.10/client/lib -L/usr/lib/oracle/19.9/client/lib -L/usr/lib/oracle/19.9/client64/lib -L/usr/lib/oracle/19.8/client/lib -L/usr/lib/oracle/19.8/client64/lib -L/usr/lib/oracle/19.6/client/lib -L/usr/lib/oracle/19.6/client64/lib -L/usr/lib/oracle/19.3/client/lib -L/usr/lib/oracle/19.3/client64/lib -L/usr/lib/oracle/18.5/client/lib -L/usr/lib/oracle/18.5/client64/lib -L/usr/lib/oracle/18.3/client/lib -L/usr/lib/oracle/18.3/client64/lib -L/usr/lib/oracle/12.2/client/lib -L/usr/lib/oracle/12.2/client64/lib -L/usr/lib/oracle/12.1/client/lib -L/usr/lib/oracle/12.1/client64/lib -L/usr/lib/oracle/11.2/client/lib -L/usr/lib/oracle/11.2/client64/lib

# don't build LLVM byte code
override with_llvm := no

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
