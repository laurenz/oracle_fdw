MODULE_big = oracle_fdw
OBJS = oracle_fdw.o oracle_utils.o oracle_gis.o
EXTENSION = oracle_fdw
DATA = oracle_fdw--1.2.sql oracle_fdw--1.0--1.1.sql oracle_fdw--1.1--1.2.sql
DOCS = README.oracle_fdw
REGRESS = oracle_fdw oracle_gis oracle_import oracle_join

# try to find Instant Client installations installed in standard paths
FIND_INCLUDE := $(wildcard /usr/include/oracle/*/client64 /usr/include/oracle/*/client)
FIND_LIBDIRS := $(wildcard /usr/lib/oracle/*/client64/lib /usr/lib/oracle/*/client/lib)

FIND_CPPFLAGS = $(foreach DIR,$(FIND_INCLUDE),-I$(DIR))
FIND_LDFLAGS = $(foreach DIR,$(FIND_LIBDIRS),-L$(DIR))

# add include and library paths for both Instant Client and regular Client
PG_CPPFLAGS = -I"$(ORACLE_HOME)/sdk/include" -I"$(ORACLE_HOME)/oci/include" -I"$(ORACLE_HOME)/rdbms/public" -I"$(ORACLE_HOME)/" $(FIND_CPPFLAGS)
SHLIB_LINK = -L"$(ORACLE_HOME)/" -L"$(ORACLE_HOME)/bin" -L"$(ORACLE_HOME)/lib" -L"$(ORACLE_HOME)/lib/amd64" $(FIND_LDFLAGS) -l$(ORACLE_SHLIB)

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
