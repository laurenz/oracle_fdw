/*-------------------------------------------------------------------------
 *
 * oracle_fdw.h
 * 		This header file contains all definitions that are shared by
 * 		oracle_fdw.c and oracle_utils.c.
 * 		It is necessary to split oracle_fdw into two source files because
 * 		PostgreSQL and Oracle headers cannot be #included at the same time.
 *
 *-------------------------------------------------------------------------
 */

/* this one is safe to include and gives us Oid */
#include "postgres_ext.h"

#include <sys/types.h>

/* oracle_fdw version */
#define ORACLE_FDW_VERSION "1.2.0"

#ifdef OCI_ORACLE
/*
 * Types for a linked list for various handles.
 * Oracle sessions can be multiplexed over one server connection.
 */
struct connEntry
{
	char *user;
	OCISvcCtx *svchp;
	OCISession *userhp;
	OCIType *geomtype;
	struct handleEntry *handlelist;
	int xact_level;  /* 0 = none, 1 = main, else subtransaction */
	struct connEntry *next;
};

struct srvEntry
{
	char *connectstring;
	OCIServer *srvhp;
	struct srvEntry *next;
	struct connEntry *connlist;
};

struct envEntry
{
	char *nls_lang;
	OCIEnv *envhp;
	OCIError *errhp;
	struct envEntry *next;
	struct srvEntry *srvlist;
};

/*
 * Represents one Oracle connection, points to cached entries.
 * This is necessary to be able to pass them back to
 * oracle_fdw.c without having to #include oci.h there.
 */
struct oracleSession
{
	struct envEntry *envp;
	struct srvEntry *srvp;
	struct connEntry *connp;
	OCIStmt *stmthp;
};
#endif
typedef struct oracleSession oracleSession;

/* types for the Oracle table description */
typedef enum
{
	ORA_TYPE_VARCHAR2,
	ORA_TYPE_CHAR,
	ORA_TYPE_NVARCHAR2,
	ORA_TYPE_NCHAR,
	ORA_TYPE_NUMBER,
	ORA_TYPE_FLOAT,
	ORA_TYPE_BINARYFLOAT,
	ORA_TYPE_BINARYDOUBLE,
	ORA_TYPE_RAW,
	ORA_TYPE_DATE,
	ORA_TYPE_TIMESTAMP,
	ORA_TYPE_TIMESTAMPTZ,
	ORA_TYPE_INTERVALY2M,
	ORA_TYPE_INTERVALD2S,
	ORA_TYPE_BLOB,
	ORA_TYPE_CLOB,
	ORA_TYPE_BFILE,
	ORA_TYPE_LONG,
	ORA_TYPE_LONGRAW,
	ORA_TYPE_GEOMETRY,
	ORA_TYPE_OTHER
} oraType;

/* Some PostgreSQL versions have no constant definition for the OID of type uuid */
#ifndef UUIDOID
#define UUIDOID 2950
#endif

struct oraColumn
{
	char *name;              /* name in Oracle */
	oraType oratype;         /* data type in Oracle */
	int scale;               /* "scale" type modifier, used for NUMBERs */
	char *pgname;            /* PostgreSQL column name */
	int pgattnum;            /* PostgreSQL attribute number */
	Oid pgtype;              /* PostgreSQL data type */
	int pgtypmod;            /* PostgreSQL type modifier */
	int used;                /* is the column used in the query? */
	int pkey;                /* nonzero for primary keys, later set to the resjunk attribute number */
	char *val;               /* buffer for Oracle to return results in (LOB locator for LOBs) */
	long val_size;           /* allocated size in val */
	unsigned short val_len;  /* actual length of val */
	unsigned int val_len4;   /* actual length of val - for bind callbacks */
	short val_null;          /* indicator for NULL value */
	int varno;               /* index of this column's relation */
};

struct oraTable
{
	char *name;    /* name in Oracle */
	char *pgname;  /* for error messages */
	int ncols;     /* number of columns */
	int npgcols;   /* number of columns (including dropped) in the PostgreSQL foreign table */
	struct oraColumn **cols;
};

/* types to store parameter descriprions */

typedef enum {
	BIND_STRING,
	BIND_NUMBER,
	BIND_TIMESTAMP,
	BIND_LONG,
	BIND_LONGRAW,
	BIND_GEOMETRY,
	BIND_OUTPUT
} oraBindType;

struct paramDesc
{
	char *name;            /* name we give the parameter */
	Oid type;              /* PostgreSQL data type */
	oraBindType bindType;  /* which type to use for binding to Oracle statement */
	char *value;           /* value rendered for Oracle */
	void *node;            /* the executable expression */
	int colnum;            /* corresponding column in oraTable (-1 in SELECT queries unless output column) */
	void *bindh;           /* bind handle */
	struct paramDesc *next;
};

/* PostgreSQL error messages we need */
typedef enum
{
	FDW_ERROR,
	FDW_UNABLE_TO_ESTABLISH_CONNECTION,
	FDW_UNABLE_TO_CREATE_REPLY,
	FDW_UNABLE_TO_CREATE_EXECUTION,
	FDW_TABLE_NOT_FOUND,
	FDW_OUT_OF_MEMORY,
	FDW_SERIALIZATION_FAILURE
} oraError;

/* encapsulates an Oracle geometry object */
typedef struct
{
	struct sdo_geometry *geometry;
	struct sdo_geometry_ind *indicator;
} ora_geometry;

/*
 * functions defined in oracle_utils.c
 */
extern oracleSession *oracleGetSession(const char *connectstring, char *user, char *password, const char *nls_lang, const char *tablename, int curlevel);
extern void oracleCloseStatement(oracleSession *session);
extern void oracleCloseConnections(void);
extern void oracleShutdown(void);
extern void oracleEndTransaction(void *arg, int is_commit, int silent);
extern void oracleEndSubtransaction(void *arg, int nest_level, int is_commit);
extern int oracleIsStatementOpen(oracleSession *session);
extern struct oraTable *oracleDescribe(oracleSession *session, char *schema, char *table, char *pgname, long max_long);
extern void oracleEstimate(oracleSession *session, const char *query, double seq_page_cost, int block_size, double *startup_cost, double *total_cost, double *rows, int *width);
extern void oracleExplain(oracleSession *session, const char *query, int *nrows, char ***plan);
extern void oraclePrepareQuery(oracleSession *session, const char *query, const struct oraTable *oraTable);
extern int oracleExecuteQuery(oracleSession *session, const struct oraTable *oraTable, struct paramDesc *paramList);
extern int oracleFetchNext(oracleSession *session);
extern void oracleGetLob(oracleSession *session, void *locptr, oraType type, char **value, long *value_len, unsigned long trunc);
extern void oracleClientVersion(int *major, int *minor, int *update, int *patch, int *port_patch);
extern void oracleServerVersion(oracleSession *session, int *major, int *minor, int *update, int *patch, int *port_patch);
extern void *oracleGetGeometryType(oracleSession *session);

/*
 * functions defined in oracle_fdw.c
 */
extern char *oracleGetShareFileName(const char *relativename);
extern void oracleRegisterCallback(void *arg);
extern void oracleUnregisterCallback(void *arg);
extern void *oracleAlloc(size_t size);
extern void *oracleRealloc(void *p, size_t size);
extern void oracleFree(void *p);
extern void oracleError_d(oraError sqlstate, const char *message, const char *detail);
extern void oracleError_sd(oraError sqlstate, const char *message, const char *arg, const char *detail);
extern void oracleError_ssdh(oraError sqlstate, const char *message, const char *arg1, const char* arg2, const char *detail, const char *hint);
extern void oracleError_ii(oraError sqlstate, const char *message, int arg1, int arg2);
extern void oracleError_i(oraError sqlstate, const char *message, int arg);
extern void oracleError(oraError sqlstate, const char *message);
extern void oracleDebug2(const char *message);

/*
 * functions defined in oracle_gis.c
 */

extern ora_geometry *oracleEWKBToGeom(oracleSession *session, unsigned int ewkb_length, char *ewkb_data);
extern unsigned int oracleGetEWKBLen(oracleSession *session, ora_geometry *geom);
extern char *oracleFillEWKB(oracleSession *session, ora_geometry *geom, unsigned int size, char *dest);
extern void oracleGeometryFree(oracleSession *session, ora_geometry *geom);
extern void oracleGeometryAlloc(oracleSession *session, ora_geometry *geom);
