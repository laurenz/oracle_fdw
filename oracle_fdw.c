/*-------------------------------------------------------------------------
 *
 * oracle_fdw.c
 * 		PostgreSQL-related functions for Oracle foreign data wrapper.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "access/htup_details.h"
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_collation.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "commands/defrem.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
/* for "hash_bytes_extended" or "hash_bytes" */
#if PG_VERSION_NUM >= 130000
#include "common/hashfn.h"
#elif PG_VERSION_NUM >= 120000
#include "utils/hashutils.h"
#else
#include "access/hash.h"
#endif  /* PG_VERSION_NUM */
#if PG_VERSION_NUM < 110000
#define hash_bytes_extended(k, keylen, seed) \
	DatumGetInt32(hash_any((k), (keylen)))
#elif PG_VERSION_NUM < 130000
#define hash_bytes_extended(k, keylen, seed) \
	DatumGetInt64(hash_any_extended((k), (keylen), (seed)))
#endif  /* PG_VERSION_NUM */
#include "libpq/pqsignal.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "optimizer/cost.h"
#if PG_VERSION_NUM >= 140000
#include "optimizer/appendinfo.h"
#endif  /* PG_VERSION_NUM */
#include "optimizer/pathnode.h"
#if PG_VERSION_NUM >= 130000
#include "optimizer/inherit.h"
#include "optimizer/paths.h"
#endif  /* PG_VERSION_NUM */
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/tlist.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "pgtime.h"
#include "port.h"
#include "storage/ipc.h"
#include "storage/lock.h"
#include "tcop/tcopprot.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/catcache.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/formatting.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"
#if PG_VERSION_NUM < 120000
#include "nodes/relation.h"
#include "optimizer/var.h"
#include "utils/tqual.h"
#else
#include "nodes/pathnodes.h"
#include "optimizer/optimizer.h"
#include "access/heapam.h"
#endif

#include <string.h>
#include <stdlib.h>

#include "oracle_fdw.h"

/* defined in backend/commands/analyze.c */
#ifndef WIDTH_THRESHOLD
#define WIDTH_THRESHOLD 1024
#endif  /* WIDTH_THRESHOLD */

#if PG_VERSION_NUM >= 90500
#define IMPORT_API

/* array_create_iterator has a new signature from 9.5 on */
#define array_create_iterator(arr, slice_ndim) array_create_iterator(arr, slice_ndim, NULL)
#else
#undef IMPORT_API
#endif  /* PG_VERSION_NUM */

#if PG_VERSION_NUM >= 90600
#define JOIN_API

/* the useful macro IS_SIMPLE_REL is defined in v10, backport */
#ifndef IS_SIMPLE_REL
#define IS_SIMPLE_REL(rel) \
	((rel)->reloptkind == RELOPT_BASEREL || \
	(rel)->reloptkind == RELOPT_OTHER_MEMBER_REL)
#endif

/* GetConfigOptionByName has a new signature from 9.6 on */
#define GetConfigOptionByName(name, varname) GetConfigOptionByName(name, varname, false)
#else
#undef JOIN_API
#endif  /* PG_VERSION_NUM */

#if PG_VERSION_NUM < 110000
/* backport macro from V11 */
#define TupleDescAttr(tupdesc, i) ((tupdesc)->attrs[(i)])
#endif  /* PG_VERSION_NUM */

/* list API has changed in v13 */
#if PG_VERSION_NUM < 130000
#define list_next(l, e) lnext((e))
#define do_each_cell(cell, list, element) for_each_cell(cell, (element))
#else
#define list_next(l, e) lnext((l), (e))
#define do_each_cell(cell, list, element) for_each_cell(cell, (list), (element))
#endif  /* PG_VERSION_NUM */

/* "table_open" was "heap_open" before v12 */
#if PG_VERSION_NUM < 120000
#define table_open(x, y) heap_open(x, y)
#define table_close(x, y) heap_close(x, y)
#endif  /* PG_VERSION_NUM */

PG_MODULE_MAGIC;

/*
 * "true" if Oracle data have been modified in the current transaction.
 */
static bool dml_in_transaction = false;

/*
 * PostGIS geometry type, set in initializePostGIS().
 */
static Oid GEOMETRYOID = InvalidOid;
static bool geometry_is_setup = false;

/*
 * Describes the valid options for objects that use this wrapper.
 */
struct OracleFdwOption
{
	const char *optname;
	Oid			optcontext;  /* Oid of catalog in which option may appear */
	bool		optrequired;
};

#define OPT_NLS_LANG "nls_lang"
#define OPT_DBSERVER "dbserver"
#define OPT_ISOLATION_LEVEL "isolation_level"
#define OPT_NCHAR "nchar"
#define OPT_USER "user"
#define OPT_PASSWORD "password"
#define OPT_DBLINK "dblink"
#define OPT_SCHEMA "schema"
#define OPT_TABLE "table"
#define OPT_MAX_LONG "max_long"
#define OPT_READONLY "readonly"
#define OPT_KEY "key"
#define OPT_STRIP_ZEROS "strip_zeros"
#define OPT_SAMPLE "sample_percent"
#define OPT_PREFETCH "prefetch"
#define OPT_LOB_PREFETCH "lob_prefetch"
#define OPT_SET_TIMEZONE "set_timezone"

#define DEFAULT_ISOLATION_LEVEL ORA_TRANS_SERIALIZABLE
#define DEFAULT_MAX_LONG 32767
#define DEFAULT_PREFETCH 50
#define DEFAULT_LOB_PREFETCH 1048576

/*
 * Options for case folding for names in IMPORT FOREIGN TABLE.
 */
typedef enum { CASE_KEEP, CASE_LOWER, CASE_SMART } fold_t;

/*
 * Valid options for oracle_fdw.
 */
static struct OracleFdwOption valid_options[] = {
	{OPT_NLS_LANG, ForeignDataWrapperRelationId, false},
	{OPT_DBSERVER, ForeignServerRelationId, true},
	{OPT_ISOLATION_LEVEL, ForeignServerRelationId, false},
	{OPT_NCHAR, ForeignServerRelationId, false},
	{OPT_USER, UserMappingRelationId, true},
	{OPT_PASSWORD, UserMappingRelationId, true},
	{OPT_DBLINK, ForeignTableRelationId, false},
	{OPT_SCHEMA, ForeignTableRelationId, false},
	{OPT_TABLE, ForeignTableRelationId, true},
	{OPT_MAX_LONG, ForeignTableRelationId, false},
	{OPT_READONLY, ForeignTableRelationId, false},
	{OPT_SAMPLE, ForeignTableRelationId, false},
	{OPT_PREFETCH, ForeignTableRelationId, false},
	{OPT_LOB_PREFETCH, ForeignTableRelationId, false},
	{OPT_KEY, AttributeRelationId, false},
	{OPT_STRIP_ZEROS, AttributeRelationId, false},
	{OPT_SET_TIMEZONE, ForeignServerRelationId, false}
};

#define option_count (sizeof(valid_options)/sizeof(struct OracleFdwOption))

/*
 * Array to hold the type output functions during table modification.
 * It is ok to hold this cache in a static variable because there cannot
 * be more than one foreign table modified at the same time.
 */

static regproc *output_funcs;

/*
 * FDW-specific information for RelOptInfo.fdw_private and ForeignScanState.fdw_state.
 * The same structure is used to hold information for query planning and execution.
 * The structure is initialized during query planning and passed on to the execution
 * step serialized as a List (see serializePlanData and deserializePlanData).
 * For DML statements, the scan stage and the modify stage both hold an
 * OracleFdwState, and the latter is initialized by copying the former (see copyPlanData).
 */
struct OracleFdwState {
	char *dbserver;                /* Oracle connect string */
	oraIsoLevel isolation_level;   /* Transaction Isolation Level */
	char *user;                    /* Oracle username */
	char *password;                /* Oracle password */
	char *nls_lang;                /* Oracle locale information */
	char *timezone;                /* session time zone */
	bool have_nchar;               /* needs support for national character conversion */
	oracleSession *session;        /* encapsulates the active Oracle session */
	char *query;                   /* query we issue against Oracle */
	List *params;                  /* list of parameters needed for the query */
	struct paramDesc *paramList;   /* description of parameters needed for the query */
	struct oraTable *oraTable;     /* description of the remote Oracle table */
	Cost startup_cost;             /* cost estimate, only needed for planning */
	Cost total_cost;               /* cost estimate, only needed for planning */
	unsigned int prefetch;         /* number of rows to prefetch */
	unsigned int lob_prefetch;     /* number of LOB bytes to prefetch */
	unsigned long rowcount;        /* rows already read from Oracle */
	int columnindex;               /* currently processed column for error context */
	MemoryContext temp_cxt;        /* short-lived memory for data modification */
	char *order_clause;            /* for ORDER BY pushdown */
	List *usable_pathkeys;         /* for ORDER BY pushdown */
	char *where_clause;            /* deparsed where clause */
	char *limit_clause;            /* deparsed limit clause */

	/*
	 * Restriction clauses, divided into safe and unsafe to pushdown subsets.
	 *
	 * For a base foreign relation this is a list of clauses along-with
	 * RestrictInfo wrapper. Keeping RestrictInfo wrapper helps while dividing
	 * scan_clauses in oracleGetForeignPlan into safe and unsafe subsets.
	 * Also it helps in estimating costs since RestrictInfo caches the
	 * selectivity and qual cost for the clause in it.
	 *
	 * For a join relation, however, they are part of otherclause list
	 * obtained from extract_actual_join_clauses, which strips RestrictInfo
	 * construct. So, for a join relation they are list of bare clauses.
	 */
	List       *remote_conds;  /* can be pushed down to remote server */
	List       *local_conds;   /* cannot be pushed down to remote server */

	/* Join information */
	RelOptInfo *outerrel;
	RelOptInfo *innerrel;
	JoinType    jointype;
	List       *joinclauses;
};

/*
 * SQL functions
 */
extern PGDLLEXPORT Datum oracle_fdw_handler(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum oracle_fdw_validator(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum oracle_close_connections(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum oracle_diag(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum oracle_execute(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(oracle_fdw_handler);
PG_FUNCTION_INFO_V1(oracle_fdw_validator);
PG_FUNCTION_INFO_V1(oracle_close_connections);
PG_FUNCTION_INFO_V1(oracle_diag);
PG_FUNCTION_INFO_V1(oracle_execute);

/*
 * on-load initializer
 */
extern PGDLLEXPORT void _PG_init(void);

/*
 * FDW callback routines
 */
static void oracleGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static void oracleGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
#ifdef JOIN_API
static void oracleGetForeignJoinPaths(PlannerInfo *root, RelOptInfo *joinrel, RelOptInfo *outerrel, RelOptInfo *innerrel, JoinType jointype, JoinPathExtraData *extra);
#endif  /* JOIN_API */
static ForeignScan *oracleGetForeignPlan(PlannerInfo *root, RelOptInfo *foreignrel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses
#if PG_VERSION_NUM >= 90500
, Plan *outer_plan
#endif  /* PG_VERSION_NUM */
);
static bool oracleAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages);
static void oracleExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void oracleBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *oracleIterateForeignScan(ForeignScanState *node);
static void oracleEndForeignScan(ForeignScanState *node);
static void oracleReScanForeignScan(ForeignScanState *node);
#if PG_VERSION_NUM < 140000
static void oracleAddForeignUpdateTargets(Query *parsetree, RangeTblEntry *target_rte, Relation target_relation);
#else
static void oracleAddForeignUpdateTargets(PlannerInfo *root, Index rtindex, RangeTblEntry *target_rte, Relation target_relation);
#endif
static List *oraclePlanForeignModify(PlannerInfo *root, ModifyTable *plan, Index resultRelation, int subplan_index);
static void oracleBeginForeignModify(ModifyTableState *mtstate, ResultRelInfo *rinfo, List *fdw_private, int subplan_index, int eflags);
#if PG_VERSION_NUM >= 110000
static void oracleBeginForeignInsert(ModifyTableState *mtstate, ResultRelInfo *rinfo);
static void oracleEndForeignInsert(EState *estate, ResultRelInfo *rinfo);
#endif  /*PG_VERSION_NUM */
static TupleTableSlot *oracleExecForeignInsert(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot);
static TupleTableSlot *oracleExecForeignUpdate(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot);
static TupleTableSlot *oracleExecForeignDelete(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot);
static void oracleEndForeignModify(EState *estate, ResultRelInfo *rinfo);
static void oracleExplainForeignModify(ModifyTableState *mtstate, ResultRelInfo *rinfo, List *fdw_private, int subplan_index, struct ExplainState *es);
static int oracleIsForeignRelUpdatable(Relation rel);
#ifdef IMPORT_API
static List *oracleImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid);
#endif  /* IMPORT_API */

/*
 * Helper functions
 */
static struct OracleFdwState *getFdwState(Oid foreigntableid, double *sample_percent, Oid userid);
static void oracleGetOptions(Oid foreigntableid, Oid userid, List **options);
static char *createQuery(struct OracleFdwState *fdwState, RelOptInfo *foreignrel, bool for_update, List *query_pathkeys);
static void deparseFromExprForRel(struct OracleFdwState *fdwState, StringInfo buf, RelOptInfo *joinrel, List **params_list);
#ifdef JOIN_API
static void appendConditions(List *exprs, StringInfo buf, RelOptInfo *joinrel, List **params_list);
static bool foreign_join_ok(PlannerInfo *root, RelOptInfo *joinrel, JoinType jointype, RelOptInfo *outerrel, RelOptInfo *innerrel, JoinPathExtraData *extra);
static const char *get_jointype_name(JoinType jointype);
static List *build_tlist_to_deparse(RelOptInfo *foreignrel);
static struct oraTable *build_join_oratable(struct OracleFdwState *fdwState, List *fdw_scan_tlist);
#endif  /* JOIN_API */
static void getColumnData(Oid foreigntableid, struct oraTable *oraTable);
static int acquireSampleRowsFunc (Relation relation, int elevel, HeapTuple *rows, int targrows, double *totalrows, double *totaldeadrows);
static void appendAsType(StringInfoData *dest, const char *s, Oid type);
static char *deparseExpr(oracleSession *session, RelOptInfo *foreignrel, Expr *expr, const struct oraTable *oraTable, List **params);
static char *datumToString(Datum datum, Oid type);
static void getUsedColumns(Expr *expr, struct oraTable *oraTable, int foreignrelid);
static void checkDataType(oraType oratype, int scale, Oid pgtype, const char *tablename, const char *colname);
static char *deparseWhereConditions(struct OracleFdwState *fdwState, RelOptInfo *baserel, List **local_conds, List **remote_conds);
static char *guessNlsLang(char *nls_lang);
static char *getTimezone(void);
static oracleSession *oracleConnectServer(Name srvname);
static List *serializePlanData(struct OracleFdwState *fdwState);
static Const *serializeString(const char *s);
static struct OracleFdwState *deserializePlanData(List *list);
static char *deserializeString(Const *constant);
static bool optionIsTrue(const char *value);
static char *deparseDate(Datum datum);
static char *deparseTimestamp(Datum datum, bool hasTimezone);
static char *deparseInterval(Datum datum);
static char *convertUUID(char *uuid);
static struct OracleFdwState *copyPlanData(struct OracleFdwState *orig);
static void subtransactionCallback(SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid, void *arg);
static void addParam(struct paramDesc **paramList, char *name, Oid pgtype, oraType oratype, int colnum);
static void setModifyParameters(struct paramDesc *paramList, TupleTableSlot *newslot, TupleTableSlot *oldslot, struct oraTable *oraTable, oracleSession *session);
static void transactionCallback(XactEvent event, void *arg);
static void exitHook(int code, Datum arg);
static void oracleDie(SIGNAL_ARGS);
static char *setSelectParameters(struct paramDesc *paramList, ExprContext *econtext);
static void convertTuple(struct OracleFdwState *fdw_state, unsigned int index, Datum *values, bool *nulls, bool trunc_lob);
static void errorContextCallback(void *arg);
static bool hasTrigger(Relation rel, CmdType cmdtype);
static void buildInsertQuery(StringInfo sql, struct OracleFdwState *fdwState);
static void buildUpdateQuery(StringInfo sql, struct OracleFdwState *fdwState, List *targetAttrs);
static void appendReturningClause(StringInfo sql, struct OracleFdwState *fdwState);
#ifdef IMPORT_API
static char *fold_case(char *name, fold_t foldcase, int collation);
#endif  /* IMPORT_API */
static oraIsoLevel getIsolationLevel(const char *isolation_level);
static bool pushdownOrderBy(PlannerInfo *root, RelOptInfo *baserel, struct OracleFdwState *fdwState);
static char *deparseLimit(PlannerInfo *root, struct OracleFdwState *fdwState, RelOptInfo *baserel);
#if PG_VERSION_NUM < 150000
/* this is new in PostgreSQL v15 */
struct pg_itm
{
	int         tm_usec;
	int         tm_sec;
	int         tm_min;
	int64       tm_hour;        /* needs to be wide */
	int         tm_mday;
	int         tm_mon;
	int         tm_year;
};

static void interval2itm(Interval span, struct pg_itm *itm);
#endif  /* PG_VERSION_NUM */

#define REL_ALIAS_PREFIX    "r"
/* Handy macro to add relation name qualification */
#define ADD_REL_QUALIFIER(buf, varno)   \
		appendStringInfo((buf), "%s%d.", REL_ALIAS_PREFIX, (varno))

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to callback routines.
 */
PGDLLEXPORT Datum
oracle_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	fdwroutine->GetForeignRelSize = oracleGetForeignRelSize;
	fdwroutine->GetForeignPaths = oracleGetForeignPaths;
#ifdef JOIN_API
	fdwroutine->GetForeignJoinPaths = oracleGetForeignJoinPaths;
#endif  /* JOIN_API */
	fdwroutine->GetForeignPlan = oracleGetForeignPlan;
	fdwroutine->AnalyzeForeignTable = oracleAnalyzeForeignTable;
	fdwroutine->ExplainForeignScan = oracleExplainForeignScan;
	fdwroutine->BeginForeignScan = oracleBeginForeignScan;
	fdwroutine->IterateForeignScan = oracleIterateForeignScan;
	fdwroutine->ReScanForeignScan = oracleReScanForeignScan;
	fdwroutine->EndForeignScan = oracleEndForeignScan;
	fdwroutine->AddForeignUpdateTargets = oracleAddForeignUpdateTargets;
	fdwroutine->PlanForeignModify = oraclePlanForeignModify;
	fdwroutine->BeginForeignModify = oracleBeginForeignModify;
#if PG_VERSION_NUM >= 110000
	fdwroutine->BeginForeignInsert = oracleBeginForeignInsert;
	fdwroutine->EndForeignInsert = oracleEndForeignInsert;
#endif  /*PG_VERSION_NUM */
	fdwroutine->ExecForeignInsert = oracleExecForeignInsert;
	fdwroutine->ExecForeignUpdate = oracleExecForeignUpdate;
	fdwroutine->ExecForeignDelete = oracleExecForeignDelete;
	fdwroutine->EndForeignModify = oracleEndForeignModify;
	fdwroutine->ExplainForeignModify = oracleExplainForeignModify;
	fdwroutine->IsForeignRelUpdatable = oracleIsForeignRelUpdatable;
#ifdef IMPORT_API
	fdwroutine->ImportForeignSchema = oracleImportForeignSchema;
#endif  /* IMPORT_API */

	PG_RETURN_POINTER(fdwroutine);
}

/*
 * oracle_fdw_validator
 * 		Validate the generic options given to a FOREIGN DATA WRAPPER, SERVER,
 * 		USER MAPPING or FOREIGN TABLE that uses oracle_fdw.
 *
 * 		Raise an ERROR if the option or its value are considered invalid
 * 		or a required option is missing.
 */
PGDLLEXPORT Datum
oracle_fdw_validator(PG_FUNCTION_ARGS)
{
	List *options_list = untransformRelOptions(PG_GETARG_DATUM(0));
	Oid catalog = PG_GETARG_OID(1);
	ListCell *cell;
	bool option_given[option_count] = { false };
	int i;

	/*
	 * Check that only options supported by oracle_fdw, and allowed for the
	 * current object type, are given.
	 */

	foreach(cell, options_list)
	{
		DefElem *def = (DefElem *)lfirst(cell);
		bool opt_found = false;

		/* search for the option in the list of valid options */
		for (i=0; i<option_count; ++i)
		{
			if (catalog == valid_options[i].optcontext && strcmp(valid_options[i].optname, def->defname) == 0)
			{
				opt_found = true;
				option_given[i] = true;
				break;
			}
		}

		/* option not found, generate error message */
		if (!opt_found)
		{
			/* generate list of options */
			StringInfoData buf;
			initStringInfo(&buf);
			for (i=0; i<option_count; ++i)
			{
				if (catalog == valid_options[i].optcontext)
					appendStringInfo(&buf, "%s%s", (buf.len > 0) ? ", " : "",  valid_options[i].optname);
			}

			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					errmsg("invalid option \"%s\"", def->defname),
					errhint("Valid options in this context are: %s", buf.data)));
		}

		/* check valid values for "isolation_level" */
		if (strcmp(def->defname, OPT_ISOLATION_LEVEL) == 0)
			(void)getIsolationLevel(strVal(def->arg));

		/* check valid values for "readonly", "key", "strip_zeros" and "nchar" */
		if (strcmp(def->defname, OPT_READONLY) == 0
				|| strcmp(def->defname, OPT_KEY) == 0
				|| strcmp(def->defname, OPT_STRIP_ZEROS) == 0
				|| strcmp(def->defname, OPT_NCHAR) == 0
				|| strcmp(def->defname, OPT_SET_TIMEZONE) == 0
			)
		{
			char *val = strVal(def->arg);
			if (pg_strcasecmp(val, "on") != 0
					&& pg_strcasecmp(val, "off") != 0
					&& pg_strcasecmp(val, "yes") != 0
					&& pg_strcasecmp(val, "no") != 0
					&& pg_strcasecmp(val, "true") != 0
					&& pg_strcasecmp(val, "false") != 0)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Valid values in this context are: on/yes/true or off/no/false")));
		}

		/* check valid values for "dblink" */
		if (strcmp(def->defname, OPT_DBLINK) == 0)
		{
			char *val = strVal(def->arg);
			if (strchr(val, '"') != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Double quotes are not allowed in the dblink name.")));
		}

		/* check valid values for "schema" */
		if (strcmp(def->defname, OPT_SCHEMA) == 0)
		{
			char *val = strVal(def->arg);
			if (strchr(val, '"') != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Double quotes are not allowed in the schema name.")));
		}

		/* check valid values for max_long */
		if (strcmp(def->defname, OPT_MAX_LONG) == 0)
		{
			char *val = strVal(def->arg);
			char *endptr;
			unsigned long max_long;

			errno = 0;
			max_long = strtoul(val, &endptr, 0);
			if (val[0] == '\0' || *endptr != '\0' || errno != 0 || max_long < 1 || max_long > 1073741823ul)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Valid values in this context are integers between 1 and 1073741823.")));
		}

		/* check valid values for "sample_percent" */
		if (strcmp(def->defname, OPT_SAMPLE) == 0)
		{
			char *val = strVal(def->arg);
			char *endptr;
			double sample_percent;

			errno = 0;
			sample_percent = strtod(val, &endptr);
			if (val[0] == '\0' || *endptr != '\0' || errno != 0 || sample_percent < 0.000001 || sample_percent > 100.0)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Valid values in this context are numbers between 0.000001 and 100.")));
		}

		/* check valid values for "prefetch" */
		if (strcmp(def->defname, OPT_PREFETCH) == 0)
		{
			char *val = strVal(def->arg);
			char *endptr;
			long prefetch;

			errno = 0;
			prefetch = strtol(val, &endptr, 0);
			if (val[0] == '\0' || *endptr != '\0' || errno != 0 || prefetch < 1 || prefetch > 1000 )
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Valid values in this context are integers between 0 and 1000.")));
		}

		/* check valid values for "lob_prefetch" */
		if (strcmp(def->defname, OPT_LOB_PREFETCH) == 0)
		{
			char *val = strVal(def->arg);
			char *endptr;
			long lob_prefetch;

			errno = 0;
			lob_prefetch = strtol(val, &endptr, 0);
			if (val[0] == '\0' || *endptr != '\0' || errno != 0 || lob_prefetch < 0 || lob_prefetch > 536870912 )
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Valid values in this context are integers between 0 and 536870912.")));
		}
	}

	/* check that all required options have been given */
	for (i=0; i<option_count; ++i)
	{
		if (catalog == valid_options[i].optcontext && valid_options[i].optrequired && !option_given[i])
		{
			ereport(ERROR,
					(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
					errmsg("missing required option \"%s\"", valid_options[i].optname)));
		}
	}

	PG_RETURN_VOID();
}

/*
 * oracle_close_connections
 * 		Close all open Oracle connections.
 */
PGDLLEXPORT Datum
oracle_close_connections(PG_FUNCTION_ARGS)
{
	if (dml_in_transaction)
		ereport(ERROR,
				(errcode(ERRCODE_ACTIVE_SQL_TRANSACTION),
				errmsg("connections with an active transaction cannot be closed"),
				errhint("The transaction that modified Oracle data must be closed first.")));

	elog(DEBUG1, "oracle_fdw: close all Oracle connections");
	oracleCloseConnections();

	PG_RETURN_VOID();
}

/*
 * oracle_diag
 * 		Get the Oracle client version.
 * 		If a non-NULL argument is supplied, it must be a foreign server name.
 * 		In this case, the remote server version is returned as well.
 */
PGDLLEXPORT Datum
oracle_diag(PG_FUNCTION_ARGS)
{
	char *pgversion;
	int major, minor, update, patch, port_patch;
	StringInfoData version;

	/*
	 * Get the PostgreSQL server version.
	 * We cannot use PG_VERSION because that would give the version against which
	 * oracle_fdw was compiled, not the version it is running with.
	 */
	pgversion = GetConfigOptionByName("server_version", NULL);

	/* get the Oracle client version */
	oracleClientVersion(&major, &minor, &update, &patch, &port_patch);

	initStringInfo(&version);
	appendStringInfo(&version, "oracle_fdw %s, PostgreSQL %s, Oracle client %d.%d.%d.%d.%d",
					ORACLE_FDW_VERSION,
					pgversion,
					major, minor, update, patch, port_patch);

	if (PG_ARGISNULL(0))
	{
		/* display some important Oracle environment variables */
		static const char * const oracle_env[] = {
			"ORACLE_HOME",
			"ORACLE_SID",
			"TNS_ADMIN",
			"TWO_TASK",
			"LDAP_ADMIN",
			NULL
		};
		int i;

		for (i=0; oracle_env[i] != NULL; ++i)
		{
			char *val = getenv(oracle_env[i]);

			if (val != NULL)
				appendStringInfo(&version, ", %s=%s", oracle_env[i], val);
		}
	}
	else
	{
		oracleSession *session;

		Name srvname = PG_GETARG_NAME(0);
		session = oracleConnectServer(srvname);

		/* get the server version */
		oracleServerVersion(session, &major, &minor, &update, &patch, &port_patch);
		appendStringInfo(&version, ", Oracle server %d.%d.%d.%d.%d",
						major, minor, update, patch, port_patch);

		/* free the session (connection will be cached) */
		pfree(session);
	}

	PG_RETURN_TEXT_P(cstring_to_text(version.data));
}

/*
 * oracle_execute
 * 		Execute a statement that returns no result values on a foreign server.
 */
PGDLLEXPORT Datum
oracle_execute(PG_FUNCTION_ARGS)
{
	Name srvname = PG_GETARG_NAME(0);
	char *stmt = text_to_cstring(PG_GETARG_TEXT_PP(1));
	oracleSession *session = oracleConnectServer(srvname);

	oracleExecuteCall(session, stmt);

	/* free the session (connection will be cached) */
	pfree(session);

	PG_RETURN_VOID();
}

/*
 * _PG_init
 * 		Library load-time initalization.
 * 		Sets exitHook() callback for backend shutdown.
 */
void
_PG_init(void)
{
	/* check for incompatible server versions */
	char *pgver_str = GetConfigOptionByName("server_version_num", NULL);
	long pgver = strtol(pgver_str, NULL, 10);

	pfree(pgver_str);

	if ((pgver >= 90600 && pgver <= 90608)
			|| (pgver >= 100000 && pgver <= 100003))
		ereport(ERROR,
				(errcode(ERRCODE_EXTERNAL_ROUTINE_INVOCATION_EXCEPTION),
				errmsg("PostgreSQL version \"%s\" not supported by oracle_fdw",
					   GetConfigOptionByName("server_version", NULL)),
				errhint("You'll have to update PostgreSQL to a later minor release.")));

	/* register an exit hook */
	on_proc_exit(&exitHook, PointerGetDatum(NULL));
}

/*
 * oracleGetForeignRelSize
 * 		Get an OracleFdwState for this foreign scan.
 * 		Construct the remote SQL query.
 * 		Provide estimates for the number of tuples, the average width and the cost.
 */
void
oracleGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	struct OracleFdwState *fdwState;
	int i, major, minor, update, patch, port_patch;
	double ntuples = -1;
	bool order_by_local;
	RangeTblEntry *rte = planner_rt_fetch(baserel->relid, root);
	Oid check_user;

	/*
	 * Get the user whose user mapping should be used (if invalid, the current
	 * user is used).
	 */
#if PG_VERSION_NUM < 160000
	check_user = rte->checkAsUser;
#else
	check_user = getRTEPermissionInfo(root->parse->rteperminfos, rte)->checkAsUser;
#endif  /* PG_VERSION_NUM < 160000 */

	elog(DEBUG1, "oracle_fdw: plan foreign table scan");

	/*
	 * Get connection options, connect and get the remote table description.
	 * To match what ExecCheckRTEPerms does, pass the user whose user mapping
	 * should be used (if invalid, the current user is used).
	 */
	fdwState = getFdwState(foreigntableid, NULL, check_user);

	/*
	 * Store the table OID in each table column.
	 * This is redundant for base relations, but join relations will
	 * have columns from different tables, and we have to keep track of them.
	 */
	for (i=0; i<fdwState->oraTable->ncols; ++i){
		fdwState->oraTable->cols[i]->varno = baserel->relid;
	}

	/*
	 * Classify conditions into remote_conds or local_conds.
	 * These parameters are used in foreign_join_ok and oracleGetForeignPlan.
	 * Those conditions that can be pushed down will be collected into
	 * an Oracle WHERE clause.
	 */
	fdwState->where_clause = deparseWhereConditions(
								fdwState,
								baserel,
								&(fdwState->local_conds),
								&(fdwState->remote_conds)
							);

	/*
	 * Determine whether we can potentially push query pathkeys to the remote
	 * side, avoiding a local sort.
	 */
	order_by_local = !pushdownOrderBy(root, baserel, fdwState);

	/* try to push down LIMIT from Oracle 12.2 on */
	oracleServerVersion(fdwState->session, &major, &minor, &update, &patch, &port_patch);
	if (major > 12 || (major == 12 && minor > 1))
	{
		/* but not if ORDER BY cannot be pushed down */
		if (!order_by_local &&
			((list_length(root->canon_pathkeys) <= 1 && !root->cte_plan_ids)
				||  (list_length(root->parse->rtable) == 1)))
		{
			fdwState->limit_clause = deparseLimit(root, fdwState, baserel);
		}
	}

	/* release Oracle session (will be cached) */
	pfree(fdwState->session);
	fdwState->session = NULL;

	/* use a random "high" value for cost */
	fdwState->startup_cost = 10000.0;

	/* if baserel->pages > 0, there was an ANALYZE; use the row count estimate */
#if PG_VERSION_NUM < 140000
	/* before v14, baserel->tuples == 0 for tables that have never been vacuumed */
	if (baserel->pages > 0)
#endif  /* PG_VERSION_NUM */
		ntuples = baserel->tuples;

	/* estimale selectivity locally for all conditions */

	/* apply statistics only if we have a reasonable row count estimate */
	if (ntuples != -1)
	{
		/* estimate how conditions will influence the row count */
		ntuples = ntuples * clauselist_selectivity(root, baserel->baserestrictinfo, 0, JOIN_INNER, NULL);
		/* make sure that the estimate is not less that 1 */
		ntuples = clamp_row_est(ntuples);
		baserel->rows = ntuples;
	}

	/* estimate total cost as startup cost + 10 * (returned rows) */
	fdwState->total_cost = fdwState->startup_cost + baserel->rows * 10.0;

	/* store the state so that the other planning functions can use it */
	baserel->fdw_private = (void *)fdwState;
}

/* oracleGetForeignPaths
 * 		Create a ForeignPath node and add it as only possible path.
 */
void
oracleGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid)
{
	struct OracleFdwState *fdwState = (struct OracleFdwState *)baserel->fdw_private;

	/* add the only path */
	add_path(baserel,
		(Path *)create_foreignscan_path(
					root,
					baserel,
#if PG_VERSION_NUM >= 90600
					NULL,  /* default pathtarget */
#endif  /* PG_VERSION_NUM */
					baserel->rows,
					fdwState->startup_cost,
					fdwState->total_cost,
					fdwState->usable_pathkeys,
					baserel->lateral_relids,
#if PG_VERSION_NUM >= 90500
					NULL,  /* no extra plan */
#endif  /* PG_VERSION_NUM */
#if PG_VERSION_NUM >= 170000
					NIL,   /* no fdw_restrictinfo */
#endif  /* PG_VERSION_NUM */
					NIL
			)
	);
}

#ifdef JOIN_API
/*
 * oracleGetForeignJoinPaths
 * 		Add possible ForeignPath to joinrel if the join is safe to push down.
 * 		For now, we can only push down 2-way joins for SELECT.
 */
static void
oracleGetForeignJoinPaths(PlannerInfo *root,
							RelOptInfo *joinrel,
							RelOptInfo *outerrel,
							RelOptInfo *innerrel,
							JoinType jointype,
							JoinPathExtraData *extra)
{
	struct OracleFdwState *fdwState;
	ForeignPath *joinpath;
	double      joinclauses_selectivity;
	double      rows;				/* estimated number of returned rows */
	Cost        startup_cost;
	Cost        total_cost;

	/*
	 * Currently we don't push-down joins in query for UPDATE/DELETE.
	 * This would require a path for EvalPlanQual.
	 * This restriction might be relaxed in a later release.
	 */
	if (root->parse->commandType != CMD_SELECT)
	{
		elog(DEBUG2, "oracle_fdw: don't push down join because it is no SELECT");
		return;
	}

	if (root->rowMarks)
	{
		elog(DEBUG2, "oracle_fdw: don't push down join with FOR UPDATE");
		return;
	}

	/*
	 * N-way join is not supported, due to the column definition infrastracture.
	 * If we can track relid mapping of join relations, we can support N-way join.
	 */
	if (! IS_SIMPLE_REL(outerrel) || ! IS_SIMPLE_REL(innerrel))
		return;

	/* skip if this join combination has been considered already */
	if (joinrel->fdw_private)
		return;

	/*
	 * Create unfinished OracleFdwState which is used to indicate
	 * that the join relation has already been considered, so that we won't waste
	 * time considering it again and don't add the same path a second time.
	 * Once we know that this join can be pushed down, we fill the data structure.
	 */
	fdwState = (struct OracleFdwState *) palloc0(sizeof(struct OracleFdwState));

	joinrel->fdw_private = fdwState;

	/* this performs further checks */
	if (!foreign_join_ok(root, joinrel, jointype, outerrel, innerrel, extra))
		return;

	/* estimate the number of result rows for the join */
#if PG_VERSION_NUM < 140000
	if (outerrel->pages > 0 && innerrel->pages > 0)
#else
	if (outerrel->tuples >= 0 && innerrel->tuples >= 0)
#endif  /* PG_VERSION_NUM */
	{
		/* both relations have been ANALYZEd, so there should be useful statistics */
		joinclauses_selectivity = clauselist_selectivity(root, fdwState->joinclauses, 0, JOIN_INNER, extra->sjinfo);
		rows = clamp_row_est(innerrel->tuples * outerrel->tuples * joinclauses_selectivity);
	}
	else
	{
		/* at least one table lacks statistics, so use a fixed estimate */
		rows = 1000.0;
	}

	/* use a random "high" value for startup cost */
	startup_cost = 10000.0;

	/* estimate total cost as startup cost + (returned rows) * 10.0 */
	total_cost = startup_cost + rows * 10.0;

	/* store cost estimation results */
	joinrel->rows = rows;
	fdwState->startup_cost = startup_cost;
	fdwState->total_cost = total_cost;

	/* create a new join path */
#if PG_VERSION_NUM < 120000
	joinpath = create_foreignscan_path(
#else
	joinpath = create_foreign_join_path(
#endif  /* PG_VERSION_NUM */
									   root,
									   joinrel,
									   NULL,	/* default pathtarget */
									   rows,
									   startup_cost,
									   total_cost,
									   NIL, 	/* no pathkeys */
									   joinrel->lateral_relids,
									   NULL,	/* no epq_path */
#if PG_VERSION_NUM >= 170000
									   NIL,		/* no fdw_restrictinfo */
#endif  /* PG_VERSION_NUM */
									   NIL);	/* no fdw_private */

	/* add generated path to joinrel */
	add_path(joinrel, (Path *) joinpath);
}
#endif  /* JOIN_API */

/*
 * oracleGetForeignPlan
 * 		Construct a ForeignScan node containing the serialized OracleFdwState,
 * 		the RestrictInfo clauses not handled entirely by Oracle and the list
 * 		of parameters we need for execution.
 * 		For join relations, the oraTable is constructed from the target list.
 */
ForeignScan
*oracleGetForeignPlan(PlannerInfo *root, RelOptInfo *foreignrel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses
#if PG_VERSION_NUM >= 90500
, Plan *outer_plan
#endif  /* PG_VERSION_NUM */
)
{
	struct OracleFdwState *fdwState = (struct OracleFdwState *)foreignrel->fdw_private;
	List *fdw_private = NIL;
	int i;
	bool need_keys = false, for_update = false, has_trigger;
	Relation rel;
	Index scan_relid;  /* will be 0 for join relations */
	List *local_exprs = fdwState->local_conds;
#if PG_VERSION_NUM >= 90500
	List *fdw_scan_tlist = NIL;
#endif  /* PG_VERSION_NUM */

#ifdef JOIN_API
	/* treat base relations and join relations differently */
	if (IS_SIMPLE_REL(foreignrel))
	{
#endif  /* JOIN_API */
		/* for base relations, set scan_relid as the relid of the relation */
		scan_relid = foreignrel->relid;

		/* check if the foreign scan is for an UPDATE or DELETE */
#if PG_VERSION_NUM < 140000
		if (foreignrel->relid == root->parse->resultRelation &&
			(root->parse->commandType == CMD_UPDATE ||
			root->parse->commandType == CMD_DELETE))
#else
		if (bms_is_member(foreignrel->relid, root->all_result_relids) &&
			(root->parse->commandType == CMD_UPDATE ||
			root->parse->commandType == CMD_DELETE))
#endif  /* PG_VERSION_NUM */
		{
			/* we need the table's primary key columns */
			need_keys = true;
		}

		/* check if FOR [KEY] SHARE/UPDATE was specified */
		if (need_keys || get_parse_rowmark(root->parse, foreignrel->relid))
		{
			/* we should add FOR UPDATE */
			for_update = true;
		}

		if (need_keys)
		{
			/* we need to fetch all primary key columns */
			for (i=0; i<fdwState->oraTable->ncols; ++i)
				if (fdwState->oraTable->cols[i]->pkey)
					fdwState->oraTable->cols[i]->used = 1;
		}

		/*
		 * Core code already has some lock on each rel being planned, so we can
		 * use NoLock here.
		 */
		rel = table_open(foreigntableid, NoLock);

		/* is there an AFTER trigger FOR EACH ROW? */
		has_trigger = (foreignrel->relid == root->parse->resultRelation)
						&& hasTrigger(rel, root->parse->commandType);

		table_close(rel, NoLock);

		if (has_trigger)
		{
			/* we need to fetch and return all columns */
			for (i=0; i<fdwState->oraTable->ncols; ++i)
				if (fdwState->oraTable->cols[i]->pgname)
					fdwState->oraTable->cols[i]->used = 1;
		}
#ifdef JOIN_API
	}
	else
	{
		/* we have a join relation, so set scan_relid to 0 */
		scan_relid = 0;

		/*
		 * create_scan_plan() and create_foreignscan_plan() pass
		 * rel->baserestrictinfo + parameterization clauses through
		 * scan_clauses. For a join rel->baserestrictinfo is NIL and we are
		 * not considering parameterization right now, so there should be no
		 * scan_clauses for a joinrel.
		 */
		Assert(!scan_clauses);

		/* Build the list of columns to be fetched from the foreign server. */
		fdw_scan_tlist = build_tlist_to_deparse(foreignrel);

		/*
		 * Ensure that the outer plan produces a tuple whose descriptor
		 * matches our scan tuple slot. This is safe because all scans and
		 * joins support projection, so we never need to insert a Result node.
		 * Also, remove the local conditions from outer plan's quals, lest
		 * they will be evaluated twice, once by the local plan and once by
		 * the scan.
		 */
		if (outer_plan)
		{
			ListCell   *lc;

			outer_plan->targetlist = fdw_scan_tlist;

			foreach(lc, local_exprs)
			{
				Join       *join_plan = (Join *) outer_plan;
				Node       *qual = lfirst(lc);

				outer_plan->qual = list_delete(outer_plan->qual, qual);

				/*
				 * For an inner join the local conditions of foreign scan plan
				 * can be part of the joinquals as well.
				 */
				if (join_plan->jointype == JOIN_INNER)
					join_plan->joinqual = list_delete(join_plan->joinqual,
													  qual);
			}
		}

		/* construct oraTable for the result of join */
		fdwState->oraTable = build_join_oratable(fdwState, fdw_scan_tlist);

	}
#endif  /* JOIN_API */

	/* create remote query */
	fdwState->query = createQuery(fdwState, foreignrel, for_update, best_path->path.pathkeys);
	elog(DEBUG1, "oracle_fdw: remote query is: %s", fdwState->query);

	/* get PostgreSQL column data types, check that they match Oracle's */
	for (i=0; i<fdwState->oraTable->ncols; ++i)
		if (fdwState->oraTable->cols[i]->used)
			checkDataType(
				fdwState->oraTable->cols[i]->oratype,
				fdwState->oraTable->cols[i]->scale,
				fdwState->oraTable->cols[i]->pgtype,
				fdwState->oraTable->pgname,
				fdwState->oraTable->cols[i]->pgname
			);

	fdw_private = serializePlanData(fdwState);

	/*
	 * Create the ForeignScan node for the given relation.
	 *
	 * Note that the remote parameter expressions are stored in the fdw_exprs
	 * field of the finished plan node; we can't keep them in private state
	 * because then they wouldn't be subject to later planner processing.
	 */
	return make_foreignscan(tlist, local_exprs, scan_relid, fdwState->params, fdw_private
#if PG_VERSION_NUM >= 90500
								, fdw_scan_tlist,
								NIL,  /* no parameterized paths */
								outer_plan
#endif  /* PG_VERSION_NUM */
							);
}

bool
oracleAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages)
{
	*func = acquireSampleRowsFunc;
	/* use positive page count as a sign that the table has been ANALYZEd */
	*totalpages = 42;

	return true;
}

/*
 * oracleExplainForeignScan
 * 		Produce extra output for EXPLAIN:
 * 		the Oracle query and, if VERBOSE was given, the execution plan.
 */
void
oracleExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)node->fdw_state;
	char **plan;
	int nrows, i;

	elog(DEBUG1, "oracle_fdw: explain foreign table scan");

	/* show query */
	ExplainPropertyText("Oracle query", fdw_state->query, es);

	if (es->verbose)
	{
		/* get the EXPLAIN PLAN */
		oracleExplain(fdw_state->session, fdw_state->query, &nrows, &plan);

		/* add it to explain text */
		for (i=0; i<nrows; ++i)
		{
			ExplainPropertyText("Oracle plan", plan[i], es);
		}
	}
}

/*
 * oracleBeginForeignScan
 * 		Recover ("deserialize") connection information, remote query,
 * 		Oracle table description and parameter list from the plan's
 * 		"fdw_private" field.
 * 		Reestablish a connection to Oracle.
 */
void
oracleBeginForeignScan(ForeignScanState *node, int eflags)
{
	ForeignScan *fsplan = (ForeignScan *)node->ss.ps.plan;
	List *fdw_private = fsplan->fdw_private;
	List *exec_exprs;
	ListCell *cell;
	int index;
	struct paramDesc *paramDesc;
	struct OracleFdwState *fdw_state;

	/* deserialize private plan data */
	fdw_state = deserializePlanData(fdw_private);
	node->fdw_state = (void *)fdw_state;

	/* create an ExprState tree for the parameter expressions */
#if PG_VERSION_NUM < 100000
	exec_exprs = (List *)ExecInitExpr((Expr *)fsplan->fdw_exprs, (PlanState *)node);
#else
	exec_exprs = (List *)ExecInitExprList(fsplan->fdw_exprs, (PlanState *)node);
#endif  /* PG_VERSION_NUM */

	/* create the list of parameters */
	index = 0;
	foreach(cell, exec_exprs)
	{
		ExprState *expr = (ExprState *)lfirst(cell);
		char parname[10];

		/* count, but skip deleted entries */
		++index;
		if (expr == NULL)
			continue;

		/* create a new entry in the parameter list */
		paramDesc = (struct paramDesc *)palloc(sizeof(struct paramDesc));
		snprintf(parname, 10, ":p%d", index);
		paramDesc->name = pstrdup(parname);
		paramDesc->type = exprType((Node *)(expr->expr));

		if (paramDesc->type == TEXTOID || paramDesc->type == VARCHAROID
				|| paramDesc->type == BPCHAROID || paramDesc->type == CHAROID
				|| paramDesc->type == DATEOID || paramDesc->type == TIMESTAMPOID
				|| paramDesc->type == TIMESTAMPTZOID || paramDesc->type == UUIDOID)
			paramDesc->bindType = BIND_STRING;
		else
			paramDesc->bindType = BIND_NUMBER;

		paramDesc->value = NULL;
		paramDesc->node = expr;
		paramDesc->bindh = NULL;
		paramDesc->colnum = -1;
		paramDesc->next = fdw_state->paramList;
		fdw_state->paramList = paramDesc;
	}

	/* add a fake parameter ":now" if that string appears in the query */
	if (strstr(fdw_state->query, ":now") != NULL)
	{
		paramDesc = (struct paramDesc *)palloc(sizeof(struct paramDesc));
		paramDesc->name = pstrdup(":now");
		paramDesc->type = TIMESTAMPTZOID;
		paramDesc->bindType = BIND_STRING;
		paramDesc->value = NULL;
		paramDesc->node = NULL;
		paramDesc->bindh = NULL;
		paramDesc->colnum = -1;
		paramDesc->next = fdw_state->paramList;
		fdw_state->paramList = paramDesc;
	}

	if (node->ss.ss_currentRelation)
		elog(DEBUG1, "oracle_fdw: begin foreign table scan on %d", RelationGetRelid(node->ss.ss_currentRelation));
	else
		elog(DEBUG1, "oracle_fdw: begin foreign join");

	/* connect to Oracle database */
	fdw_state->session = oracleGetSession(
			fdw_state->dbserver,
			(XactReadOnly ? ORA_TRANS_READ_ONLY : fdw_state->isolation_level),
			fdw_state->user,
			fdw_state->password,
			fdw_state->nls_lang,
			fdw_state->timezone,
			(int)fdw_state->have_nchar,
			fdw_state->oraTable->pgname,
			GetCurrentTransactionNestLevel()
		);

	/* initialize row count to zero */
	fdw_state->rowcount = 0;
}

/*
 * oracleIterateForeignScan
 * 		On first invocation (if there is no Oracle statement yet),
 * 		get the actual parameter values and run the remote query against
 * 		the Oracle database, retrieving the first result row.
 * 		Subsequent invocations will fetch more result rows until there
 * 		are no more.
 * 		The result is stored as a virtual tuple in the ScanState's
 * 		TupleSlot and returned.
 */
TupleTableSlot *
oracleIterateForeignScan(ForeignScanState *node)
{
	TupleTableSlot *slot = node->ss.ss_ScanTupleSlot;
	ExprContext *econtext = node->ss.ps.ps_ExprContext;
	unsigned int index;
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)node->fdw_state;

	if (!oracleIsStatementOpen(fdw_state->session))
	{
		/* fill the parameter list with the actual values */
		char *paramInfo = setSelectParameters(fdw_state->paramList, econtext);

		/* execute the Oracle statement and fetch the first row */
		elog(DEBUG1, "oracle_fdw: execute query in foreign table scan %s", paramInfo);

		oraclePrepareQuery(fdw_state->session, fdw_state->query, fdw_state->oraTable,
			fdw_state->prefetch, fdw_state->lob_prefetch);
		(void)oracleExecuteQuery(fdw_state->session, fdw_state->oraTable,
			fdw_state->paramList, fdw_state->prefetch);
	}

	elog(DEBUG3, "oracle_fdw: get next row in foreign table scan");

	/* fetch the next result row */
	index = oracleFetchNext(fdw_state->session, fdw_state->prefetch);

	/* initialize virtual tuple */
	ExecClearTuple(slot);

	if (index > 0)
	{
		/* increase row count */
		++fdw_state->rowcount;

		/* convert result to arrays of values and null indicators */
		convertTuple(fdw_state, index, slot->tts_values, slot->tts_isnull, false);

		/* store the virtual tuple */
		ExecStoreVirtualTuple(slot);
	}
	else
	{
		/* close the statement */
		oracleCloseStatement(fdw_state->session);
	}

	return slot;
}

/*
 * oracleEndForeignScan
 * 		Close the currently active Oracle statement.
 */
void
oracleEndForeignScan(ForeignScanState *node)
{
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)node->fdw_state;

	elog(DEBUG1, "oracle_fdw: end foreign table scan");

	/* release the Oracle session */
	oracleCloseStatement(fdw_state->session);
	pfree(fdw_state->session);
	fdw_state->session = NULL;
}

/*
 * oracleReScanForeignScan
 * 		Close the Oracle statement if there is any.
 * 		That causes the next oracleIterateForeignScan call to restart the scan.
 */
void
oracleReScanForeignScan(ForeignScanState *node)
{
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)node->fdw_state;

	elog(DEBUG1, "oracle_fdw: restart foreign table scan");

	/* close open Oracle statement if there is one */
	oracleCloseStatement(fdw_state->session);

	/* reset row count to zero */
	fdw_state->rowcount = 0;
}

/*
 * oracleAddForeignUpdateTargets
 * 		Add the primary key columns as resjunk entries.
 */
void
oracleAddForeignUpdateTargets(
#if PG_VERSION_NUM < 140000
	Query *parsetree,
#else
	PlannerInfo *root,
	Index rtindex,
#endif
	RangeTblEntry *target_rte,
	Relation target_relation
)
{
	Oid relid = RelationGetRelid(target_relation);
	TupleDesc tupdesc = target_relation->rd_att;
	int i;
	bool has_key = false;

	elog(DEBUG1, "oracle_fdw: add target columns for update on %d", relid);

	/* loop through all columns of the foreign table */
	for (i=0; i<tupdesc->natts; ++i)
	{
		Form_pg_attribute att = TupleDescAttr(tupdesc, i);
		AttrNumber attrno = att->attnum;
		List *options;
		ListCell *option;

		/* look for the "key" option on this column */
		options = GetForeignColumnOptions(relid, attrno);
		foreach(option, options)
		{
			DefElem *def = (DefElem *)lfirst(option);

			/* if "key" is set, add a resjunk for this column */
			if (strcmp(def->defname, OPT_KEY) == 0)
			{
				if (optionIsTrue(strVal(def->arg)))
				{
					Var *var;
#if PG_VERSION_NUM < 140000
					TargetEntry *tle;

					/* Make a Var representing the desired value */
					var = makeVar(
							parsetree->resultRelation,
							attrno,
							att->atttypid,
							att->atttypmod,
							att->attcollation,
							0);

					/* Wrap it in a resjunk TLE with the right name ... */
					tle = makeTargetEntry((Expr *)var,
							list_length(parsetree->targetList) + 1,
							pstrdup(NameStr(att->attname)),
							true);

					/* ... and add it to the query's targetlist */
					parsetree->targetList = lappend(parsetree->targetList, tle);
#else
					/* Make a Var representing the desired value */
					var = makeVar(
							rtindex,
							attrno,
							att->atttypid,
							att->atttypmod,
							att->attcollation,
							0);

					add_row_identity_var(root, var, rtindex, NameStr(att->attname));
#endif  /* PG_VERSION_NUM */

					has_key = true;
				}
			}
			else if (strcmp(def->defname, OPT_STRIP_ZEROS) != 0)
			{
				elog(ERROR, "impossible column option \"%s\"", def->defname);
			}
		}
	}

	if (! has_key)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("no primary key column specified for foreign Oracle table"),
				errdetail("For UPDATE or DELETE, at least one foreign table column must be marked as primary key column."),
				errhint("Set the option \"%s\" on the columns that belong to the primary key.", OPT_KEY)));
}

/*
 * oraclePlanForeignModify
 * 		Construct an OracleFdwState or copy it from the foreign scan plan.
 * 		Construct the Oracle DML statement and a list of necessary parameters.
 * 		Return the serialized OracleFdwState.
 */
List *
oraclePlanForeignModify(PlannerInfo *root, ModifyTable *plan, Index resultRelation, int subplan_index)
{
	CmdType operation = plan->operation;
	RangeTblEntry *rte = planner_rt_fetch(resultRelation, root);
	Relation rel = NULL;
	StringInfoData sql;
	List *targetAttrs = NIL;
	List *returningList = NIL;
	struct OracleFdwState *fdwState;
	int attnum, i;
	bool has_trigger = false, firstcol;
	char paramName[10];
	TupleDesc tupdesc;
	Bitmapset *updated_cols = NULL;
	Oid check_user;

	/*
	 * Get the user for permission checks.
	 */
#if PG_VERSION_NUM >= 160000
	RTEPermissionInfo *perminfo = getRTEPermissionInfo(root->parse->rteperminfos, rte);

	check_user = perminfo->checkAsUser;
#else
	check_user = rte->checkAsUser;
#endif  /* PG_VERSION_NUM >= 160000 */

	/*
	 * Get all updated columns.
	 * For PostgreSQL v12 and better, we have to consider generated columns.
	 * This changed quite a bit over the versions.
	 * Note also that this changed in 13.10, 14.7 and 15.2, so oracle_fdw
	 * won't build with older minor versions.
	 */
	if (operation == CMD_UPDATE)
	{
#if PG_VERSION_NUM >= 130000
		RelOptInfo *roi = find_base_rel(root, resultRelation);

		updated_cols = get_rel_all_updated_cols(root, roi);
#elif PG_VERSION_NUM >= 120000
		updated_cols = bms_union(rte->updatedCols, rte->extraUpdatedCols);
#elif PG_VERSION_NUM >= 90500
		updated_cols = rte->updatedCols;
#else
		updated_cols = bms_copy(rte->modifiedCols);
#endif  /* PG_VERSION_NUM */
	}

#if PG_VERSION_NUM >= 90500
	/* we don't support INSERT ... ON CONFLICT */
	if (plan->onConflictAction != ONCONFLICT_NONE)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("INSERT with ON CONFLICT clause is not supported")));
#endif  /* PG_VERSION_NUM */

	/* check if the foreign table is scanned and we already planned that scan */
	if (resultRelation < root->simple_rel_array_size
			&& root->simple_rel_array[resultRelation] != NULL
			&& root->simple_rel_array[resultRelation]->fdw_private != NULL)
		/* if yes, copy the foreign table information from the associated RelOptInfo */
		fdwState = copyPlanData((struct OracleFdwState *)(root->simple_rel_array[resultRelation]->fdw_private));
	else
		/*
		 * If no, we have to construct the foreign table data ourselves.
		 * To match what ExecCheckRTEPerms does, pass the user whose user mapping
		 * should be used (if invalid, the current user is used).
		 */
		fdwState = getFdwState(rte->relid, NULL, check_user);

	initStringInfo(&sql);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = table_open(rte->relid, NoLock);

	/*
	 * In an INSERT, we transmit all columns that are defined in the foreign
	 * table.  In an UPDATE, if there are BEFORE ROW UPDATE triggers on the
	 * foreign table, we transmit all columns like INSERT; else we transmit
	 * only columns that were explicitly targets of the UPDATE, so as to avoid
	 * unnecessary data transmission.  (We can't do that for INSERT since we
	 * would miss sending default values for columns not listed in the source
	 * statement, and for UPDATE if there are BEFORE ROW UPDATE triggers since
	 * those triggers might change values for non-target columns, in which
	 * case we would miss sending changed values for those columns.)
	 * In addition, set "has_trigger" if there is an AFTER trigger.
	 */
	if (operation == CMD_INSERT ||
		(operation == CMD_UPDATE &&
		 rel->trigdesc &&
		 rel->trigdesc->trig_update_before_row))
	{
		tupdesc = RelationGetDescr(rel);

		for (attnum = 1; attnum <= tupdesc->natts; attnum++)
		{
			Form_pg_attribute attr = TupleDescAttr(tupdesc, attnum - 1);

			if (!attr->attisdropped)
				targetAttrs = lappend_int(targetAttrs, attnum);
		}

		/* is there a row level AFTER trigger? */
		has_trigger = hasTrigger(rel, CMD_INSERT);
	}
	else if (operation == CMD_UPDATE)
	{
		AttrNumber col;
#if PG_VERSION_NUM >= 90500
		int col_idx = -1;
		while ((col_idx = bms_next_member(updated_cols, col_idx)) >= 0)
		{
			col = col_idx + FirstLowInvalidHeapAttributeNumber;
#else
		while ((col = bms_first_member(updated_cols)) >= 0)
		{
			col += FirstLowInvalidHeapAttributeNumber;
#endif  /* PG_VERSION_NUM >= 90500 */
			if (col <= InvalidAttrNumber)  /* shouldn't happen */
				elog(ERROR, "system-column update is not supported");
			targetAttrs = lappend_int(targetAttrs, col);
		}

		/* is there a row level AFTER trigger? */
		has_trigger = hasTrigger(rel, CMD_UPDATE);
	}
	else if (operation == CMD_DELETE)
	{
		/* is there a row level AFTER trigger? */
		has_trigger = hasTrigger(rel, CMD_DELETE);
	}
	else
		elog(ERROR, "unexpected operation: %d", (int) operation);

	table_close(rel, NoLock);

	/* mark all attributes for which we need to return column values */
	if (has_trigger)
	{
		/* all attributes are needed for the RETURNING clause */
		for (i=0; i<fdwState->oraTable->ncols; ++i)
			if (fdwState->oraTable->cols[i]->pgname != NULL)
			{
				/* throw an error if it is a LONG or LONG RAW column */
				if (fdwState->oraTable->cols[i]->oratype == ORA_TYPE_LONGRAW
						|| fdwState->oraTable->cols[i]->oratype == ORA_TYPE_LONG)
					ereport(ERROR,
							(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
							errmsg("columns with Oracle type LONG or LONG RAW cannot be used with triggers"),
							errdetail("Column \"%s\" of foreign table \"%s\" is of Oracle type LONG%s.",
								fdwState->oraTable->cols[i]->pgname,
								fdwState->oraTable->pgname,
								fdwState->oraTable->cols[i]->oratype == ORA_TYPE_LONG ? "" : " RAW")));

				fdwState->oraTable->cols[i]->used = 1;
			}
	}
	else
	{
		Bitmapset *attrs_used = NULL;

		/* extract the relevant RETURNING list if any */
		if (plan->returningLists)
			returningList = (List *) list_nth(plan->returningLists, subplan_index);

		if (returningList != NIL)
		{
			bool have_wholerow;

			/* get all the attributes mentioned there */
			pull_varattnos((Node *) returningList, resultRelation, &attrs_used);

			/* If there's a whole-row reference, we'll need all the columns. */
			have_wholerow = bms_is_member(InvalidAttrNumber - FirstLowInvalidHeapAttributeNumber,
										  attrs_used);

			/* mark the corresponding columns as used */
			for (i=0; i<fdwState->oraTable->ncols; ++i)
			{
				/* ignore columns that are not in the PostgreSQL table */
				if (fdwState->oraTable->cols[i]->pgname == NULL)
					continue;

				if (have_wholerow ||
					bms_is_member(fdwState->oraTable->cols[i]->pgattnum - FirstLowInvalidHeapAttributeNumber, attrs_used))
				{
					/* throw an error if it is a LONG or LONG RAW column */
					if (fdwState->oraTable->cols[i]->oratype == ORA_TYPE_LONGRAW
							|| fdwState->oraTable->cols[i]->oratype == ORA_TYPE_LONG)
						ereport(ERROR,
								(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
								errmsg("columns with Oracle type LONG or LONG RAW cannot be used in RETURNING clause"),
								errdetail("Column \"%s\" of foreign table \"%s\" is of Oracle type LONG%s.",
									fdwState->oraTable->cols[i]->pgname,
									fdwState->oraTable->pgname,
									fdwState->oraTable->cols[i]->oratype == ORA_TYPE_LONG ? "" : " RAW")));

					fdwState->oraTable->cols[i]->used = 1;
				}
			}
		}
	}

	/* construct the SQL command string */
	switch (operation)
	{
		case CMD_INSERT:
			buildInsertQuery(&sql, fdwState);

			break;
		case CMD_UPDATE:
			buildUpdateQuery(&sql, fdwState, targetAttrs);

			break;
		case CMD_DELETE:
			appendStringInfo(&sql, "DELETE FROM %s", fdwState->oraTable->name);

			break;
		default:
			elog(ERROR, "unexpected operation: %d", (int) operation);
	}

	if (operation == CMD_UPDATE || operation == CMD_DELETE)
	{
		/* add WHERE clause with the primary key columns */

		firstcol = true;
		for (i=0; i<fdwState->oraTable->ncols; ++i)
		{
			if (fdwState->oraTable->cols[i]->pkey)
			{
				/* add a parameter description */
				snprintf(paramName, 9, ":k%d", fdwState->oraTable->cols[i]->pgattnum);
				addParam(&fdwState->paramList, paramName, fdwState->oraTable->cols[i]->pgtype,
					fdwState->oraTable->cols[i]->oratype, i);

				/* add column and parameter name to query */
				if (firstcol)
				{
					appendStringInfo(&sql, " WHERE");
					firstcol = false;
				}
				else
					appendStringInfo(&sql, " AND");

				appendStringInfo(&sql, " %s = ", fdwState->oraTable->cols[i]->name);
				appendAsType(&sql, paramName, fdwState->oraTable->cols[i]->pgtype);
			}
		}
	}

	appendReturningClause(&sql, fdwState);

	fdwState->query = sql.data;

	elog(DEBUG1, "oracle_fdw: remote statement is: %s", fdwState->query);

	/* return a serialized form of the plan state */
	return serializePlanData(fdwState);
}

/*
 * oracleBeginForeignModify
 * 		Prepare everything for the DML query:
 * 		The SQL statement is prepared, the type output functions for
 * 		the parameters are fetched, and the column numbers of the
 * 		resjunk attributes are stored in the "pkey" field.
 */
void
oracleBeginForeignModify(ModifyTableState *mtstate, ResultRelInfo *rinfo, List *fdw_private, int subplan_index, int eflags)
{
	struct OracleFdwState *fdw_state = deserializePlanData(fdw_private);
	EState *estate = mtstate->ps.state;
	struct paramDesc *param;
	HeapTuple tuple;
	int i;
#if PG_VERSION_NUM < 140000
	Plan *subplan = mtstate->mt_plans[subplan_index]->plan;
#else
	Plan *subplan = outerPlanState(mtstate)->plan;
#endif

	elog(DEBUG1, "oracle_fdw: begin foreign table modify on %d", RelationGetRelid(rinfo->ri_RelationDesc));

	rinfo->ri_FdwState = fdw_state;

	/* connect to Oracle database */
	fdw_state->session = oracleGetSession(
			fdw_state->dbserver,
			fdw_state->isolation_level,
			fdw_state->user,
			fdw_state->password,
			fdw_state->nls_lang,
			fdw_state->timezone,
			(int)fdw_state->have_nchar,
			fdw_state->oraTable->pgname,
			GetCurrentTransactionNestLevel()
		);

	oraclePrepareQuery(fdw_state->session, fdw_state->query, fdw_state->oraTable, 1, fdw_state->lob_prefetch);

	/* get the type output functions for the parameters */
	output_funcs = (regproc *)palloc0(fdw_state->oraTable->ncols * sizeof(regproc *));
	for (param=fdw_state->paramList; param!=NULL; param=param->next)
	{
		/* ignore output parameters */
		if (param->bindType == BIND_OUTPUT)
			continue;

		tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(fdw_state->oraTable->cols[param->colnum]->pgtype));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for type %u", fdw_state->oraTable->cols[param->colnum]->pgtype);
		output_funcs[param->colnum] = ((Form_pg_type)GETSTRUCT(tuple))->typoutput;
		ReleaseSysCache(tuple);
	}

	/* loop through table columns */
	for (i=0; i<fdw_state->oraTable->ncols; ++i)
	{
		if (! fdw_state->oraTable->cols[i]->pkey)
			continue;

		/* for primary key columns, get the resjunk attribute number and store it in "pkey" */
		fdw_state->oraTable->cols[i]->pkey =
			ExecFindJunkAttributeInTlist(subplan->targetlist,
				fdw_state->oraTable->cols[i]->pgname);
	}

	/* create a memory context for short-lived memory */
	fdw_state->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
							"oracle_fdw temporary data",
							ALLOCSET_SMALL_SIZES);
}

#if PG_VERSION_NUM >= 110000
/*
 * oracleBeginForeignInsert
 * 		Initialize the FDW state for COPY to a foreign table.
 */
void oracleBeginForeignInsert(ModifyTableState *mtstate, ResultRelInfo *rinfo)
{
	ModifyTable *plan = castNode(ModifyTable, mtstate->ps.plan);
	Relation rel = rinfo->ri_RelationDesc;
	EState *estate = mtstate->ps.state;
	struct OracleFdwState *fdw_state;
#if PG_VERSION_NUM < 160000
	Index resultRelation;
	RangeTblEntry *rte;
#endif  /* PG_VERSION_NUM < 160000 */
	StringInfoData buf;
	struct paramDesc *param;
	HeapTuple tuple;
	int i;
	Oid check_user;

	elog(DEBUG3, "oracle_fdw: execute foreign table COPY on %d", RelationGetRelid(rel));

	/* we don't support INSERT ... ON CONFLICT */
	if (plan && plan->onConflictAction != ONCONFLICT_NONE)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				 errmsg("INSERT with ON CONFLICT clause is not supported")));

	/*
	 * If the foreign table we are about to insert routed rows into is also an
	 * UPDATE subplan result rel that will be updated later, proceeding with
	 * the INSERT will result in the later UPDATE incorrectly modifying those
	 * routed rows, so prevent the INSERT --- it would be nice if we could
	 * handle this case; but for now, throw an error for safety.
	 */
	if (plan && plan->operation == CMD_UPDATE &&
		(rinfo->ri_usesFdwDirectModify ||
		 rinfo->ri_FdwState))
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("cannot route tuples into foreign table to be updated")));

#if PG_VERSION_NUM < 160000
	/*
	 * If the foreign table is a partition that doesn't have a corresponding
	 * RTE entry, we need to create a new RTE describing the foreign table,
	 * so that we can get the effective user.  However, if this is invoked by UPDATE,
	 * the existing RTE may already correspond to this partition if it is one
	 * of the UPDATE subplan target rels; in that case, we can just use the
	 * existing RTE as-is.
	 */
	if (rinfo->ri_RangeTableIndex == 0)
		resultRelation = rinfo->ri_RootResultRelInfo->ri_RangeTableIndex;
	else
		resultRelation = rinfo->ri_RangeTableIndex;

#if PG_VERSION_NUM < 120000
	rte = list_nth(estate->es_range_table, resultRelation - 1);
#else
	rte = exec_rt_fetch(resultRelation, estate);
#endif  /* PG_VERSION_NUM < 120000 */

	/*
	 * Get the user whose user mapping should be used (if invalid, the current
	 * user is used).
	 */
	check_user = rte->checkAsUser;
#else  /* PG_VERSION_NUM >= 160000 */
	check_user = ExecGetResultRelCheckAsUser(rinfo, estate);
#endif  /* PG_VERSION_NUM < 160000 */

	fdw_state = getFdwState(RelationGetRelid(rel), NULL, check_user);

	/* not using "deserializePlanData", we have to initialize these ourselves */
	for (i=0; i<fdw_state->oraTable->ncols; ++i)
	{
		fdw_state->oraTable->cols[i]->val = (char *)palloc(fdw_state->oraTable->cols[i]->val_size);
		fdw_state->oraTable->cols[i]->val_len = (uint16 *)palloc(sizeof(uint16));
		fdw_state->oraTable->cols[i]->val_len4 = 0;
		fdw_state->oraTable->cols[i]->val_null = (int16 *)palloc(sizeof(int16));
	}
	fdw_state->rowcount = 0;

	fdw_state->session = oracleGetSession(
			fdw_state->dbserver,
			fdw_state->isolation_level,
			fdw_state->user,
			fdw_state->password,
			fdw_state->nls_lang,
			fdw_state->timezone,
			(int)fdw_state->have_nchar,
			fdw_state->oraTable->pgname,
			GetCurrentTransactionNestLevel()
		);

	/*
	 * We need to fetch all attributes if there is an AFTER INSERT trigger
	 * or if the foreign table is a partition, and the statement is
	 * INSERT ... RETURNING on the partitioned table.
	 * We could figure out what columns to return in the second case,
	 * but let's keep it simple for now.
	 */
	if (hasTrigger(rel, CMD_INSERT)
		|| (estate->es_plannedstmt != NULL && estate->es_plannedstmt->hasReturning))
	{
		/* mark all attributes for returning */
		for (i=0; i<fdw_state->oraTable->ncols; ++i)
			if (fdw_state->oraTable->cols[i]->pgname != NULL)
			{
				/* throw an error if it is a LONG or LONG RAW column */
				if (fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_LONGRAW
						|| fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_LONG)
					ereport(ERROR,
							(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
							errmsg("columns with Oracle type LONG or LONG RAW cannot be used with triggers or in RETURNING clause"),
							errdetail("Column \"%s\" of foreign table \"%s\" is of Oracle type LONG%s.",
								fdw_state->oraTable->cols[i]->pgname,
								fdw_state->oraTable->pgname,
								fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_LONG ? "" : " RAW")));

				fdw_state->oraTable->cols[i]->used = 1;
			}
	}

	/* construct an INSERT query */
	initStringInfo(&buf);
	buildInsertQuery(&buf, fdw_state);
	appendReturningClause(&buf, fdw_state);
	fdw_state->query = pstrdup(buf.data);

	/* get the type output functions for the parameters */
	output_funcs = (regproc *)palloc0(fdw_state->oraTable->ncols * sizeof(regproc *));
	for (param=fdw_state->paramList; param!=NULL; param=param->next)
	{
		/* ignore output parameters */
		if (param->bindType == BIND_OUTPUT)
			continue;

		tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(fdw_state->oraTable->cols[param->colnum]->pgtype));
		if (!HeapTupleIsValid(tuple))
			elog(ERROR, "cache lookup failed for type %u", fdw_state->oraTable->cols[param->colnum]->pgtype);
		output_funcs[param->colnum] = ((Form_pg_type)GETSTRUCT(tuple))->typoutput;
		ReleaseSysCache(tuple);
	}

	oraclePrepareQuery(fdw_state->session, fdw_state->query, fdw_state->oraTable, 1, fdw_state->lob_prefetch);

	/* create a memory context for short-lived memory */
	fdw_state->temp_cxt = AllocSetContextCreate(estate->es_query_cxt,
							"oracle_fdw temporary data",
							ALLOCSET_SMALL_SIZES);

	rinfo->ri_FdwState = (void *)fdw_state;
}

void
oracleEndForeignInsert(EState *estate, ResultRelInfo *rinfo)
{
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)rinfo->ri_FdwState;

	elog(DEBUG3, "oracle_fdw: end foreign table COPY on %d", RelationGetRelid(rinfo->ri_RelationDesc));

	MemoryContextDelete(fdw_state->temp_cxt);

	/* release the Oracle session */
	oracleCloseStatement(fdw_state->session);
	pfree(fdw_state->session);
	fdw_state->session = NULL;
}
#endif  /* PG_VERSION_NUM >= 110000 */

/*
 * oracleExecForeignInsert
 * 		Set the parameter values from the slots and execute the INSERT statement.
 * 		Returns a slot with the results from the RETRUNING clause.
 */
TupleTableSlot *
oracleExecForeignInsert(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)rinfo->ri_FdwState;
	unsigned int rows;
	MemoryContext oldcontext;

	elog(DEBUG3, "oracle_fdw: execute foreign table insert on %d", RelationGetRelid(rinfo->ri_RelationDesc));

	dml_in_transaction = true;

	MemoryContextReset(fdw_state->temp_cxt);
	oldcontext = MemoryContextSwitchTo(fdw_state->temp_cxt);

	/* extract the values from the slot and store them in the parameters */
	setModifyParameters(fdw_state->paramList, slot, planSlot, fdw_state->oraTable, fdw_state->session);

	/* execute the INSERT statement and store RETURNING values in oraTable's columns */
	rows = oracleExecuteQuery(fdw_state->session, fdw_state->oraTable, fdw_state->paramList, 1);

	if (rows > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("INSERT on Oracle table added %d rows instead of one in iteration %lu", rows, fdw_state->rowcount)));

	MemoryContextSwitchTo(oldcontext);

	/* empty the result slot */
	ExecClearTuple(slot);

	if (rows == 1)
	{
		++fdw_state->rowcount;

		/* convert result for RETURNING to arrays of values and null indicators */
		convertTuple(fdw_state, 1, slot->tts_values, slot->tts_isnull, false);

		/* store the virtual tuple */
		ExecStoreVirtualTuple(slot);
	}

	return slot;
}

/*
 * oracleExecForeignUpdate
 * 		Set the parameter values from the slots and execute the UPDATE statement.
 * 		Returns a slot with the results from the RETRUNING clause.
 */
TupleTableSlot *
oracleExecForeignUpdate(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)rinfo->ri_FdwState;
	unsigned int rows;
	MemoryContext oldcontext;

	elog(DEBUG3, "oracle_fdw: execute foreign table update on %d", RelationGetRelid(rinfo->ri_RelationDesc));

	dml_in_transaction = true;

	MemoryContextReset(fdw_state->temp_cxt);
	oldcontext = MemoryContextSwitchTo(fdw_state->temp_cxt);

	/* extract the values from the slot and store them in the parameters */
	setModifyParameters(fdw_state->paramList, slot, planSlot, fdw_state->oraTable, fdw_state->session);

	/* execute the UPDATE statement and store RETURNING values in oraTable's columns */
	rows = oracleExecuteQuery(fdw_state->session, fdw_state->oraTable, fdw_state->paramList, 1);

	if (rows > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("UPDATE on Oracle table changed %d rows instead of one in iteration %lu", rows, fdw_state->rowcount),
				errhint("This probably means that you did not set the \"key\" option on all primary key columns.")));

	MemoryContextSwitchTo(oldcontext);

	/* empty the result slot */
	ExecClearTuple(slot);

	if (rows == 1)
	{
		++fdw_state->rowcount;

		/* convert result for RETURNING to arrays of values and null indicators */
		convertTuple(fdw_state, 1, slot->tts_values, slot->tts_isnull, false);

		/* store the virtual tuple */
		ExecStoreVirtualTuple(slot);
	}

	return slot;
}

/*
 * oracleExecForeignDelete
 * 		Set the parameter values from the slots and execute the DELETE statement.
 * 		Returns a slot with the results from the RETRUNING clause.
 */
TupleTableSlot *
oracleExecForeignDelete(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)rinfo->ri_FdwState;
	int rows;
	MemoryContext oldcontext;

	elog(DEBUG3, "oracle_fdw: execute foreign table delete on %d", RelationGetRelid(rinfo->ri_RelationDesc));

	dml_in_transaction = true;

	MemoryContextReset(fdw_state->temp_cxt);
	oldcontext = MemoryContextSwitchTo(fdw_state->temp_cxt);

	/* extract the values from the slot and store them in the parameters */
	setModifyParameters(fdw_state->paramList, slot, planSlot, fdw_state->oraTable, fdw_state->session);

	/* execute the DELETE statement and store RETURNING values in oraTable's columns */
	rows = oracleExecuteQuery(fdw_state->session, fdw_state->oraTable, fdw_state->paramList, 1);

	if (rows > 1)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("DELETE on Oracle table removed %d rows instead of one in iteration %lu", rows, fdw_state->rowcount),
				errhint("This probably means that you did not set the \"key\" option on all primary key columns.")));

	MemoryContextSwitchTo(oldcontext);

	/* empty the result slot */
	ExecClearTuple(slot);

	if (rows == 1)
	{
		++fdw_state->rowcount;

		/* convert result for RETURNING to arrays of values and null indicators */
		convertTuple(fdw_state, 1, slot->tts_values, slot->tts_isnull, false);

		/* store the virtual tuple */
		ExecStoreVirtualTuple(slot);
	}

	return slot;
}

/*
 * oracleEndForeignModify
 * 		Close the currently active Oracle statement.
 */
void
oracleEndForeignModify(EState *estate, ResultRelInfo *rinfo){
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)rinfo->ri_FdwState;

	elog(DEBUG1, "oracle_fdw: end foreign table modify on %d", RelationGetRelid(rinfo->ri_RelationDesc));

	MemoryContextDelete(fdw_state->temp_cxt);

	/* release the Oracle session */
	oracleCloseStatement(fdw_state->session);
	pfree(fdw_state->session);
	fdw_state->session = NULL;
}

/*
 * oracleExplainForeignModify
 * 		Show the Oracle DML statement.
 * 		Nothing special is done for VERBOSE because the query plan is likely trivial.
 */
void
oracleExplainForeignModify(ModifyTableState *mtstate, ResultRelInfo *rinfo, List *fdw_private, int subplan_index, struct ExplainState *es){
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)rinfo->ri_FdwState;

	elog(DEBUG1, "oracle_fdw: explain foreign table modify on %d", RelationGetRelid(rinfo->ri_RelationDesc));

	/* show query */
	ExplainPropertyText("Oracle statement", fdw_state->query, es);
}

/*
 * oracleIsForeignRelUpdatable
 * 		Returns 0 if "readonly" is set, a value indicating that all DML is allowed.
 */
int
oracleIsForeignRelUpdatable(Relation rel)
{
	ListCell *cell;

	/* loop foreign table options */
	foreach(cell, GetForeignTable(RelationGetRelid(rel))->options)
	{
		DefElem *def = (DefElem *) lfirst(cell);
		char *value = strVal(def->arg);
		if (strcmp(def->defname, OPT_READONLY) == 0
				&& optionIsTrue(value))
			return 0;
	}

	return (1 << CMD_UPDATE) | (1 << CMD_INSERT) | (1 << CMD_DELETE);
}

#ifdef IMPORT_API
/*
 * oracleImportForeignSchema
 * 		Returns a List of CREATE FOREIGN TABLE statements.
 */
List *
oracleImportForeignSchema(ImportForeignSchemaStmt *stmt, Oid serverOid)
{
	ForeignServer *server;
	UserMapping *mapping;
	ForeignDataWrapper *wrapper;
	char *tabname, *colname, oldtabname[129] = { '\0' }, *foldedname;
	char *nls_lang = NULL, *user = NULL, *password = NULL,
		 *dbserver = NULL, *dblink = NULL, *max_long = NULL,
		 *sample_percent = NULL, *prefetch = NULL, *lob_prefetch = NULL;
	oraType type;
	int charlen, typeprec, typescale, nullable, key, rc;
	List *options, *result = NIL;
	ListCell *cell;
	oracleSession *session;
	fold_t foldcase = CASE_SMART;
	StringInfoData buf;
	bool readonly = false, firstcol = true, set_timezone = false;
	int collation = DEFAULT_COLLATION_OID;
	oraIsoLevel isolation_level_val = DEFAULT_ISOLATION_LEVEL;
	bool have_nchar = false;

	/* get the foreign server, the user mapping and the FDW */
	server = GetForeignServer(serverOid);
	mapping = GetUserMapping(GetUserId(), serverOid);
	wrapper = GetForeignDataWrapper(server->fdwid);

	/* get all options for these objects */
	options = wrapper->options;
	options = list_concat(options, server->options);
	options = list_concat(options, mapping->options);

	foreach(cell, options)
	{
		DefElem *def = (DefElem *) lfirst(cell);
		if (strcmp(def->defname, OPT_NLS_LANG) == 0)
			nls_lang = strVal(def->arg);
		if (strcmp(def->defname, OPT_DBSERVER) == 0)
			dbserver = strVal(def->arg);
		if (strcmp(def->defname, OPT_ISOLATION_LEVEL) == 0)
			isolation_level_val = getIsolationLevel(strVal(def->arg));
		if (strcmp(def->defname, OPT_USER) == 0)
			user = (strVal(def->arg));
		if (strcmp(def->defname, OPT_PASSWORD) == 0)
			password = strVal(def->arg);
		if (strcmp(def->defname, OPT_NCHAR) == 0)
		{
			char *nchar = strVal(def->arg);

			if (pg_strcasecmp(nchar, "on") == 0
					|| pg_strcasecmp(nchar, "yes") == 0
					|| pg_strcasecmp(nchar, "true") == 0)
				have_nchar = true;
		}
	}

	/* process the options of the IMPORT FOREIGN SCHEMA command */
	foreach(cell, stmt->options)
	{
		DefElem *def = (DefElem *) lfirst(cell);

		if (strcmp(def->defname, "case") == 0)
		{
			char *s = strVal(def->arg);
			if (strcmp(s, "keep") == 0)
				foldcase = CASE_KEEP;
			else if (strcmp(s, "lower") == 0)
				foldcase = CASE_LOWER;
			else if (strcmp(s, "smart") == 0)
				foldcase = CASE_SMART;
			else
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Valid values in this context are: %s", "keep, lower, smart")));
		}
		else if (strcmp(def->defname, "collation") == 0)
		{
			char *s = strVal(def->arg);
			if (pg_strcasecmp(s, "default") != 0) {

			/* look up collation within pg_catalog namespace with the name */

#if PG_VERSION_NUM >= 120000
			collation = GetSysCacheOid3(
							COLLNAMEENCNSP,
							Anum_pg_collation_oid,
							PointerGetDatum(s),
							Int32GetDatum(Int32GetDatum(-1)),
							ObjectIdGetDatum(PG_CATALOG_NAMESPACE)
						);
#else
			collation = GetSysCacheOid3(
							COLLNAMEENCNSP,
							PointerGetDatum(s),
							Int32GetDatum(Int32GetDatum(-1)),
							ObjectIdGetDatum(PG_CATALOG_NAMESPACE)
						);
#endif  /* PG_VERSION_NUM */

			if (!OidIsValid(collation))
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Check the \"pg_collation\" catalog for valid values.")));
			}
		}
		else if (strcmp(def->defname, OPT_READONLY) == 0)
		{
			char *s = strVal(def->arg);
			if (pg_strcasecmp(s, "on") == 0
					|| pg_strcasecmp(s, "yes") == 0
					|| pg_strcasecmp(s, "true") == 0)
				readonly = true;
			else if (pg_strcasecmp(s, "off") == 0
					|| pg_strcasecmp(s, "no") == 0
					|| pg_strcasecmp(s, "false") == 0)
				readonly = false;
			else
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname)));
		}
		else if (strcmp(def->defname, OPT_DBLINK) == 0)
		{
			char *s = strVal(def->arg);
			if (strchr(s, '"') != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Double quotes are not allowed in the dblink name.")));
			dblink = s;
		}
		else if (strcmp(def->defname, OPT_MAX_LONG) == 0)
		{
			char *endptr;
			unsigned long max_long_val;

			max_long = strVal(def->arg);
			errno = 0;
			max_long_val = strtoul(max_long, &endptr, 0);
			if (max_long[0] == '\0' || *endptr != '\0' || errno != 0 || max_long_val < 1 || max_long_val > 1073741823ul)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Valid values in this context are integers between 1 and 1073741823.")));
		}
		else if (strcmp(def->defname, OPT_SAMPLE) == 0)
		{
			char *endptr;
			double sample_percent_val;

			sample_percent = strVal(def->arg);
			errno = 0;
			sample_percent_val = strtod(sample_percent, &endptr);
			if (sample_percent[0] == '\0' || *endptr != '\0' || errno != 0 || sample_percent_val < 0.000001 || sample_percent_val > 100.0)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Valid values in this context are numbers between 0.000001 and 100.")));
		}
		else if (strcmp(def->defname, OPT_PREFETCH) == 0)
		{
			char *endptr;
			long prefetch_val;

			prefetch = strVal(def->arg);
			errno = 0;
			prefetch_val = strtol(prefetch, &endptr, 0);
			if (prefetch[0] == '\0' || *endptr != '\0' || errno != 0 || prefetch_val < 1 || prefetch_val > 1000 )
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Valid values in this context are integers between 0 and 1000.")));
		}
		else if (strcmp(def->defname, OPT_LOB_PREFETCH) == 0)
		{
			char *endptr;
			long lob_prefetch_val;

			lob_prefetch = strVal(def->arg);
			errno = 0;
			lob_prefetch_val = strtol(lob_prefetch, &endptr, 0);
			if (lob_prefetch[0] == '\0' || *endptr != '\0' || errno != 0 || lob_prefetch_val < 0 || lob_prefetch_val > 536870912 )
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Valid values in this context are integers between 0 and 536870912.")));
		}
		else if (strcmp(def->defname, OPT_SET_TIMEZONE) == 0)
		{
			char *s = strVal(def->arg);
			if (pg_strcasecmp(s, "on") == 0
					|| pg_strcasecmp(s, "yes") == 0
					|| pg_strcasecmp(s, "true") == 0)
				set_timezone = true;
			else if (pg_strcasecmp(s, "off") == 0
					|| pg_strcasecmp(s, "no") == 0
					|| pg_strcasecmp(s, "false") == 0)
				set_timezone = false;
			else
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname)));
		}
		else
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_OPTION_NAME),
					errmsg("invalid option \"%s\"", def->defname),
					errhint("Valid options in this context are: %s, %s, %s, %s, %s, %s, %s, %s",
						"case, collation", OPT_READONLY, OPT_DBLINK,
						OPT_MAX_LONG, OPT_SAMPLE, OPT_PREFETCH, OPT_LOB_PREFETCH, OPT_SET_TIMEZONE)));
	}

	elog(DEBUG1, "oracle_fdw: import schema \"%s\" from foreign server \"%s\"", stmt->remote_schema, server->servername);

	/* guess a good NLS_LANG environment setting */
	nls_lang = guessNlsLang(nls_lang);

	/* connect to Oracle database */
	session = oracleGetSession(
		dbserver,
		isolation_level_val,
		user,
		password,
		nls_lang,
		NULL,  /* don't need time zone */
		(int)have_nchar,
		NULL,
		1
	);

	initStringInfo(&buf);
	do {
		/* get the next column definition */
		rc = oracleGetImportColumn(session, dblink, stmt->remote_schema, &tabname, &colname, &type, &charlen, &typeprec, &typescale, &nullable, &key);

		if (rc == -1)
		{
			/* remote schema does not exist, issue a warning */
			ereport(ERROR,
					(errcode(ERRCODE_FDW_SCHEMA_NOT_FOUND),
					errmsg("remote schema \"%s\" does not exist", stmt->remote_schema),
					errhint("Enclose the schema name in double quotes to prevent case folding.")));

			return NIL;
		}

		if ((rc == 0 && oldtabname[0] != '\0')
			|| (rc == 1 && oldtabname[0] != '\0' && strcmp(tabname, oldtabname)))
		{
			/* finish previous CREATE FOREIGN TABLE statement */
			appendStringInfo(&buf, ") SERVER \"%s\" OPTIONS (schema '%s', table '%s'",
				server->servername, stmt->remote_schema, oldtabname);
			if (dblink)
				appendStringInfo(&buf, ", dblink '%s'", dblink);
			if (readonly)
				appendStringInfo(&buf, ", readonly 'true'");
			if (max_long)
				appendStringInfo(&buf, ", max_long '%s'", max_long);
			if (sample_percent)
				appendStringInfo(&buf, ", sample_percent '%s'", sample_percent);
			if (prefetch)
				appendStringInfo(&buf, ", prefetch '%s'", prefetch);
			if (lob_prefetch)
				appendStringInfo(&buf, ", lob_prefetch '%s'", lob_prefetch);
			if (set_timezone)
				appendStringInfo(&buf, ", set_timezone 'true'");
			appendStringInfo(&buf, ")");

			result = lappend(result, pstrdup(buf.data));
		}

		if (rc == 1 && (oldtabname[0] == '\0' || strcmp(tabname, oldtabname)))
		{
			/* start a new CREATE FOREIGN TABLE statement */
			resetStringInfo(&buf);
			foldedname = fold_case(tabname, foldcase, collation);
			appendStringInfo(&buf, "CREATE FOREIGN TABLE \"%s\" (", foldedname);
			pfree(foldedname);

			firstcol = true;
			strcpy(oldtabname, tabname);
		}

		if (rc == 1)
		{
			/*
			 * Add a column definition.
			 */

			if (firstcol)
				firstcol = false;
			else
				appendStringInfo(&buf, ", ");

			/* column name */
			foldedname = fold_case(colname, foldcase, collation);
			appendStringInfo(&buf, "\"%s\" ", foldedname);
			pfree(foldedname);

			/* data type */
			switch (type)
			{
				case ORA_TYPE_CHAR:
				case ORA_TYPE_NCHAR:
					appendStringInfo(&buf, "character(%d)", charlen == 0 ? 1 : charlen);
					break;
				case ORA_TYPE_VARCHAR2:
				case ORA_TYPE_NVARCHAR2:
					appendStringInfo(&buf, "character varying(%d)", charlen == 0 ? 1 : charlen);
					break;
				case ORA_TYPE_CLOB:
				case ORA_TYPE_LONG:
					appendStringInfo(&buf, "text");
					break;
				case ORA_TYPE_NUMBER:
					if (typeprec == 0)
						appendStringInfo(&buf, "numeric");
					else if (typescale == 0)
					{
						if (typeprec < 5)
							appendStringInfo(&buf, "smallint");
						else if (typeprec < 10)
							appendStringInfo(&buf, "integer");
						else if (typeprec < 19)
							appendStringInfo(&buf, "bigint");
						else
							appendStringInfo(&buf, "numeric(%d)", typeprec);
					}
					else
						/*
						 * in Oracle, precision can be less than scale
						 * (numbers like 0.023), but we have to increase
						 * the precision for such columns in PostgreSQL.
						 */
						appendStringInfo(&buf, "numeric(%d, %d)",
							(typeprec < typescale) ? typescale : typeprec,
							typescale);
					break;
				case ORA_TYPE_FLOAT:
					if (typeprec < 54)
						appendStringInfo(&buf, "float(%d)", typeprec);
					else
						appendStringInfo(&buf, "numeric");
					break;
				case ORA_TYPE_BINARYFLOAT:
					appendStringInfo(&buf, "real");
					break;
				case ORA_TYPE_BINARYDOUBLE:
					appendStringInfo(&buf, "double precision");
					break;
				case ORA_TYPE_RAW:
				case ORA_TYPE_BLOB:
				case ORA_TYPE_BFILE:
				case ORA_TYPE_LONGRAW:
					appendStringInfo(&buf, "bytea");
					break;
				case ORA_TYPE_DATE:
					appendStringInfo(&buf, "timestamp(0) without time zone");
					break;
				case ORA_TYPE_TIMESTAMP:
					appendStringInfo(&buf, "timestamp(%d) without time zone", (typescale > 6) ? 6 : typescale);
					break;
				case ORA_TYPE_TIMESTAMPTZ:
				case ORA_TYPE_TIMESTAMPLTZ:
					appendStringInfo(&buf, "timestamp(%d) with time zone", (typescale > 6) ? 6 : typescale);
					break;
				case ORA_TYPE_INTERVALD2S:
					appendStringInfo(&buf, "interval(%d)", (typescale > 6) ? 6 : typescale);
					break;
				case ORA_TYPE_INTERVALY2M:
					appendStringInfo(&buf, "interval(0)");
					break;
				case ORA_TYPE_XMLTYPE:
					appendStringInfo(&buf, "xml");
					break;
				case ORA_TYPE_GEOMETRY:
					if (GEOMETRYOID != InvalidOid)
					{
						appendStringInfo(&buf, "geometry");
						break;
					}
					/* fall through */
				default:
					elog(DEBUG2, "column \"%s\" of table \"%s\" has an untranslatable data type", colname, tabname);
					appendStringInfo(&buf, "text");
			}

			/* part of the primary key */
			if (key)
				appendStringInfo(&buf, " OPTIONS (key 'true')");

			/* not nullable */
			if (!nullable)
				appendStringInfo(&buf, " NOT NULL");
		}
	}
	while (rc == 1);

	return result;
}
#endif  /* IMPORT_API */

/*
 * getFdwState
 * 		Construct an OracleFdwState from the options of the foreign table.
 * 		Establish an Oracle connection and get a description of the
 * 		remote table.
 * 		"sample_percent" is set from the foreign table options.
 * 		"sample_percent" can be NULL, in that case it is not set.
 * 		"userid" determines the use to connect as; if invalid, the current
 * 		user is used.
 */
struct OracleFdwState
*getFdwState(Oid foreigntableid, double *sample_percent, Oid userid)
{
	struct OracleFdwState *fdwState = palloc0(sizeof(struct OracleFdwState));
	char *pgtablename = get_rel_name(foreigntableid);
	List *options;
	ListCell *cell;
	char *isolationlevel = NULL;
	char *dblink = NULL, *schema = NULL, *table = NULL, *maxlong = NULL,
		 *sample = NULL, *fetch = NULL, *lob_prefetch = NULL, *nchar = NULL,
		 *set_timezone = NULL;
	long max_long;
	int has_geometry = 0;

	/*
	 * Get all relevant options from the foreign table, the user mapping,
	 * the foreign server and the foreign data wrapper.
	 */
	oracleGetOptions(foreigntableid, userid, &options);
	foreach(cell, options)
	{
		DefElem *def = (DefElem *) lfirst(cell);
		if (strcmp(def->defname, OPT_NLS_LANG) == 0)
			fdwState->nls_lang = strVal(def->arg);
		if (strcmp(def->defname, OPT_DBSERVER) == 0)
			fdwState->dbserver = strVal(def->arg);
		if (strcmp(def->defname, OPT_ISOLATION_LEVEL) == 0)
			isolationlevel = strVal(def->arg);
		if (strcmp(def->defname, OPT_USER) == 0)
			fdwState->user = strVal(def->arg);
		if (strcmp(def->defname, OPT_PASSWORD) == 0)
			fdwState->password = strVal(def->arg);
		if (strcmp(def->defname, OPT_DBLINK) == 0)
			dblink = strVal(def->arg);
		if (strcmp(def->defname, OPT_SCHEMA) == 0)
			schema = strVal(def->arg);
		if (strcmp(def->defname, OPT_TABLE) == 0)
			table = strVal(def->arg);
		if (strcmp(def->defname, OPT_MAX_LONG) == 0)
			maxlong = strVal(def->arg);
		if (strcmp(def->defname, OPT_SAMPLE) == 0)
			sample = strVal(def->arg);
		if (strcmp(def->defname, OPT_PREFETCH) == 0)
			fetch = strVal(def->arg);
		if (strcmp(def->defname, OPT_LOB_PREFETCH) == 0)
			lob_prefetch = strVal(def->arg);
		if (strcmp(def->defname, OPT_NCHAR) == 0)
			nchar = strVal(def->arg);
		if (strcmp(def->defname, OPT_SET_TIMEZONE) == 0)
			set_timezone = strVal(def->arg);
	}

	/* set isolation_level (or use default) */
	if (isolationlevel == NULL)
		fdwState->isolation_level = DEFAULT_ISOLATION_LEVEL;
	else
		fdwState->isolation_level = getIsolationLevel(isolationlevel);

	/* convert "max_long" option to number or use default */
	if (maxlong == NULL)
		max_long = DEFAULT_MAX_LONG;
	else
		max_long = strtol(maxlong, NULL, 0);

	/* convert "sample_percent" to double */
	if (sample_percent != NULL)
	{
		if (sample == NULL)
			*sample_percent = 100.0;
		else
			*sample_percent = strtod(sample, NULL);
	}

	/* convert "prefetch" to number (or use default) */
	if (fetch == NULL)
		fdwState->prefetch = DEFAULT_PREFETCH;
	else
		fdwState->prefetch = (unsigned int)strtoul(fetch, NULL, 0);

	/* the limit for "prefetch" used to be higher than 1000 */
	if (fdwState->prefetch > 1000)
	{
		fdwState->prefetch = 1000;

		ereport(WARNING,
				(errcode(ERRCODE_WARNING),
				errmsg("option \"%s\" for foreign table \"%s\" reduced to 1000",
					   OPT_PREFETCH, pgtablename)));
	}

	/* convert "lob_prefetch" to number (or use default) */
	if (lob_prefetch == NULL)
		fdwState->lob_prefetch = DEFAULT_LOB_PREFETCH;
	else
		fdwState->lob_prefetch = (unsigned int)strtoul(lob_prefetch, NULL, 0);

	/* convert "nchar" option to boolean (or use "false") */
	if (nchar != NULL
		&& (pg_strcasecmp(nchar, "on") == 0
			|| pg_strcasecmp(nchar, "yes") == 0
			|| pg_strcasecmp(nchar, "true") == 0))
		fdwState->have_nchar = true;
	else
		fdwState->have_nchar = false;

	/* check if we should set the Oacle time zone */
	if (set_timezone != NULL
		&& (pg_strcasecmp(set_timezone, "on") == 0
			|| pg_strcasecmp(set_timezone, "yes") == 0
			|| pg_strcasecmp(set_timezone, "true") == 0))
		fdwState->timezone = getTimezone();
	else
		fdwState->timezone = NULL;

	/* check if options are ok */
	if (table == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				errmsg("required option \"%s\" in foreign table \"%s\" missing", OPT_TABLE, pgtablename)));

	/* guess a good NLS_LANG environment setting */
	fdwState->nls_lang = guessNlsLang(fdwState->nls_lang);

	/* connect to Oracle database */
	fdwState->session = oracleGetSession(
		fdwState->dbserver,
		(XactReadOnly ? ORA_TRANS_READ_ONLY : fdwState->isolation_level),
		fdwState->user,
		fdwState->password,
		fdwState->nls_lang,
		fdwState->timezone,
		(int)fdwState->have_nchar,
		pgtablename,
		GetCurrentTransactionNestLevel()
	);

	/* get remote table description */
	fdwState->oraTable = oracleDescribe(fdwState->session, dblink, schema, table, pgtablename, max_long, &has_geometry);

	/* don't try array prefetching with geometries */
	if (has_geometry)
		fdwState->prefetch = 1;

	/* add PostgreSQL data to table description */
	getColumnData(foreigntableid, fdwState->oraTable);

	return fdwState;
}

/*
 * oracleGetOptions
 * 		Fetch the options for an oracle_fdw foreign table.
 * 		Returns a union of the options of the foreign data wrapper,
 * 		the foreign server, the user mapping and the foreign table,
 * 		in that order.  Column options are ignored.
 */
void
oracleGetOptions(Oid foreigntableid, Oid userid, List **options)
{
	ForeignTable *table;
	ForeignServer *server;
	UserMapping *mapping;
	ForeignDataWrapper *wrapper;

	/*
	 * Gather all data for the foreign table.
	 */
	table = GetForeignTable(foreigntableid);
	server = GetForeignServer(table->serverid);
	mapping = GetUserMapping(
				(userid != InvalidOid) ? userid : GetUserId(),
				table->serverid
			  );
	wrapper = GetForeignDataWrapper(server->fdwid);

	/* later options override earlier ones */
	*options = NIL;
	*options = list_concat(*options, wrapper->options);
	*options = list_concat(*options, server->options);
	if (mapping != NULL)
		*options = list_concat(*options, mapping->options);
	*options = list_concat(*options, table->options);
}

/*
 * getColumnData
 * 		Get PostgreSQL column name and number, data type and data type modifier.
 * 		Set oraTable->npgcols.
 * 		For PostgreSQL 9.2 and better, find the primary key columns and mark them in oraTable.
 */
void
getColumnData(Oid foreigntableid, struct oraTable *oraTable)
{
	Relation rel;
	TupleDesc tupdesc;
	int i, index;

	rel = table_open(foreigntableid, NoLock);
	tupdesc = rel->rd_att;

	/* number of PostgreSQL columns */
	oraTable->npgcols = tupdesc->natts;

	/* loop through foreign table columns */
	index = 0;
	for (i=0; i<tupdesc->natts; ++i)
	{
		Form_pg_attribute att_tuple = TupleDescAttr(tupdesc, i);
		List *options;
		ListCell *option;

		/* ignore dropped columns */
		if (att_tuple->attisdropped)
			continue;

		++index;
		/* get PostgreSQL column number and type */
		if (index <= oraTable->ncols)
		{
			oraTable->cols[index-1]->pgattnum = att_tuple->attnum;
			oraTable->cols[index-1]->pgtype = att_tuple->atttypid;
			oraTable->cols[index-1]->pgtypmod = att_tuple->atttypmod;
			oraTable->cols[index-1]->pgname = pstrdup(NameStr(att_tuple->attname));
		}

		/* loop through column options */
		options = GetForeignColumnOptions(foreigntableid, att_tuple->attnum);
		foreach(option, options)
		{
			DefElem *def = (DefElem *)lfirst(option);

			/* is it the "key" option and is it set to "true" ? */
			if (strcmp(def->defname, OPT_KEY) == 0 && optionIsTrue(strVal(def->arg)))
			{
				/* mark the column as primary key column */
				oraTable->cols[index-1]->pkey = 1;
			}
			else if (strcmp(def->defname, OPT_STRIP_ZEROS) == 0 && optionIsTrue(strVal(def->arg)))
				oraTable->cols[index-1]->strip_zeros = 1;
		}
	}

	table_close(rel, NoLock);
}

/*
 * createQuery
 * 		Construct a query string for Oracle that
 * 		a) contains only the necessary columns in the SELECT list
 * 		b) has all the WHERE and ORDER BY clauses that can safely be translated to Oracle.
 * 		Untranslatable clauses are omitted and left for PostgreSQL to check.
 * 		"query_pathkeys" contains the desired sort order of the scan results
 * 		which will be translated to ORDER BY clauses if possible.
 *		As a side effect for base relations, we also mark the used columns in oraTable.
 */
char
*createQuery(struct OracleFdwState *fdwState, RelOptInfo *foreignrel, bool for_update, List *query_pathkeys)
{
	ListCell *cell;
	bool in_quote = false;
	int i, index;
	char *wherecopy, *p, hash_str[17], parname[10], *separator = "";
	long queryhash;
	StringInfoData query, result;
	List *columnlist,
		*conditions = foreignrel->baserestrictinfo;

#if PG_VERSION_NUM < 90600
	columnlist = foreignrel->reltargetlist;
#else
	columnlist = foreignrel->reltarget->exprs;
#endif

#ifdef JOIN_API
	if (IS_SIMPLE_REL(foreignrel))
#endif  /* JOIN_API */
	{
		/* find all the columns to include in the select list */

		/* examine each SELECT list entry for Var nodes */
		foreach(cell, columnlist)
		{
			getUsedColumns((Expr *)lfirst(cell), fdwState->oraTable, foreignrel->relid);
		}

		/* examine each condition for Var nodes */
		foreach(cell, conditions)
		{
			getUsedColumns((Expr *)lfirst(cell), fdwState->oraTable, foreignrel->relid);
		}
	}

	/* construct SELECT list */
	initStringInfo(&query);
	for (i=0; i<fdwState->oraTable->ncols; ++i)
	{
		if (fdwState->oraTable->cols[i]->used)
		{
			char *format;
			StringInfoData alias;

			initStringInfo(&alias);
			/* table alias is created from range table index */
			ADD_REL_QUALIFIER(&alias, fdwState->oraTable->cols[i]->varno);

			/* format for qualified column name */
			if (fdwState->oraTable->cols[i]->oratype == ORA_TYPE_XMLTYPE)
				/* convert XML to CLOB in the query */
				format = "%s(%s%s).getclobval()";
			else if (fdwState->oraTable->cols[i]->oratype == ORA_TYPE_TIMESTAMPLTZ)
				/* convert TIMESTAMP WITH LOCAL TIME ZONE to TIMESTAMP WITH TIME ZONE */
				format = "%s(%s%s AT TIME ZONE sessiontimezone)";
			else
				/* select the column as it is */
				format = "%s%s%s";

			appendStringInfo(&query,
							 format,
							 separator,
							 alias.data,
							 fdwState->oraTable->cols[i]->name);

			separator = ", ";
		}
	}

	/* dummy column if there is no result column we need from Oracle */
	if (separator[0] == '\0')
		appendStringInfo(&query, "'1'");

	/* append FROM clause */
	appendStringInfo(&query, " FROM ");
	deparseFromExprForRel(fdwState, &query, foreignrel,
							&(fdwState->params));

	/*
	 * For base relations and OUTER joins, add a WHERE clause if there is one.
	 *
	 * For an INNER join, all conditions that are pushed down get added
	 * to fdwState->joinclauses and have already been added above,
	 * so there is no extra WHERE clause.
	 */
	if (fdwState->where_clause)
		appendStringInfo(&query, "%s", fdwState->where_clause);

	/* append ORDER BY clause if all its expressions can be pushed down */
	if (fdwState->order_clause)
		appendStringInfo(&query, " ORDER BY%s", fdwState->order_clause);

	/* append FETCH FIRST n ROWS ONLY if the LIMIT can be pushed down */
	if (fdwState->limit_clause && !for_update)
		appendStringInfo(&query, " %s", fdwState->limit_clause);

	/* append FOR UPDATE if if the scan is for a modification */
	if (for_update)
		appendStringInfo(&query, " FOR UPDATE");

	/* get a copy of the where clause without single quoted string literals */
	wherecopy = pstrdup(query.data);
	for (p=wherecopy; *p!='\0'; ++p)
	{
		if (*p == '\'')
			in_quote = ! in_quote;
		if (in_quote)
			*p = ' ';
	}

	/* remove all parameters that do not actually occur in the query */
	index = 0;
	foreach(cell, fdwState->params)
	{
		++index;
		snprintf(parname, 10, ":p%d", index);
		if (strstr(wherecopy, parname) == NULL)
		{
			/* set the element to NULL to indicate it's gone */
			lfirst(cell) = NULL;
		}
	}

	pfree(wherecopy);

	/*
	 * Calculate a hash of the query string so far.
	 * This is needed to find the query in Oracle's library cache for EXPLAIN.
	 */
	queryhash = hash_bytes_extended((unsigned char *)query.data, strlen(query.data), 0);
	snprintf(hash_str, 17, "%08lx", queryhash);

	/* add comment with hash to query */
	initStringInfo(&result);
	appendStringInfo(&result, "SELECT /*%s*/ %s", hash_str, query.data);
	pfree(query.data);

	return result.data;
}

/*
 * deparseFromExprForRel
 * 		Construct FROM clause for given relation.
 * 		The function constructs ... JOIN ... ON ... for join relation. For a base
 * 		relation it just returns the table name.
 * 		All tables get an alias based on the range table index.
 */
static void
deparseFromExprForRel(struct OracleFdwState *fdwState, StringInfo buf, RelOptInfo *foreignrel, List **params_list)
{
#ifdef JOIN_API
	if (IS_SIMPLE_REL(foreignrel))
	{
#endif  /* JOIN_API */
		appendStringInfo(buf, "%s", fdwState->oraTable->name);

		appendStringInfo(buf, " %s%d", REL_ALIAS_PREFIX, foreignrel->relid);
#ifdef JOIN_API
	}
	else
	{
		/* join relation */
		RelOptInfo *rel_o = fdwState->outerrel;
		RelOptInfo *rel_i = fdwState->innerrel;
		StringInfoData join_sql_o;
		StringInfoData join_sql_i;
		struct OracleFdwState *fdwState_o = (struct OracleFdwState *) rel_o->fdw_private;
		struct OracleFdwState *fdwState_i = (struct OracleFdwState *) rel_i->fdw_private;

		/* Deparse outer relation */
		initStringInfo(&join_sql_o);
		deparseFromExprForRel(fdwState_o, &join_sql_o, rel_o, params_list);

		/* Deparse inner relation */
		initStringInfo(&join_sql_i);
		deparseFromExprForRel(fdwState_i, &join_sql_i, rel_i, params_list);

		/*
		 * For a join relation FROM clause entry is deparsed as
		 *
		 * (outer relation) <join type> (inner relation) ON joinclauses
		 */
		appendStringInfo(buf, "(%s %s JOIN %s ON ",
						join_sql_o.data,
						get_jointype_name(fdwState->jointype),
						join_sql_i.data
		);

		/* we can only get here if the join is pushed down, so there are join clauses */
		Assert(fdwState->joinclauses);
		appendConditions(fdwState->joinclauses, buf, foreignrel, params_list);

		/* End the FROM clause entry. */
		appendStringInfo(buf, ")");
	}
#endif  /* JOIN_API */
}

#ifdef JOIN_API
/*
 * appendConditions
 * 		Deparse conditions from the provided list and append them to buf.
 * 		The conditions in the list are assumed to be ANDed.
 * 		This function is used to deparse JOIN ... ON clauses.
 */
static void
appendConditions(List *exprs, StringInfo buf, RelOptInfo *joinrel, List **params_list)
{
	ListCell *lc;
	bool is_first = true;
	char *where;

	foreach(lc, exprs)
	{
		Expr  *expr = (Expr *) lfirst(lc);

		/* extract clause from RestrictInfo, if required */
		if (IsA(expr, RestrictInfo))
		{
			RestrictInfo *ri = (RestrictInfo *) expr;
			expr = ri->clause;
		}

		/* connect expressions with AND */
		if (!is_first)
			appendStringInfo(buf, " AND ");

		/* deparse and append a join condition */
		where = deparseExpr(NULL, joinrel, expr, NULL, params_list);
		appendStringInfo(buf, "%s", where);

		is_first = false;
	}
}

/*
 * foreign_join_ok
 * 		Assess whether the join between inner and outer relations can be pushed down
 * 		to the foreign server.
 */
static bool
foreign_join_ok(PlannerInfo *root, RelOptInfo *joinrel, JoinType jointype,
								RelOptInfo *outerrel, RelOptInfo *innerrel,
								JoinPathExtraData *extra)
{
	struct OracleFdwState *fdwState;
	struct OracleFdwState *fdwState_o;
	struct OracleFdwState *fdwState_i;

	ListCell   *lc;
	List	   *joinclauses;   /* join quals */
	List	   *otherclauses;  /* pushed-down (other) quals */

	/* we support pushing down INNER/OUTER joins */
	if (jointype != JOIN_INNER && jointype != JOIN_LEFT &&
		jointype != JOIN_RIGHT && jointype != JOIN_FULL)
		return false;

	fdwState = (struct OracleFdwState *) joinrel->fdw_private;
	fdwState_o = (struct OracleFdwState *) outerrel->fdw_private;
	fdwState_i = (struct OracleFdwState *) innerrel->fdw_private;

	if (!fdwState_o || !fdwState_i)
		return false;

	fdwState->outerrel = outerrel;
	fdwState->innerrel = innerrel;
	fdwState->jointype = jointype;

	/*
	 * If joining relations have local conditions, those conditions are
	 * required to be applied before joining the relations. Hence the join can
	 * not be pushed down.
	 */
	if (fdwState_o->local_conds || fdwState_i->local_conds)
		return false;

	/* separate restrict list into join quals and pushed-down (other) quals from extra->restrictlist */
	if (IS_OUTER_JOIN(jointype))
	{
		extract_actual_join_clauses(extra->restrictlist, joinrel->relids, &joinclauses, &otherclauses);

		/* CROSS JOIN (T1 LEFT/RIGHT/FULL JOIN T2 ON true) is not pushed down */
		if (joinclauses == NIL)
		{
			return false;
		}

		/* join quals must be safe to push down */
		foreach(lc, joinclauses)
		{
			Expr *expr = (Expr *) lfirst(lc);

			if (!deparseExpr(fdwState->session, joinrel, expr, fdwState->oraTable, &(fdwState->params)))
				return false;
		}

		/* save the join clauses, for later use */
		fdwState->joinclauses = joinclauses;
	}
	else
	{
		/*
		 * Unlike an outer join, for inner join, the join result contains only
		 * the rows which satisfy join clauses, similar to the other clause.
		 * Hence all clauses can be treated the same.
		 *
		 * Note that all join conditions will become remote_conds and
		 * eventually joinclauses again.
		 */
		otherclauses = extract_actual_clauses(extra->restrictlist, false);
		joinclauses = NIL;
	}

	/*
	 * If there is a PlaceHolderVar that needs to be evaluated at a lower level
	 * than the complete join relation, it may happen that a reference from
	 * outside is wrongly evaluated to a non-NULL value.
	 * This can happen if the reason for the value to be NULL is that it comes from
	 * the nullable side of an outer join.
	 * So we don't push down the join in this case - if PostgreSQL performs the join,
	 * it will evaluate the placeholder correctly.
	 */
	foreach(lc, root->placeholder_list)
	{
		PlaceHolderInfo *phinfo = lfirst(lc);
		Relids      relids;

#if PG_VERSION_NUM < 110000
		relids = joinrel->relids;
#else
		/* PlaceHolderInfo refers to parent relids, not child relids. */
		relids = IS_OTHER_REL(joinrel) ?
				joinrel->top_parent_relids : joinrel->relids;
#endif  /* PG_VERSION_NUM */

		if (bms_is_subset(phinfo->ph_eval_at, relids) &&
			bms_nonempty_difference(relids, phinfo->ph_eval_at))
		{
			return false;
		}
	}

	/*
	 * For inner joins, "otherclauses" contains now the join conditions.
	 * For outer joins, it means these Restrictinfos were pushed down from other relation.
	 *
	 * Check which ones can be pushed down to remote server.
	 */
	foreach(lc, otherclauses)
	{
		Expr *expr = (Expr *) lfirst(lc);
	
		if (deparseExpr(fdwState->session, joinrel, expr, fdwState->oraTable, &(fdwState->params)))
			fdwState->remote_conds = lappend(fdwState->remote_conds, expr);
		else
			fdwState->local_conds = lappend(fdwState->local_conds, expr);
	}

	/*
	 * Only push down joins for which all join conditions can be pushed down.
	 *
	 * For an INNER join it would be ok to only push own some of the join
	 * conditions and evaluate the others locally, but we cannot be certain
	 * that such a plan is a good or even a feasible one:
	 * With one of the join conditions missing in the pushed down query,
	 * it could be that the "intermediate" join result fetched from the Oracle
	 * side has many more rows than the complete join result.
	 *
	 * We could rely on estimates to see how many rows are returned from such
	 * a join where not all join conditions can be pushed down, but we choose
	 * the safe road of not pushing down such joins at all.
	 */

	if (!IS_OUTER_JOIN(jointype))
	{
		/* for an inner join, we use all or nothing approach */
		if (fdwState->local_conds != NIL)
			return false;

		/* CROSS JOIN (T1 JOIN T2 ON true) is not pushed down */
		if (fdwState->remote_conds == NIL)
			return false;
	}

	/*
	 * Pull the other remote conditions from the joining relations into join
	 * clauses or other remote clauses (remote_conds) of this relation
	 * wherever possible. This avoids building subqueries at every join step,
	 * which is not currently supported by the deparser logic.
	 *
	 * For an INNER join, clauses from both the relations are added to the
	 * other remote clauses.
	 *
	 * For LEFT and RIGHT OUTER join, the clauses from the outer side are added
	 * to remote_conds since those can be evaluated after the join is evaluated.
	 * The clauses from inner side are added to the joinclauses, since they
	 * need to evaluated while constructing the join.
	 *
	 * For a FULL OUTER JOIN, the other clauses from either relation can not
	 * be added to the joinclauses or remote_conds, since each relation acts
	 * as an outer relation for the other. Consider such full outer join as
	 * unshippable because of the reasons mentioned above in this comment.
	 *
	 * The joining sides can not have local conditions, thus no need to test
	 * shippability of the clauses being pulled up.
	 */
	switch (jointype)
	{
		case JOIN_INNER:
			fdwState->remote_conds = list_concat(fdwState->remote_conds,
										  list_copy(fdwState_i->remote_conds));
			fdwState->remote_conds = list_concat(fdwState->remote_conds,
										  list_copy(fdwState_o->remote_conds));
			break;

		case JOIN_LEFT:
			fdwState->joinclauses = list_concat(fdwState->joinclauses,
										  list_copy(fdwState_i->remote_conds));
			fdwState->remote_conds = list_concat(fdwState->remote_conds,
										  list_copy(fdwState_o->remote_conds));
			break;

		case JOIN_RIGHT:
			fdwState->joinclauses = list_concat(fdwState->joinclauses,
										  list_copy(fdwState_o->remote_conds));
			fdwState->remote_conds = list_concat(fdwState->remote_conds,
										  list_copy(fdwState_i->remote_conds));
			break;

		case JOIN_FULL:
			if (fdwState_i->remote_conds || fdwState_o->remote_conds)
				return false;

			break;

		default:
			/* Should not happen, we have just checked this above */
			elog(ERROR, "unsupported join type %d", jointype);
	}

	if (IS_OUTER_JOIN(jointype))
	{
		StringInfoData where; /* for outer join's WHERE clause */
		char *keyword = "WHERE";

		/*
		 * For outer join, deparse remote_conds and store it in fdwState->where_clause.
		 * It will be used in createQuery.
		 */
		initStringInfo(&where);
		if (fdwState->remote_conds != NIL)
		{
			foreach(lc, fdwState->remote_conds)
			{
				char *tmp = NULL;
				Expr *expr = (Expr *) lfirst(lc);

				tmp = deparseExpr(fdwState->session, joinrel, expr, fdwState->oraTable, &(fdwState->params));
				Assert(tmp);
				appendStringInfo(&where, " %s %s", keyword, tmp);
				keyword = "AND";
			}
			fdwState->where_clause = where.data;
		}
	}
	else
	{
		/* for an inner join, remote_conds has all join conditions */
		Assert(!fdwState->joinclauses);
		fdwState->joinclauses = fdwState->remote_conds;
		fdwState->remote_conds = NIL;
	}

	/* set fetch size to minimum of the joining sides */
	if (fdwState_o->prefetch < fdwState_i->prefetch)
		fdwState->prefetch = fdwState_o->prefetch;
	else
		fdwState->prefetch = fdwState_i->prefetch;

	/* set LOB prefetch size to maximum of the joining sides */
	if (fdwState_o->lob_prefetch < fdwState_i->lob_prefetch)
		fdwState->lob_prefetch = fdwState_i->lob_prefetch;
	else
		fdwState->lob_prefetch = fdwState_o->lob_prefetch;

	/* copy outerrel's infomation to fdwstate */
	fdwState->dbserver = fdwState_o->dbserver;
	fdwState->isolation_level = fdwState_o->isolation_level;
	fdwState->user     = fdwState_o->user;
	fdwState->password = fdwState_o->password;
	fdwState->nls_lang = fdwState_o->nls_lang;
	fdwState->timezone = fdwState_o->timezone;
	fdwState->have_nchar = fdwState_o->have_nchar;

	foreach(lc, pull_var_clause((Node *)joinrel->reltarget->exprs, PVC_RECURSE_PLACEHOLDERS))
	{
		Var *var = (Var *) lfirst(lc);

		Assert(IsA(var, Var));

		/*
		 * Whole-row references and system columns are not pushed down.
		 * ToDo: support whole-row by creating oraColumns for that.
		 */
		if (var->varattno <= 0)
			return false;
	}

	return true;
}

/* Output join name for given join type */
const char *
get_jointype_name(JoinType jointype)
{
	switch (jointype)
	{
		case JOIN_INNER:
			return "INNER";

		case JOIN_LEFT:
			return "LEFT";

		case JOIN_RIGHT:
			return "RIGHT";

		case JOIN_FULL:
			return "FULL";

		default:
			/* Shouldn't come here, but protect from buggy code. */
			elog(ERROR, "unsupported join type %d", jointype);
	}

	/* Keep compiler happy */
	return NULL;
}

/*
 * Build the targetlist for given relation to be deparsed as SELECT clause.
 *
 * The output targetlist contains the columns that need to be fetched from the
 * foreign server for the given relation.
 */
List *
build_tlist_to_deparse(RelOptInfo *foreignrel)
{
	List *tlist = NIL;
	struct OracleFdwState *fdwState = (struct OracleFdwState *)foreignrel->fdw_private;

	/*
	 * We require columns specified in foreignrel->reltarget->exprs and those
	 * required for evaluating the local conditions.
	 */
	tlist = add_to_flat_tlist(tlist,
							  pull_var_clause((Node *) foreignrel->reltarget->exprs,
											  PVC_RECURSE_PLACEHOLDERS));
	tlist = add_to_flat_tlist(tlist,
							  pull_var_clause((Node *) fdwState->local_conds,
											  PVC_RECURSE_PLACEHOLDERS));

	return tlist;
}

/*
 * Fill fdwState->oraTable with a table constructed from the
 * inner and outer tables using the target list in "fdw_scan_tlist".
 */
struct oraTable *
build_join_oratable(struct OracleFdwState *fdwState, List *fdw_scan_tlist)
{
	struct oraTable	*oraTable;
	char			*tabname;		/* for warning messages */
	List			*targetvars;	/* pulled Vars from targetlist */
	ListCell		*lc;
	struct OracleFdwState *fdwState_o = (struct OracleFdwState *)fdwState->outerrel->fdw_private;
	struct OracleFdwState *fdwState_i = (struct OracleFdwState *)fdwState->innerrel->fdw_private;
	struct oraTable *oraTable_o = fdwState_o->oraTable;
	struct oraTable *oraTable_i = fdwState_i->oraTable;

	oraTable = (struct oraTable *) palloc0(sizeof(struct oraTable));
	oraTable->name = pstrdup("");
	oraTable->pgname = pstrdup("");
	oraTable->ncols = 0;
	oraTable->npgcols = 0;
	oraTable->cols = (struct oraColumn **) palloc0(sizeof(struct oraColumn*) *
												(oraTable_o->ncols + oraTable_i->ncols));

	/*
	 * Search oraColumn from children's oraTable.
	 * Here we assume that children are foreign table, not foreign join.
	 * We need capability to track relid chain through join tree to support N-way join.
	 *
	 * Note: This code is O(#columns^2), but we have no better idea currently.
	 */
	tabname = "?";
	/* get only Vars because there is not only Vars but also PlaceHolderVars in below exprs */
	targetvars = pull_var_clause((Node *)fdw_scan_tlist, PVC_RECURSE_PLACEHOLDERS);
	foreach(lc, targetvars)
	{
		int i;
		Var *var = (Var *) lfirst(lc);
		struct oraColumn *col = NULL;
		struct oraColumn *newcol;
		int used_flag = 0;

		Assert(IsA(var, Var));

		/* find appropriate entry from children's oraTable */
		for (i=0; i<oraTable_o->ncols; ++i)
		{
			struct oraColumn *tmp = oraTable_o->cols[i];

			if (tmp->varno == var->varno)
			{
				tabname = oraTable_o->pgname;

				if (tmp->pgattnum == var->varattno)
				{
					col = tmp;
					break;
				}
			}
		}
		if (!col)
		{
			for (i=0; i<oraTable_i->ncols; ++i)
			{
				struct oraColumn *tmp = oraTable_i->cols[i];

				if (tmp->varno == var->varno)
				{
					tabname = oraTable_i->pgname;

					if (tmp->pgattnum == var->varattno)
					{
						col = tmp;
						break;
					}
				}
			}
		}

		newcol = (struct oraColumn*) palloc0(sizeof(struct oraColumn));
		if (col)
		{
			memcpy(newcol, col, sizeof(struct oraColumn));
			used_flag = 1;
		}
		else
			/* non-existing column, print a warning */
			ereport(WARNING,
					(errcode(ERRCODE_WARNING),
					errmsg("column number %d of foreign table \"%s\" does not exist in foreign Oracle table, will be replaced by NULL", var->varattno, tabname)));

		newcol->used = used_flag;
		/* pgattnum should be the index in SELECT clause of join query. */
		newcol->pgattnum = oraTable->ncols + 1;

		oraTable->cols[oraTable->ncols++] = newcol;
	}

	oraTable->npgcols = oraTable->ncols;

	return oraTable;
}
#endif  /* JOIN_API */

/*
 * acquireSampleRowsFunc
 * 		Perform a sequential scan on the Oracle table and return a sampe of rows.
 * 		All LOB values are truncated to WIDTH_THRESHOLD+1 because anything
 * 		exceeding this is not used by compute_scalar_stats().
 */
int
acquireSampleRowsFunc(Relation relation, int elevel, HeapTuple *rows, int targrows, double *totalrows, double *totaldeadrows)
{
	int collected_rows = 0, i;
	struct OracleFdwState *fdw_state;
	bool first_column = true;
	StringInfoData query;
	TupleDesc tupDesc = RelationGetDescr(relation);
	Datum *values = (Datum *)palloc(tupDesc->natts * sizeof(Datum));
	bool *nulls = (bool *)palloc(tupDesc->natts * sizeof(bool));
	double rstate, rowstoskip = -1, sample_percent;
	MemoryContext old_cxt, tmp_cxt;
	unsigned int index;

	elog(DEBUG1, "oracle_fdw: analyze foreign table %d", RelationGetRelid(relation));

	*totalrows = 0;

	/* create a memory context for short-lived data in convertTuple() */
	tmp_cxt = AllocSetContextCreate(CurrentMemoryContext,
								"oracle_fdw temporary data",
								ALLOCSET_SMALL_SIZES);

	/* Prepare for sampling rows */
	rstate = anl_init_selection_state(targrows);

	/*
	 * Get connection options, connect and get the remote table description.
	 * Always use the user mapping for the current user.
	 */
	fdw_state = getFdwState(RelationGetRelid(relation), &sample_percent, InvalidOid);
	fdw_state->paramList = NULL;
	fdw_state->rowcount = 0;
	/* we don't have to prefetch more than that much from a LOB */
	fdw_state->lob_prefetch = WIDTH_THRESHOLD;

	/* construct query */
	initStringInfo(&query);
	appendStringInfo(&query, "SELECT ");

	/* loop columns */
	for (i=0; i<fdw_state->oraTable->ncols; ++i)
	{
		/* don't get LONG, LONG RAW and untranslatable values */
		if (fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_LONG
				|| fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_LONGRAW
				|| fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_GEOMETRY
				|| fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_OTHER)
		{
			fdw_state->oraTable->cols[i]->used = 0;
		}
		else
		{
			/* all columns are used */
			fdw_state->oraTable->cols[i]->used = 1;

			/* allocate memory for return value */
			fdw_state->oraTable->cols[i]->val = (char *)palloc(fdw_state->oraTable->cols[i]->val_size * fdw_state->prefetch);
			fdw_state->oraTable->cols[i]->val_len = (uint16 *)palloc(sizeof(uint16) * fdw_state->prefetch);
			fdw_state->oraTable->cols[i]->val_len4 = 0;
			fdw_state->oraTable->cols[i]->val_null = (int16 *)palloc(sizeof(int16) * fdw_state->prefetch);

			if (first_column)
				first_column = false;
			else
				appendStringInfo(&query, ", ");

			/* append column name */
			appendStringInfo(&query, "%s", fdw_state->oraTable->cols[i]->name);
		}
	}

	/* if there are no columns, use NULL */
	if (first_column)
		appendStringInfo(&query, "NULL");

	/* append Oracle table name */
	appendStringInfo(&query, " FROM %s", fdw_state->oraTable->name);

	/* append SAMPLE clause if appropriate */
	if (sample_percent < 100.0)
		appendStringInfo(&query, " SAMPLE BLOCK (%f)", sample_percent);

	fdw_state->query = query.data;
	elog(DEBUG1, "oracle_fdw: remote query is %s", fdw_state->query);

	/* get PostgreSQL column data types, check that they match Oracle's */
	for (i=0; i<fdw_state->oraTable->ncols; ++i)
		if (fdw_state->oraTable->cols[i]->pgname != NULL
				&& fdw_state->oraTable->cols[i]->used)
			checkDataType(
				fdw_state->oraTable->cols[i]->oratype,
				fdw_state->oraTable->cols[i]->scale,
				fdw_state->oraTable->cols[i]->pgtype,
				fdw_state->oraTable->pgname,
				fdw_state->oraTable->cols[i]->pgname
			);

	/* execute the query */
	oraclePrepareQuery(fdw_state->session, fdw_state->query, fdw_state->oraTable, fdw_state->prefetch, fdw_state->lob_prefetch);
	(void)oracleExecuteQuery(fdw_state->session, fdw_state->oraTable, fdw_state->paramList, fdw_state->prefetch);

	/* loop through query results */
	while((index = oracleFetchNext(fdw_state->session, fdw_state->prefetch)) > 0)
	{
		/* allow user to interrupt ANALYZE */
		vacuum_delay_point();

		++fdw_state->rowcount;

		if (collected_rows < targrows)
		{
			/* the first "targrows" rows are added as samples */

			/* use a temporary memory context during convertTuple */
			old_cxt = MemoryContextSwitchTo(tmp_cxt);
			convertTuple(fdw_state, index, values, nulls, true);
			MemoryContextSwitchTo(old_cxt);

			rows[collected_rows++] = heap_form_tuple(tupDesc, values, nulls);
			MemoryContextReset(tmp_cxt);
		}
		else
		{
			/*
			 * Skip a number of rows before replacing a random sample row.
			 * A more detailed description of the algorithm can be found in analyze.c
			 */
			if (rowstoskip < 0)
				rowstoskip = anl_get_next_S(*totalrows, targrows, &rstate);

			if (rowstoskip <= 0)
			{
				int k = (int)(targrows * anl_random_fract());

				heap_freetuple(rows[k]);

				/* use a temporary memory context during convertTuple */
				old_cxt = MemoryContextSwitchTo(tmp_cxt);
				convertTuple(fdw_state, index, values, nulls, true);
				MemoryContextSwitchTo(old_cxt);

				rows[k] = heap_form_tuple(tupDesc, values, nulls);
				MemoryContextReset(tmp_cxt);
			}
		}
	}

	oracleCloseStatement(fdw_state->session);

	MemoryContextDelete(tmp_cxt);

	*totalrows = (double)fdw_state->rowcount / sample_percent * 100.0;
	*totaldeadrows = 0;

	/* report report */
	ereport(elevel, (errmsg("\"%s\": table contains %lu rows; %d rows in sample",
			RelationGetRelationName(relation), fdw_state->rowcount, collected_rows)));

	return collected_rows;
}

/*
 * appendAsType
 * 		Append "s" to "dest", adding appropriate casts for datetime "type".
 */
void
appendAsType(StringInfoData *dest, const char *s, Oid type)
{
	switch (type)
	{
		case DATEOID:
			appendStringInfo(dest, "CAST (%s AS DATE)", s);
			break;
		case TIMESTAMPOID:
			appendStringInfo(dest, "CAST (%s AS TIMESTAMP)", s);
			break;
		case TIMESTAMPTZOID:
			appendStringInfo(dest, "CAST (%s AS TIMESTAMP WITH TIME ZONE)", s);
			break;
		default:
			appendStringInfo(dest, "%s", s);
	}
}

/*
 * This macro is used by deparseExpr to identify PostgreSQL
 * types that can be translated to Oracle SQL.
 */
#define canHandleType(x) ((x) == TEXTOID || (x) == CHAROID || (x) == BPCHAROID \
			|| (x) == VARCHAROID || (x) == NAMEOID || (x) == INT8OID || (x) == INT2OID \
			|| (x) == INT4OID || (x) == OIDOID || (x) == FLOAT4OID || (x) == FLOAT8OID \
			|| (x) == NUMERICOID || (x) == DATEOID || (x) == TIMESTAMPOID || (x) == TIMESTAMPTZOID \
			|| (x) == INTERVALOID || (x) == UUIDOID)

/*
 * deparseExpr
 * 		Create and return an Oracle SQL string from "expr".
 * 		Returns NULL if that is not possible, else a palloc'ed string.
 * 		As a side effect, all Params incorporated in the WHERE clause
 * 		will be stored in "params".
 */
char *
deparseExpr(oracleSession *session, RelOptInfo *foreignrel, Expr *expr, const struct oraTable *oraTable, List **params)
{
	char *opername, *left, *right, *arg, oprkind;
	char parname[10];
	Param *param;
	Const *constant;
	OpExpr *oper;
	ScalarArrayOpExpr *arrayoper;
	CaseExpr *caseexpr;
	BoolExpr *boolexpr;
	CoalesceExpr *coalesceexpr;
	CoerceViaIO *coerce;
	Var *variable;
	FuncExpr *func;
	Expr *rightexpr;
	ArrayExpr *array;
	ArrayCoerceExpr *arraycoerce;
#if PG_VERSION_NUM >= 100000
	SQLValueFunction *sqlvalfunc;
#endif  /* PG_VERSION_NUM */
	regproc typoutput;
	HeapTuple tuple;
	ListCell *cell;
	StringInfoData result;
	Oid leftargtype, rightargtype, schema;
	oraType oratype;
	ArrayIterator iterator;
	Datum datum;
	bool first_arg, isNull;
	int index;
	StringInfoData alias;
	const struct oraTable *var_table;  /* oraTable that belongs to a Var */

	if (expr == NULL)
		return NULL;

	switch(expr->type)
	{
		case T_Const:
			constant = (Const *)expr;
			if (constant->constisnull)
			{
				/* only translate NULLs of a type Oracle can handle */
				if (canHandleType(constant->consttype))
				{
					initStringInfo(&result);
					appendStringInfo(&result, "NULL");
				}
				else
					return NULL;
			}
			else
			{
				/* get a string representation of the value */
				char *c = datumToString(constant->constvalue, constant->consttype);
				if (c == NULL)
					return NULL;
				else
				{
					initStringInfo(&result);
					appendStringInfo(&result, "%s", c);
				}
			}
			break;
		case T_Param:
			param = (Param *)expr;

			/* don't try to handle interval parameters */
			if (! canHandleType(param->paramtype) || param->paramtype == INTERVALOID)
				return NULL;

			/* find the index in the parameter list */
			index = 0;
			foreach(cell, *params)
			{
				++index;
				if (equal(param, (Node *)lfirst(cell)))
					break;
			}
			if (cell == NULL)
			{
				/* add the parameter to the list */
				++index;
				*params = lappend(*params, param);
			}

			/* parameters will be called :p1, :p2 etc. */
			snprintf(parname, 10, ":p%d", index);
			initStringInfo(&result);
			appendAsType(&result, parname, param->paramtype);

			break;
		case T_Var:
			variable = (Var *)expr;
			var_table = NULL;

			/* check if the variable belongs to one of our foreign tables */
#ifdef JOIN_API
			if (IS_SIMPLE_REL(foreignrel))
			{
#endif  /* JOIN_API */
				if (variable->varno == foreignrel->relid && variable->varlevelsup == 0)
					var_table = oraTable;
#ifdef JOIN_API
			}
			else
			{
				struct OracleFdwState *joinstate = (struct OracleFdwState *)foreignrel->fdw_private;
				struct OracleFdwState *outerstate = (struct OracleFdwState *)joinstate->outerrel->fdw_private;
				struct OracleFdwState *innerstate = (struct OracleFdwState *)joinstate->innerrel->fdw_private;

				/* we can't get here if the foreign table has no columns, so this is safe */
				if (variable->varno == outerstate->oraTable->cols[0]->varno && variable->varlevelsup == 0)
					var_table = outerstate->oraTable;
				if (variable->varno == innerstate->oraTable->cols[0]->varno && variable->varlevelsup == 0)
					var_table = innerstate->oraTable;
			}
#endif  /* JOIN_API */

			if (var_table)
			{
				/* the variable belongs to a foreign table, replace it with the name */

				/* we cannot handle system columns */
				if (variable->varattno < 1)
					return NULL;

				/*
				 * Allow boolean columns here.
				 * They will be rendered as ("COL" <> 0).
				 */
				if (! (canHandleType(variable->vartype) || variable->vartype == BOOLOID))
					return NULL;

				/* get var_table column index corresponding to this column (-1 if none) */
				index = var_table->ncols - 1;
				while (index >= 0 && var_table->cols[index]->pgattnum != variable->varattno)
					--index;

				/* if no Oracle column corresponds, translate as NULL */
				if (index == -1)
				{
					initStringInfo(&result);
					appendStringInfo(&result, "NULL");
					break;
				}

				/*
				 * Don't try to convert a column reference if the type is
				 * converted from a non-string type in Oracle to a string type
				 * in PostgreSQL because functions and operators won't work the same.
				 */
				oratype = var_table->cols[index]->oratype;
				if ((variable->vartype == TEXTOID
						|| variable->vartype == BPCHAROID
						|| variable->vartype == VARCHAROID)
						&& oratype != ORA_TYPE_VARCHAR2
						&& oratype != ORA_TYPE_CHAR
						&& oratype != ORA_TYPE_NVARCHAR2
						&& oratype != ORA_TYPE_NCHAR)
					return NULL;

				initStringInfo(&result);

				/* work around the lack of booleans in Oracle */
				if (variable->vartype == BOOLOID)
					appendStringInfo(&result, "(");

				/* qualify with an alias based on the range table index */
				initStringInfo(&alias);
				ADD_REL_QUALIFIER(&alias, var_table->cols[index]->varno);

				appendStringInfo(&result, "%s%s", alias.data, var_table->cols[index]->name);

				/* work around the lack of booleans in Oracle */
				if (variable->vartype == BOOLOID)
					appendStringInfo(&result, " <> 0)");
			}
			else
			{
				/* treat it like a parameter */
				/* don't try to handle type interval */
				if (! canHandleType(variable->vartype) || variable->vartype == INTERVALOID)
					return NULL;

				/* find the index in the parameter list */
				index = 0;
				foreach(cell, *params)
				{
					++index;
					if (equal(variable, (Node *)lfirst(cell)))
						break;
				}
				if (cell == NULL)
				{
					/* add the parameter to the list */
					++index;
					*params = lappend(*params, variable);
				}

				/* parameters will be called :p1, :p2 etc. */
				initStringInfo(&result);
				appendStringInfo(&result, ":p%d", index);
			}

			break;
		case T_OpExpr:
			oper = (OpExpr *)expr;

			/* get operator name, kind, argument type and schema */
			tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(oper->opno));
			if (! HeapTupleIsValid(tuple))
			{
				elog(ERROR, "cache lookup failed for operator %u", oper->opno);
			}
			opername = pstrdup(((Form_pg_operator)GETSTRUCT(tuple))->oprname.data);
			oprkind = ((Form_pg_operator)GETSTRUCT(tuple))->oprkind;
			leftargtype = ((Form_pg_operator)GETSTRUCT(tuple))->oprleft;
			rightargtype = ((Form_pg_operator)GETSTRUCT(tuple))->oprright;
			schema = ((Form_pg_operator)GETSTRUCT(tuple))->oprnamespace;
			ReleaseSysCache(tuple);

			/* ignore operators in other than the pg_catalog schema */
			if (schema != PG_CATALOG_NAMESPACE)
				return NULL;

			if (! canHandleType(rightargtype))
				return NULL;

			/*
			 * Don't translate operations on two intervals.
			 * INTERVAL YEAR TO MONTH and INTERVAL DAY TO SECOND don't mix well.
			 */
			if (leftargtype == INTERVALOID && rightargtype == INTERVALOID)
				return NULL;

			/* the operators that we can translate */
			if (strcmp(opername, "=") == 0
				|| strcmp(opername, "<>") == 0
				/* string comparisons are not safe */
				|| (strcmp(opername, ">") == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID
					&& rightargtype != NAMEOID && rightargtype != CHAROID)
				|| (strcmp(opername, "<") == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID
					&& rightargtype != NAMEOID && rightargtype != CHAROID)
				|| (strcmp(opername, ">=") == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID
					&& rightargtype != NAMEOID && rightargtype != CHAROID)
				|| (strcmp(opername, "<=") == 0 && rightargtype != TEXTOID && rightargtype != BPCHAROID
					&& rightargtype != NAMEOID && rightargtype != CHAROID)
				|| strcmp(opername, "+") == 0
				/* subtracting DATEs yields a NUMBER in Oracle */
				|| (strcmp(opername, "-") == 0 && rightargtype != DATEOID && rightargtype != TIMESTAMPOID
					&& rightargtype != TIMESTAMPTZOID)
				|| strcmp(opername, "*") == 0
				|| strcmp(opername, "~~") == 0
				|| strcmp(opername, "!~~") == 0
				|| strcmp(opername, "~~*") == 0
				|| strcmp(opername, "!~~*") == 0
				|| strcmp(opername, "^") == 0
				|| strcmp(opername, "%") == 0
				|| strcmp(opername, "&") == 0
				|| strcmp(opername, "|/") == 0
				|| strcmp(opername, "@") == 0)
			{
				left = deparseExpr(session, foreignrel, linitial(oper->args), oraTable, params);
				if (left == NULL)
				{
					pfree(opername);
					return NULL;
				}

				if (oprkind == 'b')
				{
					/* binary operator */
					right = deparseExpr(session, foreignrel, lsecond(oper->args), oraTable, params);
					if (right == NULL)
					{
						pfree(left);
						pfree(opername);
						return NULL;
					}

					initStringInfo(&result);
					if (strcmp(opername, "~~") == 0)
					{
						appendStringInfo(&result, "(%s LIKE %s ESCAPE '\\')", left, right);
					}
					else if (strcmp(opername, "!~~") == 0)
					{
						appendStringInfo(&result, "(%s NOT LIKE %s ESCAPE '\\')", left, right);
					}
					else if (strcmp(opername, "~~*") == 0)
					{
						appendStringInfo(&result, "(UPPER(%s) LIKE UPPER(%s) ESCAPE '\\')", left, right);
					}
					else if (strcmp(opername, "!~~*") == 0)
					{
						appendStringInfo(&result, "(UPPER(%s) NOT LIKE UPPER(%s) ESCAPE '\\')", left, right);
					}
					else if (strcmp(opername, "^") == 0)
					{
						appendStringInfo(&result, "POWER(%s, %s)", left, right);
					}
					else if (strcmp(opername, "%") == 0)
					{
						appendStringInfo(&result, "MOD(%s, %s)", left, right);
					}
					else if (strcmp(opername, "&") == 0)
					{
						appendStringInfo(&result, "BITAND(%s, %s)", left, right);
					}
					else
					{
						/* the other operators have the same name in Oracle */
						appendStringInfo(&result, "(%s %s %s)", left, opername, right);
					}
					pfree(right);
					pfree(left);
				}
				else
				{
					/* unary operator */
					initStringInfo(&result);
					if (strcmp(opername, "|/") == 0)
					{
						appendStringInfo(&result, "SQRT(%s)", left);
					}
					else if (strcmp(opername, "@") == 0)
					{
						appendStringInfo(&result, "ABS(%s)", left);
					}
					else
					{
						/* unary + or - */
						appendStringInfo(&result, "(%s%s)", opername, left);
					}
					pfree(left);
				}
			}
			else
			{
				/* cannot translate this operator */
				pfree(opername);
				return NULL;
			}

			pfree(opername);
			break;
		case T_ScalarArrayOpExpr:
			arrayoper = (ScalarArrayOpExpr *)expr;

			/* get operator name, left argument type and schema */
			tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(arrayoper->opno));
			if (! HeapTupleIsValid(tuple))
			{
				elog(ERROR, "cache lookup failed for operator %u", arrayoper->opno);
			}
			opername = pstrdup(((Form_pg_operator)GETSTRUCT(tuple))->oprname.data);
			leftargtype = ((Form_pg_operator)GETSTRUCT(tuple))->oprleft;
			schema = ((Form_pg_operator)GETSTRUCT(tuple))->oprnamespace;
			ReleaseSysCache(tuple);

			/* get the type's output function */
			tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(leftargtype));
			if (!HeapTupleIsValid(tuple))
			{
				elog(ERROR, "cache lookup failed for type %u", leftargtype);
			}
			typoutput = ((Form_pg_type)GETSTRUCT(tuple))->typoutput;
			ReleaseSysCache(tuple);

			/* ignore operators in other than the pg_catalog schema */
			if (schema != PG_CATALOG_NAMESPACE)
				return NULL;

			/* don't try to push down anything but IN and NOT IN expressions */
			if ((strcmp(opername, "=") != 0 || ! arrayoper->useOr)
					&& (strcmp(opername, "<>") != 0 || arrayoper->useOr))
				return NULL;

			if (! canHandleType(leftargtype))
				return NULL;

			left = deparseExpr(session, foreignrel, linitial(arrayoper->args), oraTable, params);
			if (left == NULL)
				return NULL;

			/* begin to compose result */
			initStringInfo(&result);
			appendStringInfo(&result, "(%s %s (", left, arrayoper->useOr ? "IN" : "NOT IN");

			/* the second (=last) argument can be Const, ArrayExpr or ArrayCoerceExpr */
			rightexpr = (Expr *)llast(arrayoper->args);
			switch (rightexpr->type)
			{
				case T_Const:
					/* the second (=last) argument is a Const of ArrayType */
					constant = (Const *)rightexpr;

					/* using NULL in place of an array or value list is valid in Oracle and PostgreSQL */
					if (constant->constisnull)
						appendStringInfo(&result, "NULL");
					else
					{
						ArrayType *arr = DatumGetArrayTypeP(constant->constvalue);

						/* loop through the array elements */
						iterator = array_create_iterator(arr, 0);
						first_arg = true;
						while (array_iterate(iterator, &datum, &isNull))
						{
							char *c;

							if (isNull)
								c = "NULL";
							else
							{
								c = datumToString(datum, ARR_ELEMTYPE(arr));
								if (c == NULL)
								{
									array_free_iterator(iterator);
									return NULL;
								}
							}

							/* append the argument */
							appendStringInfo(&result, "%s%s", first_arg ? "" : ", ", c);
							first_arg = false;
						}
						array_free_iterator(iterator);

						/* don't push down empty arrays, since the semantics for NOT x = ANY(<empty array>) differ */
						if (first_arg)
							return NULL;
					}

					break;

				case T_ArrayCoerceExpr:
					/* the second (=last) argument is an ArrayCoerceExpr */
					arraycoerce = (ArrayCoerceExpr *)rightexpr;

					/* if the conversion requires more than binary coercion, don't push it down */
#if PG_VERSION_NUM < 110000
					if (arraycoerce->elemfuncid != InvalidOid)
						return NULL;
#else
					if (arraycoerce->elemexpr && arraycoerce->elemexpr->type != T_RelabelType)
						return NULL;
#endif

					/* punt on anything but ArrayExpr (e.g, parameters) */
					if (arraycoerce->arg->type != T_ArrayExpr)
						return NULL;

					/* the actual array is here */
					rightexpr = arraycoerce->arg;

					/* fall through ! */

				case T_ArrayExpr:
					/* the second (=last) argument is an ArrayExpr */
					array = (ArrayExpr *)rightexpr;

					/* loop the array arguments */
					first_arg = true;
					foreach(cell, array->elements)
					{
						/* convert the argument to a string */
						char *element = deparseExpr(session, foreignrel, (Expr *)lfirst(cell), oraTable, params);

						/* if any element cannot be converted, give up */
						if (element == NULL)
							return NULL;

						/* append the argument */
						appendStringInfo(&result, "%s%s", first_arg ? "" : ", ", element);
						first_arg = false;
					}

					/* don't push down empty arrays, since the semantics for NOT x = ANY(<empty array>) differ */
					if (first_arg)
						return NULL;

					break;

				default:
					return NULL;
			}

			/* two parentheses close the expression */
			appendStringInfo(&result, "))");

			break;
		case T_NullIfExpr:
			/* get argument type */
			tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(((NullIfExpr *)expr)->opno));
			if (! HeapTupleIsValid(tuple))
			{
				elog(ERROR, "cache lookup failed for operator %u", ((NullIfExpr *)expr)->opno);
			}
			rightargtype = ((Form_pg_operator)GETSTRUCT(tuple))->oprright;
			ReleaseSysCache(tuple);

			if (! canHandleType(rightargtype))
				return NULL;

			left = deparseExpr(session, foreignrel, linitial(((NullIfExpr *)expr)->args), oraTable, params);
			if (left == NULL)
			{
				return NULL;
			}
			right = deparseExpr(session, foreignrel, lsecond(((NullIfExpr *)expr)->args), oraTable, params);
			if (right == NULL)
			{
				pfree(left);
				return NULL;
			}

			initStringInfo(&result);
			appendStringInfo(&result, "NULLIF(%s, %s)", left, right);

			break;
		case T_BoolExpr:
			boolexpr = (BoolExpr *)expr;

			arg = deparseExpr(session, foreignrel, linitial(boolexpr->args), oraTable, params);
			if (arg == NULL)
				return NULL;

			initStringInfo(&result);
			appendStringInfo(&result, "(%s%s",
					boolexpr->boolop == NOT_EXPR ? "NOT " : "",
					arg);

			do_each_cell(cell, boolexpr->args, list_next(boolexpr->args, list_head(boolexpr->args)))
			{
				arg = deparseExpr(session, foreignrel, (Expr *)lfirst(cell), oraTable, params);
				if (arg == NULL)
				{
					pfree(result.data);
					return NULL;
				}

				appendStringInfo(&result, " %s %s",
						boolexpr->boolop == AND_EXPR ? "AND" : "OR",
						arg);
			}
			appendStringInfo(&result, ")");

			break;
		case T_RelabelType:
			return deparseExpr(session, foreignrel, ((RelabelType *)expr)->arg, oraTable, params);
			break;
		case T_CoerceToDomain:
			return deparseExpr(session, foreignrel, ((CoerceToDomain *)expr)->arg, oraTable, params);
			break;
		case T_CaseExpr:
			caseexpr = (CaseExpr *)expr;

			if (! canHandleType(caseexpr->casetype))
				return NULL;

			initStringInfo(&result);
			appendStringInfo(&result, "CASE");

			/* for the form "CASE arg WHEN ...", add first expression */
			if (caseexpr->arg != NULL)
			{
				arg = deparseExpr(session, foreignrel, caseexpr->arg, oraTable, params);
				if (arg == NULL)
				{
					pfree(result.data);
					return NULL;
				}
				else
				{
					appendStringInfo(&result, " %s", arg);
				}
			}

			/* append WHEN ... THEN clauses */
			foreach(cell, caseexpr->args)
			{
				CaseWhen *whenclause = (CaseWhen *)lfirst(cell);

				/* WHEN */
				if (caseexpr->arg == NULL)
				{
					/* for CASE WHEN ..., use the whole expression */
					arg = deparseExpr(session, foreignrel, whenclause->expr, oraTable, params);
				}
				else
				{
					/* for CASE arg WHEN ..., use only the right branch of the equality */
					arg = deparseExpr(session, foreignrel, lsecond(((OpExpr *)whenclause->expr)->args), oraTable, params);
				}

				if (arg == NULL)
				{
					pfree(result.data);
					return NULL;
				}
				else
				{
					appendStringInfo(&result, " WHEN %s", arg);
					pfree(arg);
				}

				/* THEN */
				arg = deparseExpr(session, foreignrel, whenclause->result, oraTable, params);
				if (arg == NULL)
				{
					pfree(result.data);
					return NULL;
				}
				else
				{
					appendStringInfo(&result, " THEN %s", arg);
					pfree(arg);
				}
			}

			/* append ELSE clause if appropriate */
			if (caseexpr->defresult != NULL)
			{
				arg = deparseExpr(session, foreignrel, caseexpr->defresult, oraTable, params);
				if (arg == NULL)
				{
					pfree(result.data);
					return NULL;
				}
				else
				{
					appendStringInfo(&result, " ELSE %s", arg);
					pfree(arg);
				}
			}

			/* append END */
			appendStringInfo(&result, " END");
			break;
		case T_CoalesceExpr:
			coalesceexpr = (CoalesceExpr *)expr;

			if (! canHandleType(coalesceexpr->coalescetype))
				return NULL;

			initStringInfo(&result);
			appendStringInfo(&result, "COALESCE(");

			first_arg = true;
			foreach(cell, coalesceexpr->args)
			{
				arg = deparseExpr(session, foreignrel, (Expr *)lfirst(cell), oraTable, params);
				if (arg == NULL)
				{
					pfree(result.data);
					return NULL;
				}

				if (first_arg)
				{
					appendStringInfo(&result, "%s", arg);
					first_arg = false;
				}
				else
				{
					appendStringInfo(&result, ", %s", arg);
				}
				pfree(arg);
			}

			appendStringInfo(&result, ")");

			break;
		case T_NullTest:
			rightexpr = ((NullTest *)expr)->arg;

			/* since booleans are translated as (expr <> 0), we cannot push them down */
			if (exprType((Node *)rightexpr) == BOOLOID)
				return NULL;

			arg = deparseExpr(session, foreignrel, rightexpr, oraTable, params);
			if (arg == NULL)
				return NULL;

			initStringInfo(&result);
			appendStringInfo(&result, "(%s IS %sNULL)",
					arg,
					((NullTest *)expr)->nulltesttype == IS_NOT_NULL ? "NOT " : "");
			break;
		case T_FuncExpr:
			func = (FuncExpr *)expr;

			if (! canHandleType(func->funcresulttype))
				return NULL;

			/* do nothing for implicit casts */
			if (func->funcformat == COERCE_IMPLICIT_CAST)
				return deparseExpr(session, foreignrel, linitial(func->args), oraTable, params);

			/* get function name and schema */
			tuple = SearchSysCache1(PROCOID, ObjectIdGetDatum(func->funcid));
			if (! HeapTupleIsValid(tuple))
			{
				elog(ERROR, "cache lookup failed for function %u", func->funcid);
			}
			opername = pstrdup(((Form_pg_proc)GETSTRUCT(tuple))->proname.data);
			schema = ((Form_pg_proc)GETSTRUCT(tuple))->pronamespace;
			ReleaseSysCache(tuple);

			/* ignore functions in other than the pg_catalog schema */
			if (schema != PG_CATALOG_NAMESPACE)
				return NULL;

			/* the "normal" functions that we can translate */
			if (strcmp(opername, "abs") == 0
				|| strcmp(opername, "acos") == 0
				|| strcmp(opername, "asin") == 0
				|| strcmp(opername, "atan") == 0
				|| strcmp(opername, "atan2") == 0
				|| strcmp(opername, "ceil") == 0
				|| strcmp(opername, "ceiling") == 0
				|| strcmp(opername, "char_length") == 0
				|| strcmp(opername, "character_length") == 0
				|| strcmp(opername, "concat") == 0
				|| strcmp(opername, "cos") == 0
				|| strcmp(opername, "exp") == 0
				|| strcmp(opername, "initcap") == 0
				|| strcmp(opername, "length") == 0
				|| strcmp(opername, "lower") == 0
				|| strcmp(opername, "lpad") == 0
				|| strcmp(opername, "ltrim") == 0
				|| strcmp(opername, "mod") == 0
				|| strcmp(opername, "octet_length") == 0
				|| strcmp(opername, "position") == 0
				|| strcmp(opername, "pow") == 0
				|| strcmp(opername, "power") == 0
				|| strcmp(opername, "replace") == 0
				|| strcmp(opername, "round") == 0
				|| strcmp(opername, "rpad") == 0
				|| strcmp(opername, "rtrim") == 0
				|| strcmp(opername, "sign") == 0
				|| strcmp(opername, "sin") == 0
				|| strcmp(opername, "sqrt") == 0
				|| strcmp(opername, "strpos") == 0
				|| strcmp(opername, "substr") == 0
				|| (strcmp(opername, "substring") == 0 && list_length(func->args) == 3)
				|| strcmp(opername, "tan") == 0
				|| strcmp(opername, "to_char") == 0
				|| strcmp(opername, "to_date") == 0
				|| strcmp(opername, "to_number") == 0
				|| strcmp(opername, "to_timestamp") == 0
				|| strcmp(opername, "translate") == 0
				|| strcmp(opername, "trunc") == 0
				|| strcmp(opername, "upper") == 0)
			{
				initStringInfo(&result);

				if (strcmp(opername, "ceiling") == 0)
					appendStringInfo(&result, "CEIL(");
				else if (strcmp(opername, "char_length") == 0
						|| strcmp(opername, "character_length") == 0)
					appendStringInfo(&result, "LENGTH(");
				else if (strcmp(opername, "pow") == 0)
					appendStringInfo(&result, "POWER(");
				else if (strcmp(opername, "octet_length") == 0)
					appendStringInfo(&result, "LENGTHB(");
				else if (strcmp(opername, "position") == 0
						|| strcmp(opername, "strpos") == 0)
					appendStringInfo(&result, "INSTR(");
				else if (strcmp(opername, "substring") == 0)
					appendStringInfo(&result, "SUBSTR(");
				else
					appendStringInfo(&result, "%s(", opername);

				first_arg = true;
				foreach(cell, func->args)
				{
					arg = deparseExpr(session, foreignrel, lfirst(cell), oraTable, params);
					if (arg == NULL)
					{
						pfree(result.data);
						pfree(opername);
						return NULL;
					}

					if (first_arg)
					{
						first_arg = false;
						appendStringInfo(&result, "%s", arg);
					}
					else
					{
						appendStringInfo(&result, ", %s", arg);
					}
					pfree(arg);
				}

				appendStringInfo(&result, ")");
			}
			else if (strcmp(opername, "date_part") == 0)
			{
				/* special case: EXTRACT */
				left = deparseExpr(session, foreignrel, linitial(func->args), oraTable, params);
				if (left == NULL)
				{
					pfree(opername);
					return NULL;
				}

				/* can only handle these fields in Oracle */
				if (strcmp(left, "'year'") == 0
					|| strcmp(left, "'month'") == 0
					|| strcmp(left, "'day'") == 0
					|| strcmp(left, "'hour'") == 0
					|| strcmp(left, "'minute'") == 0
					|| strcmp(left, "'second'") == 0
					|| strcmp(left, "'timezone_hour'") == 0
					|| strcmp(left, "'timezone_minute'") == 0)
				{
					/* remove final quote */
					left[strlen(left) - 1] = '\0';

					right = deparseExpr(session, foreignrel, lsecond(func->args), oraTable, params);
					if (right == NULL)
					{
						pfree(opername);
						pfree(left);
						return NULL;
					}

					initStringInfo(&result);
					appendStringInfo(&result, "EXTRACT(%s FROM %s)", left + 1, right);
				}
				else
				{
					pfree(opername);
					pfree(left);
					return NULL;
				}

				pfree(left);
				pfree(right);
			}
			else if (strcmp(opername, "now") == 0
				|| strcmp(opername, "current_timestamp") == 0
				|| strcmp(opername, "transaction_timestamp") == 0)
			{
				/* special case: current timestamp */
				initStringInfo(&result);
				appendStringInfo(&result, "(CAST (:now AS TIMESTAMP WITH TIME ZONE))");
			}
			else if (strcmp(opername, "current_date") == 0)
			{
				/* special case: current_date */
				initStringInfo(&result);
				appendStringInfo(&result, "TRUNC(CAST (CAST(:now AS TIMESTAMP WITH TIME ZONE) AS DATE))");
			}
			else if (strcmp(opername, "localtimestamp") == 0)
			{
				/* special case: localtimestamp */
				initStringInfo(&result);
				appendStringInfo(&result, "(CAST (CAST (:now AS TIMESTAMP WITH TIME ZONE) AS TIMESTAMP))");
			}
			else
			{
				/* function that we cannot render for Oracle */
				pfree(opername);
				return NULL;
			}

			pfree(opername);
			break;
		case T_CoerceViaIO:
			/*
			 * We will only handle casts of 'now'.
			 *
			 * This is how "current_timestamp", "current_date" and
			 * "localtimestamp" were represented before PostgreSQL v10.
			 * I am not sure whether this code path can be reached in later
			 * versions, but it doesn't hurt to leave the code for now.
			 */
			coerce = (CoerceViaIO *)expr;

			/* only casts to these types are handled */
			if (coerce->resulttype != DATEOID
					&& coerce->resulttype != TIMESTAMPOID
					&& coerce->resulttype != TIMESTAMPTZOID)
				return NULL;

			/* the argument must be a Const */
			if (coerce->arg->type != T_Const)
				return NULL;

			/* the argument must be a not-NULL text constant */
			constant = (Const *)coerce->arg;
			if (constant->constisnull || (constant->consttype != CSTRINGOID && constant->consttype != TEXTOID))
				return NULL;

			/* get the type's output function */
			tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(constant->consttype));
			if (!HeapTupleIsValid(tuple))
			{
				elog(ERROR, "cache lookup failed for type %u", constant->consttype);
			}
			typoutput = ((Form_pg_type)GETSTRUCT(tuple))->typoutput;
			ReleaseSysCache(tuple);

			/* the value must be "now" */
			if (strcmp(DatumGetCString(OidFunctionCall1(typoutput, constant->constvalue)), "now") != 0)
				return NULL;

			initStringInfo(&result);
			switch (coerce->resulttype)
			{
				case DATEOID:
					appendStringInfo(&result, "TRUNC(CAST (CAST(:now AS TIMESTAMP WITH TIME ZONE) AS DATE))");
					break;
				case TIMESTAMPOID:
					appendStringInfo(&result, "(CAST (CAST (:now AS TIMESTAMP WITH TIME ZONE) AS TIMESTAMP))");
					break;
				case TIMESTAMPTZOID:
					appendStringInfo(&result, "(CAST (:now AS TIMESTAMP WITH TIME ZONE))");
			}

			break;
#if PG_VERSION_NUM >= 100000
		case T_SQLValueFunction:
			sqlvalfunc = (SQLValueFunction *)expr;

			switch (sqlvalfunc->op)
			{
				case SVFOP_CURRENT_DATE:
					initStringInfo(&result);
					appendStringInfo(&result, "TRUNC(CAST (CAST(:now AS TIMESTAMP WITH TIME ZONE) AS DATE))");
					break;
				case SVFOP_CURRENT_TIMESTAMP:
					initStringInfo(&result);
					appendStringInfo(&result, "(CAST (:now AS TIMESTAMP WITH TIME ZONE))");
					break;
				case SVFOP_LOCALTIMESTAMP:
					initStringInfo(&result);
					appendStringInfo(&result, "(CAST (CAST (:now AS TIMESTAMP WITH TIME ZONE) AS TIMESTAMP))");
					break;
				default:
					return NULL;  /* don't push down other functions */
			}

			break;
#endif  /* PG_VERSION_NUM >= 100000 */
		default:
			/* we cannot translate this to Oracle */
			return NULL;
	}

	return result.data;
}

/*
 * datumToString
 * 		Convert a Datum to a string by calling the type output function.
 * 		Returns the result or NULL if it cannot be converted to Oracle SQL.
 */
static char
*datumToString(Datum datum, Oid type)
{
	StringInfoData result;
	regproc typoutput;
	HeapTuple tuple;
	char *str, *p;

	/* get the type's output function */
	tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(type));
	if (!HeapTupleIsValid(tuple))
	{
		elog(ERROR, "cache lookup failed for type %u", type);
	}
	typoutput = ((Form_pg_type)GETSTRUCT(tuple))->typoutput;
	ReleaseSysCache(tuple);

	/* render the constant in Oracle SQL */
	switch (type)
	{
		case TEXTOID:
		case CHAROID:
		case BPCHAROID:
		case VARCHAROID:
		case NAMEOID:
		case UUIDOID:
			str = DatumGetCString(OidFunctionCall1(typoutput, datum));

			/*
			 * Don't try to convert empty strings to Oracle.
			 * Oracle treats empty strings as NULL.
			 */
			if (str[0] == '\0')
				return NULL;

			/* strip "-" from "uuid" values */
			if (type == UUIDOID)
				convertUUID(str);

			/* quote string */
			initStringInfo(&result);
			appendStringInfo(&result, "'");
			for (p=str; *p; ++p)
			{
				if (*p == '\'')
					appendStringInfo(&result, "'");
				appendStringInfo(&result, "%c", *p);
			}
			appendStringInfo(&result, "'");
			break;
		case INT8OID:
		case INT2OID:
		case INT4OID:
		case OIDOID:
		case FLOAT4OID:
		case FLOAT8OID:
		case NUMERICOID:
			str = DatumGetCString(OidFunctionCall1(typoutput, datum));
			initStringInfo(&result);
			appendStringInfo(&result, "%s", str);
			break;
		case DATEOID:
			str = deparseDate(datum);
			initStringInfo(&result);
			appendStringInfo(&result, "(CAST ('%s' AS DATE))", str);
			break;
		case TIMESTAMPOID:
			str = deparseTimestamp(datum, false);
			initStringInfo(&result);
			appendStringInfo(&result, "(CAST ('%s' AS TIMESTAMP))", str);
			break;
		case TIMESTAMPTZOID:
			str = deparseTimestamp(datum, true);
			initStringInfo(&result);
			appendStringInfo(&result, "(CAST ('%s' AS TIMESTAMP WITH TIME ZONE))", str);
			break;
		case INTERVALOID:
			str = deparseInterval(datum);
			if (str == NULL)
				return NULL;
			initStringInfo(&result);
			appendStringInfo(&result, "%s", str);
			break;
		default:
			return NULL;
	}

	return result.data;
}

/*
 * getUsedColumns
 * 		Set "used=true" in oraTable for all columns used in the expression.
 */
void
getUsedColumns(Expr *expr, struct oraTable *oraTable, int foreignrelid)
{
	ListCell *cell;
	Var *variable;
	int index;

	if (expr == NULL)
		return;

	switch(expr->type)
	{
		case T_RestrictInfo:
			getUsedColumns(((RestrictInfo *)expr)->clause, oraTable, foreignrelid);
			break;
		case T_TargetEntry:
			getUsedColumns(((TargetEntry *)expr)->expr, oraTable, foreignrelid);
			break;
		case T_Const:
		case T_Param:
		case T_CaseTestExpr:
		case T_CoerceToDomainValue:
		case T_CurrentOfExpr:
#if PG_VERSION_NUM >= 100000
		case T_NextValueExpr:
#endif
			break;
		case T_Var:
			variable = (Var *)expr;

			/* ignore columns belonging to a different foreign table */
			if (variable->varno != foreignrelid)
				break;

			/* ignore system columns */
			if (variable->varattno < 0)
				break;

			/* if this is a wholerow reference, we need all columns */
			if (variable->varattno == 0) {
				for (index=0; index<oraTable->ncols; ++index)
					if (oraTable->cols[index]->pgname)
						oraTable->cols[index]->used = 1;
				break;
			}

			/* get oraTable column index corresponding to this column (-1 if none) */
			index = oraTable->ncols - 1;
			while (index >= 0 && oraTable->cols[index]->pgattnum != variable->varattno)
				--index;

			if (index == -1)
			{
				ereport(WARNING,
						(errcode(ERRCODE_WARNING),
						errmsg("column number %d of foreign table \"%s\" does not exist in foreign Oracle table, will be replaced by NULL", variable->varattno, oraTable->pgname)));
			}
			else
			{
				oraTable->cols[index]->used = 1;
			}
			break;
		case T_Aggref:
			foreach(cell, ((Aggref *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			foreach(cell, ((Aggref *)expr)->aggorder)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			foreach(cell, ((Aggref *)expr)->aggdistinct)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_WindowFunc:
			foreach(cell, ((WindowFunc *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
#if PG_VERSION_NUM < 120000
		case T_ArrayRef:
			{
				ArrayRef *ref = (ArrayRef *)expr;
#else
		case T_SubscriptingRef:
			{
				SubscriptingRef *ref = (SubscriptingRef *)expr;
#endif

				foreach(cell, ref->refupperindexpr)
				{
					getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
				}
				foreach(cell, ref->reflowerindexpr)
				{
					getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
				}
				getUsedColumns(ref->refexpr, oraTable, foreignrelid);
				getUsedColumns(ref->refassgnexpr, oraTable, foreignrelid);
				break;
			}
		case T_FuncExpr:
			foreach(cell, ((FuncExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_OpExpr:
			foreach(cell, ((OpExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_DistinctExpr:
			foreach(cell, ((DistinctExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_NullIfExpr:
			foreach(cell, ((NullIfExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_ScalarArrayOpExpr:
			foreach(cell, ((ScalarArrayOpExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_BoolExpr:
			foreach(cell, ((BoolExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_SubPlan:
			{
				SubPlan *subplan = (SubPlan *)expr;

				getUsedColumns((Expr *)(subplan->testexpr), oraTable, foreignrelid);

				foreach(cell, subplan->args)
				{
					getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
				}
			}
			break;
		case T_AlternativeSubPlan:
			/* examine only first alternative */
			getUsedColumns((Expr *)linitial(((AlternativeSubPlan *)expr)->subplans), oraTable, foreignrelid);
			break;
		case T_NamedArgExpr:
			getUsedColumns(((NamedArgExpr *)expr)->arg, oraTable, foreignrelid);
			break;
		case T_FieldSelect:
			getUsedColumns(((FieldSelect *)expr)->arg, oraTable, foreignrelid);
			break;
		case T_RelabelType:
			getUsedColumns(((RelabelType *)expr)->arg, oraTable, foreignrelid);
			break;
		case T_CoerceViaIO:
			getUsedColumns(((CoerceViaIO *)expr)->arg, oraTable, foreignrelid);
			break;
		case T_ArrayCoerceExpr:
			getUsedColumns(((ArrayCoerceExpr *)expr)->arg, oraTable, foreignrelid);
			break;
		case T_ConvertRowtypeExpr:
			getUsedColumns(((ConvertRowtypeExpr *)expr)->arg, oraTable, foreignrelid);
			break;
		case T_CollateExpr:
			getUsedColumns(((CollateExpr *)expr)->arg, oraTable, foreignrelid);
			break;
		case T_CaseExpr:
			foreach(cell, ((CaseExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			getUsedColumns(((CaseExpr *)expr)->arg, oraTable, foreignrelid);
			getUsedColumns(((CaseExpr *)expr)->defresult, oraTable, foreignrelid);
			break;
		case T_CaseWhen:
			getUsedColumns(((CaseWhen *)expr)->expr, oraTable, foreignrelid);
			getUsedColumns(((CaseWhen *)expr)->result, oraTable, foreignrelid);
			break;
		case T_ArrayExpr:
			foreach(cell, ((ArrayExpr *)expr)->elements)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_RowExpr:
			foreach(cell, ((RowExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_RowCompareExpr:
			foreach(cell, ((RowCompareExpr *)expr)->largs)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			foreach(cell, ((RowCompareExpr *)expr)->rargs)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_CoalesceExpr:
			foreach(cell, ((CoalesceExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_MinMaxExpr:
			foreach(cell, ((MinMaxExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_XmlExpr:
			foreach(cell, ((XmlExpr *)expr)->named_args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			foreach(cell, ((XmlExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable, foreignrelid);
			}
			break;
		case T_NullTest:
			getUsedColumns(((NullTest *)expr)->arg, oraTable, foreignrelid);
			break;
		case T_BooleanTest:
			getUsedColumns(((BooleanTest *)expr)->arg, oraTable, foreignrelid);
			break;
		case T_CoerceToDomain:
			getUsedColumns(((CoerceToDomain *)expr)->arg, oraTable, foreignrelid);
			break;
		case T_PlaceHolderVar:
			getUsedColumns(((PlaceHolderVar *)expr)->phexpr, oraTable, foreignrelid);
			break;
#if PG_VERSION_NUM >= 100000
		case T_SQLValueFunction:
			break;  /* contains no column references */
#endif  /* PG_VERSION_NUM */
		default:
			/*
			 * We must be able to handle all node types that can
			 * appear because we cannot omit a column from the remote
			 * query that will be needed.
			 * Throw an error if we encounter an unexpected node type.
			 */
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_REPLY),
					errmsg("Internal oracle_fdw error: encountered unknown node type %d.", expr->type)));
	}
}

/*
 * checkDataType
 * 		Check that the Oracle data type of a column can be
 * 		converted to the PostgreSQL data type, raise an error if not.
 */
void
checkDataType(oraType oratype, int scale, Oid pgtype, const char *tablename, const char *colname)
{
	/* the binary Oracle types can be converted to bytea */
	if ((oratype == ORA_TYPE_RAW
			|| oratype == ORA_TYPE_BLOB
			|| oratype == ORA_TYPE_BFILE
			|| oratype == ORA_TYPE_LONGRAW)
			&& pgtype == BYTEAOID)
		return;

	/* Oracle RAW can be converted to uuid */
	if (oratype == ORA_TYPE_RAW && pgtype == UUIDOID)
		return;

	/* all other Oracle types can be transformed to strings */
	if (oratype != ORA_TYPE_OTHER
			&& oratype != ORA_TYPE_RAW
			&& oratype != ORA_TYPE_BLOB
			&& oratype != ORA_TYPE_BFILE
			&& oratype != ORA_TYPE_LONGRAW
			&& (pgtype == TEXTOID || pgtype == VARCHAROID || pgtype == BPCHAROID))
		return;

	/* all numeric Oracle types can be transformed to floating point types */
	if ((oratype == ORA_TYPE_NUMBER
			|| oratype == ORA_TYPE_FLOAT
			|| oratype == ORA_TYPE_BINARYFLOAT
			|| oratype == ORA_TYPE_BINARYDOUBLE)
			&& (pgtype == NUMERICOID
			|| pgtype == FLOAT4OID
			|| pgtype == FLOAT8OID))
		return;

	/*
	 * NUMBER columns without decimal fractions can be transformed to
	 * integers or booleans
	 */
	if (oratype == ORA_TYPE_NUMBER && scale <= 0
			&& (pgtype == INT2OID
			|| pgtype == INT4OID
			|| pgtype == INT8OID
			|| pgtype == BOOLOID))
		return;

	/* DATE and timestamps can be transformed to each other */
	if ((oratype == ORA_TYPE_DATE
			|| oratype == ORA_TYPE_TIMESTAMP
			|| oratype == ORA_TYPE_TIMESTAMPTZ
			|| oratype == ORA_TYPE_TIMESTAMPLTZ)
			&& (pgtype == DATEOID
			|| pgtype == TIMESTAMPOID
			|| pgtype == TIMESTAMPTZOID))
		return;

	/* interval types can be transformed to interval */
	if ((oratype == ORA_TYPE_INTERVALY2M
			|| oratype == ORA_TYPE_INTERVALD2S)
			&& pgtype == INTERVALOID)
		return;
	/* SDO_GEOMETRY can be converted to geometry */
	if (oratype == ORA_TYPE_GEOMETRY
			&& pgtype == GEOMETRYOID)
		return;

	/* VARCHAR2 and CLOB can be converted to json */
	if ((oratype == ORA_TYPE_VARCHAR2
			|| oratype == ORA_TYPE_CLOB)
			&& pgtype == JSONOID)
		return;

	/* XMLTYPE can be converted to xml */
	if (oratype == ORA_TYPE_XMLTYPE && pgtype == XMLOID)
		return;

	/* otherwise, report an error */
	ereport(ERROR,
			(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
			errmsg(
				"column \"%s\" (%d) of foreign table \"%s\" cannot be converted to or from Oracle data type (%d)",
				colname, pgtype, tablename, oratype)));
}

/*
 * deparseWhereConditions
 * 		Classify conditions into remote_conds or local_conds.
 * 		Those conditions that can be pushed down will be collected into
 * 		an Oracle WHERE clause that is returned.
 */
char *
deparseWhereConditions(struct OracleFdwState *fdwState, RelOptInfo *baserel, List **local_conds, List **remote_conds)
{
	List *conditions = baserel->baserestrictinfo;
	ListCell *cell;
	char *where;
	char *keyword = "WHERE";
	StringInfoData where_clause;

	initStringInfo(&where_clause);
	foreach(cell, conditions)
	{
		/* check if the condition can be pushed down */
		where = deparseExpr(
					fdwState->session, baserel,
					((RestrictInfo *)lfirst(cell))->clause,
					fdwState->oraTable,
					&(fdwState->params)
				);
		if (where != NULL) {
			*remote_conds = lappend(*remote_conds, ((RestrictInfo *)lfirst(cell))->clause);

			/* append new WHERE clause to query string */
			appendStringInfo(&where_clause, " %s %s", keyword, where);
			keyword = "AND";
			pfree(where);
		}
		else
			*local_conds = lappend(*local_conds, ((RestrictInfo *)lfirst(cell))->clause);
	}
	return where_clause.data;
}

/*
 * guessNlsLang
 * 		If nls_lang is not NULL, return "NLS_LANG=<nls_lang>".
 * 		Otherwise, return a good guess for Oracle's NLS_LANG.
 */
char
*guessNlsLang(char *nls_lang)
{
	char *server_encoding, *lc_messages, *language = "AMERICAN_AMERICA", *charset = NULL;
	StringInfoData buf;

	initStringInfo(&buf);
	if (nls_lang == NULL)
	{
		server_encoding = pstrdup(GetConfigOption("server_encoding", false, true));

		/* find an Oracle client character set that matches the database encoding */
		if (strcmp(server_encoding, "UTF8") == 0)
			charset = "AL32UTF8";
		else if (strcmp(server_encoding, "EUC_JP") == 0)
			charset = "JA16EUC";
		else if (strcmp(server_encoding, "EUC_JIS_2004") == 0)
			charset = "JA16SJIS";
		else if (strcmp(server_encoding, "EUC_TW") == 0)
			charset = "ZHT32EUC";
		else if (strcmp(server_encoding, "ISO_8859_5") == 0)
			charset = "CL8ISO8859P5";
		else if (strcmp(server_encoding, "ISO_8859_6") == 0)
			charset = "AR8ISO8859P6";
		else if (strcmp(server_encoding, "ISO_8859_7") == 0)
			charset = "EL8ISO8859P7";
		else if (strcmp(server_encoding, "ISO_8859_8") == 0)
			charset = "IW8ISO8859P8";
		else if (strcmp(server_encoding, "KOI8R") == 0)
			charset = "CL8KOI8R";
		else if (strcmp(server_encoding, "KOI8U") == 0)
			charset = "CL8KOI8U";
		else if (strcmp(server_encoding, "LATIN1") == 0)
			charset = "WE8ISO8859P1";
		else if (strcmp(server_encoding, "LATIN2") == 0)
			charset = "EE8ISO8859P2";
		else if (strcmp(server_encoding, "LATIN3") == 0)
			charset = "SE8ISO8859P3";
		else if (strcmp(server_encoding, "LATIN4") == 0)
			charset = "NEE8ISO8859P4";
		else if (strcmp(server_encoding, "LATIN5") == 0)
			charset = "WE8ISO8859P9";
		else if (strcmp(server_encoding, "LATIN6") == 0)
			charset = "NE8ISO8859P10";
		else if (strcmp(server_encoding, "LATIN7") == 0)
			charset = "BLT8ISO8859P13";
		else if (strcmp(server_encoding, "LATIN8") == 0)
			charset = "CEL8ISO8859P14";
		else if (strcmp(server_encoding, "LATIN9") == 0)
			charset = "WE8ISO8859P15";
		else if (strcmp(server_encoding, "WIN866") == 0)
			charset = "RU8PC866";
		else if (strcmp(server_encoding, "WIN1250") == 0)
			charset = "EE8MSWIN1250";
		else if (strcmp(server_encoding, "WIN1251") == 0)
			charset = "CL8MSWIN1251";
		else if (strcmp(server_encoding, "WIN1252") == 0)
			charset = "WE8MSWIN1252";
		else if (strcmp(server_encoding, "WIN1253") == 0)
			charset = "EL8MSWIN1253";
		else if (strcmp(server_encoding, "WIN1254") == 0)
			charset = "TR8MSWIN1254";
		else if (strcmp(server_encoding, "WIN1255") == 0)
			charset = "IW8MSWIN1255";
		else if (strcmp(server_encoding, "WIN1256") == 0)
			charset = "AR8MSWIN1256";
		else if (strcmp(server_encoding, "WIN1257") == 0)
			charset = "BLT8MSWIN1257";
		else if (strcmp(server_encoding, "WIN1258") == 0)
			charset = "VN8MSWIN1258";
		else
		{
			/* warn if we have to resort to 7-bit ASCII */
			charset = "US7ASCII";

			ereport(WARNING,
					(errcode(ERRCODE_WARNING),
					errmsg("no Oracle character set for database encoding \"%s\"", server_encoding),
					errdetail("All but ASCII characters will be lost."),
					errhint("You can set the option \"%s\" on the foreign data wrapper to force an Oracle character set.", OPT_NLS_LANG)));
		}

		lc_messages = pstrdup(GetConfigOption("lc_messages", false, true));
		/* try to guess those for which there is a backend translation */
		if (strncmp(lc_messages, "de_", 3) == 0 || pg_strncasecmp(lc_messages, "german", 6) == 0)
			language = "GERMAN_GERMANY";
		if (strncmp(lc_messages, "es_", 3) == 0 || pg_strncasecmp(lc_messages, "spanish", 7) == 0)
			language = "SPANISH_SPAIN";
		if (strncmp(lc_messages, "fr_", 3) == 0 || pg_strncasecmp(lc_messages, "french", 6) == 0)
			language = "FRENCH_FRANCE";
		if (strncmp(lc_messages, "in_", 3) == 0 || pg_strncasecmp(lc_messages, "indonesian", 10) == 0)
			language = "INDONESIAN_INDONESIA";
		if (strncmp(lc_messages, "it_", 3) == 0 || pg_strncasecmp(lc_messages, "italian", 7) == 0)
			language = "ITALIAN_ITALY";
		if (strncmp(lc_messages, "ja_", 3) == 0 || pg_strncasecmp(lc_messages, "japanese", 8) == 0)
			language = "JAPANESE_JAPAN";
		if (strncmp(lc_messages, "pt_", 3) == 0 || pg_strncasecmp(lc_messages, "portuguese", 10) == 0)
			language = "BRAZILIAN PORTUGUESE_BRAZIL";
		if (strncmp(lc_messages, "ru_", 3) == 0 || pg_strncasecmp(lc_messages, "russian", 7) == 0)
			language = "RUSSIAN_RUSSIA";
		if (strncmp(lc_messages, "tr_", 3) == 0 || pg_strncasecmp(lc_messages, "turkish", 7) == 0)
			language = "TURKISH_TURKEY";
		if (strncmp(lc_messages, "zh_CN", 5) == 0 || pg_strncasecmp(lc_messages, "chinese-simplified", 18) == 0)
			language = "SIMPLIFIED CHINESE_CHINA";
		if (strncmp(lc_messages, "zh_TW", 5) == 0 || pg_strncasecmp(lc_messages, "chinese-traditional", 19) == 0)
			language = "TRADITIONAL CHINESE_TAIWAN";

		appendStringInfo(&buf, "NLS_LANG=%s.%s", language, charset);
	}
	else
	{
		appendStringInfo(&buf, "NLS_LANG=%s", nls_lang);
	}

	elog(DEBUG1, "oracle_fdw: set %s", buf.data);

	return buf.data;
}

/*
 * getTimeZone
 * 		session time zone in the format ORA_SDTZ=...
 */
char *
getTimezone()
{
	StringInfoData buf;

	initStringInfo(&buf);
	appendStringInfo(&buf, "ORA_SDTZ=%s", pg_get_timezone_name(session_timezone));

	elog(DEBUG1, "oracle_fdw: set %s", buf.data);
	return buf.data;
}

oracleSession *
oracleConnectServer(Name srvname)
{
	Oid srvId = InvalidOid;
	HeapTuple tup;
	Relation rel;
	ForeignServer *server;
	UserMapping *mapping;
	ForeignDataWrapper *wrapper;
	List *options;
	ListCell *cell;
	char *nls_lang = NULL, *timezone = NULL, *user = NULL, *password = NULL, *dbserver = NULL;
	oraIsoLevel isolation_level = DEFAULT_ISOLATION_LEVEL;
	bool have_nchar = false;

	/* look up foreign server with this name */
	rel = table_open(ForeignServerRelationId, AccessShareLock);

	tup = SearchSysCacheCopy1(FOREIGNSERVERNAME, NameGetDatum(srvname));
	if (!HeapTupleIsValid(tup))
		ereport(ERROR,
			(errcode(ERRCODE_UNDEFINED_OBJECT),
			errmsg("server \"%s\" does not exist", NameStr(*srvname))));

#if PG_VERSION_NUM < 120000
	srvId = HeapTupleGetOid(tup);
#else
	srvId = ((Form_pg_foreign_server)GETSTRUCT(tup))->oid;
#endif

	table_close(rel, AccessShareLock);

	/* get the foreign server, the user mapping and the FDW */
	server = GetForeignServer(srvId);
	mapping = GetUserMapping(GetUserId(), srvId);
	wrapper = GetForeignDataWrapper(server->fdwid);

	/* get all options for these objects */
	options = wrapper->options;
	options = list_concat(options, server->options);
	options = list_concat(options, mapping->options);

	foreach(cell, options)
	{
		DefElem *def = (DefElem *) lfirst(cell);
		if (strcmp(def->defname, OPT_NLS_LANG) == 0)
			nls_lang = strVal(def->arg);
		if (strcmp(def->defname, OPT_DBSERVER) == 0)
			dbserver = strVal(def->arg);
		if (strcmp(def->defname, OPT_ISOLATION_LEVEL) == 0)
			isolation_level = getIsolationLevel(strVal(def->arg));
		if (strcmp(def->defname, OPT_USER) == 0)
			user = strVal(def->arg);
		if (strcmp(def->defname, OPT_PASSWORD) == 0)
			password = strVal(def->arg);
		if (strcmp(def->defname, OPT_NCHAR) == 0)
		{
			char *nchar = strVal(def->arg);

			if ((pg_strcasecmp(nchar, "on") == 0
				|| pg_strcasecmp(nchar, "yes") == 0
				|| pg_strcasecmp(nchar, "true") == 0))
			have_nchar = true;
		}
		if (strcmp(def->defname, OPT_SET_TIMEZONE) == 0)
		{
			char *settz = strVal(def->arg);

			if ((pg_strcasecmp(settz, "on") == 0
				|| pg_strcasecmp(settz, "yes") == 0
				|| pg_strcasecmp(settz, "true") == 0))
			timezone = getTimezone();
		}
	}

	/* guess a good NLS_LANG environment setting */
	nls_lang = guessNlsLang(nls_lang);

	/* connect to Oracle database */
	return oracleGetSession(
		dbserver,
		isolation_level,
		user,
		password,
		nls_lang,
		timezone,
		(int)have_nchar,
		NULL,
		1
	);
}


#define serializeInt(x) makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum((int32)(x)), false, true)
#define serializeOid(x) makeConst(OIDOID, -1, InvalidOid, 4, ObjectIdGetDatum(x), false, true)

/*
 * serializePlanData
 * 		Create a List representation of plan data that copyObject can copy.
 * 		This List can be parsed by deserializePlanData.
 */

List
*serializePlanData(struct OracleFdwState *fdwState)
{
	List *result = NIL;
	int i, len = 0;
	const struct paramDesc *param;

	/* dbserver */
	result = lappend(result, serializeString(fdwState->dbserver));
	/* isolation_level */
	result = lappend(result, serializeInt((int)fdwState->isolation_level));
	/* have_nchar */
	result = lappend(result, serializeInt((int)fdwState->have_nchar));
	/* user name */
	result = lappend(result, serializeString(fdwState->user));
	/* password */
	result = lappend(result, serializeString(fdwState->password));
	/* nls_lang */
	result = lappend(result, serializeString(fdwState->nls_lang));
	/* timezone */
	result = lappend(result, serializeString(fdwState->timezone));
	/* query */
	result = lappend(result, serializeString(fdwState->query));
	/* Oracle prefetch count */
	result = lappend(result, serializeInt((int)fdwState->prefetch));
	/* Oracle LOB prefetch size */
	result = lappend(result, serializeInt((int)fdwState->lob_prefetch));
	/* Oracle table name */
	result = lappend(result, serializeString(fdwState->oraTable->name));
	/* PostgreSQL table name */
	result = lappend(result, serializeString(fdwState->oraTable->pgname));
	/* number of columns in Oracle table */
	result = lappend(result, serializeInt(fdwState->oraTable->ncols));
	/* number of columns in PostgreSQL table */
	result = lappend(result, serializeInt(fdwState->oraTable->npgcols));
	/* column data */
	for (i=0; i<fdwState->oraTable->ncols; ++i)
	{
		result = lappend(result, serializeString(fdwState->oraTable->cols[i]->name));
		result = lappend(result, serializeInt(fdwState->oraTable->cols[i]->oratype));
		result = lappend(result, serializeInt(fdwState->oraTable->cols[i]->scale));
		result = lappend(result, serializeString(fdwState->oraTable->cols[i]->pgname));
		result = lappend(result, serializeInt(fdwState->oraTable->cols[i]->pgattnum));
		result = lappend(result, serializeOid(fdwState->oraTable->cols[i]->pgtype));
		result = lappend(result, serializeInt(fdwState->oraTable->cols[i]->pgtypmod));
		result = lappend(result, serializeInt(fdwState->oraTable->cols[i]->used));
		result = lappend(result, serializeInt(fdwState->oraTable->cols[i]->strip_zeros));
		result = lappend(result, serializeInt(fdwState->oraTable->cols[i]->pkey));
		result = lappend(result, serializeInt(fdwState->oraTable->cols[i]->val_size));
		/* don't serialize val, val_len, val_len4, val_null and varno */
	}

	/* find length of parameter list */
	for (param=fdwState->paramList; param; param=param->next)
		++len;
	/* serialize length */
	result = lappend(result, serializeInt(len));
	/* parameter list entries */
	for (param=fdwState->paramList; param; param=param->next)
	{
		result = lappend(result, serializeString(param->name));
		result = lappend(result, serializeOid(param->type));
		result = lappend(result, serializeInt((int)param->bindType));
		result = lappend(result, serializeInt((int)param->colnum));
		/* don't serialize value, node and bindh */
	}
	/*
	 * Don't serialize params, startup_cost, total_cost, rowcount, columnindex,
	 * temp_cxt, order_clause, usable_pathkeys and where_clause.
	 */

	return result;
}

/*
 * serializeString
 * 		Create a Const that contains the string.
 */

Const
*serializeString(const char *s)
{
	if (s == NULL)
		return makeNullConst(TEXTOID, -1, InvalidOid);
	else
		return makeConst(TEXTOID, -1, InvalidOid, -1, PointerGetDatum(cstring_to_text(s)), false, false);
}

/*
 * deserializePlanData
 * 		Extract the data structures from a List created by serializePlanData.
 * 		Allocates memory for values returned from Oracle.
 */

struct OracleFdwState
*deserializePlanData(List *list)
{
	struct OracleFdwState *state = palloc(sizeof(struct OracleFdwState));
	ListCell *cell = list_head(list);
	int i, len;
	struct paramDesc *param;

	/* session will be set upon connect */
	state->session = NULL;
	/* these fields are not needed during execution */
	state->startup_cost = 0;
	state->total_cost = 0;
	state->order_clause = NULL;
	state->usable_pathkeys = NULL;
	/* these are not serialized */
	state->rowcount = 0;
	state->columnindex = 0;
	state->params = NULL;
	state->temp_cxt = NULL;

	/* dbserver */
	state->dbserver = deserializeString(lfirst(cell));
	cell = list_next(list, cell);

	/* isolation_level */
	state->isolation_level = (oraIsoLevel)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = list_next(list, cell);

	/* have_nchar */
	state->have_nchar = (bool)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = list_next(list, cell);

	/* user */
	state->user = deserializeString(lfirst(cell));
	cell = list_next(list, cell);

	/* password */
	state->password = deserializeString(lfirst(cell));
	cell = list_next(list, cell);

	/* nls_lang */
	state->nls_lang = deserializeString(lfirst(cell));
	cell = list_next(list, cell);

	/* timezone */
	state->timezone = deserializeString(lfirst(cell));
	cell = list_next(list, cell);

	/* query */
	state->query = deserializeString(lfirst(cell));
	cell = list_next(list, cell);

	/* Oracle prefetch count */
	state->prefetch = (unsigned int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = list_next(list, cell);

	/* Oracle LOB prefetch size */
	state->lob_prefetch = (unsigned int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = list_next(list, cell);

	/* table data */
	state->oraTable = (struct oraTable *)palloc(sizeof(struct oraTable));
	state->oraTable->name = deserializeString(lfirst(cell));
	cell = list_next(list, cell);
	state->oraTable->pgname = deserializeString(lfirst(cell));
	cell = list_next(list, cell);
	state->oraTable->ncols = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = list_next(list, cell);
	state->oraTable->npgcols = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = list_next(list, cell);
	state->oraTable->cols = (struct oraColumn **)palloc(sizeof(struct oraColumn *) * state->oraTable->ncols);

	/* loop columns */
	for (i=0; i<state->oraTable->ncols; ++i)
	{
		state->oraTable->cols[i] = (struct oraColumn *)palloc(sizeof(struct oraColumn));
		state->oraTable->cols[i]->name = deserializeString(lfirst(cell));
		cell = list_next(list, cell);
		state->oraTable->cols[i]->oratype = (oraType)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
		state->oraTable->cols[i]->scale = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
		state->oraTable->cols[i]->pgname = deserializeString(lfirst(cell));
		cell = list_next(list, cell);
		state->oraTable->cols[i]->pgattnum = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
		state->oraTable->cols[i]->pgtype = DatumGetObjectId(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
		state->oraTable->cols[i]->pgtypmod = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
		state->oraTable->cols[i]->used = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
		state->oraTable->cols[i]->strip_zeros = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
		state->oraTable->cols[i]->pkey = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
		state->oraTable->cols[i]->val_size = DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
		/*
		 * Allocate memory for the result value.
		 * Multiply the space to allocate with the prefetch count.
		 */
		state->oraTable->cols[i]->val = (char *)palloc(state->oraTable->cols[i]->val_size * state->prefetch);
		state->oraTable->cols[i]->val_len = (uint16 *)palloc(sizeof(uint16) * state->prefetch);
		state->oraTable->cols[i]->val_len4 = 0;
		state->oraTable->cols[i]->val_null = (int16 *)palloc(sizeof(int16) * state->prefetch);
	}

	/* length of parameter list */
	len = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = list_next(list, cell);

	/* parameter table entries */
	state->paramList = NULL;
	for (i=0; i<len; ++i)
	{
		param = (struct paramDesc *)palloc(sizeof(struct paramDesc));
		param->name = deserializeString(lfirst(cell));
		cell = list_next(list, cell);
		param->type = DatumGetObjectId(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
		param->bindType = (oraBindType)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
		if (param->bindType == BIND_OUTPUT)
			param->value = (void *)42;  /* something != NULL */
		else
			param->value = NULL;
		param->node = NULL;
		param->bindh = NULL;
		param->colnum = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = list_next(list, cell);
		param->next = state->paramList;
		state->paramList = param;
	}

	return state;
}

/*
 * deserializeString
 * 		Extracts a string from a Const, returns a palloc'ed copy.
 */

char
*deserializeString(Const *constant)
{
	if (constant->constisnull)
		return NULL;
	else
		return text_to_cstring(DatumGetTextP(constant->constvalue));
}

/*
 * optionIsTrue
 * 		Returns true if the string is "true", "on" or "yes".
 */
bool
optionIsTrue(const char *value)
{
	if (pg_strcasecmp(value, "on") == 0
			|| pg_strcasecmp(value, "yes") == 0
			|| pg_strcasecmp(value, "true") == 0)
		return true;
	else
		return false;
}

/*
 * deparseDate
 * 		Render a PostgreSQL date so that Oracle can parse it.
 */
char *
deparseDate(Datum datum)
{
	struct pg_tm datetime_tm;
	StringInfoData s;

	if (DATE_NOT_FINITE(DatumGetDateADT(datum)))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				errmsg("infinite date value cannot be stored in Oracle")));

	/* get the parts */
	(void)j2date(DatumGetDateADT(datum) + POSTGRES_EPOCH_JDATE,
			&(datetime_tm.tm_year),
			&(datetime_tm.tm_mon),
			&(datetime_tm.tm_mday));

	initStringInfo(&s);
	appendStringInfo(&s, "%04d-%02d-%02d 00:00:00 %s",
			datetime_tm.tm_year > 0 ? datetime_tm.tm_year : -datetime_tm.tm_year + 1,
			datetime_tm.tm_mon, datetime_tm.tm_mday,
			(datetime_tm.tm_year > 0) ? "AD" : "BC");

	return s.data;
}

/*
 * deparseTimestamp
 * 		Render a PostgreSQL timestamp so that Oracle can parse it.
 */
char *
deparseTimestamp(Datum datum, bool hasTimezone)
{
	struct pg_tm datetime_tm;
	int32 tzoffset;
	fsec_t datetime_fsec;
	StringInfoData s;

	/* this is sloppy, but DatumGetTimestampTz and DatumGetTimestamp are the same */
	if (TIMESTAMP_NOT_FINITE(DatumGetTimestampTz(datum)))
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				errmsg("infinite timestamp value cannot be stored in Oracle")));

	/* get the parts */
	tzoffset = 0;
	(void)timestamp2tm(DatumGetTimestampTz(datum),
				hasTimezone ? &tzoffset : NULL,
				&datetime_tm,
				&datetime_fsec,
				NULL,
				NULL);

	initStringInfo(&s);
	if (hasTimezone)
		appendStringInfo(&s, "%04d-%02d-%02d %02d:%02d:%02d.%06d%+03d:%02d %s",
			datetime_tm.tm_year > 0 ? datetime_tm.tm_year : -datetime_tm.tm_year + 1,
			datetime_tm.tm_mon, datetime_tm.tm_mday, datetime_tm.tm_hour,
			datetime_tm.tm_min, datetime_tm.tm_sec, (int32)datetime_fsec,
			-tzoffset / 3600, ((tzoffset > 0) ? tzoffset % 3600 : -tzoffset % 3600) / 60,
			(datetime_tm.tm_year > 0) ? "AD" : "BC");
	else
		appendStringInfo(&s, "%04d-%02d-%02d %02d:%02d:%02d.%06d %s",
			datetime_tm.tm_year > 0 ? datetime_tm.tm_year : -datetime_tm.tm_year + 1,
			datetime_tm.tm_mon, datetime_tm.tm_mday, datetime_tm.tm_hour,
			datetime_tm.tm_min, datetime_tm.tm_sec, (int32)datetime_fsec,
			(datetime_tm.tm_year > 0) ? "AD" : "BC");

	return s.data;
}

/*
 * deparseInterval
 * 		Render a PostgreSQL timestamp so that Oracle can parse it.
 */
char
*deparseInterval(Datum datum)
{
	struct pg_itm itm;
	StringInfoData s;
	char *sign;

	interval2itm(*DatumGetIntervalP(datum), &itm);

	/* only translate intervals that can be translated to INTERVAL DAY TO SECOND */
	if (itm.tm_year != 0 || itm.tm_mon != 0)
		return NULL;

	/* Oracle intervals have only one sign */
	if (itm.tm_mday < 0 || itm.tm_hour < 0 || itm.tm_min < 0 || itm.tm_sec < 0 || itm.tm_usec < 0)
	{
		sign = "-";
		/* all signs must match */
		if (itm.tm_mday > 0 || itm.tm_hour > 0 || itm.tm_min > 0 || itm.tm_sec > 0 || itm.tm_usec > 0)
			return NULL;
		itm.tm_mday = -itm.tm_mday;
		itm.tm_hour = -itm.tm_hour;
		itm.tm_min = -itm.tm_min;
		itm.tm_sec = -itm.tm_sec;
		itm.tm_usec = -itm.tm_usec;
	}
	else
		sign = "";

	if (itm.tm_hour > 23)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				 errmsg("invalid value for Oracle INTERVAL DAY TO SECOND"),
				 errdetail("The \"hour\" must be less than 24.")));

	initStringInfo(&s);
	appendStringInfo(&s, "INTERVAL '%s%d %02d:%02d:%02d.%06d' DAY(9) TO SECOND(6)",
					 sign, itm.tm_mday, (int)itm.tm_hour, itm.tm_min, itm.tm_sec, itm.tm_usec);

	return s.data;
}

/*
 * convertUUID
 * 		Strip "-" from a PostgreSQL "uuid" so that Oracle can parse it.
 * 		In addition, convert the string to upper case.
 * 		This modifies the argument in place!
 */
char
*convertUUID(char *uuid)
{
	char *p = uuid, *q = uuid, c;

	while (*p != '\0')
	{
		if (*p == '-')
			++p;
		c = *(p++);
		if (c >= 'a' && c <= 'f')
			*(q++) = c - ('a' - 'A');
		else
			*(q++) = c;
	}
	*q = '\0';

	return uuid;
}

/*
 * copyPlanData
 * 		Create a deep copy of the argument, copy only those fields needed for planning.
 */

struct OracleFdwState
*copyPlanData(struct OracleFdwState *orig)
{
	int i;
	struct OracleFdwState *copy = palloc(sizeof(struct OracleFdwState));

	copy->dbserver = pstrdup(orig->dbserver);
	copy->isolation_level = orig->isolation_level;
	copy->have_nchar = orig->have_nchar;
	copy->user = pstrdup(orig->user);
	copy->password = pstrdup(orig->password);
	copy->nls_lang = pstrdup(orig->nls_lang);
	if (orig->timezone == NULL)
		copy->timezone = NULL;
	else
		copy->timezone = pstrdup(orig->timezone);
	copy->session = NULL;
	copy->query = NULL;
	copy->paramList = NULL;
	copy->oraTable = (struct oraTable *)palloc(sizeof(struct oraTable));
	copy->oraTable->name = pstrdup(orig->oraTable->name);
	copy->oraTable->pgname = pstrdup(orig->oraTable->pgname);
	copy->oraTable->ncols = orig->oraTable->ncols;
	copy->oraTable->npgcols = orig->oraTable->npgcols;
	copy->oraTable->cols = (struct oraColumn **)palloc(sizeof(struct oraColumn *) * orig->oraTable->ncols);
	for (i=0; i<orig->oraTable->ncols; ++i)
	{
		copy->oraTable->cols[i] = (struct oraColumn *)palloc(sizeof(struct oraColumn));
		copy->oraTable->cols[i]->name = pstrdup(orig->oraTable->cols[i]->name);
		copy->oraTable->cols[i]->oratype = orig->oraTable->cols[i]->oratype;
		copy->oraTable->cols[i]->scale = orig->oraTable->cols[i]->scale;
		if (orig->oraTable->cols[i]->pgname == NULL)
			copy->oraTable->cols[i]->pgname = NULL;
		else
			copy->oraTable->cols[i]->pgname = pstrdup(orig->oraTable->cols[i]->pgname);
		copy->oraTable->cols[i]->pgattnum = orig->oraTable->cols[i]->pgattnum;
		copy->oraTable->cols[i]->pgtype = orig->oraTable->cols[i]->pgtype;
		copy->oraTable->cols[i]->pgtypmod = orig->oraTable->cols[i]->pgtypmod;
		copy->oraTable->cols[i]->used = 0;
		copy->oraTable->cols[i]->strip_zeros = orig->oraTable->cols[i]->strip_zeros;
		copy->oraTable->cols[i]->pkey = orig->oraTable->cols[i]->pkey;
		/* these are not needed for planning */
		copy->oraTable->cols[i]->val = NULL;
		copy->oraTable->cols[i]->val_size = orig->oraTable->cols[i]->val_size;
		copy->oraTable->cols[i]->val_len = NULL;
		copy->oraTable->cols[i]->val_len4 = 0;
		copy->oraTable->cols[i]->val_null = NULL;
	}
	copy->startup_cost = 0.0;
	copy->total_cost = 0.0;
	copy->rowcount = 0;
	copy->columnindex = 0;
	copy->temp_cxt = NULL;
	copy->order_clause = NULL;
	copy->prefetch = orig->prefetch;
	copy->lob_prefetch = orig->lob_prefetch;

	return copy;
}

/*
 * subtransactionCallback
 * 		Set or rollback to Oracle savepoints when appropriate.
 */
void
subtransactionCallback(SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid, void *arg)
{
	/* rollback to the appropriate savepoint on subtransaction abort */
	if (event == SUBXACT_EVENT_ABORT_SUB || event == SUBXACT_EVENT_PRE_COMMIT_SUB)
		oracleEndSubtransaction(arg, GetCurrentTransactionNestLevel(), event == SUBXACT_EVENT_PRE_COMMIT_SUB);
}

/*
 * addParam
 * 		Creates a new struct paramDesc with the given values and adds it to the list.
 * 		A palloc'ed copy of "name" is used.
 */
void
addParam(struct paramDesc **paramList, char *name, Oid pgtype, oraType oratype, int colnum)
{
	struct paramDesc *param;

	param = palloc(sizeof(struct paramDesc));
	param->name = pstrdup(name);
	param->type = pgtype;
	switch (oratype)
	{
		case ORA_TYPE_NUMBER:
		case ORA_TYPE_FLOAT:
			param->bindType = BIND_NUMBER;
			break;
		case ORA_TYPE_LONG:
		case ORA_TYPE_CLOB:
			param->bindType = BIND_LONG;
			break;
		case ORA_TYPE_RAW:
			if (param->type == UUIDOID)
				param->bindType = BIND_STRING;
			else
				param->bindType = BIND_LONGRAW;
			break;
		case ORA_TYPE_LONGRAW:
		case ORA_TYPE_BLOB:
			param->bindType = BIND_LONGRAW;
			break;
		case ORA_TYPE_BFILE:
			ereport(ERROR,
					(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
					errmsg("cannot update or insert BFILE column in Oracle foreign table")));
			break;
		case ORA_TYPE_GEOMETRY:
			param->bindType = BIND_GEOMETRY;
			break;
		default:
			param->bindType = BIND_STRING;
	}
	param->value = NULL;
	param->node = NULL;
	param->bindh = NULL;
	param->colnum = colnum;
	param->next = *paramList;
	*paramList = param;
}

/*
 * setModifyParameters
 * 		Set the parameter values from the values in the slots.
 * 		"newslot" contains the new values, "oldslot" the old ones.
 */
void
setModifyParameters(struct paramDesc *paramList, TupleTableSlot *newslot, TupleTableSlot *oldslot, struct oraTable *oraTable, oracleSession *session)
{
	struct paramDesc *param;
	Datum datum;
	bool isnull;
	int32 value_len;
	struct pg_itm datetime_itm;
	StringInfoData s;
	Oid pgtype;

	for (param=paramList; param != NULL; param=param->next)
	{
		/* don't do anything for output parameters */
		if (param->bindType == BIND_OUTPUT)
			continue;

		if (param->name[1] == 'k')
		{
			/* for primary key parameters extract the resjunk entry */
			datum = ExecGetJunkAttribute(oldslot, oraTable->cols[param->colnum]->pkey, &isnull);
		}
		else
		{
			/* for other parameters extract the datum from newslot */
			datum = slot_getattr(newslot, oraTable->cols[param->colnum]->pgattnum, &isnull);
		}

		switch (param->bindType)
		{
			case BIND_STRING:
			case BIND_NUMBER:
				if (isnull)
				{
					param->value = NULL;
					break;
				}

				pgtype = oraTable->cols[param->colnum]->pgtype;

				/* special treatment for date, timestamps and intervals */
				if (pgtype == DATEOID)
				{
					param->value = deparseDate(datum);
					break;  /* from switch (param->bindType) */
				}
				else if (pgtype == TIMESTAMPOID || pgtype == TIMESTAMPTZOID)
				{
					param->value = deparseTimestamp(datum, (pgtype == TIMESTAMPTZOID));
					break;  /* from switch (param->bindType) */
				}
				else if (pgtype == INTERVALOID)
				{
					char sign = '+';

					/* get the parts */
					interval2itm(*DatumGetIntervalP(datum), &datetime_itm);

					switch (oraTable->cols[param->colnum]->oratype)
					{
						case ORA_TYPE_INTERVALY2M:
							if (datetime_itm.tm_mday != 0 || datetime_itm.tm_hour != 0
									|| datetime_itm.tm_min != 0 || datetime_itm.tm_sec != 0 || datetime_itm.tm_usec != 0)
								ereport(ERROR,
										(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
										errmsg("invalid value for Oracle INTERVAL YEAR TO MONTH"),
										errdetail("Only year and month can be non-zero for such an interval.")));
							if (datetime_itm.tm_year < 0 || datetime_itm.tm_mon < 0)
							{
								if (datetime_itm.tm_year > 0 || datetime_itm.tm_mon > 0)
									ereport(ERROR,
											(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
											errmsg("invalid value for Oracle INTERVAL YEAR TO MONTH"),
											errdetail("Year and month must be either both positive or both negative.")));
								sign = '-';
								datetime_itm.tm_year = -datetime_itm.tm_year;
								datetime_itm.tm_mon = -datetime_itm.tm_mon;
							}

							initStringInfo(&s);
							appendStringInfo(&s, "%c%d-%d", sign, datetime_itm.tm_year, datetime_itm.tm_mon);
							param->value = s.data;
							break;
						case ORA_TYPE_INTERVALD2S:
							if (datetime_itm.tm_year != 0 || datetime_itm.tm_mon != 0)
								ereport(ERROR,
										(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
										errmsg("invalid value for Oracle INTERVAL DAY TO SECOND"),
										errdetail("Year and month must be zero for such an interval.")));
							if (datetime_itm.tm_mday < 0 || datetime_itm.tm_hour < 0 || datetime_itm.tm_min < 0
								|| datetime_itm.tm_sec < 0 || datetime_itm.tm_usec < 0)
							{
								if (datetime_itm.tm_mday > 0 || datetime_itm.tm_hour > 0 || datetime_itm.tm_min > 0
									|| datetime_itm.tm_sec > 0 || datetime_itm.tm_usec > 0)
									ereport(ERROR,
											(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
											errmsg("invalid value for Oracle INTERVAL DAY TO SECOND"),
											errdetail("Interval elements must be either all positive or all negative.")));
								sign = '-';
								datetime_itm.tm_mday = -datetime_itm.tm_mday;
								datetime_itm.tm_hour = -datetime_itm.tm_hour;
								datetime_itm.tm_min = -datetime_itm.tm_min;
								datetime_itm.tm_sec = -datetime_itm.tm_sec;
								datetime_itm.tm_usec = -datetime_itm.tm_usec;
							}

							if (datetime_itm.tm_hour > 23)
								ereport(ERROR,
										(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
										 errmsg("invalid value for Oracle INTERVAL DAY TO SECOND"),
										 errdetail("The \"hour\" must be less than 24.")));

							initStringInfo(&s);
							appendStringInfo(&s, "%c%d %02d:%02d:%02d.%06d",
									sign, datetime_itm.tm_mday, (int)datetime_itm.tm_hour, datetime_itm.tm_min,
									datetime_itm.tm_sec, (int32)datetime_itm.tm_usec);
							param->value = s.data;
							break;
						default:
							elog(ERROR, "impossible Oracle type for interval");
					}
					break;  /* from switch (param->bindType) */
				}

				/* convert the parameter value into a string */
				param->value = DatumGetCString(OidFunctionCall1(output_funcs[param->colnum], datum));

				/* some data types need additional processing */
				switch (oraTable->cols[param->colnum]->pgtype)
				{
					case UUIDOID:
						/* remove the minus signs for UUIDs */
						convertUUID(param->value);
						break;
					case BOOLOID:
						/* convert booleans to numbers */
						if (param->value[0] == 't')
							param->value[0] = '1';
						else
							param->value[0] = '0';
						param->value[1] = '\0';
						break;
					default:
						/* nothing to be done */
						break;
				}
				break;
			case BIND_LONG:
			case BIND_LONGRAW:
				if (isnull)
				{
					param->value = NULL;
					break;
				}

				/* detoast it if necessary */
				datum = (Datum)PG_DETOAST_DATUM(datum);

				value_len = VARSIZE(datum) - VARHDRSZ;

				/* the first 4 bytes contain the length */
				param->value = palloc(value_len + 4);
				memcpy(param->value, (const char *)&value_len, 4);
				memcpy(param->value + 4, VARDATA(datum), value_len);
				break;
			case BIND_GEOMETRY:
				if (isnull)
				{
					param->value = (char *)oracleEWKBToGeom(session, 0, NULL);
				}
				else
				{
					/* detoast it if necessary */
					datum = (Datum)PG_DETOAST_DATUM(datum);

					/* will allocate objects in the Oracle object cache */
					param->value = (char *)oracleEWKBToGeom(session, VARSIZE(datum) - VARHDRSZ, VARDATA(datum));
				}
				value_len = 0;  /* not used */
				break;
			case BIND_OUTPUT:
				/* unreachable */
				break;
		}
	}
}

bool
hasTrigger(Relation rel, CmdType cmdtype)
{
	return rel->trigdesc
			&& ((cmdtype == CMD_UPDATE && rel->trigdesc->trig_update_after_row)
				|| (cmdtype == CMD_INSERT && rel->trigdesc->trig_insert_after_row)
				|| (cmdtype == CMD_DELETE && rel->trigdesc->trig_delete_after_row));
}

void
buildInsertQuery(StringInfo sql, struct OracleFdwState *fdwState)
{
	bool firstcol;
	int i;
	char paramName[10];

	appendStringInfo(sql, "INSERT INTO %s (", fdwState->oraTable->name);

	firstcol = true;
	for (i = 0; i < fdwState->oraTable->ncols; ++i)
	{
		/* don't add columns beyond the end of the PostgreSQL table */
		if (fdwState->oraTable->cols[i]->pgname == NULL)
			continue;

		if (firstcol)
			firstcol = false;
		else
			appendStringInfo(sql, ", ");
		appendStringInfo(sql, "%s", fdwState->oraTable->cols[i]->name);
	}

	appendStringInfo(sql, ") VALUES (");

	firstcol = true;
	for (i = 0; i < fdwState->oraTable->ncols; ++i)
	{
		/* don't add columns beyond the end of the PostgreSQL table */
		if (fdwState->oraTable->cols[i]->pgname == NULL)
			continue;

		/* check that the data types can be converted */
		checkDataType(
			fdwState->oraTable->cols[i]->oratype,
			fdwState->oraTable->cols[i]->scale,
			fdwState->oraTable->cols[i]->pgtype,
			fdwState->oraTable->pgname,
			fdwState->oraTable->cols[i]->pgname
		);

		/* add a parameter description for the column */
		snprintf(paramName, 9, ":p%d", fdwState->oraTable->cols[i]->pgattnum);
		addParam(&fdwState->paramList, paramName, fdwState->oraTable->cols[i]->pgtype,
			fdwState->oraTable->cols[i]->oratype, i);

		/* add parameter name */
		if (firstcol)
			firstcol = false;
		else
			appendStringInfo(sql, ", ");

		appendAsType(sql, paramName, fdwState->oraTable->cols[i]->pgtype);
	}

	appendStringInfo(sql, ")");
}

void
buildUpdateQuery(StringInfo sql, struct OracleFdwState *fdwState, List *targetAttrs)
{
	bool firstcol;
	int i;
	char paramName[10];
	ListCell *cell;

	appendStringInfo(sql, "UPDATE %s SET ", fdwState->oraTable->name);

	firstcol = true;
	i = 0;
	foreach(cell, targetAttrs)
	{
		/* find the corresponding oraTable entry */
		while (i < fdwState->oraTable->ncols && fdwState->oraTable->cols[i]->pgattnum < lfirst_int(cell))
			++i;
		if (i == fdwState->oraTable->ncols)
			break;

		/* ignore columns that don't occur in the foreign table */
		if (fdwState->oraTable->cols[i]->pgtype == 0)
			continue;

		/* check that the data types can be converted */
		checkDataType(
			fdwState->oraTable->cols[i]->oratype,
			fdwState->oraTable->cols[i]->scale,
			fdwState->oraTable->cols[i]->pgtype,
			fdwState->oraTable->pgname,
			fdwState->oraTable->cols[i]->pgname
		);

		/* add a parameter description for the column */
		snprintf(paramName, 9, ":p%d", lfirst_int(cell));
		addParam(&fdwState->paramList, paramName, fdwState->oraTable->cols[i]->pgtype,
			fdwState->oraTable->cols[i]->oratype, i);

		/* add the parameter name to the query */
		if (firstcol)
			firstcol = false;
		else
			appendStringInfo(sql, ", ");

		appendStringInfo(sql, "%s = ", fdwState->oraTable->cols[i]->name);
		appendAsType(sql, paramName, fdwState->oraTable->cols[i]->pgtype);
	}

	/* throw a meaningful error if nothing is updated */
	if (firstcol)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("no Oracle column modified by UPDATE"),
				errdetail("The UPDATE statement only changes colums that do not exist in the Oracle table.")));
}

void
appendReturningClause(StringInfo sql, struct OracleFdwState *fdwState)
{
	int i;
	bool firstcol;
	struct paramDesc *param;
	char paramName[10];

	/* add the RETURNING clause itself */
	firstcol = true;
	for (i=0; i<fdwState->oraTable->ncols; ++i)
		if (fdwState->oraTable->cols[i]->used)
		{
			if (firstcol)
			{
				firstcol = false;
				appendStringInfo(sql, " RETURNING ");
			}
			else
				appendStringInfo(sql, ", ");
			if (fdwState->oraTable->cols[i]->oratype == ORA_TYPE_XMLTYPE)
				appendStringInfo(sql, "(%s).getclobval()", fdwState->oraTable->cols[i]->name);
			else
				appendStringInfo(sql, "%s", fdwState->oraTable->cols[i]->name);
		}

	/* add the parameters for the RETURNING clause */
	firstcol = true;
	for (i=0; i<fdwState->oraTable->ncols; ++i)
		if (fdwState->oraTable->cols[i]->used)
		{
			/* check that the data types can be converted */
			checkDataType(
				fdwState->oraTable->cols[i]->oratype,
				fdwState->oraTable->cols[i]->scale,
				fdwState->oraTable->cols[i]->pgtype,
				fdwState->oraTable->pgname,
				fdwState->oraTable->cols[i]->pgname
			);

			/* create a new entry in the parameter list */
			param = (struct paramDesc *)palloc(sizeof(struct paramDesc));
			snprintf(paramName, 9, ":r%d", fdwState->oraTable->cols[i]->pgattnum);
			param->name = pstrdup(paramName);
			param->type = fdwState->oraTable->cols[i]->pgtype;
			param->bindType = BIND_OUTPUT;
			param->value = (void *)42;  /* something != NULL */
			param->node = NULL;
			param->bindh = NULL;
			param->colnum = i;
			param->next = fdwState->paramList;
			fdwState->paramList = param;

			if (firstcol)
			{
				firstcol = false;
				appendStringInfo(sql, " INTO ");
			}
			else
				appendStringInfo(sql, ", ");
			appendStringInfo(sql, "%s", paramName);
		}
}

/*
 * transactionCallback
 * 		Commit or rollback Oracle transactions when appropriate.
 */
void
transactionCallback(XactEvent event, void *arg)
{
	switch(event)
	{
		case XACT_EVENT_PRE_COMMIT:
#if PG_VERSION_NUM >= 90500
		case XACT_EVENT_PARALLEL_PRE_COMMIT:
#endif  /* PG_VERSION_NUM */
			/* remote commit */
			oracleEndTransaction(arg, 1, 0);
			break;
		case XACT_EVENT_PRE_PREPARE:
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					errmsg("cannot prepare a transaction that used remote tables")));
			break;
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PREPARE:
#if PG_VERSION_NUM >= 90500
		case XACT_EVENT_PARALLEL_COMMIT:
#endif  /* PG_VERSION_NUM */
			/*
			 * Commit the remote transaction ignoring errors.
			 * In 9.3 or higher, the transaction must already be closed, so this does nothing.
			 * In 9.2 or lower, this is ok since nothing can have been modified remotely.
			 */
			oracleEndTransaction(arg, 1, 1);
			break;
		case XACT_EVENT_ABORT:
#if PG_VERSION_NUM >= 90500
		case XACT_EVENT_PARALLEL_ABORT:
#endif  /* PG_VERSION_NUM */
			/* remote rollback */
			oracleEndTransaction(arg, 0, 1);
			break;
	}

	dml_in_transaction = false;
}

/*
 * exitHook
 * 		Close all Oracle connections on process exit.
 */

void
exitHook(int code, Datum arg)
{
	oracleShutdown();
}

/*
 * oracleDie
 * 		Terminate the current query and prepare backend shutdown.
 * 		This is a signal handler function.
 */
void
oracleDie(SIGNAL_ARGS)
{
	/*
	 * Terminate any running queries.
	 * The Oracle sessions will be terminated by exitHook().
	 */
	oracleCancel();

	/*
	 * Call the original backend shutdown function.
	 * If a query was canceled above, an error from Oracle would result.
	 * To have the backend report the correct FATAL error instead,
	 * we have to call CHECK_FOR_INTERRUPTS() before we report that error;
	 * this is done in oracleError_d.
	 */
	die(postgres_signal_arg);
}

/*
 * setSelectParameters
 * 		Set the current values of the parameters into paramList.
 * 		Return a string containing the parameters set for a DEBUG message.
 */
char *
setSelectParameters(struct paramDesc *paramList, ExprContext *econtext)
{
	struct paramDesc *param;
	Datum datum;
	HeapTuple tuple;
	TimestampTz tstamp;
	bool is_null;
	bool first_param = true;
	MemoryContext oldcontext;
	StringInfoData info;  /* list of parameters for DEBUG message */
	initStringInfo(&info);

	/* switch to short lived memory context */
	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);

	/* iterate parameter list and fill values */
	for (param=paramList; param; param=param->next)
	{
		if (strcmp(param->name, ":now") == 0)
		{
			/* get transaction start timestamp */
			tstamp = GetCurrentTransactionStartTimestamp();

			datum = TimestampGetDatum(tstamp);
			is_null = false;
		}
		else
		{
			/*
			 * Evaluate the expression.
			 * This code path cannot be reached in 9.1
			 */
#if PG_VERSION_NUM < 100000
			datum = ExecEvalExpr((ExprState *)(param->node), econtext, &is_null, NULL);
#else
			datum = ExecEvalExpr((ExprState *)(param->node), econtext, &is_null);
#endif  /* PG_VERSION_NUM */
		}

		if (is_null)
		{
			param->value = NULL;
		}
		else
		{
			if (param->type == DATEOID)
				param->value = deparseDate(datum);
			else if (param->type == TIMESTAMPOID || param->type == TIMESTAMPTZOID)
				param->value = deparseTimestamp(datum, (param->type == TIMESTAMPTZOID));
			else
			{
				regproc typoutput;

				/* get the type's output function */
				tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(param->type));
				if (!HeapTupleIsValid(tuple))
				{
					elog(ERROR, "cache lookup failed for type %u", param->type);
				}
				typoutput = ((Form_pg_type)GETSTRUCT(tuple))->typoutput;
				ReleaseSysCache(tuple);

				/* convert the parameter value into a string */
				param->value = DatumGetCString(OidFunctionCall1(typoutput, datum));

				/* strip hyphens from UUID values */
				if (param->type == UUIDOID)
					convertUUID(param->value);
			}
		}

		/* build a parameter list for the DEBUG message */
		if (first_param)
		{
			first_param = false;
			appendStringInfo(&info, ", parameters %s=\"%s\"", param->name,
				(param->value ? param->value : "(null)"));
		}
		else
		{
			appendStringInfo(&info, ", %s=\"%s\"", param->name,
				(param->value ? param->value : "(null)"));
		}
	}

	/* reset memory context */
	MemoryContextSwitchTo(oldcontext);

	return info.data;
}

/*
 * convertTuple
 * 		Convert a result row from Oracle stored in oraTable
 * 		into arrays of values and null indicators.
 * 		"index" is the (1 based) index into the array of results.
 * 		If trunc_lob it true, truncate LOBs to WIDTH_THRESHOLD+1 bytes.
 */
void
convertTuple(struct OracleFdwState *fdw_state, unsigned int index, Datum *values, bool *nulls, bool trunc_lob)
{
	char *value = NULL, *oraval;
	long value_len = 0;
	int j, i = -1;
	unsigned short oralen;
	ErrorContextCallback errcb;
	Oid pgtype;

	/* initialize error context callback, install it only during conversions */
	errcb.callback = errorContextCallback;
	errcb.arg = (void *)fdw_state;

	/* assign result values */
	for (j=0; j<fdw_state->oraTable->npgcols; ++j)
	{
		/* for dropped columns, insert a NULL */
		if ((i + 1 < fdw_state->oraTable->ncols)
				&& (fdw_state->oraTable->cols[i + 1]->pgattnum > j + 1))
		{
			nulls[j] = true;
			values[j] = PointerGetDatum(NULL);
			continue;
		}
		else
			++i;

		/*
		 * Columns exceeding the length of the Oracle table will be NULL,
		 * as well as columns that are not used in the query.
		 * Geometry columns are NULL if the value is NULL,
		 * for all other types use the NULL indicator.
		 */
		if (i >= fdw_state->oraTable->ncols
			|| fdw_state->oraTable->cols[i]->used == 0
			|| (fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_GEOMETRY
				&& ((ora_geometry *)fdw_state->oraTable->cols[i]->val)->geometry == NULL)
			|| fdw_state->oraTable->cols[i]->val_null[index-1] == -1)
		{
			nulls[j] = true;
			values[j] = PointerGetDatum(NULL);
			continue;
		}

		/* from here on, we can assume columns to be NOT NULL */
		nulls[j] = false;
		pgtype = fdw_state->oraTable->cols[i]->pgtype;

		/* calculate the offset into the arays in "val" and "val_len" */
		oraval = fdw_state->oraTable->cols[i]->val +
			(index - 1) * fdw_state->oraTable->cols[i]->val_size;
		oralen = (fdw_state->oraTable->cols[i]->val_len)[index - 1];

		/* get the data and its length */
		if (fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_BLOB
				|| fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_BFILE
				|| fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_CLOB)
		{
			/* for LOBs, get the actual LOB contents (palloc'ed), truncated if desired */
			oracleGetLob(fdw_state->session,
				(void *)oraval, fdw_state->oraTable->cols[i]->oratype,
				&value, &value_len, trunc_lob ? (WIDTH_THRESHOLD+1) : 0);
		}
		else if (fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_GEOMETRY)
		{
			ora_geometry *geom = (ora_geometry *)fdw_state->oraTable->cols[i]->val;

			/* install error context callback */
			errcb.previous = error_context_stack;
			error_context_stack = &errcb;
			fdw_state->columnindex = i;

			value_len = oracleGetEWKBLen(fdw_state->session, geom);

			/* uninstall error context callback */
			error_context_stack = errcb.previous;

			value = NULL;  /* we will fetch that later to avoid unnecessary copying */
		}
		else if (fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_LONG
				|| fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_LONGRAW)
		{
			/* for LONG and LONG RAW, the first 4 bytes contain the length */
			value_len = *((int32 *)oraval);
			/* the rest is the actual data */
			value = oraval + 4;
			/* terminating zero byte (needed for LONGs) */
			value[value_len] = '\0';
		}
		else
		{
			/* special handling for NUMBER's "infinity tilde" */
			if ((fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_FLOAT
					|| fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_NUMBER)
				&& (oraval[0] == '~' || (oraval[0] == '-' && oraval[1] == '~')))
			{
				/* "numeric" does not know infinity, so map to NaN */
				if (pgtype == NUMERICOID)
					strcpy(oraval, "Nan");
				else
					strcpy(oraval, (oraval[0] == '-' ? "-inf" : "inf"));
			}

			/* for other data types, oraTable contains the results */
			value = oraval;
			value_len = oralen;
		}

		/* fill the TupleSlot with the data (after conversion if necessary) */
		if (fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_GEOMETRY)
		{
			ora_geometry *geom = (ora_geometry *)fdw_state->oraTable->cols[i]->val;
			struct varlena *result = NULL;

			/* install error context callback */
			errcb.previous = error_context_stack;
			error_context_stack = &errcb;
			fdw_state->columnindex = i;

			result = (bytea *)palloc(value_len + VARHDRSZ);
			oracleFillEWKB(fdw_state->session, geom, value_len, VARDATA(result));
			SET_VARSIZE(result, value_len + VARHDRSZ);

			/* uninstall error context callback */
			error_context_stack = errcb.previous;

			values[j] = PointerGetDatum(result);

			/* free the storage for the object */
			oracleGeometryFree(fdw_state->session, geom);
		}
		else if (pgtype == BYTEAOID)
		{
			/* binary columns are not converted */
			bytea *result = (bytea *)palloc(value_len + VARHDRSZ);
			memcpy(VARDATA(result), value, value_len);
			SET_VARSIZE(result, value_len + VARHDRSZ);

			values[j] = PointerGetDatum(result);
		}
		else if (pgtype == BOOLOID)
			values[j] = BoolGetDatum(value[0] != '0' || value_len > 1);
		else
		{
			regproc typinput;
			HeapTuple tuple;
			Datum dat;

			/*
			 * Negative INTERVAL DAY TO SECOND need some preprocessing:
			 * In Oracle they are rendered like this: "-01 12:00:00.000000"
			 * They have to be changed to "-01 -12:00:00.000000" for PostgreSQL.
			 */
			if (fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_INTERVALD2S
				&& value[0] == '-')
			{
				char *newval = palloc(strlen(value) + 2);
				char *pos = strchr(value, ' ');

				if (pos == NULL)
					elog(ERROR, "no space in INTERVAL DAY TO SECOND");
				strncpy(newval, value, pos - value + 1);
				newval[pos - value + 1] = '\0';
				strcat(newval, "-");
				strcat(newval, pos + 1);

				value = newval;
			}

			/* find the appropriate conversion function */
			tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(pgtype));
			if (!HeapTupleIsValid(tuple))
			{
				elog(ERROR, "cache lookup failed for type %u", pgtype);
			}
			typinput = ((Form_pg_type)GETSTRUCT(tuple))->typinput;
			ReleaseSysCache(tuple);

			dat = CStringGetDatum(value);

			/* install error context callback */
			errcb.previous = error_context_stack;
			error_context_stack = &errcb;
			fdw_state->columnindex = i;

			if (pgtype == BPCHAROID || pgtype == VARCHAROID || pgtype == TEXTOID)
			{
				/* optionally strip zero bytes from string types */
				if (fdw_state->oraTable->cols[i]->strip_zeros)
				{
					char *from_p, *to_p = value;
					long new_length = value_len;

					for (from_p = value; from_p < value + value_len; ++from_p)
						if (*from_p != '\0')
							*to_p++ = *from_p;
						else
							--new_length;

					value_len = new_length;
					value[value_len] = '\0';
				}

				/* check that the string types are in the database encoding */
				(void)pg_verify_mbstr(GetDatabaseEncoding(), value, value_len, false);
			}

			/* call the type input function */
			switch (pgtype)
			{
				case BPCHAROID:
				case VARCHAROID:
				case TIMESTAMPOID:
				case TIMESTAMPTZOID:
				case INTERVALOID:
				case NUMERICOID:
					/* these functions require the type modifier */
					values[j] = OidFunctionCall3(typinput,
						dat,
						ObjectIdGetDatum(InvalidOid),
						Int32GetDatum(fdw_state->oraTable->cols[i]->pgtypmod));
					break;
				default:
					/* the others don't */
					values[j] = OidFunctionCall1(typinput, dat);
			}

			/* uninstall error context callback */
			error_context_stack = errcb.previous;
		}

		/* free the data buffer for LOBs */
		if (fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_BLOB
				|| fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_BFILE
				|| fdw_state->oraTable->cols[i]->oratype == ORA_TYPE_CLOB)
			pfree(value);
	}
}

/*
 * errorContextCallback
 * 		Provides the context for an error message during a type input conversion.
 * 		The argument must be a pointer to a "struct OracleFdwState".
 */
void
errorContextCallback(void *arg)
{
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)arg;

	errcontext("converting column \"%s\" for foreign table scan of \"%s\", row %lu",
		quote_identifier(fdw_state->oraTable->cols[fdw_state->columnindex]->pgname),
		quote_identifier(fdw_state->oraTable->pgname),
		fdw_state->rowcount);
}

#ifdef IMPORT_API
/*
 * fold_case
 * 		Returns a palloc'ed string that is the case-folded first argument.
 */
char *
fold_case(char *name, fold_t foldcase, int collation)
{
	if (foldcase == CASE_KEEP)
		return pstrdup(name);

	if (foldcase == CASE_LOWER)
		return str_tolower(name, strlen(name), collation);

	if (foldcase == CASE_SMART)
	{
		char *upstr = str_toupper(name, strlen(name), collation);

		/* fold case only if it does not contain lower case characters */
		if (strcmp(upstr, name) == 0)
			return str_tolower(name, strlen(name), collation);
		else
			return pstrdup(name);
	}

	elog(ERROR, "impossible case folding type %d", foldcase);

	return NULL;  /* unreachable, but keeps compiler happy */
}
#endif  /* IMPORT_API */

/*
 * getIsolationLevel
 *		Converts Oracle isolation level string to oraIsoLevel.
 *      Throws an error for invalid values.
 */
oraIsoLevel
getIsolationLevel(const char *isolation_level)
{
	oraIsoLevel val = 0;

	Assert(isolation_level);

	if (strcmp(isolation_level, "serializable") == 0)
		val = ORA_TRANS_SERIALIZABLE;
	else if (strcmp(isolation_level, "read_committed") == 0)
		val = ORA_TRANS_READ_COMMITTED;
	else if (strcmp(isolation_level, "read_only") == 0)
		val = ORA_TRANS_READ_ONLY;
	else
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
				errmsg("invalid value for option \"%s\"", OPT_ISOLATION_LEVEL),
				errhint("Valid values in this context are: serializable/read_committed/read_only")));

	return val;
}

/*
 * pushdownOrderBy
 * 		Attempt to push down the ORDER BY clause.
 * 		If there is an ORDER BY clause and the complete clause can be pushed
 * 		down, save "order_clause" and "usable_pathkeys" in "fdwState" and
 * 		return "true".
 */
bool
pushdownOrderBy(PlannerInfo *root, RelOptInfo *baserel, struct OracleFdwState *fdwState)
{
	StringInfoData orderedquery;
	List *usable_pathkeys = NIL;
	ListCell *cell;
	char *delim = " ";

	initStringInfo(&orderedquery);

	foreach(cell, root->query_pathkeys)
	{
		PathKey *pathkey = (PathKey *)lfirst(cell);
		EquivalenceClass *pathkey_ec = pathkey->pk_eclass;
		EquivalenceMember *em = NULL;
		Expr *em_expr = NULL;
		char *sort_clause;
		Oid em_type;
		ListCell *lc;
		bool can_pushdown;

		/* ec_has_volatile saves some cycles */
		if (pathkey_ec->ec_has_volatile)
			return false;

		/*
		 * Given an EquivalenceClass and a foreign relation, find an EC member
		 * that can be used to sort the relation remotely according to a pathkey
		 * using this EC.
		 *
		 * If there is more than one suitable candidate, use an arbitrary
		 * one of them.
		 *
		 * This checks that the EC member expression uses only Vars from the given
		 * rel and is shippable.  Caller must separately verify that the pathkey's
		 * ordering operator is shippable.
		 */
		foreach(lc, pathkey_ec->ec_members)
		{
			EquivalenceMember *some_em = (EquivalenceMember *) lfirst(lc);

			/*
			 * Note we require !bms_is_empty, else we'd accept constant
			 * expressions which are not suitable for the purpose.
			 */
			if (bms_is_subset(some_em->em_relids, baserel->relids) &&
				!bms_is_empty(some_em->em_relids))
			{
				em = some_em;
				break;
			}
		}

		if (em == NULL)
			return false;

		em_expr = em->em_expr;
		em_type = exprType((Node *)em_expr);

		/* expressions of a type different from this are not safe to push down into ORDER BY clauses */
		can_pushdown = (em_type == INT8OID || em_type == INT2OID || em_type == INT4OID || em_type == OIDOID
				|| em_type == FLOAT4OID || em_type == FLOAT8OID || em_type == NUMERICOID || em_type == DATEOID
				|| em_type == TIMESTAMPOID || em_type == TIMESTAMPTZOID || em_type == INTERVALOID);

		if (can_pushdown &&
			((sort_clause = deparseExpr(fdwState->session, baserel, em_expr, fdwState->oraTable, &(fdwState->params))) != NULL))
		{
			/* keep usable_pathkeys for later use. */
			usable_pathkeys = lappend(usable_pathkeys, pathkey);

			/* create orderedquery */
			appendStringInfoString(&orderedquery, delim);
			appendStringInfoString(&orderedquery, sort_clause);
			delim = ", ";

			if (pathkey->pk_strategy == BTLessStrategyNumber)
				appendStringInfoString(&orderedquery, " ASC");
			else
				appendStringInfoString(&orderedquery, " DESC");

			if (pathkey->pk_nulls_first)
				appendStringInfoString(&orderedquery, " NULLS FIRST");
			else
				appendStringInfoString(&orderedquery, " NULLS LAST");
		}
		else
		{
			/*
			 * Before PostgreSQL v13, the planner and executor don't have
			 * any clever strategy for taking data sorted by a prefix of the
			 * query's pathkeys and getting it to be sorted by all of those
			 * pathekeys.
			 * So, unless we can push down all of the query pathkeys, forget it.
			 * This could be improved from v13 on!
			 */
			list_free(usable_pathkeys);
			usable_pathkeys = NIL;
			break;
		}
	}

	/* set ORDER BY clause and remember pushed down path keys */
	if (usable_pathkeys != NIL)
	{
		fdwState->order_clause = orderedquery.data;
		fdwState->usable_pathkeys = usable_pathkeys;
	}

	return (root->query_pathkeys != NIL && usable_pathkeys != NIL);
}

/*
 * deparseLimit
 * 		Deparse LIMIT clause into FETCH FIRST N ROWS ONLY.
 * 		If OFFSET is set, the offset value is added to the LIMIT value
 * 		to give the Oracle optimizer the right clue.
 */
char *
deparseLimit(PlannerInfo *root, struct OracleFdwState *fdwState, RelOptInfo *baserel)
{
	StringInfoData limit_clause;
	char *limit_val, *offset_val = NULL;

	/* don't push down LIMIT if the query has a GROUP BY clause or aggregates */
	if (root->parse->groupClause != NULL || root->parse->hasAggs)
		return NULL;

	/* only push down LIMIT if all WHERE conditions can be pushed down */
	if (fdwState->local_conds != NIL)
		return NULL;

	/* only push down constant LIMITs that are not NULL */
	if (root->parse->limitCount != NULL && IsA(root->parse->limitCount, Const))
	{
		Const *limit = (Const *)root->parse->limitCount;

		if (limit->constisnull)
			return NULL;

		limit_val = datumToString(limit->constvalue, limit->consttype);
	}
	else
		return NULL;

	/* only consider OFFSETS that are non-NULL constants */
	if (root->parse->limitOffset != NULL && IsA(root->parse->limitOffset, Const))
	{
		Const *offset = (Const *)root->parse->limitOffset;

		if (! offset->constisnull)
			offset_val = datumToString(offset->constvalue, offset->consttype);
	}

	initStringInfo(&limit_clause);

	if (offset_val)
		appendStringInfo(&limit_clause,
						 "FETCH FIRST %s+%s ROWS ONLY",
						 limit_val, offset_val);
	else
		appendStringInfo(&limit_clause,
						 "FETCH FIRST %s ROWS ONLY",
						 limit_val);

	return limit_clause.data;
}

#if PG_VERSION_NUM < 150000
/* interval2itm()
 * Convert an Interval to a pg_itm structure.
 * Note: overflow is not possible, because the pg_itm fields are
 * wide enough for all possible conversion results.
 */
void
interval2itm(Interval span, struct pg_itm *itm)
{
	TimeOffset  time;
	Offset  tfrac;

	itm->tm_year = span.month / MONTHS_PER_YEAR;
	itm->tm_mon = span.month % MONTHS_PER_YEAR;
	itm->tm_mday = span.day;
	time = span.time;

	tfrac = time / USECS_PER_HOUR;
	time -= tfrac * USECS_PER_HOUR;
	itm->tm_hour = tfrac;
	tfrac = time / USECS_PER_MINUTE;
	time -= tfrac * USECS_PER_MINUTE;
	itm->tm_min = (int) tfrac;
	tfrac = time / USECS_PER_SEC;
	time -= tfrac * USECS_PER_SEC;
	itm->tm_sec = (int) tfrac;
	itm->tm_usec = (int) time;
}
#endif  /* PG_VERSION_NUM */

/*
 * oracleGetShareFileName
 * 		Returns the (palloc'ed) absolute path of a file in the "share" directory.
 */
char *
oracleGetShareFileName(const char *relativename)
{
	char share_path[MAXPGPATH], *result;

	get_share_path(my_exec_path, share_path);

	result = palloc(MAXPGPATH);
	snprintf(result, MAXPGPATH, "%s/%s", share_path, relativename);

	return result;
}

/*
 * oracleRegisterCallback
 * 		Register a callback for PostgreSQL transaction events.
 */
void
oracleRegisterCallback(void *arg)
{
	RegisterXactCallback(transactionCallback, arg);
	RegisterSubXactCallback(subtransactionCallback, arg);
}

/*
 * oracleUnregisterCallback
 * 		Unregister a callback for PostgreSQL transaction events.
 */
void
oracleUnregisterCallback(void *arg)
{
	UnregisterXactCallback(transactionCallback, arg);
	UnregisterSubXactCallback(subtransactionCallback, arg);
}

/*
 * oracleAlloc
 * 		Expose palloc() to Oracle functions.
 */
void
*oracleAlloc(size_t size)
{
	return palloc(size);
}

/*
 * oracleRealloc
 * 		Expose repalloc() to Oracle functions.
 */
void
*oracleRealloc(void *p, size_t size)
{
	return repalloc(p, size);
}

/*
 * oracleFree
 * 		Expose pfree() to Oracle functions.
 */
void
oracleFree(void *p)
{
	pfree(p);
}

/*
 * oracleSetHandlers
 * 		Set signal handler for SIGTERM.
 */
void
oracleSetHandlers()
{
	pqsignal(SIGTERM, oracleDie);
}

/* get a PostgreSQL error code from an oraError */
#define to_sqlstate(x) \
	(x==FDW_UNABLE_TO_ESTABLISH_CONNECTION ? ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION : \
	(x==FDW_UNABLE_TO_CREATE_REPLY ? ERRCODE_FDW_UNABLE_TO_CREATE_REPLY : \
	(x==FDW_TABLE_NOT_FOUND ? ERRCODE_FDW_TABLE_NOT_FOUND : \
	(x==FDW_UNABLE_TO_CREATE_EXECUTION ? ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION : \
	(x==FDW_OUT_OF_MEMORY ? ERRCODE_FDW_OUT_OF_MEMORY : \
	(x==FDW_SERIALIZATION_FAILURE ? ERRCODE_T_R_SERIALIZATION_FAILURE : \
	(x==FDW_UNIQUE_VIOLATION ? ERRCODE_UNIQUE_VIOLATION : \
	(x==FDW_DEADLOCK_DETECTED ? ERRCODE_T_R_DEADLOCK_DETECTED : \
	(x==FDW_NOT_NULL_VIOLATION ? ERRCODE_NOT_NULL_VIOLATION : \
	(x==FDW_CHECK_VIOLATION ? ERRCODE_CHECK_VIOLATION : \
	(x==FDW_FOREIGN_KEY_VIOLATION ? ERRCODE_FOREIGN_KEY_VIOLATION : ERRCODE_FDW_ERROR)))))))))))

/*
 * oracleError_d
 * 		Report a PostgreSQL error with a detail message.
 */
void
oracleError_d(oraError sqlstate, const char *message, const char *detail)
{
	/* if the backend was terminated, report that rather than the Oracle error */
	CHECK_FOR_INTERRUPTS();

	ereport(ERROR,
			(errcode(to_sqlstate(sqlstate)),
			errmsg("%s", message),
			errdetail("%s", detail)));
}

/*
 * oracleError_sd
 * 		Report a PostgreSQL error with a string argument and a detail message.
 */
void
oracleError_sd(oraError sqlstate, const char *message, const char *arg, const char *detail)
{
	ereport(ERROR,
			(errcode(to_sqlstate(sqlstate)),
			errmsg(message, arg),
			errdetail("%s", detail)));
}

/*
 * oracleError_ssdh
 * 		Report a PostgreSQL error with two string arguments, a detail message and a hint.
 */
void
oracleError_ssdh(oraError sqlstate, const char *message, const char *arg1, const char* arg2, const char *detail, const char *hint)
{
	ereport(ERROR,
			(errcode(to_sqlstate(sqlstate)),
			errmsg(message, arg1, arg2),
			errdetail("%s", detail),
			errhint("%s", hint)));
}

/*
 * oracleError_ii
 * 		Report a PostgreSQL error with 2 integer arguments.
 */
void
oracleError_ii(oraError sqlstate, const char *message, int arg1, int arg2)
{
	ereport(ERROR,
			(errcode(to_sqlstate(sqlstate)),
			errmsg(message, arg1, arg2)));
}

/*
 * oracleError_i
 * 		Report a PostgreSQL error with integer argument.
 */
void
oracleError_i(oraError sqlstate, const char *message, int arg)
{
	ereport(ERROR,
			(errcode(to_sqlstate(sqlstate)),
			errmsg(message, arg)));
}

/*
 * oracleError
 * 		Report a PostgreSQL error without detail message.
 */
void
oracleError(oraError sqlstate, const char *message)
{
	/* use errcode_for_file_access() if the message contains %m */
	if (strstr(message, "%m")) {
		ereport(ERROR,
				(errcode_for_file_access(),
				errmsg(message, "")));
	} else {
		ereport(ERROR,
				(errcode(to_sqlstate(sqlstate)),
				errmsg("%s", message)));
	}
}

/*
 * oracleDebug2
 * 		Report a PostgreSQL message at level DEBUG2.
 */
void
oracleDebug2(const char *message)
{
	elog(DEBUG2, "%s", message);
}

/*
 * initializePostGIS
 * 		Checks if PostGIS is installed and sets GEOMETRYOID if it is.
 */
void
initializePostGIS()
{
	CatCList *catlist;
	int i, argcount = 1;
	Oid argtypes[] = { INTERNALOID };

	/* this needs to be done only once per database session */
	if (geometry_is_setup)
		return;

	geometry_is_setup = true;

	/* find all functions called "geometry_recv" with "internal" argument type */
	catlist = SearchSysCacheList2(
					PROCNAMEARGSNSP,
					CStringGetDatum("geometry_recv"),
					PointerGetDatum(buildoidvector(argtypes, argcount)));

	for (i = 0; i < catlist->n_members; i++)
	{
		HeapTuple proctup = &catlist->members[i]->tuple;
		Form_pg_proc procform = (Form_pg_proc)GETSTRUCT(proctup);

		/*
		 * If we find more than one "geometry_recv" function, there is
		 * probably more than one installation of PostGIS.
		 * We don't know which one to use and give up trying.
		 */
		if (GEOMETRYOID != InvalidOid)
		{
			elog(DEBUG1, "oracle_fdw: more than one PostGIS installation found, giving up");

			GEOMETRYOID = InvalidOid;
			break;
		}

		/* "geometry" is the return type of the "geometry_recv" function */
		GEOMETRYOID = procform->prorettype;

		elog(DEBUG1, "oracle_fdw: PostGIS is installed, GEOMETRYOID = %d", GEOMETRYOID);
	}
	ReleaseSysCacheList(catlist);
}
