/*-------------------------------------------------------------------------
 *
 * oracle_fdw.c
 * 		PostgreSQL-related functions for Oracle foreign data wrapper.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#if PG_VERSION_NUM < 90300
#include "access/htup.h"
#else
#include "access/htup_details.h"
#endif  /* PG_VERSION_NUM */
#include "access/reloptions.h"
#include "access/sysattr.h"
#include "access/xact.h"
#include "catalog/indexing.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_cast.h"
#include "catalog/pg_foreign_data_wrapper.h"
#include "catalog/pg_foreign_server.h"
#include "catalog/pg_foreign_table.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_operator.h"
#include "catalog/pg_proc.h"
#include "catalog/pg_user_mapping.h"
#include "catalog/pg_type.h"
#include "commands/explain.h"
#include "commands/vacuum.h"
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "libpq/md5.h"
#include "mb/pg_wchar.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "nodes/nodeFuncs.h"
#include "nodes/pg_list.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "optimizer/planmain.h"
#include "optimizer/restrictinfo.h"
#include "optimizer/var.h"
#include "parser/parse_relation.h"
#include "parser/parsetree.h"
#include "port.h"
#include "storage/ipc.h"
#include "storage/lock.h"
#include "utils/array.h"
#include "utils/builtins.h"
#include "utils/date.h"
#include "utils/datetime.h"
#include "utils/elog.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/memutils.h"
#include "utils/rel.h"
#include "utils/resowner.h"
#include "utils/timestamp.h"
#include "utils/tqual.h"
#include "utils/snapmgr.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#include <string.h>
#include <stdlib.h>

#include "oracle_fdw.h"

/* defined in backend/commands/analyze.c */
#ifndef WIDTH_THRESHOLD
#define WIDTH_THRESHOLD 1024
#endif  /* WIDTH_THRESHOLD */

#if PG_VERSION_NUM < 90200
#define OLD_FDW_API
#else
#undef OLD_FDW_API
#endif  /* PG_VERSION_NUM */

#if PG_VERSION_NUM >= 90300
#define WRITE_API
#else
#undef WRITE_API
#endif  /* PG_VERSION_NUM */

PG_MODULE_MAGIC;

/*
 * PostGIS geometry type, set upon library initialization.
 */
static Oid GEOMETRYOID = InvalidOid;

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
#define OPT_USER "user"
#define OPT_PASSWORD "password"
#define OPT_SCHEMA "schema"
#define OPT_TABLE "table"
#define OPT_PLAN_COSTS "plan_costs"
#define OPT_MAX_LONG "max_long"
#define OPT_READONLY "readonly"
#define OPT_KEY "key"

#define DEFAULT_MAX_LONG 32767

/*
 * Valid options for oracle_fdw.
 */
static struct OracleFdwOption valid_options[] = {
	{OPT_NLS_LANG, ForeignDataWrapperRelationId, false},
	{OPT_DBSERVER, ForeignServerRelationId, true},
	{OPT_USER, UserMappingRelationId, true},
	{OPT_PASSWORD, UserMappingRelationId, true},
	{OPT_SCHEMA, ForeignTableRelationId, false},
	{OPT_TABLE, ForeignTableRelationId, true},
	{OPT_PLAN_COSTS, ForeignTableRelationId, false},
	{OPT_MAX_LONG, ForeignTableRelationId, false},
	{OPT_READONLY, ForeignTableRelationId, false}
#ifndef OLD_FDW_API
	,{OPT_KEY, AttributeRelationId, false}
#endif	/* OLD_FDW_API */
};

#define option_count (sizeof(valid_options)/sizeof(struct OracleFdwOption))

#ifdef WRITE_API
/*
 * Array to hold the type output functions during table modification.
 * It is ok to hold this cache in a static variable because there cannot
 * be more than one foreign table modified at the same time.
 */

static regproc *output_funcs;
#endif  /* WRITE_API */

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
	char *user;                    /* Oracle username */
	char *password;                /* Oracle password */
	char *nls_lang;                /* Oracle locale information */
	oracleSession *session;        /* encapsulates the active Oracle session */
	char *query;                   /* query we issue against Oracle */
	List *params;                  /* list of parameters needed for the query */
	struct paramDesc *paramList;   /* description of parameters needed for the query */
	struct oraTable *oraTable;     /* description of the remote Oracle table */
	Cost startup_cost;             /* cost estimate, only needed for planning */
	Cost total_cost;               /* cost estimate, only needed for planning */
	bool *pushdown_clauses;        /* array, true if the corresponding clause can be pushed down */
	unsigned long rowcount;        /* rows already read from Oracle */
	int columnindex;               /* currently processed column for error context */
	MemoryContext temp_cxt;        /* short-lived memory for data modification */
};

/*
 * SQL functions
 */
extern PGDLLEXPORT Datum oracle_fdw_handler(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum oracle_fdw_validator(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum oracle_close_connections(PG_FUNCTION_ARGS);
extern PGDLLEXPORT Datum oracle_diag(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(oracle_fdw_handler);
PG_FUNCTION_INFO_V1(oracle_fdw_validator);
PG_FUNCTION_INFO_V1(oracle_close_connections);
PG_FUNCTION_INFO_V1(oracle_diag);

/*
 * on-load initializer
 */
extern PGDLLEXPORT void _PG_init(void);

/*
 * FDW callback routines
 */
#ifdef OLD_FDW_API
static FdwPlan *oraclePlanForeignScan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel);
#else
static void oracleGetForeignRelSize(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static void oracleGetForeignPaths(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid);
static ForeignScan *oracleGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses);
static bool oracleAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages);
#endif  /* OLD_FDW_API */
static void oracleExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void oracleBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *oracleIterateForeignScan(ForeignScanState *node);
static void oracleEndForeignScan(ForeignScanState *node);
static void oracleReScanForeignScan(ForeignScanState *node);
#ifdef WRITE_API
static void oracleAddForeignUpdateTargets(Query *parsetree, RangeTblEntry *target_rte, Relation target_relation);
static List *oraclePlanForeignModify(PlannerInfo *root, ModifyTable *plan, Index resultRelation, int subplan_index);
static void oracleBeginForeignModify(ModifyTableState *mtstate, ResultRelInfo *rinfo, List *fdw_private, int subplan_index, int eflags);
static TupleTableSlot *oracleExecForeignInsert(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot);
static TupleTableSlot *oracleExecForeignUpdate(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot);
static TupleTableSlot *oracleExecForeignDelete(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot);
static void oracleEndForeignModify(EState *estate, ResultRelInfo *rinfo);
static void oracleExplainForeignModify(ModifyTableState *mtstate, ResultRelInfo *rinfo, List *fdw_private, int subplan_index, struct ExplainState *es);
static int oracleIsForeignRelUpdatable(Relation rel);
#endif  /* WRITE_API */

/*
 * Helper functions
 */
static struct OracleFdwState *getFdwState(Oid foreigntableid, bool *plan_costs);
static void oracleGetOptions(Oid foreigntableid, List **options);
static char *createQuery(oracleSession *session, RelOptInfo *foreignrel, bool modify, struct oraTable *oraTable, List **params, bool **pushdown_clauses);
static void getColumnData(Oid foreigntableid, struct oraTable *oraTable);
#ifndef OLD_FDW_API
static int acquireSampleRowsFunc (Relation relation, int elevel, HeapTuple *rows, int targrows, double *totalrows, double *totaldeadrows);
#endif  /* OLD_FDW_API */
static char *getOracleWhereClause(oracleSession *session, RelOptInfo *foreignrel, Expr *expr, const struct oraTable *oraTable, List **params);
static char *datumToString(Datum datum, Oid type);
static void getUsedColumns(Expr *expr, struct oraTable *oraTable);
static void checkDataType(oraType oratype, int scale, Oid pgtype, const char *tablename, const char *colname);
static char *guessNlsLang(char *nls_lang);
static List *serializePlanData(struct OracleFdwState *fdwState);
static Const *serializeString(const char *s);
static Const *serializeLong(long i);
static struct OracleFdwState *deserializePlanData(List *list);
static char *deserializeString(Const *constant);
static long deserializeLong(Const *constant);
static bool optionIsTrue(const char *value);
#ifdef WRITE_API
static struct OracleFdwState *copyPlanData(struct OracleFdwState *orig);
static void subtransactionCallback(SubXactEvent event, SubTransactionId mySubid, SubTransactionId parentSubid, void *arg);
static void addParam(struct paramDesc **paramList, char *name, Oid pgtype, oraType oratype, int colnum);
static void setModifyParameters(struct paramDesc *paramList, TupleTableSlot *newslot, TupleTableSlot *oldslot, struct oraTable *oraTable, oracleSession *session);
#endif  /* WRITE_API */
static void transactionCallback(XactEvent event, void *arg);
static void exitHook(int code, Datum arg);
static char *setSelectParameters(struct paramDesc *paramList, ExprContext *econtext);
static void convertTuple(struct OracleFdwState *fdw_state, Datum *values, bool *nulls, bool trunc_lob);
static void errorContextCallback(void *arg);

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to callback routines.
 */
PGDLLEXPORT Datum
oracle_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

#ifdef OLD_FDW_API
	fdwroutine->PlanForeignScan = oraclePlanForeignScan;
#else
	fdwroutine->GetForeignRelSize = oracleGetForeignRelSize;
	fdwroutine->GetForeignPaths = oracleGetForeignPaths;
	fdwroutine->GetForeignPlan = oracleGetForeignPlan;
	fdwroutine->AnalyzeForeignTable = oracleAnalyzeForeignTable;
#endif  /* OLD_FDW_API */
	fdwroutine->ExplainForeignScan = oracleExplainForeignScan;
	fdwroutine->BeginForeignScan = oracleBeginForeignScan;
	fdwroutine->IterateForeignScan = oracleIterateForeignScan;
	fdwroutine->ReScanForeignScan = oracleReScanForeignScan;
	fdwroutine->EndForeignScan = oracleEndForeignScan;
#ifdef WRITE_API
	fdwroutine->AddForeignUpdateTargets = oracleAddForeignUpdateTargets;
	fdwroutine->PlanForeignModify = oraclePlanForeignModify;
	fdwroutine->BeginForeignModify = oracleBeginForeignModify;
	fdwroutine->ExecForeignInsert = oracleExecForeignInsert;
	fdwroutine->ExecForeignUpdate = oracleExecForeignUpdate;
	fdwroutine->ExecForeignDelete = oracleExecForeignDelete;
	fdwroutine->EndForeignModify = oracleEndForeignModify;
	fdwroutine->ExplainForeignModify = oracleExplainForeignModify;
	fdwroutine->IsForeignRelUpdatable = oracleIsForeignRelUpdatable;
#endif  /* WRITE_API */

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

		/* check valid values for plan_costs */
		if (strcmp(def->defname, OPT_PLAN_COSTS) == 0
				|| strcmp(def->defname, OPT_READONLY) == 0
#ifndef OLD_FDW_API
				|| strcmp(def->defname, OPT_KEY) == 0
#endif	/* OLD_FDW_API */
			)
		{
			char *val = ((Value *)(def->arg))->val.str;
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

		/* check valid values for "table" and "schema" */
		if (strcmp(def->defname, OPT_TABLE) == 0
				|| strcmp(def->defname, OPT_SCHEMA) == 0)
		{
			char *val = ((Value *)(def->arg))->val.str;
			if (strchr(val, '"') != NULL)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Double quotes are not allowed in names.")));
		}

		/* check valid values for max_long */
		if (strcmp(def->defname, OPT_MAX_LONG) == 0)
		{
			char *val = ((Value *) (def->arg))->val.str;
			char *endptr;
			unsigned long max_long = strtoul(val, &endptr, 0);
			if (val[0] == '\0' || *endptr != '\0' || max_long < 1 || max_long > 1073741823ul)
				ereport(ERROR,
						(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
						errmsg("invalid value for option \"%s\"", def->defname),
						errhint("Valid values in this context are integers between 1 and 1073741823.")));
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
	Oid srvId = InvalidOid;
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
	appendStringInfo(&version, "oracle_fdw %s, PostgreSQL %s, Oracle client %d.%d.%d.%d.%d", ORACLE_FDW_VERSION, pgversion, major, minor, update, patch, port_patch);

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
		/* get the server version only if a non-null argument was given */
		HeapTuple tup;
		Relation rel;
		Name srvname = PG_GETARG_NAME(0);
		ForeignServer *server;
		UserMapping *mapping;
		ForeignDataWrapper *wrapper;
		List *options;
		ListCell *cell;
		char *nls_lang = NULL, *user = NULL, *password = NULL, *dbserver = NULL;
		oracleSession *session;

		/* look up foreign server with this name */
		rel = heap_open(ForeignServerRelationId, AccessShareLock);

		tup = SearchSysCacheCopy1(FOREIGNSERVERNAME, NameGetDatum(srvname));
		if (!HeapTupleIsValid(tup))
			ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_OBJECT),
				errmsg("server \"%s\" does not exist", NameStr(*srvname))));

		srvId = HeapTupleGetOid(tup);

		heap_close(rel, AccessShareLock);

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
				nls_lang = ((Value *) (def->arg))->val.str;
			if (strcmp(def->defname, OPT_DBSERVER) == 0)
				dbserver = ((Value *) (def->arg))->val.str;
			if (strcmp(def->defname, OPT_USER) == 0)
				user = ((Value *) (def->arg))->val.str;
			if (strcmp(def->defname, OPT_PASSWORD) == 0)
				password = ((Value *) (def->arg))->val.str;
		}

		/* guess a good NLS_LANG environment setting */
		nls_lang = guessNlsLang(nls_lang);

		/* connect to Oracle database */
		session = oracleGetSession(
			dbserver,
			user,
			password,
			nls_lang,
			NULL,
			1
		);

		/* get the server version */
		oracleServerVersion(session, &major, &minor, &update, &patch, &port_patch);
		appendStringInfo(&version, ", Oracle server %d.%d.%d.%d.%d", major, minor, update, patch, port_patch);

		/* free the session (connection will be cached) */
		pfree(session);
	}

	PG_RETURN_TEXT_P(cstring_to_text(version.data));
}

/*
 * _PG_init
 * 		Library load-time initalization.
 * 		Sets exitHook() callback for backend shutdown.
 * 		Also finds the OIDs of PostGIS the PostGIS geometry type.
 */
void
_PG_init(void)
{
	Relation proc_rel;
	ScanKeyData key;
	SysScanDesc scan;
	HeapTuple tuple;

	/* register an exit hook */
	on_proc_exit(&exitHook, PointerGetDatum(NULL));

	/* initialize index scan for "st_geomfromwkb" in pg_proc */
	proc_rel = heap_open(ProcedureRelationId, AccessShareLock);
	ScanKeyInit(&key, Anum_pg_proc_proname, BTEqualStrategyNumber, F_NAMEEQ, CStringGetDatum("st_geomfromwkb"));
	scan = systable_beginscan(proc_rel, ProcedureNameArgsNspIndexId, true, GetTransactionSnapshot(), 1, &key);

	/* find the first function with two arguments */
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		oidvector argtypes;
		Form_pg_proc proc_tuple = (Form_pg_proc)GETSTRUCT(tuple);

		argtypes = proc_tuple->proargtypes;
		if (argtypes.dim1 != 2)
			continue;

		/*
		 * If we find two two-argument functions called 
		 * "st_geomfromwkb", assume that two copies of PostGIS
		 * are installed and give up.
		 */
		if (GEOMETRYOID != InvalidOid)
		{
			elog(DEBUG1, "oracle_fdw: more than one PostGIS installation found, giving up");

			GEOMETRYOID = InvalidOid;
			break;
		}

		/* the return type must be the PostGIS geometry type */
		GEOMETRYOID = proc_tuple->prorettype;

		elog(DEBUG1, "oracle_fdw: PostGIS is installed, GEOMETRYOID = %d", GEOMETRYOID);
	}

	/* end scan */
	systable_endscan(scan);
	heap_close(proc_rel, AccessShareLock);
}

#ifdef OLD_FDW_API
/*
 * oraclePlanForeignScan
 * 		Get an OracleFdwState for this foreign scan.
 * 		A FdwPlan is created and the state is are stored
 * 		("serialized") in its fdw_private field.
 */
FdwPlan *
oraclePlanForeignScan(Oid foreigntableid,
					PlannerInfo *root,
					RelOptInfo *baserel)
{
	struct OracleFdwState *fdwState;
	FdwPlan *fdwplan;
	List *fdw_private;
	bool plan_costs;
	int i;

	elog(DEBUG1, "oracle_fdw: plan foreign table scan on %d", foreigntableid);

	/* get connection options, connect and get the remote table description */
	fdwState = getFdwState(foreigntableid, &plan_costs);

	/* construct Oracle query and get the list of parameters and actions for RestrictInfos */
	fdwState->query = createQuery(fdwState->session, baserel, false, fdwState->oraTable, &(fdwState->params), &(fdwState->pushdown_clauses));
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

	/* get Oracle's (bad) estimate only if plan_costs is set */
	if (plan_costs)
	{
		/* get Oracle's cost estimates for the query */
		oracleEstimate(fdwState->session, fdwState->query, seq_page_cost, BLCKSZ, &(fdwState->startup_cost), &(fdwState->total_cost), &baserel->rows, &baserel->width);
	}
	else
	{
		/* otherwise, use a random "high" value */
		fdwState->startup_cost = fdwState->total_cost = 10000.0;
	}

	/* release Oracle session (will be cached) */
	pfree(fdwState->session);
	fdwState->session = NULL;

	/* "serialize" all necessary information in the private area */
	fdw_private = serializePlanData(fdwState);

	/* construct FdwPlan */
	fdwplan = makeNode(FdwPlan);
	fdwplan->startup_cost = fdwState->startup_cost;
	fdwplan->total_cost = fdwState->total_cost;
	fdwplan->fdw_private = fdw_private;

	return fdwplan;
}
#else
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
	bool plan_costs, need_keys = false, for_update = false, has_trigger;
	List *local_conditions = NIL;
	int i;
	double ntuples = -1;
	Relation rel;

	elog(DEBUG1, "oracle_fdw: plan foreign table scan on %d", foreigntableid);

	/* check if the foreign scan is for an UPDATE or DELETE */
	if (baserel->relid == root->parse->resultRelation &&
			(root->parse->commandType == CMD_UPDATE ||
			root->parse->commandType == CMD_DELETE))
	{
		/* we need the table's primary key columns */
		need_keys = true;
	}

	/* check if FOR [KEY] SHARE/UPDATE was specified */
	if (need_keys || get_parse_rowmark(root->parse, baserel->relid))
	{
		/* we should add FOR UPDATE */
		for_update = true;
	}

	/* get connection options, connect and get the remote table description */
	fdwState = getFdwState(foreigntableid, &plan_costs);

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
	rel = heap_open(foreigntableid, NoLock);

	/* is there an AFTER trigger FOR EACH ROW? */
	has_trigger = (baserel->relid == root->parse->resultRelation) && rel->trigdesc
					&& ((root->parse->commandType == CMD_UPDATE && rel->trigdesc->trig_update_after_row)
						|| (root->parse->commandType == CMD_DELETE && rel->trigdesc->trig_delete_after_row));

	heap_close(rel, NoLock);

	if (has_trigger)
	{
		/* we need to fetch and return all columns */
		for (i=0; i<fdwState->oraTable->ncols; ++i)
			if (fdwState->oraTable->cols[i]->pgname)
				fdwState->oraTable->cols[i]->used = 1;
	}

	/* construct Oracle query and get the list of parameters and actions for RestrictInfos */
	fdwState->query = createQuery(fdwState->session, baserel, for_update, fdwState->oraTable, &(fdwState->params), &(fdwState->pushdown_clauses));
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

	/* get Oracle's (bad) estimate only if plan_costs is set */
	if (plan_costs)
	{
		/* get Oracle's cost estimates for the query */
		oracleEstimate(fdwState->session, fdwState->query, seq_page_cost, BLCKSZ, &(fdwState->startup_cost), &(fdwState->total_cost), &ntuples, &baserel->width);

		/* estimate selectivity only for conditions that are not pushed down */
		for (i=list_length(baserel->baserestrictinfo)-1; i>=0; --i)
			if (! fdwState->pushdown_clauses[i])
				local_conditions = lcons(list_nth(baserel->baserestrictinfo, i), local_conditions);
	}
	else
	{
		/* otherwise, use a random "high" value for cost */
		fdwState->startup_cost = fdwState->total_cost = 10000.0;

		/* if baserel->pages > 0, there was an ANALYZE; use the row count estimate */
		if (baserel->pages > 0)
			ntuples = baserel->tuples;

		/* estimale selectivity locally for all conditions */
		local_conditions = baserel->baserestrictinfo;
	}

	/* release Oracle session (will be cached) */
	pfree(fdwState->session);
	fdwState->session = NULL;

	/* apply statistics only if we have a reasonable row count estimate */
	if (ntuples != -1)
	{
		/* estimate how clauses that are not pushed down will influence row count */
		ntuples = ntuples * clauselist_selectivity(root, local_conditions, 0, JOIN_INNER, NULL);
		/* make sure that the estimate is not less that 1 */
		ntuples = clamp_row_est(ntuples);
		baserel->rows = ntuples;
	}

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

	add_path(baserel,
		(Path *)create_foreignscan_path(root, baserel, baserel->rows,
				fdwState->startup_cost, fdwState->total_cost,
				NIL, NULL, NIL));
}

/*
 * oracleGetForeignPlan
 * 		Construct a ForeignScan node containing the serialized OracleFdwState,
 * 		the RestrictInfo clauses not handled entirely by Oracle and the list
 * 		of parameters we need for execution.
 */
ForeignScan
*oracleGetForeignPlan(PlannerInfo *root, RelOptInfo *baserel, Oid foreigntableid, ForeignPath *best_path, List *tlist, List *scan_clauses)
{
	struct OracleFdwState *fdwState = (struct OracleFdwState *)baserel->fdw_private;
	List *fdw_private, *keep_clauses = NIL;
	ListCell *cell1, *cell2;
	int i;

	/* "serialize" all necessary information for the path private area */
	fdw_private = serializePlanData(fdwState);

	/* keep only those clauses that are not handled by Oracle */
	foreach(cell1, scan_clauses)
	{
		i = 0;
		foreach(cell2, baserel->baserestrictinfo)
		{
			if (equal(lfirst(cell1), lfirst(cell2)) && ! fdwState->pushdown_clauses[i])
			{
				keep_clauses = lcons(lfirst(cell1), keep_clauses);
				break;
			}
			++i;
		}
	}

	/* remove the RestrictInfo node from all remaining clauses */
	keep_clauses = extract_actual_clauses(keep_clauses, false);

	/* Create the ForeignScan node */
	return make_foreignscan(tlist, keep_clauses, baserel->relid, fdwState->params, fdw_private);
}

bool
oracleAnalyzeForeignTable(Relation relation, AcquireSampleRowsFunc *func, BlockNumber *totalpages)
{
	*func = acquireSampleRowsFunc;
	/* use positive page count as a sign that the table has been ANALYZEd */
	*totalpages = 42;

	return true;
}
#endif  /* OLD_FDW_API */

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

	elog(DEBUG1, "oracle_fdw: explain foreign table scan on %d", RelationGetRelid(node->ss.ss_currentRelation));

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
#ifdef OLD_FDW_API
	List *fdw_private = ((FdwPlan *)fsplan->fdwplan)->fdw_private;
#else
	List *fdw_private = fsplan->fdw_private;
	List *exec_exprs;
	ListCell *cell;
	int index;
#endif  /* OLD_FDW_API */
	struct paramDesc *paramDesc;
	struct OracleFdwState *fdw_state;

	/* deserialize private plan data */
	fdw_state = deserializePlanData(fdw_private);
	node->fdw_state = (void *)fdw_state;

#ifndef OLD_FDW_API
	/* create an ExprState tree for the parameter expressions */
	exec_exprs = (List *)ExecInitExpr((Expr *)fsplan->fdw_exprs, (PlanState *)node);

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
				|| paramDesc->type == BPCHAROID || paramDesc->type == CHAROID)
			paramDesc->bindType = BIND_STRING;
		else if (paramDesc->type == DATEOID || paramDesc->type == TIMESTAMPOID
				|| paramDesc->type == TIMESTAMPTZOID)
			paramDesc->bindType = BIND_TIMESTAMP;
		else
			paramDesc->bindType = BIND_NUMBER;

		paramDesc->value = NULL;
		paramDesc->node = expr;
		paramDesc->bindh = NULL;
		paramDesc->colnum = -1;
		paramDesc->next = fdw_state->paramList;
		fdw_state->paramList = paramDesc;
	}
#endif  /* OLD_FDW_API */

	/* add a fake parameter ":now" if that string appears in the query */
	if (strstr(fdw_state->query, ":now") != NULL)
	{
		paramDesc = (struct paramDesc *)palloc(sizeof(struct paramDesc));
		paramDesc->name = pstrdup(":now");
		paramDesc->type = TIMESTAMPTZOID;
		paramDesc->bindType = BIND_TIMESTAMP;
		paramDesc->value = NULL;
		paramDesc->node = NULL;
		paramDesc->bindh = NULL;
		paramDesc->colnum = -1;
		paramDesc->next = fdw_state->paramList;
		fdw_state->paramList = paramDesc;
	}

	elog(DEBUG1, "oracle_fdw: begin foreign table scan on %d", RelationGetRelid(node->ss.ss_currentRelation));

	/* connect to Oracle database */
	fdw_state->session = oracleGetSession(
			fdw_state->dbserver,
			fdw_state->user,
			fdw_state->password,
			fdw_state->nls_lang,
			fdw_state->oraTable->pgname,
#ifdef WRITE_API
			GetCurrentTransactionNestLevel()
#else
			1
#endif  /* WRITE_API */
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
	int have_result;
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)node->fdw_state;

	if (oracleIsStatementOpen(fdw_state->session))
	{
		elog(DEBUG3, "oracle_fdw: get next row in foreign table scan on %d", RelationGetRelid(node->ss.ss_currentRelation));

		/* fetch the next result row */
		have_result = oracleFetchNext(fdw_state->session);
	}
	else
	{
		/* fill the parameter list with the actual values */
		char *paramInfo = setSelectParameters(fdw_state->paramList, econtext);

		/* execute the Oracle statement and fetch the first row */
		elog(DEBUG1, "oracle_fdw: execute query in foreign table scan on %d%s", RelationGetRelid(node->ss.ss_currentRelation), paramInfo);
		oraclePrepareQuery(fdw_state->session, fdw_state->query, fdw_state->oraTable);
		have_result = oracleExecuteQuery(fdw_state->session, fdw_state->oraTable, fdw_state->paramList);
	}

	/* initialize virtual tuple */
	ExecClearTuple(slot);

	if (have_result)
	{
		/* increase row count */
		++fdw_state->rowcount;

		/* convert result to arrays of values and null indicators */
		convertTuple(fdw_state, slot->tts_values, slot->tts_isnull, false);

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

	elog(DEBUG1, "oracle_fdw: end foreign table scan on %d", RelationGetRelid(node->ss.ss_currentRelation));

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

	elog(DEBUG1, "oracle_fdw: restart foreign table scan on %d", RelationGetRelid(node->ss.ss_currentRelation));

	/* close open Oracle statement if there is one */
	oracleCloseStatement(fdw_state->session);

	/* reset row count to zero */
	fdw_state->rowcount = 0;
}

#ifdef WRITE_API
/*
 * oracleAddForeignUpdateTargets
 * 		Add the primary key columns as resjunk entries.
 */
void
oracleAddForeignUpdateTargets(Query *parsetree, RangeTblEntry *target_rte, Relation target_relation)
{
	Oid relid = RelationGetRelid(target_relation);
	TupleDesc tupdesc = target_relation->rd_att;
	int i;
	bool has_key = false;

	elog(DEBUG1, "oracle_fdw: add target columns for update on %d", relid);

	/* loop through all columns of the foreign table */
	for (i=0; i<tupdesc->natts; ++i)
	{
		Form_pg_attribute att = tupdesc->attrs[i];
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
				if (optionIsTrue(((Value *)(def->arg))->val.str))
				{
					Var *var;
					TargetEntry *tle;

					/* Make a Var representing the desired value */
					var = makeVar(parsetree->resultRelation,
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

					has_key = true;
				}
			}
			else
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
	ListCell *cell;
	bool has_trigger = false, firstcol;
	struct paramDesc *param;
	char paramName[10];
	TupleDesc tupdesc;
	Bitmapset *tmpset;
	AttrNumber col;

	/* check if the foreign table is scanned */
	if (resultRelation < root->simple_rel_array_size
			&& root->simple_rel_array[resultRelation] != NULL)
	{
		/* if yes, copy the foreign table information from the associated RelOptInfo */
		fdwState = copyPlanData((struct OracleFdwState *)(root->simple_rel_array[resultRelation]->fdw_private));
	}
	else
	{
		bool plan_costs;

		/* if no, we have to construct it ourselves */
		fdwState = getFdwState(rte->relid, &plan_costs);
	}

	initStringInfo(&sql);

	/*
	 * Core code already has some lock on each rel being planned, so we can
	 * use NoLock here.
	 */
	rel = heap_open(rte->relid, NoLock);

	/* figure out which attributes are affected and if there is a trigger */
	switch (operation)
	{
		case CMD_INSERT:
			/*
	 		* In an INSERT, we transmit all columns that are defined in the foreign
	 		* table.  In an UPDATE, we transmit only columns that were explicitly
	 		* targets of the UPDATE, so as to avoid unnecessary data transmission.
	 		* (We can't do that for INSERT since we would miss sending default values
	 		* for columns not listed in the source statement.)
	 		*/

			tupdesc = RelationGetDescr(rel);

			for (attnum = 1; attnum <= tupdesc->natts; attnum++)
			{
				Form_pg_attribute attr = tupdesc->attrs[attnum - 1];

				if (!attr->attisdropped)
					targetAttrs = lappend_int(targetAttrs, attnum);
			}

			/* is there a row level AFTER trigger? */
			has_trigger = rel->trigdesc && rel->trigdesc->trig_insert_after_row;

			break;
		case CMD_UPDATE:
			tmpset = bms_copy(rte->modifiedCols);

			while ((col = bms_first_member(tmpset)) >= 0)
			{
				col += FirstLowInvalidHeapAttributeNumber;
				if (col <= InvalidAttrNumber)  /* shouldn't happen */
					elog(ERROR, "system-column update is not supported");
				targetAttrs = lappend_int(targetAttrs, col);
			}

			/* is there a row level AFTER trigger? */
			has_trigger = rel->trigdesc && rel->trigdesc->trig_update_after_row;

			break;
		case CMD_DELETE:

			/* is there a row level AFTER trigger? */
			has_trigger = rel->trigdesc && rel->trigdesc->trig_delete_after_row;

			break;
		default:
			elog(ERROR, "unexpected operation: %d", (int) operation);
	}

	heap_close(rel, NoLock);

	/* mark all attributes for which we need a RETURNING clause */
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
							errmsg("columns with Oracle type LONG or LONG RAW cannot be used in RETURNING clause"),
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
			/* get all the attributes mentioned there */
			pull_varattnos((Node *) returningList, resultRelation, &attrs_used);

			/* mark the corresponding columns as used */
			for (i=0; i<fdwState->oraTable->ncols; ++i)
			{
				/* ignore columns that are not in the PostgreSQL table */
				if (fdwState->oraTable->cols[i]->pgname == NULL)
					continue;

				if (bms_is_member(fdwState->oraTable->cols[i]->pgattnum - FirstLowInvalidHeapAttributeNumber, attrs_used))
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
			appendStringInfo(&sql, "INSERT INTO %s (", fdwState->oraTable->name);

			firstcol = true;
			for (i = 0; i < fdwState->oraTable->ncols; ++i)
			{
				/* don't add columns beyond the end of the PostgreSQL table */
				if (fdwState->oraTable->cols[i]->pgname == NULL)
					continue;

				if (firstcol)
					firstcol = false;
				else
					appendStringInfo(&sql, ", ");
				appendStringInfo(&sql, "%s", fdwState->oraTable->cols[i]->name);
			}

			appendStringInfo(&sql, ") VALUES (");

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
					appendStringInfo(&sql, ", ");

				appendStringInfo(&sql, "%s", paramName);
			}

			appendStringInfo(&sql, ")");

			break;
		case CMD_UPDATE:
			appendStringInfo(&sql, "UPDATE %s SET ", fdwState->oraTable->name);

			firstcol = true;
			i = 0;
			foreach(cell, targetAttrs)
			{
				/* find the corresponding oraTable entry */
				while (fdwState->oraTable->cols[i]->pgattnum < lfirst_int(cell))
					++i;

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
					appendStringInfo(&sql, ", ");

				appendStringInfo(&sql, "%s = %s", fdwState->oraTable->cols[i]->name, paramName);
			}

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

				appendStringInfo(&sql, " %s = %s", fdwState->oraTable->cols[i]->name, paramName);
			}
		}
	}

	/* add RETURNING clause if appropriate */
	firstcol = true;
	for (i=0; i<fdwState->oraTable->ncols; ++i)
		if (fdwState->oraTable->cols[i]->used)
		{
			if (firstcol)
			{
				firstcol = false;
				appendStringInfo(&sql, " RETURNING ");
			}
			else
				appendStringInfo(&sql, ", ");
			appendStringInfo(&sql, "%s", fdwState->oraTable->cols[i]->name);
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
			param->value = NULL;
			param->node = NULL;
			param->bindh = NULL;
			param->colnum = i;
			param->next = fdwState->paramList;
			fdwState->paramList = param;

			if (firstcol)
			{
				firstcol = false;
				appendStringInfo(&sql, " INTO ");
			}
			else
				appendStringInfo(&sql, ", ");
			appendStringInfo(&sql, "%s", paramName);
		}

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
	Plan *subplan = mtstate->mt_plans[subplan_index]->plan;

	elog(DEBUG1, "oracle_fdw: begin foreign table modify on %d", RelationGetRelid(rinfo->ri_RelationDesc));

	rinfo->ri_FdwState = fdw_state;

	/* connect to Oracle database */
	fdw_state->session = oracleGetSession(
			fdw_state->dbserver,
			fdw_state->user,
			fdw_state->password,
			fdw_state->nls_lang,
			fdw_state->oraTable->pgname,
			GetCurrentTransactionNestLevel()
		);

	oraclePrepareQuery(fdw_state->session, fdw_state->query, fdw_state->oraTable);

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
							ALLOCSET_SMALL_MINSIZE,
							ALLOCSET_SMALL_INITSIZE,
							ALLOCSET_SMALL_MAXSIZE);
}

/*
 * oracleExecForeignInsert
 * 		Set the parameter values from the slots and execute the INSERT statement.
 * 		Returns a slot with the results from the RETRUNING clause.
 */
TupleTableSlot *
oracleExecForeignInsert(EState *estate, ResultRelInfo *rinfo, TupleTableSlot *slot, TupleTableSlot *planSlot)
{
	struct OracleFdwState *fdw_state = (struct OracleFdwState *)rinfo->ri_FdwState;
	int rows;
	MemoryContext oldcontext;

	elog(DEBUG3, "oracle_fdw: execute foreign table insert on %d", RelationGetRelid(rinfo->ri_RelationDesc));

	++fdw_state->rowcount;

	MemoryContextReset(fdw_state->temp_cxt);
	oldcontext = MemoryContextSwitchTo(fdw_state->temp_cxt);

	/* extract the values from the slot and store them in the parameters */
	setModifyParameters(fdw_state->paramList, slot, planSlot, fdw_state->oraTable, fdw_state->session);

	/* execute the INSERT statement and store RETURNING values in oraTable's columns */
	rows = oracleExecuteQuery(fdw_state->session, fdw_state->oraTable, fdw_state->paramList);

	if (rows != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("INSERT on Oracle table added %d rows instead of one in iteration %lu", rows, fdw_state->rowcount)));

	MemoryContextSwitchTo(oldcontext);

	/* empty the result slot */
	ExecClearTuple(slot);

	/* convert result for RETURNING to arrays of values and null indicators */
	convertTuple(fdw_state, slot->tts_values, slot->tts_isnull, false);

	/* store the virtual tuple */
	ExecStoreVirtualTuple(slot);

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
	int rows;
	MemoryContext oldcontext;

	elog(DEBUG3, "oracle_fdw: execute foreign table update on %d", RelationGetRelid(rinfo->ri_RelationDesc));

	++fdw_state->rowcount;

	MemoryContextReset(fdw_state->temp_cxt);
	oldcontext = MemoryContextSwitchTo(fdw_state->temp_cxt);

	/* extract the values from the slot and store them in the parameters */
	setModifyParameters(fdw_state->paramList, slot, planSlot, fdw_state->oraTable, fdw_state->session);

	/* execute the UPDATE statement and store RETURNING values in oraTable's columns */
	rows = oracleExecuteQuery(fdw_state->session, fdw_state->oraTable, fdw_state->paramList);

	if (rows != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("UPDATE on Oracle table changed %d rows instead of one in iteration %lu", rows, fdw_state->rowcount),
				errhint("This probably means that you did not set the \"key\" option on all primary key columns.")));

	MemoryContextSwitchTo(oldcontext);

	/* empty the result slot */
	ExecClearTuple(slot);

	/* convert result for RETURNING to arrays of values and null indicators */
	convertTuple(fdw_state, slot->tts_values, slot->tts_isnull, false);

	/* store the virtual tuple */
	ExecStoreVirtualTuple(slot);

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

	++fdw_state->rowcount;

	MemoryContextReset(fdw_state->temp_cxt);
	oldcontext = MemoryContextSwitchTo(fdw_state->temp_cxt);

	/* extract the values from the slot and store them in the parameters */
	setModifyParameters(fdw_state->paramList, slot, planSlot, fdw_state->oraTable, fdw_state->session);

	/* execute the DELETE statement and store RETURNING values in oraTable's columns */
	rows = oracleExecuteQuery(fdw_state->session, fdw_state->oraTable, fdw_state->paramList);

	if (rows != 1)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
				errmsg("DELETE on Oracle table removed %d rows instead of one in iteration %lu", rows, fdw_state->rowcount),
				errhint("This probably means that you did not set the \"key\" option on all primary key columns.")));

	MemoryContextSwitchTo(oldcontext);

	/* empty the result slot */
	ExecClearTuple(slot);

	/* convert result for RETURNING to arrays of values and null indicators */
	convertTuple(fdw_state, slot->tts_values, slot->tts_isnull, false);

	/* store the virtual tuple */
	ExecStoreVirtualTuple(slot);

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
		char *value = ((Value *)(def->arg))->val.str;
		if (strcmp(def->defname, OPT_READONLY) == 0
				&& optionIsTrue(value))
			return 0;
	}

	return (1 << CMD_UPDATE) | (1 << CMD_INSERT) | (1 << CMD_DELETE);
}
#endif  /* WRITE_API */

/*
 * getFdwState
 * 		Construct an OracleFdwState from the options of the foreign table.
 * 		Establish an Oracle connection and get a description of the
 * 		remote table.
 */
struct OracleFdwState
*getFdwState(Oid foreigntableid, bool *plan_costs)
{
	struct OracleFdwState *fdwState = palloc(sizeof(struct OracleFdwState));
	char *pgtablename = get_rel_name(foreigntableid);
	List *options;
	ListCell *cell;
	char *schema = NULL, *table = NULL, *plancosts = NULL, *maxlong = NULL;
	long max_long;

	fdwState->nls_lang = NULL;
	fdwState->dbserver = NULL;
	fdwState->user = NULL;
	fdwState->password = NULL;
	fdwState->params = NIL;
	fdwState->paramList = NULL;
	fdwState->pushdown_clauses = NULL;
	fdwState->temp_cxt = NULL;

	/*
	 * Get all relevant options from the foreign table, the user mapping,
	 * the foreign server and the foreign data wrapper.
	 */
	oracleGetOptions(foreigntableid, &options);
	foreach(cell, options)
	{
		DefElem *def = (DefElem *) lfirst(cell);
		if (strcmp(def->defname, OPT_NLS_LANG) == 0)
			fdwState->nls_lang = ((Value *) (def->arg))->val.str;
		if (strcmp(def->defname, OPT_DBSERVER) == 0)
			fdwState->dbserver = ((Value *) (def->arg))->val.str;
		if (strcmp(def->defname, OPT_USER) == 0)
			fdwState->user = ((Value *) (def->arg))->val.str;
		if (strcmp(def->defname, OPT_PASSWORD) == 0)
			fdwState->password = ((Value *) (def->arg))->val.str;
		if (strcmp(def->defname, OPT_SCHEMA) == 0)
			schema = ((Value *) (def->arg))->val.str;
		if (strcmp(def->defname, OPT_TABLE) == 0)
			table = ((Value *) (def->arg))->val.str;
		if (strcmp(def->defname, OPT_PLAN_COSTS) == 0)
			plancosts = ((Value *) (def->arg))->val.str;
		if (strcmp(def->defname, OPT_MAX_LONG) == 0)
			maxlong = ((Value *) (def->arg))->val.str;
	}

	/* convert "max_long" option to number or use default */
	if (maxlong == NULL)
		max_long = DEFAULT_MAX_LONG;
	else
		max_long = strtol(maxlong, NULL, 0);

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
		fdwState->user,
		fdwState->password,
		fdwState->nls_lang,
		pgtablename,
#ifdef WRITE_API
		GetCurrentTransactionNestLevel()
#else
		1
#endif  /* WRITE_API */
	);

	/* get remote table description */
	fdwState->oraTable = oracleDescribe(fdwState->session, schema, table, pgtablename, max_long);

	/* add PostgreSQL data to table description */
	getColumnData(foreigntableid, fdwState->oraTable);

	/* test if we should invoke Oracle's optimizer for cost planning */
	*plan_costs = plancosts != NULL && optionIsTrue(plancosts);

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
oracleGetOptions(Oid foreigntableid, List **options)
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
	mapping = GetUserMapping(GetUserId(), table->serverid);
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

	rel = heap_open(foreigntableid, NoLock);
	tupdesc = rel->rd_att;

	/* number of PostgreSQL columns */
	oraTable->npgcols = tupdesc->natts;

	/* loop through foreign table columns */
	index = 0;
	for (i=0; i<tupdesc->natts; ++i)
	{
		Form_pg_attribute att_tuple = tupdesc->attrs[i];
#ifndef OLD_FDW_API
		List *options;
		ListCell *option;
#endif  /* OLD_FDW_API */

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

#ifndef OLD_FDW_API
		/* loop through column options */
		options = GetForeignColumnOptions(foreigntableid, att_tuple->attnum);
		foreach(option, options)
		{
			DefElem *def = (DefElem *)lfirst(option);

			/* is it the "key" option and is it set to "true" ? */
			if (strcmp(def->defname, OPT_KEY) == 0 && optionIsTrue(((Value *)(def->arg))->val.str))
			{
				/* mark the column as primary key column */
				oraTable->cols[index-1]->pkey = 1;
			}
		}
#endif	/* OLD_FDW_API */
	}

	heap_close(rel, NoLock);
}

/*
 * createQuery
 * 		Construct a query string for Oracle that
 * 		a) contains only the necessary columns in the SELECT list
 * 		b) has all the WHERE clauses that can safely be translated to Oracle.
 * 		Untranslatable WHERE clauses are omitted and left for PostgreSQL to check.
 * 		In "pushdown_clauses" an array is stored that contains "true" for all clauses
 * 		that will be pushed down and "false" for those that are filtered locally.
 * 		As a side effect, we also mark the used columns in oraTable.
 */
char
*createQuery(oracleSession *session, RelOptInfo *foreignrel, bool modify, struct oraTable *oraTable, List **params, bool **pushdown_clauses)
{
	ListCell *cell;
	bool first_col = true, in_quote = false;
	int i, clause_count = -1, index;
	char *where, *wherecopy, *p, md5[33], parname[10];
	StringInfoData query, result;
	List *columnlist = foreignrel->reltargetlist,
		*conditions = foreignrel->baserestrictinfo;

	/* first, find all the columns to include in the select list */

	/* examine each SELECT list entry for Var nodes */
	foreach(cell, columnlist)
	{
		getUsedColumns((Expr *)lfirst(cell), oraTable);
	}

	/* examine each condition for Var nodes */
	foreach(cell, conditions)
	{
		getUsedColumns((Expr *)lfirst(cell), oraTable);
	}

	/* construct SELECT list */
	initStringInfo(&query);
	for (i=0; i<oraTable->ncols; ++i)
	{
		if (oraTable->cols[i]->used)
		{
			if (first_col)
			{
				first_col = false;
				appendStringInfo(&query, "%s", oraTable->cols[i]->name);
			}
			else
			{
				appendStringInfo(&query, ", %s", oraTable->cols[i]->name);
			}
		}
	}
	/* dummy column if there is no result column we need from Oracle */
	if (first_col)
		appendStringInfo(&query, "'1'");
	appendStringInfo(&query, " FROM %s", oraTable->name);

	/* allocate enough space for pushdown_clauses */
	if (conditions != NIL)
	{
		*pushdown_clauses = (bool *)palloc(sizeof(bool) * list_length(conditions));
	}

	/* append WHERE clauses */
	first_col = true;
	foreach(cell, conditions)
	{
		/* try to convert each condition to Oracle SQL */
		where = getOracleWhereClause(session, foreignrel, ((RestrictInfo *)lfirst(cell))->clause, oraTable, params);
		if (where != NULL) {
			/* append new WHERE clause to query string */
			if (first_col)
			{
				first_col = false;
				appendStringInfo(&query, " WHERE %s", where);
			}
			else
			{
				appendStringInfo(&query, " AND %s", where);
			}
			pfree(where);

			(*pushdown_clauses)[++clause_count] = true;
		}
		else
			(*pushdown_clauses)[++clause_count] = false;
	}

	/* append FOR UPDATE if if the scan is for a modification */
	if (modify)
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
	foreach(cell, *params)
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
	 * Calculate MD5 hash of the query string so far.
	 * This is needed to find the query in Oracle's library cache for EXPLAIN.
	 */
	if (! pg_md5_hash(query.data, strlen(query.data), md5))
	{
		ereport(ERROR,
				(errcode(ERRCODE_OUT_OF_MEMORY),
				errmsg("out of memory")));
	}

	/* add comment with MD5 hash to query */
	initStringInfo(&result);
	appendStringInfo(&result, "SELECT /*%s*/ %s", md5, query.data);
	pfree(query.data);

	return result.data;
}

#ifndef OLD_FDW_API
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
	bool plan_costs, first_column = true;
	StringInfoData query;
	TupleDesc tupDesc = RelationGetDescr(relation);
	Datum *values = (Datum *)palloc(tupDesc->natts * sizeof(Datum));
	bool *nulls = (bool *)palloc(tupDesc->natts * sizeof(bool));
	double rstate, rowstoskip = -1;

	elog(DEBUG1, "oracle_fdw: analyze foreign table %d", RelationGetRelid(relation));

	*totalrows = 0;

	 /* Prepare for sampling rows */
	rstate = anl_init_selection_state(targrows);

	/* get connection options, connect and get the remote table description */
	fdw_state = getFdwState(RelationGetRelid(relation), &plan_costs);
	fdw_state ->paramList = NULL;
	fdw_state->pushdown_clauses = NULL;
	fdw_state->rowcount = 0;

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
			fdw_state->oraTable->cols[i]->val = (char *)palloc(fdw_state->oraTable->cols[i]->val_size);
			fdw_state->oraTable->cols[i]->val_len = 0;
			fdw_state->oraTable->cols[i]->val_len4 = 0;
			fdw_state->oraTable->cols[i]->val_null = 1;
	
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

	appendStringInfo(&query, " FROM %s", fdw_state->oraTable->name);
	fdw_state->query = query.data;
	elog(DEBUG1, "oracle_fdw: remote query is %s", fdw_state->query);

	/* get PostgreSQL column data types, check that they match Oracle's */
	for (i=0; i<fdw_state->oraTable->ncols; ++i)
		if (fdw_state->oraTable->cols[i]->used)
			checkDataType(
				fdw_state->oraTable->cols[i]->oratype,
				fdw_state->oraTable->cols[i]->scale,
				fdw_state->oraTable->cols[i]->pgtype,
				fdw_state->oraTable->pgname,
				fdw_state->oraTable->cols[i]->pgname
			);

	/* loop through query results */
	while(oracleIsStatementOpen(fdw_state->session)
			? oracleFetchNext(fdw_state->session)
			: (oraclePrepareQuery(fdw_state->session, fdw_state->query, fdw_state->oraTable),
				oracleExecuteQuery(fdw_state->session, fdw_state->oraTable, fdw_state->paramList)))
	{
		/* allow user to interrupt ANALYZE */
		vacuum_delay_point();

		++fdw_state->rowcount;

		if (collected_rows < targrows)
		{
			/* the first "targrows" rows are added as samples */
			convertTuple(fdw_state, values, nulls, true);
			rows[collected_rows++] = heap_form_tuple(tupDesc, values, nulls);
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
				convertTuple(fdw_state, values, nulls, true);
				rows[k] = heap_form_tuple(tupDesc, values, nulls);
			}
		}
	}

	*totalrows = (double)fdw_state->rowcount;
	*totaldeadrows = 0;

	/* report report */
	ereport(elevel, (errmsg("\"%s\": table contains %lu rows; %d rows in sample",
			RelationGetRelationName(relation), fdw_state->rowcount, collected_rows)));

	return collected_rows;
}
#endif  /* OLD_FDW_API */

/*
 * This macro is used by getOracleWhereClause to identify PostgreSQL
 * types that can be translated to Oracle SQL.
 */
#define canHandleType(x) ((x) == TEXTOID || (x) == CHAROID || (x) == BPCHAROID \
			|| (x) == VARCHAROID || (x) == NAMEOID || (x) == INT8OID || (x) == INT2OID \
			|| (x) == INT4OID || (x) == OIDOID || (x) == FLOAT4OID || (x) == FLOAT8OID \
			|| (x) == NUMERICOID || (x) == DATEOID || (x) == TIMESTAMPOID || (x) == TIMESTAMPTZOID \
			|| (x) == INTERVALOID)

/*
 * getOracleWhereClause
 * 		Create an Oracle SQL WHERE clause from the expression and store in in "where".
 * 		Returns NULL if that is not possible, else a palloc'ed string.
 * 		As a side effect, all Params incorporated in the WHERE clause
 * 		will be stored in paramList.
 */
char *
getOracleWhereClause(oracleSession *session, RelOptInfo *foreignrel, Expr *expr, const struct oraTable *oraTable, List **params)
{
	char *opername, *left, *right, *arg, oprkind;
	Const *constant;
	OpExpr *oper;
	ScalarArrayOpExpr *arrayoper;
	CaseExpr *caseexpr;
	BoolExpr *boolexpr;
	CoalesceExpr *coalesceexpr;
	CoerceViaIO *coerce;
	Param *param;
	Var *variable;
	FuncExpr *func;
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
#ifdef OLD_FDW_API
			/* don't try to push down parameters with 9.1 */
			return NULL;
#else
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
			initStringInfo(&result);
			appendStringInfo(&result, ":p%d", index);

			break;
#endif  /* OLD_FDW_API */
		case T_Var:
			variable = (Var *)expr;

			if (variable->varno == foreignrel->relid && variable->varlevelsup == 0)
			{
				/* the variable belongs to the foreign table, replace it with the name */

				/* we cannot handle system columns */
				if (variable->varattno < 1)
					return NULL;
	
				/*
				 * Allow boolean columns here.
				 * They will be rendered as ("COL" <> 0).
				 */
				if (! (canHandleType(variable->vartype) || variable->vartype == BOOLOID))
					return NULL;
	
				/* get oraTable column index corresponding to this column (-1 if none) */
				index = oraTable->ncols - 1;
				while (index >= 0 && oraTable->cols[index]->pgattnum != variable->varattno)
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
				oratype = oraTable->cols[index]->oratype;
				if ((variable->vartype == TEXTOID
						|| variable->vartype == BPCHAROID
						|| variable->vartype == VARCHAROID)
						&& oratype != ORA_TYPE_VARCHAR2
						&& oratype != ORA_TYPE_CHAR
						&& oratype != ORA_TYPE_NVARCHAR2
						&& oratype != ORA_TYPE_NCHAR
						&& oratype != ORA_TYPE_CLOB)
					return NULL;
	
				initStringInfo(&result);
	
				/* work around the lack of booleans in Oracle */
				if (variable->vartype == BOOLOID)
				{
					appendStringInfo(&result, "(");
				}
	
				appendStringInfo(&result, "%s", oraTable->cols[index]->name);
	
				/* work around the lack of booleans in Oracle */
				if (variable->vartype == BOOLOID)
				{
					appendStringInfo(&result, " <> 0)");
				}
			}
			else
			{
				/* treat it like a parameter */
#ifdef OLD_FDW_API
				/* don't try to push down parameters with 9.1 */
				return NULL;
#else
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
#endif  /* OLD_FDW_API */
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
				left = getOracleWhereClause(session, foreignrel, linitial(oper->args), oraTable, params);
				if (left == NULL)
				{
					pfree(opername);
					return NULL;
				}

				if (oprkind == 'b')
				{
					/* binary operator */
					right = getOracleWhereClause(session, foreignrel, lsecond(oper->args), oraTable, params);
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

			left = getOracleWhereClause(session, foreignrel, linitial(arrayoper->args), oraTable, params);
			if (left == NULL)
				return NULL;

			/* only push down IN expressions with constant second (=last) argument */
			if (((Expr *)llast(arrayoper->args))->type != T_Const)
				return NULL;

			/* begin to compose result */
			initStringInfo(&result);
			appendStringInfo(&result, "(%s %s (", left, arrayoper->useOr ? "IN" : "NOT IN");

			/* the second (=last) argument must be a Const of ArrayType */
			constant = (Const *)llast(arrayoper->args);

			/* loop through the array elements */
			iterator = array_create_iterator(DatumGetArrayTypeP(constant->constvalue), 0);
			first_arg = true;
			while (array_iterate(iterator, &datum, &isNull))
			{
				char *c;

				if (isNull)
					c = "NULL";
				else
				{
					c = datumToString(datum, leftargtype);
					if (c == NULL)
					{
						array_free_iterator(iterator);
						return NULL;
					}
				}

				/* append the srgument */
				appendStringInfo(&result, "%s%s", first_arg ? "" : ", ", c);
				first_arg = false;
			}
			array_free_iterator(iterator);

			/* don't allow empty arrays */
			if (first_arg)
				return NULL;

			/* two parentheses close the expression */
			appendStringInfo(&result, "))");

			break;
		case T_DistinctExpr:
			/* get argument type */
			tuple = SearchSysCache1(OPEROID, ObjectIdGetDatum(((DistinctExpr *)expr)->opno));
			if (! HeapTupleIsValid(tuple))
			{
				elog(ERROR, "cache lookup failed for operator %u", ((DistinctExpr *)expr)->opno);
			}
			rightargtype = ((Form_pg_operator)GETSTRUCT(tuple))->oprright;
			ReleaseSysCache(tuple);

			if (! canHandleType(rightargtype))
				return NULL;

			left = getOracleWhereClause(session, foreignrel, linitial(((DistinctExpr *)expr)->args), oraTable, params);
			if (left == NULL)
			{
				return NULL;
			}
			right = getOracleWhereClause(session, foreignrel, lsecond(((DistinctExpr *)expr)->args), oraTable, params);
			if (right == NULL)
			{
				pfree(left);
				return NULL;
			}

			initStringInfo(&result);
			appendStringInfo(&result, "(%s IS DISTINCT FROM %s)", left, right);

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

			left = getOracleWhereClause(session, foreignrel, linitial(((NullIfExpr *)expr)->args), oraTable, params);
			if (left == NULL)
			{
				return NULL;
			}
			right = getOracleWhereClause(session, foreignrel, lsecond(((NullIfExpr *)expr)->args), oraTable, params);
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

			arg = getOracleWhereClause(session, foreignrel, linitial(boolexpr->args), oraTable, params);
			if (arg == NULL)
				return NULL;

			initStringInfo(&result);
			appendStringInfo(&result, "(%s%s",
					boolexpr->boolop == NOT_EXPR ? "NOT " : "",
					arg);

			for_each_cell(cell, lnext(list_head(boolexpr->args)))
			{
				arg = getOracleWhereClause(session, foreignrel, (Expr *)lfirst(cell), oraTable, params);
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
			return getOracleWhereClause(session, foreignrel, ((RelabelType *)expr)->arg, oraTable, params);
			break;
		case T_CoerceToDomain:
			return getOracleWhereClause(session, foreignrel, ((CoerceToDomain *)expr)->arg, oraTable, params);
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
				arg = getOracleWhereClause(session, foreignrel, caseexpr->arg, oraTable, params);
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
					arg = getOracleWhereClause(session, foreignrel, whenclause->expr, oraTable, params);
				}
				else
				{
					/* for CASE arg WHEN ..., use only the right branch of the equality */
					arg = getOracleWhereClause(session, foreignrel, lsecond(((OpExpr *)whenclause->expr)->args), oraTable, params);
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
				arg = getOracleWhereClause(session, foreignrel, whenclause->result, oraTable, params);
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
				arg = getOracleWhereClause(session, foreignrel, caseexpr->defresult, oraTable, params);
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
				arg = getOracleWhereClause(session, foreignrel, (Expr *)lfirst(cell), oraTable, params);
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
			arg = getOracleWhereClause(session, foreignrel, ((NullTest *)expr)->arg, oraTable, params);
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
				return getOracleWhereClause(session, foreignrel, linitial(func->args), oraTable, params);

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
					arg = getOracleWhereClause(session, foreignrel, lfirst(cell), oraTable, params);
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
				left = getOracleWhereClause(session, foreignrel, linitial(func->args), oraTable, params);
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

					right = getOracleWhereClause(session, foreignrel, lsecond(func->args), oraTable, params);
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
			else if (strcmp(opername, "now") == 0 || strcmp(opername, "transaction_timestamp") == 0)
			{
				/* special case: current timestamp */
				initStringInfo(&result);
				appendStringInfo(&result, ":now");
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
					appendStringInfo(&result, "TRUNC(CAST (:now AS DATE))");
					break;
				case TIMESTAMPOID:
					appendStringInfo(&result, "(CAST (:now AS TIMESTAMP))");
					break;
				case TIMESTAMPTZOID:
					appendStringInfo(&result, ":now");
			}

			break;
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
	struct pg_tm tm;
	fsec_t fsec;

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
			str = DatumGetCString(OidFunctionCall1(typoutput, datum));

			/*
			 * Don't try to convert empty strings to Oracle.
			 * Oracle treats empty strings as NULL.
			 */
			if (str[0] == '\0')
				return NULL;

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
			str = DatumGetCString(OidFunctionCall1(typoutput, datum));
			initStringInfo(&result);
			appendStringInfo(&result, "TO_TIMESTAMP('%s', 'YYYY-MM-DD')", str);
			break;
		case TIMESTAMPOID:
			str = DatumGetCString(OidFunctionCall1(typoutput, datum));
			initStringInfo(&result);
			appendStringInfo(&result, "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF')", str);
			break;
		case TIMESTAMPTZOID:
			str = DatumGetCString(OidFunctionCall1(typoutput, datum));
			initStringInfo(&result);
			appendStringInfo(&result, "TO_TIMESTAMP_TZ('%s', 'YYYY-MM-DD HH24:MI:SS.FFTZH:TZM')", str);
			break;
		case INTERVALOID:
			if (interval2tm(*DatumGetIntervalP(datum), &tm, &fsec) != 0)
			{
				elog(ERROR, "could not convert interval to tm");
			}

			/* only translate intervals that can be translated to INTERVAL DAY TO SECOND */
			if (tm.tm_year != 0 || tm.tm_mon != 0)
				return NULL;

			/* Oracle intervals have only one sign */
			if (tm.tm_mday < 0 || tm.tm_hour < 0 || tm.tm_min < 0 || tm.tm_sec < 0 || fsec < 0)
			{
				str = "-";
				/* all signs must match */
				if (tm.tm_mday > 0 || tm.tm_hour > 0 || tm.tm_min > 0 || tm.tm_sec > 0 || fsec > 0)
					return false;
				tm.tm_mday = -tm.tm_mday;
				tm.tm_hour = -tm.tm_hour;
				tm.tm_min = -tm.tm_min;
				tm.tm_sec = -tm.tm_sec;
				fsec = -fsec;
			}
			else
				str = "";

			initStringInfo(&result);
			appendStringInfo(&result, "INTERVAL '%s%d %02d:%02d:%02d.%06d' DAY(9) TO SECOND(6)", str, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, fsec);

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
getUsedColumns(Expr *expr, struct oraTable *oraTable)
{
	ListCell *cell;
	Var *variable;
	int index;

	if (expr == NULL)
		return;

	switch(expr->type)
	{
		case T_RestrictInfo:
			getUsedColumns(((RestrictInfo *)expr)->clause, oraTable);
			break;
		case T_TargetEntry:
			getUsedColumns(((TargetEntry *)expr)->expr, oraTable);
			break;
		case T_Const:
		case T_Param:
		case T_CaseTestExpr:
		case T_CoerceToDomainValue:
		case T_CurrentOfExpr:
			break;
		case T_Var:
			variable = (Var *)expr;

			/* ignore system columns */
			if (variable->varattno < 1)
				break;

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
				getUsedColumns((Expr *)lfirst(cell), oraTable);
			}
			foreach(cell, ((Aggref *)expr)->aggorder)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable);
			}
			foreach(cell, ((Aggref *)expr)->aggdistinct)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable);
			}
			break;
		case T_WindowFunc:
			foreach(cell, ((WindowFunc *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable);
			}
			break;
		case T_ArrayRef:
			foreach(cell, ((ArrayRef *)expr)->refupperindexpr)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable);
			}
			foreach(cell, ((ArrayRef *)expr)->reflowerindexpr)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable);
			}
			getUsedColumns(((ArrayRef *)expr)->refexpr, oraTable);
			getUsedColumns(((ArrayRef *)expr)->refassgnexpr, oraTable);
			break;
		case T_FuncExpr:
			foreach(cell, ((FuncExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable);
			}
			break;
		case T_OpExpr:
			foreach(cell, ((OpExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable);
			}
			break;
		case T_DistinctExpr:
			foreach(cell, ((DistinctExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable);
			}
			break;
		case T_NullIfExpr:
			foreach(cell, ((NullIfExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable);
			}
			break;
		case T_ScalarArrayOpExpr:
			foreach(cell, ((ScalarArrayOpExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable);
			}
			break;
		case T_BoolExpr:
			foreach(cell, ((BoolExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable);
			}
			break;
		case T_SubPlan:
			foreach(cell, ((SubPlan *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable);
			}
			break;
		case T_AlternativeSubPlan:
			/* examine only first alternative */
			getUsedColumns((Expr *)linitial(((AlternativeSubPlan *)expr)->subplans), oraTable);
			break;
		case T_NamedArgExpr:
			getUsedColumns(((NamedArgExpr *)expr)->arg, oraTable);
			break;
		case T_FieldSelect:
			getUsedColumns(((FieldSelect *)expr)->arg, oraTable);
			break;
		case T_RelabelType:
			getUsedColumns(((RelabelType *)expr)->arg, oraTable);
			break;
		case T_CoerceViaIO:
			getUsedColumns(((CoerceViaIO *)expr)->arg, oraTable);
			break;
		case T_ArrayCoerceExpr:
			getUsedColumns(((ArrayCoerceExpr *)expr)->arg, oraTable);
			break;
		case T_ConvertRowtypeExpr:
			getUsedColumns(((ConvertRowtypeExpr *)expr)->arg, oraTable);
			break;
		case T_CollateExpr:
			getUsedColumns(((CollateExpr *)expr)->arg, oraTable);
			break;
		case T_CaseExpr:
			foreach(cell, ((CaseExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable);
			}
			getUsedColumns(((CaseExpr *)expr)->arg, oraTable);
			getUsedColumns(((CaseExpr *)expr)->defresult, oraTable);
			break;
		case T_CaseWhen:
			getUsedColumns(((CaseWhen *)expr)->expr, oraTable);
			getUsedColumns(((CaseWhen *)expr)->result, oraTable);
			break;
		case T_ArrayExpr:
			foreach(cell, ((ArrayExpr *)expr)->elements)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable);
			}
			break;
		case T_RowExpr:
			foreach(cell, ((RowExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable);
			}
			break;
		case T_RowCompareExpr:
			foreach(cell, ((RowCompareExpr *)expr)->largs)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable);
			}
			foreach(cell, ((RowCompareExpr *)expr)->rargs)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable);
			}
			break;
		case T_CoalesceExpr:
			foreach(cell, ((CoalesceExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable);
			}
			break;
		case T_MinMaxExpr:
			foreach(cell, ((MinMaxExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable);
			}
			break;
		case T_XmlExpr:
			foreach(cell, ((XmlExpr *)expr)->named_args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable);
			}
			foreach(cell, ((XmlExpr *)expr)->args)
			{
				getUsedColumns((Expr *)lfirst(cell), oraTable);
			}
			break;
		case T_NullTest:
			getUsedColumns(((NullTest *)expr)->arg, oraTable);
			break;
		case T_BooleanTest:
			getUsedColumns(((BooleanTest *)expr)->arg, oraTable);
			break;
		case T_CoerceToDomain:
			getUsedColumns(((CoerceToDomain *)expr)->arg, oraTable);
			break;
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
			|| oratype == ORA_TYPE_TIMESTAMPTZ)
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

	/* otherwise, report an error */
	ereport(ERROR,
			(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
			errmsg("column \"%s\" of foreign table \"%s\" cannot be converted to or from Oracle data type", colname, tablename)));
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
		if (strncmp(lc_messages, "ja_", 3) == 0 || pg_strncasecmp(lc_messages, "japanese", 8) == 0)
			language = "JAPANESE_JAPAN";
		if (strncmp(lc_messages, "pt_", 3) == 0 || pg_strncasecmp(lc_messages, "portuguese", 10) == 0)
			language = "BRAZILIAN PORTUGUESE_BRAZIL";
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

#define serializeInt(x) makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum((int32)(x)), 0, 1)
#define serializeOid(x) makeConst(OIDOID, -1, InvalidOid, 4, ObjectIdGetDatum(x), 0, 1)

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
	/* user name */
	result = lappend(result, serializeString(fdwState->user));
	/* password */
	result = lappend(result, serializeString(fdwState->password));
	/* nls_lang */
	result = lappend(result, serializeString(fdwState->nls_lang));
	/* query */
	result = lappend(result, serializeString(fdwState->query));
	/* Oracle table data */
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
		result = lappend(result, serializeInt(fdwState->oraTable->cols[i]->pkey));
		result = lappend(result, serializeLong(fdwState->oraTable->cols[i]->val_size));
		/* don't serialize val, val_len, val_len4 and val_null */
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
	/* don't serialize params, startup_cost, total_cost, pushdown_clauses, rowcount, columnindex and temp_cxt */

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
		return makeConst(TEXTOID, -1, InvalidOid, -1, PointerGetDatum(cstring_to_text(s)), 0, 0);
}

/*
 * serializeLong
 * 		Create a Const that contains the long integer.
 */

Const
*serializeLong(long i)
{
	if (sizeof(long) <= 4)
		return makeConst(INT4OID, -1, InvalidOid, 4, Int32GetDatum((int32)i), 1, 0);
	else
		return makeConst(INT4OID, -1, InvalidOid, 8, Int64GetDatum((int64)i),
#ifdef USE_FLOAT8_BYVAL
				1,
#else
				0,
#endif  /* USE_FLOAT8_BYVAL */
				0);
}

/*
 * deserializePlanData
 * 		Extract the data structures from a List created by serializePlanData.
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
	state->pushdown_clauses = NULL;
	/* these are not serialized */
	state->rowcount = 0;
	state->columnindex = 0;
	state->params = NULL;
	state->temp_cxt = NULL;

	/* dbserver */
	state->dbserver = deserializeString(lfirst(cell));
	cell = lnext(cell);

	/* user */
	state->user = deserializeString(lfirst(cell));
	cell = lnext(cell);

	/* password */
	state->password = deserializeString(lfirst(cell));
	cell = lnext(cell);

	/* nls_lang */
	state->nls_lang = deserializeString(lfirst(cell));
	cell = lnext(cell);

	/* query */
	state->query = deserializeString(lfirst(cell));
	cell = lnext(cell);

	/* table data */
	state->oraTable = (struct oraTable *)palloc(sizeof(struct oraTable));
	state->oraTable->name = deserializeString(lfirst(cell));
	cell = lnext(cell);
	state->oraTable->pgname = deserializeString(lfirst(cell));
	cell = lnext(cell);
	state->oraTable->ncols = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = lnext(cell);
	state->oraTable->npgcols = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = lnext(cell);
	state->oraTable->cols = (struct oraColumn **)palloc(sizeof(struct oraColumn *) * state->oraTable->ncols);

	/* loop columns */
	for (i=0; i<state->oraTable->ncols; ++i)
	{
		state->oraTable->cols[i] = (struct oraColumn *)palloc(sizeof(struct oraColumn));
		state->oraTable->cols[i]->name = deserializeString(lfirst(cell));
		cell = lnext(cell);
		state->oraTable->cols[i]->oratype = (oraType)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = lnext(cell);
		state->oraTable->cols[i]->scale = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = lnext(cell);
		state->oraTable->cols[i]->pgname = deserializeString(lfirst(cell));
		cell = lnext(cell);
		state->oraTable->cols[i]->pgattnum = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = lnext(cell);
		state->oraTable->cols[i]->pgtype = DatumGetObjectId(((Const *)lfirst(cell))->constvalue);
		cell = lnext(cell);
		state->oraTable->cols[i]->pgtypmod = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = lnext(cell);
		state->oraTable->cols[i]->used = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = lnext(cell);
		state->oraTable->cols[i]->pkey = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = lnext(cell);
		state->oraTable->cols[i]->val_size = deserializeLong(lfirst(cell));
		cell = lnext(cell);
		/* allocate memory for the result value */
		state->oraTable->cols[i]->val = (char *)palloc(state->oraTable->cols[i]->val_size + 1);
		state->oraTable->cols[i]->val_len = 0;
		state->oraTable->cols[i]->val_len4 = 0;
		state->oraTable->cols[i]->val_null = 1;
	}

	/* length of parameter list */
	len = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = lnext(cell);

	/* parameter table entries */
	state->paramList = NULL;
	for (i=0; i<len; ++i)
	{
		param = (struct paramDesc *)palloc(sizeof(struct paramDesc));
		param->name = deserializeString(lfirst(cell));
		cell = lnext(cell);
		param->type = DatumGetObjectId(((Const *)lfirst(cell))->constvalue);
		cell = lnext(cell);
		param->bindType = (oraBindType)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = lnext(cell);
		if (param->bindType == BIND_OUTPUT)
			param->value = (void *)42;  /* something != NULL */
		else
			param->value = NULL;
		param->node = NULL;
		param->bindh = NULL;
		param->colnum = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = lnext(cell);
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
 * deserializeLong
 * 		Extracts a long integer from a Const.
 */

long
deserializeLong(Const *constant)
{
	if (sizeof(long) <= 4)
		return (long)DatumGetInt32(constant->constvalue);
	else
		return (long)DatumGetInt64(constant->constvalue);
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

#ifdef WRITE_API
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
	copy->user = pstrdup(orig->user);
	copy->password = pstrdup(orig->password);
	copy->nls_lang = pstrdup(orig->nls_lang);
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
		copy->oraTable->cols[i]->pkey = orig->oraTable->cols[i]->pkey;
		copy->oraTable->cols[i]->val = NULL;
		copy->oraTable->cols[i]->val_size = orig->oraTable->cols[i]->val_size;
		copy->oraTable->cols[i]->val_len = 0;
		copy->oraTable->cols[i]->val_len4 = 0;
		copy->oraTable->cols[i]->val_null = 0;
	}
	copy->startup_cost = 0.0;
	copy->total_cost = 0.0;
	copy->pushdown_clauses = NULL;
	copy->rowcount = 0;
	copy->columnindex = 0;
	copy->temp_cxt = NULL;

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
		case ORA_TYPE_BINARYFLOAT:
		case ORA_TYPE_BINARYDOUBLE:
			param->bindType = BIND_NUMBER;
			break;
		case ORA_TYPE_DATE:
		case ORA_TYPE_TIMESTAMP:
		case ORA_TYPE_TIMESTAMPTZ:
			param->bindType = BIND_TIMESTAMP;
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
	int32 value_len, tzoffset;
	char *p, *q;
	struct pg_tm datetime_tm;
	fsec_t datetime_fsec;
	StringInfoData s;

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

				/* special treatment for intervals */
				if (oraTable->cols[param->colnum]->pgtype == INTERVALOID)
				{
					char sign = '+';

					/* get the parts */
					(void)interval2tm(*DatumGetIntervalP(datum), &datetime_tm, &datetime_fsec);

					switch (oraTable->cols[param->colnum]->oratype)
					{
						case ORA_TYPE_INTERVALY2M:
							if (datetime_tm.tm_mday != 0 || datetime_tm.tm_hour != 0
									|| datetime_tm.tm_min != 0 || datetime_tm.tm_sec != 0 || datetime_fsec != 0)
								ereport(ERROR,
										(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
										errmsg("invalid value for Oracle INTERVAL YEAR TO MONTH"),
										errdetail("Only year and month can be non-zero for such an interval.")));
							if (datetime_tm.tm_year < 0 || datetime_tm.tm_mon < 0)
							{
								if (datetime_tm.tm_year > 0 || datetime_tm.tm_mon > 0)
									ereport(ERROR,
											(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
											errmsg("invalid value for Oracle INTERVAL YEAR TO MONTH"),
											errdetail("Year and month must be either both positive or both negative.")));
								sign = '-';
								datetime_tm.tm_year = -datetime_tm.tm_year;
								datetime_tm.tm_mon = -datetime_tm.tm_mon;
							}

							initStringInfo(&s);
							appendStringInfo(&s, "%c%d-%d", sign, datetime_tm.tm_year, datetime_tm.tm_mon);
							param->value = s.data;
							break;
						case ORA_TYPE_INTERVALD2S:
							if (datetime_tm.tm_year != 0 || datetime_tm.tm_mon != 0)
								ereport(ERROR,
										(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
										errmsg("invalid value for Oracle INTERVAL DAY TO SECOND"),
										errdetail("Year and month must be zero for such an interval.")));
							if (datetime_tm.tm_mday < 0 || datetime_tm.tm_hour < 0 || datetime_tm.tm_min < 0
								|| datetime_tm.tm_sec < 0 || datetime_fsec < 0)
							{
								if (datetime_tm.tm_mday > 0 || datetime_tm.tm_hour > 0 || datetime_tm.tm_min > 0
									|| datetime_tm.tm_sec > 0 || datetime_fsec > 0)
									ereport(ERROR,
											(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
											errmsg("invalid value for Oracle INTERVAL DAY TO SECOND"),
											errdetail("Interval elements must be either all positive or all negative.")));
								sign = '-';
								datetime_tm.tm_mday = -datetime_tm.tm_mday;
								datetime_tm.tm_hour = -datetime_tm.tm_hour;
								datetime_tm.tm_min = -datetime_tm.tm_min;
								datetime_tm.tm_sec = -datetime_tm.tm_sec;
								datetime_fsec = -datetime_fsec;
							}

							initStringInfo(&s);
							appendStringInfo(&s, "%c%d %02d:%02d:%02d.%06d",
									sign, datetime_tm.tm_mday, datetime_tm.tm_hour, datetime_tm.tm_min,
									datetime_tm.tm_sec, (int32)datetime_fsec);
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
						for (p = q = param->value; *p != '\0'; ++p, ++q)
						{
							if (*p == '-')
								++p;
							*q = *p;
						}
						*q = '\0';
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
			case BIND_TIMESTAMP:
				if (isnull)
				{
					param->value = NULL;
					break;
				}

				/*
				 * We have to handle datetime types specially, because
				 * their string representation depends on DateStyle.
				 */
				switch(oraTable->cols[param->colnum]->pgtype)
				{
					case DATEOID:
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
						appendStringInfo(&s, "%04d-%02d-%02d", datetime_tm.tm_year, datetime_tm.tm_mon, datetime_tm.tm_mday);
						param->value = s.data;

						break;
					case TIMESTAMPOID:
					case TIMESTAMPTZOID:
						/* this is sloppy, but DatumGetTimestampTz and DatumGetTimestamp are the same */
						if (TIMESTAMP_NOT_FINITE(DatumGetTimestampTz(datum)))
							ereport(ERROR,
									(errcode(ERRCODE_FDW_INVALID_ATTRIBUTE_VALUE),
									errmsg("infinite timestamp value cannot be stored in Oracle")));

						/* get the parts */
						(void)timestamp2tm(DatumGetTimestampTz(datum),
									(oraTable->cols[param->colnum]->pgtype == TIMESTAMPOID) ? NULL : &tzoffset,
									&datetime_tm,
									&datetime_fsec,
									NULL,
									NULL);

						initStringInfo(&s);
						appendStringInfo(&s, "%04d-%02d-%02d %02d:%02d:%02d.%06d",
								datetime_tm.tm_year, datetime_tm.tm_mon, datetime_tm.tm_mday,
								datetime_tm.tm_hour, datetime_tm.tm_min, datetime_tm.tm_sec, (int32)datetime_fsec);
						if (oraTable->cols[param->colnum]->pgtype == TIMESTAMPTZOID)
							appendStringInfo(&s, "%+03d:%02d", -tzoffset / 3600,
									(tzoffset > 0) ? tzoffset % 3600 : -tzoffset % 3600);
						param->value = s.data;

						break;
					default:
						elog(ERROR, "impossible datetime type for binding as timestamp");
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
#endif  /* WRITE_API */

/*
 * transactionCallback
 * 		Commit or rollback Oracle transactions when appropriate.
 */
void
transactionCallback(XactEvent event, void *arg)
{
	switch(event)
	{
#ifdef WRITE_API
		case XACT_EVENT_PRE_COMMIT:
			/* remote commit */
			oracleEndTransaction(arg, 1, 0);
			break;
		case XACT_EVENT_PRE_PREPARE:
			ereport(ERROR,
					(errcode(ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION),
					errmsg("cannot prepare a transaction that used remote tables")));
			break;
#endif  /* WRITE_API */
		case XACT_EVENT_COMMIT:
		case XACT_EVENT_PREPARE:
			/*
			 * Commit the remote transaction ignoring errors.
			 * In 9.3 or higher, the transaction must already be closed, so this does nothing.
			 * In 9.2 or lower, this is ok since nothing can have been modified remotely.
			 */
			oracleEndTransaction(arg, 1, 1);
			break;
		case XACT_EVENT_ABORT:
			/* remote rollback */
			oracleEndTransaction(arg, 0, 1);
			break;
	}
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
#ifndef OLD_FDW_API
	MemoryContext oldcontext;
#endif  /* OLD_FDW_API */
	StringInfoData info;  /* list of parameters for DEBUG message */
	initStringInfo(&info);

#ifndef OLD_FDW_API
	/* switch to short lived memory context */
	oldcontext = MemoryContextSwitchTo(econtext->ecxt_per_tuple_memory);
#endif  /* OLD_FDW_API */

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
			datum = ExecEvalExpr((ExprState *)(param->node), econtext, &is_null, NULL);
		}

		if (is_null)
		{
			param->value = NULL;
		}
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
		}

		/* build a parameter list for the DEBUG message */
		if (first_param)
		{
			first_param = false;
			appendStringInfo(&info, ", parameters %s=\"%s\"", param->name, param->value);
		}
		else
		{
			appendStringInfo(&info, ", %s=\"%s\"", param->name, param->value);
		}
	}

#ifndef OLD_FDW_API
	/* reset memory context */
	MemoryContextSwitchTo(oldcontext);
#endif  /* OLD_FDW_API */

	return info.data;
}

/*
 * convertTuple
 * 		Convert a result row from Oracle stored in oraTable
 * 		into arrays of values and null indicators.
 * 		If trunc_lob it true, truncate LOBs to WIDTH_THRESHOLD+1 bytes.
 */
void
convertTuple(struct OracleFdwState *fdw_state, Datum *values, bool *nulls, bool trunc_lob)
{
	char *value = NULL;
	long value_len = 0;
	int j, index = -1;
	ErrorContextCallback errcb;
	Oid pgtype;

	/* initialize error context callback, install it only during conversions */
	errcb.callback = errorContextCallback;
	errcb.arg = (void *)fdw_state;

	/* assign result values */
	for (j=0; j<fdw_state->oraTable->npgcols; ++j)
	{
		/* for dropped columns, insert a NULL */
		if ((index + 1 < fdw_state->oraTable->ncols)
				&& (fdw_state->oraTable->cols[index + 1]->pgattnum > j + 1))
		{
			nulls[j] = true;
			values[j] = PointerGetDatum(NULL);
			continue;
		}
		else
			++index;

		/*
		 * Columns exceeding the length of the Oracle table will be NULL,
		 * as well as columns that are not used in the query.
		 * Geometry columns are NULL if the value is NULL,
		 * for all other types use the NULL indicator.
		 */
		if (index >= fdw_state->oraTable->ncols
			|| fdw_state->oraTable->cols[index]->used == 0
			|| (fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_GEOMETRY
				&& ((ora_geometry *)fdw_state->oraTable->cols[index]->val)->geometry == NULL)
			|| fdw_state->oraTable->cols[index]->val_null == -1)
		{
			nulls[j] = true;
			values[j] = PointerGetDatum(NULL);
			continue;
		}

		/* from here on, we can assume columns to be NOT NULL */
		nulls[j] = false;
		pgtype = fdw_state->oraTable->cols[index]->pgtype;

		/* get the data and its length */
		if (fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_BLOB
				|| fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_BFILE
				|| fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_CLOB)
		{
			/* for LOBs, get the actual LOB contents (palloc'ed), truncated if desired */
			oracleGetLob(fdw_state->session,
				(void *)fdw_state->oraTable->cols[index]->val, fdw_state->oraTable->cols[index]->oratype,
				&value, &value_len, trunc_lob ? (WIDTH_THRESHOLD+1) : 0);
		}
		else if (fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_GEOMETRY)
		{
			ora_geometry *geom = (ora_geometry *)fdw_state->oraTable->cols[index]->val;

			/* install error context callback */
			errcb.previous = error_context_stack;
			error_context_stack = &errcb;
			fdw_state->columnindex = index;

			value_len = oracleGetEWKBLen(fdw_state->session, geom);

			/* uninstall error context callback */
			error_context_stack = errcb.previous;

			value = NULL;  /* we will fetch that later to avoid unnecessary copying */
		}
		else if (fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_LONG
				|| fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_LONGRAW)
		{
			/* for LONG and LONG RAW, the first 4 bytes contain the length */
			value_len = *((int32 *)fdw_state->oraTable->cols[index]->val);
			/* the rest is the actual data */
			value = fdw_state->oraTable->cols[index]->val + 4;
			/* terminating zero byte (needed for LONGs) */
			value[value_len] = '\0';
		}
		else
		{
			/* for other data types, oraTable contains the results */
			value = fdw_state->oraTable->cols[index]->val;
			value_len = fdw_state->oraTable->cols[index]->val_len;
		}

		/* fill the TupleSlot with the data (after conversion if necessary) */
		if (fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_GEOMETRY)
		{
			ora_geometry *geom = (ora_geometry *)fdw_state->oraTable->cols[index]->val;
			struct varlena *result = NULL;

			/* install error context callback */
			errcb.previous = error_context_stack;
			error_context_stack = &errcb;
			fdw_state->columnindex = index;

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
			if (fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_INTERVALD2S
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
			fdw_state->columnindex = index;

			/* for string types, check that the data are in the database encoding */
			if (pgtype == BPCHAROID || pgtype == VARCHAROID || pgtype == TEXTOID)
				(void)pg_verify_mbstr(GetDatabaseEncoding(), value, value_len, false);

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
						Int32GetDatum(fdw_state->oraTable->cols[index]->pgtypmod));
					break;
				default:
					/* the others don't */
					values[j] = OidFunctionCall1(typinput, dat);
			}

			/* uninstall error context callback */
			error_context_stack = errcb.previous;
		}

		/* free the data buffer for LOBs */
		if (fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_BLOB
				|| fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_BFILE
				|| fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_CLOB)
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
#ifdef WRITE_API
	RegisterSubXactCallback(subtransactionCallback, arg);
#endif  /* WRITE_API */
}

/*
 * oracleUnregisterCallback
 * 		Unregister a callback for PostgreSQL transaction events.
 */
void
oracleUnregisterCallback(void *arg)
{
	UnregisterXactCallback(transactionCallback, arg);
#ifdef WRITE_API
	UnregisterSubXactCallback(subtransactionCallback, arg);
#endif  /* WRITE_API */
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

/* get a PostgreSQL error code from an oraError */
#define to_sqlstate(x) \
	(x==FDW_UNABLE_TO_ESTABLISH_CONNECTION ? ERRCODE_FDW_UNABLE_TO_ESTABLISH_CONNECTION : \
	(x==FDW_UNABLE_TO_CREATE_REPLY ? ERRCODE_FDW_UNABLE_TO_CREATE_REPLY : \
	(x==FDW_TABLE_NOT_FOUND ? ERRCODE_FDW_TABLE_NOT_FOUND : \
	(x==FDW_UNABLE_TO_CREATE_EXECUTION ? ERRCODE_FDW_UNABLE_TO_CREATE_EXECUTION : \
	(x==FDW_OUT_OF_MEMORY ? ERRCODE_FDW_OUT_OF_MEMORY : \
	(x==FDW_SERIALIZATION_FAILURE ? ERRCODE_T_R_SERIALIZATION_FAILURE : ERRCODE_FDW_ERROR))))))

/*
 * oracleError_d
 * 		Report a PostgreSQL error with a detail message.
 */
void
oracleError_d(oraError sqlstate, const char *message, const char *detail)
{
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
	ereport(ERROR,
			(errcode(to_sqlstate(sqlstate)),
			errmsg("%s", message)));
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
