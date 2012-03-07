/*-------------------------------------------------------------------------
 *
 * oracle_fdw.c
 * 		PostgreSQL-related functions for Oracle foreign data wrapper.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "fmgr.h"
#include "access/reloptions.h"
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
#include "foreign/fdwapi.h"
#include "foreign/foreign.h"
#include "libpq/md5.h"
#include "miscadmin.h"
#include "nodes/makefuncs.h"
#include "optimizer/cost.h"
#include "optimizer/pathnode.h"
#include "port.h"
#include "storage/ipc.h"
#include "storage/lock.h"
#include "utils/fmgroids.h"
#include "utils/guc.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/resowner.h"
#include "utils/tqual.h"
#include "utils/syscache.h"
#include "utils/timestamp.h"

#include <string.h>

#include "oracle_fdw.h"

#if PG_VERSION_NUM < 90200
#define OLD_FDW_API
#else
#undef OLD_FDW_API
#endif

PG_MODULE_MAGIC;

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
	{OPT_PLAN_COSTS, ForeignTableRelationId, false}
};

#define option_count (sizeof(valid_options)/sizeof(struct OracleFdwOption))

/*
 * FDW-specific information for ForeignScanState.fdw_state.
 */
struct OracleFdwExecutionState {
	oracleSession *session;       /* encapsulates the active Oracle session */
	char *query;                  /* query we issue against Oracle */
	struct paramDesc *paramList;  /* description of parameters needed for the query */
	struct oraTable *oraTable;    /* description of the remote Oracle table */
};

/*
 * SQL functions
 */
extern Datum oracle_fdw_handler(PG_FUNCTION_ARGS);
extern Datum oracle_fdw_validator(PG_FUNCTION_ARGS);
extern Datum oracle_close_connections(PG_FUNCTION_ARGS);

PG_FUNCTION_INFO_V1(oracle_fdw_handler);
PG_FUNCTION_INFO_V1(oracle_fdw_validator);
PG_FUNCTION_INFO_V1(oracle_close_connections);

/*
 * on-load initializer
 */
extern void _PG_init(void);

/*
 * FDW callback routines
 */
#ifdef OLD_FDW_API
static FdwPlan *oraclePlanForeignScan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel);
#else
static void oraclePlanForeignScan(Oid foreigntableid, PlannerInfo *root, RelOptInfo *baserel);
#endif
static void oracleExplainForeignScan(ForeignScanState *node, ExplainState *es);
static void oracleBeginForeignScan(ForeignScanState *node, int eflags);
static TupleTableSlot *oracleIterateForeignScan(ForeignScanState *node);
static void oracleReScanForeignScan(ForeignScanState *node);
static void oracleEndForeignScan(ForeignScanState *node);

/*
 * Helper functions
 */
static void oracleGetOptions(Oid foreigntableid, List **options);
static char *createQuery(oracleSession *session, List *columnlist, List *conditions, struct oraTable *oraTable, struct paramDesc **paramList);
static void getColumnData(Oid foreigntableid, struct oraTable *oraTable);
static char *getOracleWhereClause(oracleSession *session, Expr *expr, const struct oraTable *oraTable, struct paramDesc **paramList);
static void getUsedColumns(Expr *expr, struct oraTable *oraTable);
static void checkDataTypes(oracleSession *session, struct oraTable *oraTable);
static char *guessNlsLang(char *nls_lang);
static List *serializePlanData(const char *dbserver, const char *user, const char *password, const char *nls_lang, const char *query, const struct oraTable *oraTable, const struct paramDesc *paramList);
static Const *serializeString(const char *s);
static Const *serializeLong(long i);
static void deserializePlanData(List *list, char **dbserver, char **user, char **password, char **nls_lang, struct OracleFdwExecutionState **state);
static char *deserializeString(Const *constant);
static long deserializeLong(Const *constant);
static void cleanupTransaction(ResourceReleasePhase phase, bool isCommit, bool isTopLevel, void *arg);
static void exitHook(int code, Datum arg);
static char *setParameters(struct paramDesc *paramList, EState *execstate);

/*
 * Foreign-data wrapper handler function: return a struct with pointers
 * to callback routines.
 */
Datum
oracle_fdw_handler(PG_FUNCTION_ARGS)
{
	FdwRoutine *fdwroutine = makeNode(FdwRoutine);

	fdwroutine->PlanForeignScan = oraclePlanForeignScan;
	fdwroutine->ExplainForeignScan = oracleExplainForeignScan;
	fdwroutine->BeginForeignScan = oracleBeginForeignScan;
	fdwroutine->IterateForeignScan = oracleIterateForeignScan;
	fdwroutine->ReScanForeignScan = oracleReScanForeignScan;
	fdwroutine->EndForeignScan = oracleEndForeignScan;

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
Datum
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
		if (strcmp(def->defname, OPT_PLAN_COSTS) == 0)
		{
			char *val = ((Value *) (def->arg))->val.str;
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
Datum
oracle_close_connections(PG_FUNCTION_ARGS)
{
	elog(DEBUG1, "oracle_fdw: close all Oracle connections");
	oracleCloseConnections();

	PG_RETURN_VOID();
}

/*
 * _PG_init
 * 		Library load-time initalization, sets exitHook() callback for
 * 		backend shutdown.
 */
void
_PG_init()
{
	on_proc_exit(&exitHook, PointerGetDatum(NULL));
}

/*
 * oraclePlanForeignScan
 * 		Establish an Oracle connection, get a description of the
 * 		remote table, construct the query to issue against Oracle
 * 		and a list of parameters for the query, verify that column
 * 		data types can be converted, get Oracle's cost estimates if
 * 		desired.
 *
 * 		A FdwPlan is created and the connection information, the
 * 		remote table description, the query and the parameter info
 * 		are stored ("serialized") in its fdw_private field.
 */
#ifdef OLD_FDW_API
FdwPlan *
#else
void
#endif
oraclePlanForeignScan(Oid foreigntableid,
					PlannerInfo *root,
					RelOptInfo *baserel)
{
	List *options, *fdw_private;
	ListCell *cell;
	char *dbserver = NULL, *user = NULL, *password = NULL, *schema = NULL, *table = NULL,
		*nls_lang = NULL, *plan_costs = NULL, *qualtable, *query, *pgtablename;
	oracleSession *session;
	struct oraTable *oraTable;
	struct paramDesc *paramList = NULL, *l, *l1;
	StringInfoData buf;
	Cost startup_cost, total_cost;
	int i;

	pgtablename = get_rel_name(foreigntableid);

	/*
	 * Get all relevant options from the foreign table, the user mapping,
	 * the foreign server and the foreign data wrapper.
	 */
	oracleGetOptions(foreigntableid, &options);
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
		if (strcmp(def->defname, OPT_SCHEMA) == 0)
			schema = ((Value *) (def->arg))->val.str;
		if (strcmp(def->defname, OPT_TABLE) == 0)
			table = ((Value *) (def->arg))->val.str;
		if (strcmp(def->defname, OPT_PLAN_COSTS) == 0)
			plan_costs = ((Value *) (def->arg))->val.str;
	}

	/* check if options are ok */
	if (table == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_FDW_OPTION_NAME_NOT_FOUND),
				errmsg("required option \"%s\" in foreign table \"%s\" missing", OPT_TABLE, pgtablename)));

	elog(DEBUG1, "oracle_fdw: plan foreign table scan on %d", foreigntableid);

	/* guess a good NLS_LANG environment setting */
	nls_lang = guessNlsLang(nls_lang);

	/* connect to Oracle database */
	session = oracleGetSession(dbserver, user, password, nls_lang, pgtablename, 0);

	/* (optionally) qualified table name for Oracle */
	if (schema == NULL)
	{
		qualtable = table;
	}
	else
	{
		initStringInfo(&buf);
		appendStringInfo(&buf, "%s.%s", schema, table);
		qualtable = buf.data;
	}

	/* get remote table description */
	oraTable = oracleDescribe(session, qualtable, pgtablename);

	/* add PostgreSQL data to table description */
	getColumnData(foreigntableid, oraTable);

	/* construct Oracle query and get the list of parameters */
	query = createQuery(session, baserel->reltargetlist, baserel->baserestrictinfo, oraTable, &paramList);
	elog(DEBUG1, "oracle_fdw: remote query is: %s", query);

	/* get PostgreSQL column data types, check that they match Oracle's */
	checkDataTypes(session, oraTable);

	/* get Oracle's (bad) estimate only if plan_costs is set */
	if (plan_costs != NULL
			&& (strcmp(plan_costs, "on") == 0
			|| strcmp(plan_costs, "ON") == 0
			|| strcmp(plan_costs, "yes") == 0
			|| strcmp(plan_costs, "YES") == 0
			|| strcmp(plan_costs, "true") == 0
			|| strcmp(plan_costs, "TRUE") == 0))
	{
		/* get Oracle's cost estimates for the query */
		oracleEstimate(session, query, seq_page_cost, BLCKSZ, &startup_cost, &total_cost, &baserel->rows, &baserel->width);
	}
	else
	{
		/* otherwise, use a random "high" value */
		startup_cost = total_cost = 10000.0;
	}

	/* "serialize" all necessary information in the private area */
	fdw_private = serializePlanData(dbserver, user, password, nls_lang, query, oraTable, paramList);

	/* release Oracle session (will be cached) */
	oracleReleaseSession(session, NULL, 0, 0);

	/* free query */
	if (schema != NULL)
		pfree(qualtable);
	pfree(query);

	/* free oraTable */
	pfree(oraTable->name);
	pfree(oraTable->pgname);
	for (i=0; i<oraTable->ncols; ++i)
	{
		pfree(oraTable->cols[i]->name);
		if (oraTable->cols[i]->pgname != NULL)
			pfree(oraTable->cols[i]->pgname);
		pfree(oraTable->cols[i]);
	}
	pfree(oraTable);

	/* free parameter list */
	l = paramList;
	while (l)
	{
		pfree(l->name);
		l1 = l->next;
		pfree(l);
		l = l1;
	}

	{
#ifdef OLD_FDW_API
		/* construct FdwPlan */
		FdwPlan *fdwplan = makeNode(FdwPlan);
		fdwplan->startup_cost = startup_cost;
		fdwplan->total_cost = total_cost;
		fdwplan->fdw_private = fdw_private;

		return fdwplan;
#else
		add_path(baserel,
			(Path *)create_foreignscan_path(
				root, baserel, baserel->rows,
				startup_cost, total_cost,
				NIL, NULL, NIL, fdw_private));

		return;
#endif
	}
}

/*
 * oracleExplainForeignScan
 * 		Produce extra output for EXPLAIN:
 * 		the Oracle query and, if VERBOSE was given, the execution plan.
 */
void
oracleExplainForeignScan(ForeignScanState *node, ExplainState *es)
{
	struct OracleFdwExecutionState *fdw_state = (struct OracleFdwExecutionState *)node->fdw_state;
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
#ifdef OLD_FDW_API
	List *fdw_private = ((FdwPlan *)((ForeignScan *)node->ss.ps.plan)->fdwplan)->fdw_private;
#else
	List *fdw_private = ((ForeignScan *)node->ss.ps.plan)->fdw_private;
#endif
	char *dbserver, *user, *password, *nls_lang;
	struct OracleFdwExecutionState *fdw_state;

	/* deserialize private plan data */
	deserializePlanData(fdw_private, &dbserver, &user, &password, &nls_lang, (struct OracleFdwExecutionState **)&node->fdw_state);
	fdw_state = (struct OracleFdwExecutionState *)node->fdw_state;

	elog(DEBUG1, "oracle_fdw: begin foreign table scan on %d", RelationGetRelid(node->ss.ss_currentRelation));

	/* connect to Oracle database, don't start transaction for explain only */
	fdw_state->session = oracleGetSession(dbserver, user, password, nls_lang, fdw_state->oraTable->pgname,
			(eflags & EXEC_FLAG_EXPLAIN_ONLY) ? 0 : 1);
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
	EState *execstate = node->ss.ps.state;
	int have_result;
	struct OracleFdwExecutionState *fdw_state = (struct OracleFdwExecutionState *)node->fdw_state;
	int j, index;
	char *value = NULL;
	long value_len = 0;

	if (oracleIsStatementOpen(fdw_state->session))
	{
		elog(DEBUG3, "oracle_fdw: get next row in foreign table scan on %d", RelationGetRelid(node->ss.ss_currentRelation));

		/* fetch the next result row */
		have_result = oracleFetchNext(fdw_state->session, fdw_state->oraTable);
	}
	else
	{
		/* fill the parameter list with the actual values in execstate */
		char *paramInfo = setParameters(fdw_state->paramList, execstate);

		/* execute the Oracle statement and fetch the first row */
		elog(DEBUG1, "oracle_fdw: execute query in foreign table scan on %d%s", RelationGetRelid(node->ss.ss_currentRelation), paramInfo);
		have_result = oracleExecuteQuery(fdw_state->session, fdw_state->query, fdw_state->oraTable, fdw_state->paramList);
	}

	/* we return a virtual tuple */
	slot->tts_mintuple = NULL;
	slot->tts_shouldFreeMin = false;
	slot->tts_buffer = InvalidBuffer;
	slot->tts_tuple = NULL;
	slot->tts_shouldFree = false;

	if (have_result)
	{
		slot->tts_isempty = false;
		slot->tts_nvalid = fdw_state->oraTable->npgcols;
		slot->tts_values = (Datum *)palloc(sizeof(Datum) * slot->tts_nvalid);
		slot->tts_isnull = (bool *)palloc(sizeof(bool) * slot->tts_nvalid);

		/* assign result values */
		index = -1;
		for (j=0; j<fdw_state->oraTable->npgcols; ++j)
		{
			/* for dropped columns, insert a NULL */
			if (fdw_state->oraTable->cols[index + 1]->pgattnum > j + 1)
			{
				slot->tts_isnull[j] = true;
				slot->tts_values[j] = PointerGetDatum(NULL);
				continue;
			}
			else
				++index;

			/*
		 	* Columns exceeding the length of the Oracle table will be NULL,
		 	* as well as columns that are not used in the query.
		 	*/
			if (index >= fdw_state->oraTable->ncols
					|| fdw_state->oraTable->cols[index]->used == 0
					|| fdw_state->oraTable->cols[index]->val_null == -1)
			{
				slot->tts_isnull[j] = true;
				slot->tts_values[j] = PointerGetDatum(NULL);
				continue;
			}

			slot->tts_isnull[j] = false;

			/* get the data and its length */
			if (fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_BLOB
					|| fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_BFILE
					|| fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_CLOB)
			{
				/* get the actual LOB contents (palloc'ed) */
				oracleGetLob(fdw_state->session, fdw_state->oraTable, (void *)fdw_state->oraTable->cols[index]->val, fdw_state->oraTable->cols[index]->oratype, &value, &value_len);
			}
			else
			{
				/* for other data types, oraTable contains the results */
				value = fdw_state->oraTable->cols[index]->val;
				value_len = fdw_state->oraTable->cols[index]->val_len;
			}

			/* fill the TupleSlot with the data (after conversion if necessary) */
			if (fdw_state->oraTable->cols[index]->pgtype == BYTEAOID)
			{
				/* binary columns are not converted */
				bytea *result = (bytea *)palloc(value_len + VARHDRSZ);
				memcpy(VARDATA(result), value, value_len);
				SET_VARSIZE(result, value_len + VARHDRSZ);

				slot->tts_values[j] = PointerGetDatum(result);
			}
			else
			{
				regproc typinput;
				HeapTuple tuple;
				Datum dat;

				/* find the appropriate conversion function */
				tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(fdw_state->oraTable->cols[index]->pgtype));
				if (!HeapTupleIsValid(tuple))
				{
					elog(ERROR, "cache lookup failed for type %u", fdw_state->oraTable->cols[index]->pgtype);
				}
				typinput = ((Form_pg_type)GETSTRUCT(tuple))->typinput;
				ReleaseSysCache(tuple);

				/* and call it */
				dat = CStringGetDatum(value);
				switch (fdw_state->oraTable->cols[index]->pgtype)
				{
					case BPCHAROID:
					case VARCHAROID:
					case TIMESTAMPOID:
					case TIMESTAMPTZOID:
					case INTERVALOID:
					case NUMERICOID:
						/* these functions require the type modifier */
						slot->tts_values[j] = OidFunctionCall3(typinput,
								dat,
								ObjectIdGetDatum(InvalidOid),
								Int32GetDatum(fdw_state->oraTable->cols[index]->pgtypmod));
						break;
					default:
						/* the others don't */
						slot->tts_values[j] = OidFunctionCall1(typinput, dat);
				}
			}

			/* free the data buffer for LOBs */
			if (fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_BLOB
					|| fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_BFILE
					|| fdw_state->oraTable->cols[index]->oratype == ORA_TYPE_CLOB)
				pfree(value);
		}
	}
	else
	{
		/* if there are no more results, return an empty tuple */
		slot->tts_isempty = true;
		slot->tts_nvalid = 0;

		/* close the statement */
		oracleCloseStatement(fdw_state->session, fdw_state->oraTable);
	}

	return slot;
}

/*
 * oracleEndForeignScan
 * 		Release the Oracle session (will be cached).
 */
void
oracleEndForeignScan(ForeignScanState *node)
{
	struct OracleFdwExecutionState *fdw_state = (struct OracleFdwExecutionState *)node->fdw_state;

	elog(DEBUG1, "oracle_fdw: end foreign table scan on %d", RelationGetRelid(node->ss.ss_currentRelation));

	/* release the Oracle session */
	oracleReleaseSession(fdw_state->session, fdw_state->oraTable, 0, 0);
}

/*
 * oracleReScanForeignScan
 * 		Close the Oracle statement if there is any.
 * 		That causes the next oracleIterateForeignScan call to restart the scan.
 */
void
oracleReScanForeignScan(ForeignScanState *node)
{
	struct OracleFdwExecutionState *fdw_state = (struct OracleFdwExecutionState *)node->fdw_state;

	elog(DEBUG1, "oracle_fdw: restart foreign table scan on %d", RelationGetRelid(node->ss.ss_currentRelation));

	/* close open Oracle statement if there is one */
	oracleCloseStatement(fdw_state->session, fdw_state->oraTable);
}

/*
 * oracleGetOptions
 * 		Fetch the options for an oracle_fdw foreign table.
 * 		Returns a union of the options of the foreign data wrapper,
 * 		the foreign server, the user mapping and the foreign table,
 * 		in that order.
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
 * 		Set the oraTable->npgcols.
 */
void
getColumnData(Oid foreigntableid, struct oraTable *oraTable)
{
	HeapTuple tuple;
	Relation att_rel;
	SysScanDesc scan;
	ScanKeyData key[2];
	Form_pg_attribute att_tuple;
	int index = 0;

	/*
	 * Scan pg_attribute for all columns of the foreign table.
	 * This is an index scan over pg_attribute_relid_attnum_index where attnum>0
	 */
	att_rel = heap_open(AttributeRelationId, AccessShareLock);
	ScanKeyInit(&key[0], Anum_pg_attribute_attrelid, BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(foreigntableid));
	ScanKeyInit(&key[1], Anum_pg_attribute_attnum, BTGreaterStrategyNumber, F_INT2GT, Int16GetDatum((int2)0));
	scan = systable_beginscan(att_rel, AttributeRelidNumIndexId, true, SnapshotNow, 2, key);

	/* loop through columns */
	oraTable->npgcols = 0;
	while (HeapTupleIsValid(tuple = systable_getnext(scan)))
	{
		++oraTable->npgcols;

		att_tuple = (Form_pg_attribute)GETSTRUCT(tuple);
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
	}

	/* end scan */
	systable_endscan(scan);
	heap_close(att_rel, AccessShareLock);
}

/*
 * createQuery
 * 		Construct a query string for Oracle that
 * 		a) contains only the necessary columns in the SELECT list
 * 		b) has all the WHERE clauses that can safely be translated to Oracle.
 * 		Untranslatable WHERE clauses are omitted since PostgreSQL will check
 * 		them anyway.
 * 		As a side effect. we also mark the used columns in oraTable.
 */
char
*createQuery(oracleSession *session, List *columnlist, List *conditions, struct oraTable *oraTable, struct paramDesc **paramList)
{
	ListCell *cell;
	bool first_col = true, in_quote = false;
	int i;
	char *where, *wherecopy, *p, md5[33];
	StringInfoData query, result;
	struct paramDesc *param, *prev_param = NULL;

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
		appendStringInfo(&query, "NULL");
	appendStringInfo(&query, " FROM %s", oraTable->name);

	/* append WHERE clauses */
	first_col = true;
	foreach(cell, conditions)
	{
		/* try to convert each condition to Oracle SQL */
		where = getOracleWhereClause(session, ((RestrictInfo *)lfirst(cell))->clause, oraTable, paramList);
		if (where != NULL) {
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
		}
	}

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
	param = *paramList;
	while (param != NULL)
	{
		if (strstr(wherecopy, param->name) == NULL)
		{
			/* remove parameter from linked list */
			if (prev_param == NULL)
				*paramList = param->next;
			else
				prev_param->next = param->next;

			/* free memory */
			pfree(param->name);
			pfree(param);
		}
		else
		{
			prev_param = param;
		}

		if (prev_param == NULL)
			param = *paramList;
		else
			param = prev_param->next;
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
 * 		Create and return an Oracle SQL WHERE clause from the expression.
 * 		If that is not possible, return NULL, else a palloc'ed string.
 * 		As a side effect, all Params incorporated in the WHERE clause
 * 		will be stored in paramList.
 */
char
*getOracleWhereClause(oracleSession *session, Expr *expr, const struct oraTable *oraTable, struct paramDesc **paramList)
{
	char *opername, *left, *right, *arg, oprkind;
	Const *constant;
	OpExpr *oper;
	CaseExpr *caseexpr;
	BoolExpr *boolexpr;
	CoalesceExpr *coalesceexpr;
	CoerceViaIO *coerce;
	Param *param;
	struct paramDesc *paramDesc;
	Var *variable;
	FuncExpr *func;
	regproc typoutput;
	HeapTuple tuple;
	ListCell *cell;
	StringInfoData result;
	Oid leftargtype, rightargtype, schema;
	oraType oratype;
	bool first_arg;
	int index;

	if (expr == NULL)
		return NULL;

	switch(expr->type)
	{
		case T_Const:
			constant = (Const *)expr;
			if (constant->constisnull)
			{
				initStringInfo(&result);
				appendStringInfo(&result, "NULL");
			}
			else
			{
				Datum dat = constant->constvalue;
				char *str, *p;
				struct pg_tm tm;
				fsec_t fsec;

				/* get the type's output function */
				tuple = SearchSysCache1(TYPEOID, ObjectIdGetDatum(constant->consttype));
				if (!HeapTupleIsValid(tuple))
				{
					elog(ERROR, "cache lookup failed for type %u", constant->consttype);
				}
				typoutput = ((Form_pg_type)GETSTRUCT(tuple))->typoutput;
				ReleaseSysCache(tuple);

				/* render the constant in Oracle SQL */
				switch (constant->consttype) {
					case TEXTOID:
					case CHAROID:
					case BPCHAROID:
					case VARCHAROID:
					case NAMEOID:
						str = DatumGetCString(OidFunctionCall1(typoutput, dat));

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
						str = DatumGetCString(OidFunctionCall1(typoutput, dat));
						initStringInfo(&result);
						appendStringInfo(&result, "%s", str);
						break;
					case DATEOID:
						str = DatumGetCString(OidFunctionCall1(typoutput, dat));
						initStringInfo(&result);
						appendStringInfo(&result, "TO_TIMESTAMP('%s', 'YYYY-MM-DD')", str);
						break;
					case TIMESTAMPOID:
						str = DatumGetCString(OidFunctionCall1(typoutput, dat));
						initStringInfo(&result);
						appendStringInfo(&result, "TO_TIMESTAMP('%s', 'YYYY-MM-DD HH24:MI:SS.FF')", str);
						break;
					case TIMESTAMPTZOID:
						str = DatumGetCString(OidFunctionCall1(typoutput, dat));
						initStringInfo(&result);
						appendStringInfo(&result, "TO_TIMESTAMP_TZ('%s', 'YYYY-MM-DD HH24:MI:SS.FFTZH:TZM')", str);
						break;
					case INTERVALOID:
						if (interval2tm(*DatumGetIntervalP(dat), &tm, &fsec) != 0)
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
								return NULL;
							tm.tm_mday = -tm.tm_mday;
							tm.tm_hour = -tm.tm_hour;
							tm.tm_min = -tm.tm_min;
							tm.tm_sec = -tm.tm_sec;
							fsec = -fsec;
						}
						else
							str = "";

						initStringInfo(&result);
						appendStringInfo(&result, "INTERVAL '%s%d %d:%d:%d.%d' DAY(9) TO SECOND(9)", str, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec, fsec);

						break;
					default:
						return NULL;
				}
			}
			break;
		case T_Param:
			param = (Param *)expr;

			/* don't try to handle interval parameters */
			if (! canHandleType(param->paramtype) || param->paramtype == INTERVALOID)
				return NULL;

			/*
			 * external parameters will be called :e1, :e2, etc.
			 * internal parameters will be called :i0, :i1, etc.
			 */
			initStringInfo(&result);
			appendStringInfo(&result,
					":%c%d",
					(param->paramkind == PARAM_EXTERN ? 'e' : 'i'),
					param->paramid);

			/* create a new entry in the parameter list */
			paramDesc = (struct paramDesc *)palloc(sizeof(struct paramDesc));
			paramDesc->name = pstrdup(result.data);
			paramDesc->number = param->paramid;
			paramDesc->type = param->paramtype;

			if (param->paramtype == TEXTOID || param->paramtype == VARCHAROID
					|| param->paramtype == BPCHAROID || param->paramtype == CHAROID)
				paramDesc->bindType = BIND_STRING;
			else if (param->paramtype == DATEOID || param->paramtype == TIMESTAMPOID
					|| param->paramtype == TIMESTAMPTZOID)
				paramDesc->bindType = BIND_TIMESTAMP;
			else
				paramDesc->bindType = BIND_NUMBER;

			paramDesc->value = NULL;
			paramDesc->isExtern = (param->paramkind == PARAM_EXTERN);
			paramDesc->next = *paramList;
			*paramList = paramDesc;

			break;
		case T_Var:
			variable = (Var *)expr;

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
				|| strcmp(opername, "/") == 0
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
				left = getOracleWhereClause(session, linitial(oper->args), oraTable, paramList);
				if (left == NULL)
				{
					pfree(opername);
					return NULL;
				}

				if (oprkind == 'b')
				{
					/* binary operator */
					right = getOracleWhereClause(session, lsecond(oper->args), oraTable, paramList);
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

			left = getOracleWhereClause(session, linitial(((DistinctExpr *)expr)->args), oraTable, paramList);
			if (left == NULL)
			{
				return NULL;
			}
			right = getOracleWhereClause(session, lsecond(((DistinctExpr *)expr)->args), oraTable, paramList);
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

			left = getOracleWhereClause(session, linitial(((NullIfExpr *)expr)->args), oraTable, paramList);
			if (left == NULL)
			{
				return NULL;
			}
			right = getOracleWhereClause(session, lsecond(((NullIfExpr *)expr)->args), oraTable, paramList);
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

			arg = getOracleWhereClause(session, linitial(boolexpr->args), oraTable, paramList);
			if (arg == NULL)
				return NULL;

			initStringInfo(&result);
			appendStringInfo(&result, "(%s%s",
					boolexpr->boolop == NOT_EXPR ? "NOT " : "",
					arg);

			for_each_cell(cell, lnext(boolexpr->args->head))
			{
				arg = getOracleWhereClause(session, (Expr *)lfirst(cell), oraTable, paramList);
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
			return getOracleWhereClause(session, ((RelabelType *)expr)->arg, oraTable, paramList);
			break;
		case T_CoerceToDomain:
			return getOracleWhereClause(session, ((CoerceToDomain *)expr)->arg, oraTable, paramList);
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
				arg = getOracleWhereClause(session, caseexpr->arg, oraTable, paramList);
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
					arg = getOracleWhereClause(session, whenclause->expr, oraTable, paramList);
				}
				else
				{
					/* for CASE arg WHEN ..., use only the right branch of the equality */
					arg = getOracleWhereClause(session, lsecond(((OpExpr *)whenclause->expr)->args), oraTable, paramList);
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
				arg = getOracleWhereClause(session, whenclause->result, oraTable, paramList);
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
				arg = getOracleWhereClause(session, caseexpr->defresult, oraTable, paramList);
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
				arg = getOracleWhereClause(session, (Expr *)lfirst(cell), oraTable, paramList);
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
			arg = getOracleWhereClause(session, ((NullTest *)expr)->arg, oraTable, paramList);
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
				return getOracleWhereClause(session, linitial(func->args), oraTable, paramList);

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
					arg = getOracleWhereClause(session, lfirst(cell), oraTable, paramList);
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
				left = getOracleWhereClause(session, linitial(func->args), oraTable, paramList);
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

					right = getOracleWhereClause(session, lsecond(func->args), oraTable, paramList);
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

				/* create a new entry in the parameter list */
				paramDesc = (struct paramDesc *)palloc(sizeof(struct paramDesc));
				paramDesc->name = pstrdup(":now");
				paramDesc->number = 0;
				paramDesc->type = TIMESTAMPTZOID;
				paramDesc->bindType = BIND_TIMESTAMP;
				paramDesc->value = NULL;
				paramDesc->isExtern = 1;
				paramDesc->next = *paramList;
				*paramList = paramDesc;
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
			if (constant->constisnull || constant->consttype != TEXTOID)
				return NULL;

			/* the value must be "now" */
			if (VARSIZE(constant->constvalue) - VARHDRSZ != 3
					|| strncmp(VARDATA(constant->constvalue), "now", 3) != 0)
				return NULL;

			initStringInfo(&result);
			appendStringInfo(&result, ":now");

			/* create a new entry in the parameter list */
			paramDesc = (struct paramDesc *)palloc(sizeof(struct paramDesc));
			paramDesc->name = pstrdup(":now");
			paramDesc->number = 0;
			paramDesc->type = coerce->resulttype;
			paramDesc->bindType = BIND_TIMESTAMP;
			paramDesc->value = NULL;
			paramDesc->isExtern = 1;
			paramDesc->next = *paramList;
			*paramList = paramDesc;

			break;
		default:
			/* we cannot translate this to Oracle */
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
 * checkDataTypes
 * 		Check that the Oracle data types of all used columns can be
 * 		converted to the PostgreSQL data types, raise an error if not.
 */
void
checkDataTypes(oracleSession *session, struct oraTable *oraTable)
{
	int i;

	/* loop through columns and compare type */
	for (i=0; i<oraTable->ncols; ++i)
	{
		oraType oratype = oraTable->cols[i]->oratype;
		Oid pgtype = oraTable->cols[i]->pgtype;

		if (! oraTable->cols[i]->used)
			continue;

		/* the binary Oracle types can be converted to bytea */
		if ((oratype == ORA_TYPE_RAW
				|| oratype == ORA_TYPE_BLOB
				|| oratype == ORA_TYPE_BFILE)
				&& pgtype == BYTEAOID)
			continue;

		/* Oracle RAW can be converted to uuid */
		if (oratype == ORA_TYPE_RAW && pgtype == UUIDOID)
			continue;

		/* all other Oracle types can be transformed to strings */
		if (oratype != ORA_TYPE_OTHER
				&& (pgtype == TEXTOID || pgtype == VARCHAROID || pgtype == BPCHAROID))
			continue;

		/* all numeric Oracle types can be transformed to floating point types */
		if ((oratype == ORA_TYPE_NUMBER
				|| oratype == ORA_TYPE_FLOAT
				|| oratype == ORA_TYPE_BINARYFLOAT
				|| oratype == ORA_TYPE_BINARYDOUBLE)
				&& (pgtype == NUMERICOID
				|| pgtype == FLOAT4OID
				|| pgtype == FLOAT8OID))
			continue;

		/*
		 * NUMBER columns without decimal fractions can be transformed to
		 * integers or booleans
		 */
		if (oratype == ORA_TYPE_NUMBER && oraTable->cols[i]->scale <= 0
				&& (pgtype == INT2OID
				|| pgtype == INT4OID
				|| pgtype == INT8OID
				|| pgtype == BOOLOID))
			continue;

		/* DATE and timestamps can be transformed to each other */
		if ((oratype == ORA_TYPE_DATE
				|| oratype == ORA_TYPE_TIMESTAMP
				|| oratype == ORA_TYPE_TIMESTAMPTZ)
				&& (pgtype == DATEOID
				|| pgtype == TIMESTAMPOID
				|| pgtype == TIMESTAMPTZOID))
			continue;

		/* interval types can be transformed to interval */
		if ((oratype == ORA_TYPE_INTERVALY2M
				|| oratype == ORA_TYPE_INTERVALD2S)
				&& pgtype == INTERVALOID)
			continue;

		/* otherwise, report an error */
		ereport(ERROR,
				(errcode(ERRCODE_FDW_INVALID_DATA_TYPE),
				errmsg("column \"%s\" of foreign table \"%s\" cannot be converted to from Oracle data type", oraTable->cols[i]->pgname, oraTable->pgname)));
	}
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
*serializePlanData(const char *dbserver, const char *user, const char *password, const char *nls_lang, const char *query, const struct oraTable *oraTable, const struct paramDesc *paramList)
{
	List *result = NIL;
	int i, len = 0;
	const struct paramDesc *param;

	/* dbserver */
	result = lappend(result, serializeString(dbserver));
	/* user name */
	result = lappend(result, serializeString(user));
	/* password */
	result = lappend(result, serializeString(password));
	/* nls_lang */
	result = lappend(result, serializeString(nls_lang));
	/* query */
	result = lappend(result, serializeString(query));
	/* Oracle table data */
	result = lappend(result, serializeString(oraTable->name));
	/* PostgreSQL table name */
	result = lappend(result, serializeString(oraTable->pgname));
	/* number of columns in Oracle table */
	result = lappend(result, serializeInt(oraTable->ncols));
	/* number of columns in PostgreSQL table */
	result = lappend(result, serializeInt(oraTable->npgcols));
	/* column data */
	for (i=0; i<oraTable->ncols; ++i)
	{
		result = lappend(result, serializeString(oraTable->cols[i]->name));
		result = lappend(result, serializeInt(oraTable->cols[i]->oratype));
		result = lappend(result, serializeInt(oraTable->cols[i]->scale));
		result = lappend(result, serializeString(oraTable->cols[i]->pgname));
		result = lappend(result, serializeInt(oraTable->cols[i]->pgattnum));
		result = lappend(result, serializeOid(oraTable->cols[i]->pgtype));
		result = lappend(result, serializeInt(oraTable->cols[i]->pgtypmod));
		result = lappend(result, serializeInt(oraTable->cols[i]->used));
		result = lappend(result, serializeLong(oraTable->cols[i]->val_size));
		/* don't serialize val, val_len and val_null */
	}
	/* find length of parameter list */
	for (param=paramList; param; param=param->next)
		++len;
	/* serialize length */
	result = lappend(result, serializeInt(len));
	/* parameter list entries */
	for (param=paramList; param; param=param->next)
	{
		result = lappend(result, serializeString(param->name));
		result = lappend(result, serializeInt(param->number));
		result = lappend(result, serializeOid(param->type));
		result = lappend(result, serializeInt((int)param->bindType));
		/* don't serialize value */
		result = lappend(result, serializeInt(param->isExtern));
	}

	return result;
}

/*
 * serializeString
 * 		Create a Const that contains the string.
 */

Const
*serializeString(const char *s)
{
	text *datum;
	size_t length;

	if (s == NULL)
		return makeNullConst(TEXTOID, -1, InvalidOid);

	length = strlen(s);
	datum = (text *)palloc(length + VARHDRSZ);
	memcpy(VARDATA(datum), s, length);
	SET_VARSIZE(datum, length + VARHDRSZ);

	return makeConst(TEXTOID, -1, InvalidOid, -1, PointerGetDatum(datum), 0, 0);
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
#endif
				0);
}

/*
 * deserializePlanData
 * 		Extract the data structures from a List created by serializePlanData.
 */

void
deserializePlanData(List *list, char **dbserver, char **user, char **password, char **nls_lang, struct OracleFdwExecutionState **state)
{
	ListCell *cell = list_head(list);
	int i, len;
	struct paramDesc *param;

	/* fdw_state */
	*state = (struct OracleFdwExecutionState *)palloc(sizeof(struct OracleFdwExecutionState));
	(*state)->session = NULL;

	/* dbserver */
	*dbserver = deserializeString(lfirst(cell));
	cell = lnext(cell);

	/* user */
	*user = deserializeString(lfirst(cell));
	cell = lnext(cell);

	/* password */
	*password = deserializeString(lfirst(cell));
	cell = lnext(cell);

	/* nls_lang */
	*nls_lang = deserializeString(lfirst(cell));
	cell = lnext(cell);

	/* query */
	(*state)->query = deserializeString(lfirst(cell));
	cell = lnext(cell);

	/* table data */
	(*state)->oraTable = (struct oraTable *)palloc(sizeof(struct oraTable));
	(*state)->oraTable->name = deserializeString(lfirst(cell));
	cell = lnext(cell);
	(*state)->oraTable->pgname = deserializeString(lfirst(cell));
	cell = lnext(cell);
	(*state)->oraTable->ncols = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = lnext(cell);
	(*state)->oraTable->npgcols = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = lnext(cell);
	(*state)->oraTable->cols = (struct oraColumn **)palloc(sizeof(struct oraColumn *) * (*state)->oraTable->ncols);

	/* loop columns */
	for (i=0; i<(*state)->oraTable->ncols; ++i)
	{
		(*state)->oraTable->cols[i] = (struct oraColumn *)palloc(sizeof(struct oraColumn));
		(*state)->oraTable->cols[i]->name = deserializeString(lfirst(cell));
		cell = lnext(cell);
		(*state)->oraTable->cols[i]->oratype = (oraType)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = lnext(cell);
		(*state)->oraTable->cols[i]->scale = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = lnext(cell);
		(*state)->oraTable->cols[i]->pgname = deserializeString(lfirst(cell));
		cell = lnext(cell);
		(*state)->oraTable->cols[i]->pgattnum = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = lnext(cell);
		(*state)->oraTable->cols[i]->pgtype = DatumGetObjectId(((Const *)lfirst(cell))->constvalue);
		cell = lnext(cell);
		(*state)->oraTable->cols[i]->pgtypmod = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = lnext(cell);
		(*state)->oraTable->cols[i]->used = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = lnext(cell);
		(*state)->oraTable->cols[i]->val_size = deserializeLong(lfirst(cell));
		cell = lnext(cell);
		/* allocate memory for the result value */
		(*state)->oraTable->cols[i]->val = (char *)palloc((*state)->oraTable->cols[i]->val_size);
		(*state)->oraTable->cols[i]->val_len = 0;
		(*state)->oraTable->cols[i]->val_null = 1;
	}

	/* length of parameter list */
	len = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
	cell = lnext(cell);

	/* parameter table entries */
	(*state)->paramList = NULL;
	for (i=0; i<len; ++i)
	{
		param = (struct paramDesc *)palloc(sizeof(struct paramDesc));
		param->name = deserializeString(lfirst(cell));
		cell = lnext(cell);
		param->number = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = lnext(cell);
		param->type = DatumGetObjectId(((Const *)lfirst(cell))->constvalue);
		cell = lnext(cell);
		param->bindType = (oraBindType)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = lnext(cell);
		param->value = NULL;
		param->isExtern = (int)DatumGetInt32(((Const *)lfirst(cell))->constvalue);
		cell = lnext(cell);
		param->next = (*state)->paramList;
		(*state)->paramList = param;
	}
}

/*
 * deserializeString
 * 		Extracts a string from a Const, returns a palloc'ed copy.
 */

char
*deserializeString(Const *constant)
{
	char *result;
	Datum datum = constant->constvalue;

	if (constant->constisnull)
		return NULL;

	result = palloc(VARSIZE(datum) - VARHDRSZ + 1);
	memcpy(result, VARDATA(datum), VARSIZE(datum) - VARHDRSZ);
	result[VARSIZE(datum) - VARHDRSZ] = '\0';

	return result;
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
 * cleanupTransaction
 * 		If the transaction was rolled back, close remote transaction.
 */
void
cleanupTransaction(ResourceReleasePhase phase, bool isCommit, bool isTopLevel, void *arg)
{
	if (! isCommit && isTopLevel && phase == RESOURCE_RELEASE_AFTER_LOCKS
			&& CurrentResourceOwner == CurTransactionResourceOwner)
		oracleCleanupTransaction(arg);
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
 * setParameters
 * 		Set the current values of the parameters in execstate into paramList.
 * 		Return a string containing the parameters set for a DEBUG message.
 */
char *
setParameters(struct paramDesc *paramList, EState *execstate)
{
	struct paramDesc *param;
	Datum datum;
	HeapTuple tuple;
	TimestampTz tstamp;
	int is_null;
	bool first_param = true;
	StringInfoData info;  /* list of parameters for DEBUG message */
	initStringInfo(&info);

	/*
 	* There is no open statement yet.
 	* Run the query and get the first result row.
 	*/

	/* iterate parameter list and fill values */
	for (param=paramList; param; param=param->next)
	{
		if (strcmp(param->name, ":now") == 0)
		{
			/*
		 	* This parameter will be set to the transaction start timestamp.
		 	*/
			regproc castfunc;

			/* get transaction start timestamp */
			tstamp = GetCurrentTransactionStartTimestamp();

			if (param->type == TIMESTAMPTZOID)
			{
				/* no need to cast */
				datum = TimestampGetDatum(tstamp);
			}
			else
			{
				/* find the cast function to the desired target type */
				tuple = SearchSysCache2(CASTSOURCETARGET, ObjectIdGetDatum(TIMESTAMPTZOID), ObjectIdGetDatum(param->type));
				if (!HeapTupleIsValid(tuple))
				{
					elog(ERROR, "cache lookup failed for cast from %u to %u", TIMESTAMPTZOID, param->type);
				}
				castfunc = ((Form_pg_cast)GETSTRUCT(tuple))->castfunc;
				ReleaseSysCache(tuple);

				/* cast */
				datum = OidFunctionCall1(castfunc, TimestampGetDatum(tstamp));
			}
			is_null = 0;
		}
		else if (param->isExtern)
		{
			/* external parameters are numbered from 1 on */
			datum = execstate->es_param_list_info->params[param->number-1].value;
			is_null = execstate->es_param_list_info->params[param->number-1].isnull;
		}
		else
		{
			/* internal parameters are numbered from 0 on */
			datum = execstate->es_param_exec_vals[param->number].value;
			is_null = execstate->es_param_exec_vals[param->number].isnull;
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

	return info.data;
}

/*
 * oracleRegisterCallback
 * 		Register a callback for rolled back PostgreSQL transactions.
 */
void
oracleRegisterCallback(void *arg)
{
	RegisterResourceReleaseCallback(cleanupTransaction, arg);
}

/*
 * oracleUnregisterCallback
 * 		Unregister a callback for rolled back PostgreSQL transactions.
 */
void
oracleUnregisterCallback(void *arg)
{
	UnregisterResourceReleaseCallback(cleanupTransaction, arg);
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
	(x==FDW_OUT_OF_MEMORY ? ERRCODE_FDW_OUT_OF_MEMORY : ERRCODE_FDW_ERROR)))))

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
