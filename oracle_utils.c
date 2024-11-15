/*-------------------------------------------------------------------------
 *
 * oracle_utils.c
 * 		routines that use OCI (Oracle's C API)
 *
 *-------------------------------------------------------------------------
 */

#include <string.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdio.h>
#if defined _WIN32 || defined _WIN64
/* for getpid */
#include <process.h>
/* Windows doesn't have snprintf */
#define snprintf _snprintf
#endif

/* Oracle header */
#include <oci.h>

#include "oracle_fdw.h"

/* number of bytes to read per LOB chunk */
#define LOB_CHUNK_SIZE 65536

/* emit no error messages when set, used for shutdown */
static int silent = 0;

/* contains Oracle error messages, set by checkerr() */
#define ERRBUFSIZE 500
static char oraMessage[ERRBUFSIZE];
static sb4 err_code;

/* set to "1" as soon as OCIEnvCreate is called */
static int oci_initialized = 0;

/* is the current transaction read-only? */
static int readonly = 0;

/*
 * Linked list of open Oracle statement handles.
 * Each of these has a linked list of LOB locators.
 */

struct lobLocatorEntry
{
	OCILobLocator *lobloc;
	struct lobLocatorEntry *next;
};

struct stmtHandleEntry
{
	OCIStmt *stmthp;
	struct lobLocatorEntry *loclist;
	struct stmtHandleEntry *next;
};

/*
 * Linked list of handles for cached Oracle connections.
 */
static struct envEntry *envlist = NULL;

/*
 * NULL value used for "in" callback in RETURNING clauses.
 */
static ora_geometry null_geometry = { NULL, NULL, -1, NULL, -1, NULL };

/*
 * Helper functions
 */
static void getServerVersion(struct srvEntry *srvp, OCIError *errhp);
static void oracleSetSavepoint(oracleSession *session, int nest_level);
static void setOracleEnvironment(char *nls_lang, char *timezone);
static OCIStmt *oracleQueryPlan(oracleSession *session, const char *query, const char *desc_query, int nres, dvoid **res, sb4 *res_size, ub2 *res_type, ub2 *res_len, sb2 *res_ind);
static sword checkerr(sword status, dvoid *handle, ub4 handleType);
static char *copyOraText(const char *string, int size, int quote);
static void closeSession(OCIEnv *envhp, OCIServer *srvhp, OCISession *userhp, int disconnect);
static void disconnectServer(OCIEnv *envhp, OCIServer *srvhp);
static void removeEnvironment(OCIEnv *envhp);
static void registerStmt(OCIStmt *stmthp, OCIEnv *envhp, struct connEntry *connp);
static void allocLobLocator(OCILobLocator **locpp, OCIStmt *stmthp, OCIEnv *envhp, struct connEntry *connp, oraError error, const char *errmsg);
static void freeStmt(OCIStmt *stmthp, struct connEntry *connp, OCIError *errhp);
static ub2 getOraType(oraType arg);
static sb4 bind_out_callback(void *octxp, OCIBind *bindp, ub4 iter, ub4 index, void **bufpp, ub4 **alenp, ub1 *piecep, void **indp, ub2 **rcodep);
static sb4 bind_in_callback(void *ictxp, OCIBind *bindp, ub4 iter, ub4 index, void **bufpp, ub4 *alenp, ub1 *piecep, void **indpp);
static void setNullGeometry(oracleSession *session, ora_geometry *geom);

/*
 * oracleGetSession
 * 		Look up an Oracle connection in the cache, create a new one if there is none.
 * 		The result is a palloc'ed data structure containing the connection.
 * 		"curlevel" is the current PostgreSQL transaction level.
 */
oracleSession
*oracleGetSession(
	const char *connectstring, oraIsoLevel isolation_level, char *user, char *password,
	const char *nls_lang, const char *timezone, int have_nchar, const char *tablename, int curlevel)
{
	OCIEnv *envhp = NULL;
	OCIError *errhp = NULL;
	OCISvcCtx *svchp = NULL;
	OCIServer *srvhp = NULL;
	OCISession *userhp = NULL;
	OCITrans *txnhp = NULL;
	oracleSession *session;
	struct envEntry *envp;
	struct srvEntry *srvp;
	struct connEntry *connp;
	char pid[30], *nlscopy = NULL, *timezonecopy = NULL;
	ub4 is_connected;
	int retry = 1, i;
	ub4 isolevel = OCI_TRANS_SERIALIZABLE;

	/* convert isolation_level to Oracle OCI value */
	switch(isolation_level)
	{
		case ORA_TRANS_SERIALIZABLE:
			isolevel = OCI_TRANS_SERIALIZABLE;
			break;
		case ORA_TRANS_READ_COMMITTED:
			isolevel = OCI_TRANS_NEW;
			break;
		case ORA_TRANS_READ_ONLY:
			isolevel = OCI_TRANS_READONLY;
			break;
	}

	/* it's easier to deal with empty strings */
	if (!connectstring)
		connectstring = "";
	if (!user)
		user = "";
	if (!password)
		password = "";
	if (!nls_lang)
		nls_lang = "";
	if (!timezone)
		timezone = "";

	/*
	 * Check if PostGIS is installed and initialize GEOMETRYOID if it is.
	 * We have to initialize that here rather than in _PG_init()
	 * because we need to look up system catalogs.
	 */
	initializePostGIS();

	/* search environment and server handle in cache */
	for (envp = envlist; envp != NULL; envp = envp->next)
	{
		if (strcmp(envp->nls_lang, nls_lang) == 0)
		{
			envhp = envp->envhp;
			errhp = envp->errhp;
			break;
		}
	}

	if (envhp == NULL)
	{
		ub4 handle_mode;
		/*
		 * Create environment and error handle.
		 */

		/* create persistent copy of "nls_lang" and "timezone" */
		if ((nlscopy = strdup(nls_lang)) == NULL)
			oracleError_i(FDW_OUT_OF_MEMORY,
				"error connecting to Oracle: failed to allocate %d bytes of memory",
				strlen(nls_lang) + 1);
		if ((timezonecopy = strdup(timezone)) == NULL)
			oracleError_i(FDW_OUT_OF_MEMORY,
				"error connecting to Oracle: failed to allocate %d bytes of memory",
				strlen(timezone) + 1);

		/* set Oracle environment */
		setOracleEnvironment(nlscopy, timezonecopy);

		/* use OCI_NCHAR_LITERAL_REPLACE_ON only if we have to convert national characters */
		if (have_nchar)
			handle_mode = OCI_OBJECT | OCI_NCHAR_LITERAL_REPLACE_ON;
		else
			handle_mode = OCI_OBJECT;

		/* create environment handle */
		if (checkerr(
			OCIEnvCreate((OCIEnv **) &envhp, handle_mode,
				(dvoid *) 0, (dvoid * (*)(dvoid *,size_t)) 0,
				(dvoid * (*)(dvoid *, dvoid *, size_t)) 0,
				(void (*)(dvoid *, dvoid *)) 0, (size_t) 0, (dvoid **) 0),
			(dvoid *)envhp, OCI_HTYPE_ENV) != 0)
		{
			free(nlscopy);
			oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
				"error connecting to Oracle: OCIEnvCreate failed to create environment handle",
				oraMessage);
		}

		/* we can call OCITerminate now */
		oci_initialized = 1;

		/*
		 * Oracle overwrites PostgreSQL's signal handlers, so we have to restore them.
		 * Oracle's SIGINT handler is ok (it cancels the query), but we must do something
		 * reasonable for SIGTERM.
		 */
		oracleSetHandlers();

		/* allocate error handle */
		if (checkerr(
			OCIHandleAlloc((dvoid *) envhp, (dvoid **) &errhp,
				(ub4) OCI_HTYPE_ERROR,
				(size_t) 0, (dvoid **) 0),
			(dvoid *)envhp, OCI_HTYPE_ENV) != OCI_SUCCESS)
		{
			free(nlscopy);
			oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
				"error connecting to Oracle: OCIHandleAlloc failed to allocate error handle",
				oraMessage);
		}

		/* add handles to cache */
		if ((envp = malloc(sizeof(struct envEntry))) == NULL)
		{
			oracleError_i(FDW_OUT_OF_MEMORY,
				"error connecting to Oracle: failed to allocate %d bytes of memory",
				sizeof(struct envEntry));
		}

		envp->nls_lang = nlscopy;
		envp->timezone = timezonecopy;
		envp->envhp = envhp;
		envp->errhp = errhp;
		envp->srvlist = NULL;
		envp->next = envlist;
		envlist = envp;
	}

	/* search connect string in cache */
	for (srvp = envp->srvlist; srvp != NULL; srvp = srvp->next)
	{
		if (strcmp(srvp->connectstring, connectstring) == 0)
		{
			srvhp = srvp->srvhp;
			break;
		}
	}

	if (srvp != NULL)
	{
		/*
		 * Test if we are still connected.
		 * If not, clean up the mess.
		 */

		if (checkerr(
			OCIAttrGet((dvoid *)srvhp, (ub4)OCI_HTYPE_SERVER,
				(dvoid *)&is_connected, (ub4 *)0, (ub4)OCI_ATTR_SERVER_STATUS, errhp),
			(dvoid *)errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_REPLY,
				"error connecting to Oracle: OCIAttrGet failed to get connection status",
				oraMessage);
		}

		if (is_connected == OCI_SERVER_NOT_CONNECTED)
		{
			/* clean up */
			silent = 1;
			while (srvp->connlist != NULL)
				closeSession(envhp, srvhp, srvp->connlist->userhp, 0);
			disconnectServer(envhp, srvhp);
			silent = 0;

			srvp = NULL;
		}
	}

	retry_connect:
	if (srvp == NULL)
	{
		/*
		 * No cache entry was found, we have to create a new server connection.
		 */

		/* create new server handle */
		if (checkerr(
			OCIHandleAlloc((dvoid *) envhp, (dvoid **) &srvhp,
				(ub4) OCI_HTYPE_SERVER,
				(size_t) 0, (dvoid **) 0),
			(dvoid *)envhp, OCI_HTYPE_ENV) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
				"error connecting to Oracle: OCIHandleAlloc failed to allocate server handle",
				oraMessage);
		}

		/* connect to the Oracle server */
		if (checkerr(
			OCIServerAttach(srvhp, errhp, (text *)connectstring, strlen(connectstring), 0),
			(dvoid *)errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			if (tablename)
				oracleError_sd(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
					"connection for foreign table \"%s\" cannot be established", tablename,
					oraMessage);
			else
				oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
					"cannot connect to foreign Oracle server",
					oraMessage);
		}

		/* add server handle to cache */
		if ((srvp = malloc(sizeof(struct srvEntry))) == NULL)
		{
			oracleError_i(FDW_OUT_OF_MEMORY,
				"error connecting to Oracle: failed to allocate %d bytes of memory",
				sizeof(struct srvEntry));
		}
		if ((srvp->connectstring = strdup(connectstring)) == NULL)
		{
			oracleError_i(FDW_OUT_OF_MEMORY,
				"error connecting to Oracle: failed to allocate %d bytes of memory",
				strlen(connectstring) + 1);
		}
		srvp->srvhp = srvhp;
		srvp->next = envp->srvlist;
		srvp->connlist = NULL;
		envp->srvlist = srvp;
	}

	/* search user session for this server in cache */
	for (connp = srvp->connlist; connp != NULL; connp = connp->next)
	{
		if (strcmp(connp->user, user) == 0)
		{
			svchp = connp->svchp;
			userhp = connp->userhp;
			break;
		}
	}

	if (userhp == NULL)
	{
		/*
		 * If no cached user session was found, authenticate.
		 */

		/* allocate service handle */
		if (checkerr(
			OCIHandleAlloc((dvoid *) envhp, (dvoid **) &svchp,
				(ub4) OCI_HTYPE_SVCCTX,
				(size_t) 0, (dvoid **) 0),
			(dvoid *)envhp, OCI_HTYPE_ENV) != OCI_SUCCESS)
		{
			free(nlscopy);
			oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
				"error connecting to Oracle: OCIHandleAlloc failed to allocate service handle",
				oraMessage);
		}

		/* set server handle in service handle */
		if (checkerr(
			OCIAttrSet(svchp, OCI_HTYPE_SVCCTX, srvhp, 0,
				OCI_ATTR_SERVER, errhp),
			(dvoid *)errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
				"error connecting to Oracle: OCIAttrSet failed to set server handle in service handle",
				oraMessage);
		}

		/* create transaction handle */
		if (checkerr(
			OCIHandleAlloc((dvoid *) envhp, (dvoid **) &txnhp,
				(ub4) OCI_HTYPE_TRANS,
				(size_t) 0, (dvoid **) 0),
			(dvoid *)envhp, OCI_HTYPE_ENV) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
				"error connecting to Oracle: OCIHandleAlloc failed to allocate transaction handle",
				oraMessage);
		}

		/* set transaction handle in service handle */
		if (checkerr(
			OCIAttrSet(svchp, OCI_HTYPE_SVCCTX, txnhp, 0,
				OCI_ATTR_TRANS, errhp),
			(dvoid *)errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
				"error connecting to Oracle: OCIAttrSet failed to set transaction handle in service handle",
				oraMessage);
		}

		/* create session handle */
		if (checkerr(
			OCIHandleAlloc((dvoid *) envhp, (dvoid **) &userhp,
				(ub4) OCI_HTYPE_SESSION,
				(size_t) 0, (dvoid **) 0),
			(dvoid *)envhp, OCI_HTYPE_ENV) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
				"error connecting to Oracle: OCIHandleAlloc failed to allocate session handle",
				oraMessage);
		}

		/* set MODULE for the Oracle session */
		sprintf(pid, "%lu", (unsigned long)getpid());
		pid[29] = '\0';

		if (checkerr(
			OCIAttrSet(userhp, OCI_HTYPE_SESSION, "postgres", (ub4)8,
				OCI_ATTR_MODULE, errhp),
			(dvoid *)errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
				"error connecting to Oracle: OCIAttrSet failed to set module in session handle",
				oraMessage);
		}

		/* set ACTION for the Oracle session */
		if (checkerr(
			OCIAttrSet(userhp, OCI_HTYPE_SESSION, pid, (ub4)strlen(pid),
				OCI_ATTR_ACTION, errhp),
			(dvoid *)errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
				"error connecting to Oracle: OCIAttrSet failed to set action in session handle",
				oraMessage);
		}

		/* set driver name for the Oracle session */
		if (checkerr(
			OCIAttrSet(userhp, OCI_HTYPE_SESSION, "oracle_fdw", (ub4)10,
				OCI_ATTR_DRIVER_NAME, errhp),
			(dvoid *)errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
				"error connecting to Oracle: OCIAttrSet failed to set driver name in session handle",
				oraMessage);
		}

		/* set user name */
		if (checkerr(
			OCIAttrSet(userhp, OCI_HTYPE_SESSION, user, strlen(user),
				OCI_ATTR_USERNAME, errhp),
			(dvoid *)errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
				"error connecting to Oracle: OCIAttrSet failed to set user name in session handle",
				oraMessage);
		}

		/* set password */
		if (checkerr(
			OCIAttrSet(userhp, OCI_HTYPE_SESSION, password, strlen(password),
				OCI_ATTR_PASSWORD, errhp),
			(dvoid *)errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
				"error connecting to Oracle: OCIAttrSet failed to set password in session handle",
				oraMessage);
		}

		/* authenticate; use external authentication if no username has been supplied */
		if (checkerr(
			OCISessionBegin(svchp, errhp, userhp,
				(user[0] == '\0' ? OCI_CRED_EXT : OCI_CRED_RDBMS), OCI_DEFAULT),
			(dvoid *)errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			if (tablename)
				oracleError_sd(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
					"connection for foreign table \"%s\" cannot be authenticated", tablename,
					oraMessage);
			else
				oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
					"cannot authenticate connection to foreign Oracle server",
					oraMessage);
		}

		/* set session handle in service handle */
		if (checkerr(
			OCIAttrSet(svchp, OCI_HTYPE_SVCCTX, userhp, 0,
				OCI_ATTR_SESSION, errhp),
			(dvoid *)errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
				"error connecting to Oracle: OCIAttrSet failed to set session handle in service handle",
				oraMessage);
		}

		/* store the server version in the service handle cache */
		getServerVersion(srvp, errhp);

		/* add session handle to cache */
		if ((connp = malloc(sizeof(struct connEntry))) == NULL)
		{
			oracleError_i(FDW_OUT_OF_MEMORY,
				"error connecting to Oracle: failed to allocate %d bytes of memory",
				sizeof(struct connEntry));
		}
		if ((connp->user = strdup(user)) == NULL)
		{
			oracleError_i(FDW_OUT_OF_MEMORY,
				"error connecting to Oracle: failed to allocate %d bytes of memory",
				strlen(user) + 1);
		}
		connp->svchp = svchp;
		connp->userhp = userhp;
		connp->geomtype = NULL;
		connp->stmtlist = NULL;
		connp->xact_level = 0;
		connp->next = srvp->connlist;
		srvp->connlist = connp;

		/* register callback for PostgreSQL transaction events */
		oracleRegisterCallback(connp);
	}

	if (connp->xact_level <= 0)
	{
		oracleDebug2("oracle_fdw: begin remote transaction");

		/* start a transaction */
		if (checkerr(
			OCITransStart(svchp, errhp, (uword)0, isolevel),
			(dvoid *)errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			/*
			 * Certain Oracle errors mean that the session or the server connection
			 * got terminated.  Retry once in that case.
			 */
			if (retry && (err_code == 1012 || err_code == 28 || err_code == 3113 || err_code == 3135))
			{
				oracleDebug2("oracle_fdw: session has been terminated, try to reconnect");

				silent = 1;
				while (srvp->connlist != NULL)
					closeSession(envhp, srvhp, srvp->connlist->userhp, 0);
				disconnectServer(envhp, srvhp);
				silent = 0;
				srvp = NULL;
				userhp = NULL;

				retry = 0;
				goto retry_connect;
			}
			else
				oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
					"error connecting to Oracle: OCITransStart failed to start a transaction",
					oraMessage);
		}

		connp->xact_level = 1;

		readonly = (isolation_level == ORA_TRANS_READ_ONLY);
	}

	/* palloc a data structure pointing to the cached entries */
	session = oracleAlloc(sizeof(struct oracleSession));
	session->envp = envp;
	session->srvp = srvp;
	session->connp = connp;
	session->stmthp = NULL;
	session->have_nchar = have_nchar;
	for (i=0; i<5; ++i)
		session->server_version[i] = srvp->server_version[i];
	session->last_batch = 0;
	session->fetched_rows = 0;
	session->current_row = 0;

	/* set savepoints up to the current level */
	oracleSetSavepoint(session, curlevel);

	return session;
}

/*
 * oracleCloseStatement
 * 		Close the current statement associated with the session.
 */
void
oracleCloseStatement(oracleSession *session)
{
	/* free statement handle, if it exists */
	if (session->stmthp != NULL)
	{
		freeStmt(session->stmthp, session->connp, session->envp->errhp);
		session->stmthp = NULL;
	}
}

/*
 * oracleCloseConnections
 * 		Close everything in the cache.
 */
void
oracleCloseConnections(void)
{
	while (envlist != NULL)
	{
		while (envlist->srvlist != NULL)
		{
			while (envlist->srvlist->connlist != NULL)
				closeSession(envlist->envhp, envlist->srvlist->srvhp, envlist->srvlist->connlist->userhp, 0);
			disconnectServer(envlist->envhp, envlist->srvlist->srvhp);
		}
		removeEnvironment(envlist->envhp);
	}
}

/*
 * oracleShutdown
 * 		Close all open connections, free handles, terminate Oracle.
 * 		This will be called at the end of the PostgreSQL session.
 */
void
oracleShutdown(void)
{
	/* don't report error messages */
	silent = 1;

	oracleCloseConnections();

	/* done with Oracle */
	if (oci_initialized)
		(void)OCITerminate(OCI_DEFAULT);
}

/*
 * oracleCancel
 * 		Cancel all running Oracle queries.
 */
void
oracleCancel(void)
{
	struct envEntry *envp;
	struct srvEntry *srvp;

	/* send a cancel request for all servers ignoring errors */
	for (envp = envlist; envp != NULL; envp = envp->next)
		for (srvp = envp->srvlist; srvp != NULL; srvp = srvp->next)
			(void)OCIBreak(srvp->srvhp, envp->errhp);
}

/*
 * oracleEndTransaction
 * 		Commit or rollback the transaction.
 * 		The first argument must be a connEntry.
 * 		If "noerror" is true, don't throw errors.
 */
void oracleEndTransaction(void *arg, int is_commit, int noerror)
{
	struct connEntry *connp = NULL, *connarg = (struct connEntry *)arg;
	struct srvEntry *srvp = NULL;
	struct envEntry *envp = NULL;
	int found = 0;

	/* don't report errors on commit or rollback of read-only transactions */
	noerror = noerror || readonly;

	/* clear read-only status if set */
	readonly = 0;

	/* find the cached handles for the argument */
	envp = envlist;
	while (envp)
	{
		srvp = envp->srvlist;
		while (srvp)
		{
			connp = srvp->connlist;
			while (connp)
			{
				if (connp == connarg)
				{
					found = 1;
					break;
				}
				connp = connp->next;
			}
			if (found)
				break;
			srvp = srvp->next;
		}
		if (found)
			break;
		envp = envp->next;
	}

	if (! found)
	{
		connarg->xact_level = 0;
		oracleError(FDW_ERROR, "oracleEndTransaction internal error: handle not found in cache");
	}

	/* close all statements and free their LOB descriptors */
	while (connp->stmtlist != NULL)
		freeStmt(connp->stmtlist->stmthp, connp, envp->errhp);

	/* free objects in cache (might be left behind in case of errors) */
	(void)OCICacheFree(envp->envhp, envp->errhp, NULL);

	/* null_geometry has been freed */
	null_geometry.geometry = NULL;
	null_geometry.indicator = NULL;

	/* do nothing if there is no transaction */
	if (connarg->xact_level == 0)
		return;
	else
		connarg->xact_level = 0;

	/* commit or rollback */
	if (is_commit)
	{
		oracleDebug2("oracle_fdw: commit remote transaction");

		if (checkerr(
			OCITransCommit(connp->svchp, envp->errhp, OCI_DEFAULT),
			(dvoid *)envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS && !noerror)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error committing transaction: OCITransCommit failed",
				oraMessage);
		}
	}
	else
	{
		oracleDebug2("oracle_fdw: roll back remote transaction");

		if (checkerr(
			OCITransRollback(connp->svchp, envp->errhp, OCI_DEFAULT),
			(dvoid *)envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS && !noerror)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error rolling back transaction: OCITransRollback failed",
				oraMessage);
		}
	}
}

/*
 * oracleEndSubtransaction
 * 		Commit or rollback all subtransaction up to savepoint "nest_nevel".
 * 		The first argument must be a connEntry.
 * 		If "is_commit" is not true, rollback.
 */
void
oracleEndSubtransaction(void *arg, int nest_level, int is_commit)
{
	struct connEntry *ce = (struct connEntry *)arg;
	char query[50], message[60];
	struct connEntry *connp = NULL;
	struct srvEntry *srvp = NULL;
	struct envEntry *envp = NULL;
	OCIStmt *stmthp = NULL;
	int found = 0;

	/* do nothing if the transaction level is lower than nest_level */
	if (ce->xact_level < nest_level)
		return;

	ce->xact_level = nest_level - 1;

	/*
	 * There is nothing else to do in read-only transactions, since Oracle
	 * has statement level rollback, so there is no need for savepoint
	 * processing in read-only transactions.
	 */
	if (readonly)
		return;

	if (is_commit)
	{
		/*
		 * There is nothing to do as savepoints don't get released in Oracle:
		 * Setting the same savepoint again just overwrites the previous one.
		 */
		return;
	}

	/* find the cached handles for the argument */
	envp = envlist;
	while (envp)
	{
		srvp = envp->srvlist;
		while (srvp)
		{
			connp = srvp->connlist;
			while (connp)
			{
				if (connp == ce)
				{
					found = 1;
					break;
				}
				connp = connp->next;
			}
			if (found)
				break;
			srvp = srvp->next;
		}
		if (found)
			break;
		envp = envp->next;
	}

	if (! found)
		oracleError(FDW_ERROR, "oracleEndSubtransaction internal error: handle not found in cache");

	snprintf(message, 59, "oracle_fdw: rollback to savepoint s%d", nest_level);
	oracleDebug2(message);

	snprintf(query, 49, "ROLLBACK TO SAVEPOINT s%d", nest_level);

	/* prepare the query */
	if (checkerr(
		OCIStmtPrepare2(ce->svchp, &stmthp, envp->errhp, (text *)query, (ub4) strlen(query),
			(text *)NULL, (ub4)0, (ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT),
		(dvoid *)envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{
		oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
			"error rolling back to savepoint: OCIStmtPrepare2 failed to prepare rollback statement",
			oraMessage);
	}

	/* register the statement handle */
	registerStmt(stmthp, envp->envhp, connp);

	/* rollback to savepoint */
	if (checkerr(
		OCIStmtExecute(connp->svchp, stmthp, envp->errhp, (ub4)1, (ub4)0,
			(CONST OCISnapshot *)NULL, (OCISnapshot *)NULL, OCI_DEFAULT),
		(dvoid *)envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{
		oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
			"error rolling back to savepoint: OCIStmtExecute failed on ROLLBACK TO SAVEPOINT",
			oraMessage);
	}

	/* free the statement handle */
	freeStmt(stmthp, connp, envp->errhp);
}

/*
 * oracleIsStatementOpen
 * 		Return 1 if there is a statement handle, else 0.
 */
int
oracleIsStatementOpen(oracleSession *session)
{
	return (session->stmthp != NULL);
}

/*
 * oracleDescribe
 * 		Find the remote Oracle table and describe it.
 * 		Returns a palloc'ed data structure with the results.
 */
struct oraTable
*oracleDescribe(oracleSession *session, char *dblink, char *schema, char *table, char *pgname, long max_long, int *has_geometry)
{
	struct oraTable *reply;
	OCIStmt *stmthp = NULL;
	OCIParam *colp;
	ub2 oraType, charsize, bin_size;
	ub1 csfrm;
	sb2 precision;
	sb1 scale;
	char *qtable, *qdblink = NULL, *qschema = NULL, *tablename, *query;
	OraText *ident, *typname, *typschema;
	char *type_name, *type_schema;
	ub4 ncols, ident_size, typname_size, typschema_size;
	int i, length;

	/* get a complete quoted table name */
	qtable = copyOraText(table, strlen(table), 1);
	length = strlen(qtable);
	if (dblink != NULL)
	{
		qdblink = copyOraText(dblink, strlen(dblink), 1);
		length += strlen(qdblink) + 1;
	}
	if (schema != NULL)
	{
		qschema = copyOraText(schema, strlen(schema), 1);
		length += strlen(qschema) + 1;
	}
	tablename = oracleAlloc(length + 1);
	tablename[0] = '\0';  /* empty */
	if (schema != NULL)
	{
		strcat(tablename, qschema);
		strcat(tablename, ".");
	}
	strcat(tablename, qtable);
	if (dblink != NULL)
	{
		strcat(tablename, "@");
		strcat(tablename, qdblink);
	}
	oracleFree(qtable);
	if (dblink != NULL)
		oracleFree(qdblink);
	if (schema != NULL)
		oracleFree(qschema);

	/* construct a "SELECT * FROM ..." query to describe columns */
	length += 14;
	query = oracleAlloc(length + 1);
	strcpy(query, "SELECT * FROM ");
	strcat(query, tablename);

	/* prepare the query */
	if (checkerr(
		OCIStmtPrepare2(session->connp->svchp, &stmthp, session->envp->errhp,
			(text *)query, (ub4)strlen(query), (text *)NULL, (ub4)0,
			(ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT),
		(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{
		oracleError_d(FDW_UNABLE_TO_CREATE_REPLY,
			"error describing remote table: OCIStmtPrepare2 failed to prepare query",
			oraMessage);
	}

	/* register statement handle */
	registerStmt(stmthp, session->envp->envhp, session->connp);

	/* execute query */
	if (checkerr(
		OCIStmtExecute(session->connp->svchp, stmthp, session->envp->errhp, (ub4)0, (ub4)0,
			(CONST OCISnapshot *)NULL, (OCISnapshot *)NULL, OCI_DESCRIBE_ONLY),
		(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{
		if (err_code == 942)
			oracleError_ssdh(FDW_TABLE_NOT_FOUND,
				"Oracle table %s for foreign table \"%s\" does not exist or does not allow read access", tablename, pgname,
				oraMessage, "Oracle table names are case sensitive (normally all uppercase).");
		else
			oracleError_d(FDW_UNABLE_TO_CREATE_REPLY,
				"error describing remote table: OCIStmtExecute failed to describe table",
				oraMessage);
	}

	/* allocate an oraTable struct for the results */
	reply = oracleAlloc(sizeof(struct oraTable));
	reply->name = tablename;
	reply->pgname = pgname;
	reply->npgcols = 0;

	/* get the number of columns */
	if (checkerr(
		OCIAttrGet((dvoid *)stmthp, (ub4)OCI_HTYPE_STMT,
			(dvoid *)&ncols, (ub4 *)0, (ub4)OCI_ATTR_PARAM_COUNT, session->envp->errhp),
		(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{
		oracleError_d(FDW_UNABLE_TO_CREATE_REPLY,
			"error describing remote table: OCIAttrGet failed to get number of columns",
			oraMessage);
	}

	reply->ncols = ncols;
	reply->cols = (struct oraColumn **)oracleAlloc(sizeof(struct oraColumn *) * reply->ncols);

	/* loop through the column list */
	for (i=1; i<=reply->ncols; ++i)
	{
		/* allocate an oraColumn struct for the column */
		reply->cols[i-1] = (struct oraColumn *)oracleAlloc(sizeof(struct oraColumn));
		reply->cols[i-1]->pgname = NULL;
		reply->cols[i-1]->pgattnum = 0;
		reply->cols[i-1]->pgtype = 0;
		reply->cols[i-1]->pgtypmod = 0;
		reply->cols[i-1]->used = 0;
		reply->cols[i-1]->strip_zeros = 0;
		reply->cols[i-1]->pkey = 0;
		reply->cols[i-1]->val = NULL;
		reply->cols[i-1]->val_len = NULL;
		reply->cols[i-1]->val_null = NULL;

		/* get the parameter descriptor for the column */
		if (checkerr(
			OCIParamGet((void *)stmthp, OCI_HTYPE_STMT, session->envp->errhp, (dvoid **)&colp, i),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_REPLY,
				"error describing remote table: OCIParamGet failed to get column data",
				oraMessage);
		}

		/* get the column name */
		if (checkerr(
			OCIAttrGet((dvoid*)colp, OCI_DTYPE_PARAM, (dvoid*)&ident,
				&ident_size, (ub4)OCI_ATTR_NAME, session->envp->errhp),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_REPLY,
				"error describing remote table: OCIAttrGet failed to get column name",
				oraMessage);
		}

		reply->cols[i-1]->name = copyOraText((char *)ident, (int)ident_size, 1);

		/* get the data type */
		if (checkerr(
			OCIAttrGet((dvoid*)colp, OCI_DTYPE_PARAM, (dvoid*)&oraType,
				(ub4 *)0, (ub4)OCI_ATTR_DATA_TYPE, session->envp->errhp),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_REPLY,
				"error describing remote table: OCIAttrGet failed to get column type",
				oraMessage);
		}

		/* get the column type name */
		if (checkerr(
			OCIAttrGet((dvoid*)colp, OCI_DTYPE_PARAM, (dvoid*)&typname,
				&typname_size, (ub4)OCI_ATTR_TYPE_NAME, session->envp->errhp),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_REPLY,
				"error describing remote table: OCIAttrGet failed to get column type name",
				oraMessage);
		}

		/* create a zero-terminated copy */
		type_name = oracleAlloc(typname_size + 1);
		strncpy(type_name, (char *)typname, typname_size);
		type_name[typname_size] = '\0';

		/* get the column type schema */
		if (checkerr(
			OCIAttrGet((dvoid*)colp, OCI_DTYPE_PARAM, (dvoid*)&typschema,
				&typschema_size, (ub4)OCI_ATTR_SCHEMA_NAME, session->envp->errhp),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_REPLY,
				"error describing remote table: OCIAttrGet failed to get column type schema name",
				oraMessage);
		}

		/* create a zero-terminated copy */
		type_schema = oracleAlloc(typschema_size + 1);
		strncpy(type_schema, (char *)typschema, typschema_size);
		type_schema[typschema_size] = '\0';

		/* get the character set form */
		if (checkerr(
			OCIAttrGet((dvoid*)colp, OCI_DTYPE_PARAM, (dvoid*)&csfrm,
				(ub4 *)0, (ub4)OCI_ATTR_CHARSET_FORM, session->envp->errhp),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_REPLY,
				"error describing remote table: OCIAttrGet failed to get character set form",
				oraMessage);
		}

		/* get the number of characters for string fields */
		if (checkerr(
			OCIAttrGet((dvoid*)colp, OCI_DTYPE_PARAM, (dvoid*)&charsize,
				(ub4 *)0, (ub4)OCI_ATTR_CHAR_SIZE, session->envp->errhp),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_REPLY,
				"error describing remote table: OCIAttrGet failed to get column length",
				oraMessage);
		}

		/* get the binary length for RAW fields */
		if (checkerr(
			OCIAttrGet((dvoid*)colp, OCI_DTYPE_PARAM, (dvoid*)&bin_size,
				(ub4 *)0, (ub4)OCI_ATTR_DATA_SIZE, session->envp->errhp),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_REPLY,
				"error describing remote table: OCIAttrGet failed to get column size",
				oraMessage);
		}

		/* get the precision */
		if (checkerr(
			OCIAttrGet((dvoid*)colp, OCI_DTYPE_PARAM, (dvoid*)&precision,
				(ub4 *)0, (ub4)OCI_ATTR_PRECISION, session->envp->errhp),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_REPLY,
				"error describing remote table: OCIAttrGet failed to get column precision",
				oraMessage);
		}

		/* get the scale */
		if (checkerr(
			OCIAttrGet((dvoid*)colp, OCI_DTYPE_PARAM, (dvoid*)&scale,
				(ub4 *)0, (ub4)OCI_ATTR_SCALE, session->envp->errhp),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_REPLY,
				"error describing remote table: OCIAttrGet failed to get column scale",
				oraMessage);
		}

		reply->cols[i-1]->scale = scale;

		/* determine oraType and length to allocate */
		switch (oraType)
		{
			case SQLT_AFC:
				/* CHAR(n) */
				reply->cols[i-1]->oratype =
					(csfrm == SQLCS_NCHAR) ? ORA_TYPE_NCHAR : ORA_TYPE_CHAR;
				reply->cols[i-1]->val_size = charsize * 4 + 1;
				break;
			case SQLT_CHR:
			case SQLT_VCS:
				/* VARCHAR(n) and VARCHAR2(n) */
				reply->cols[i-1]->oratype =
					(csfrm == SQLCS_NCHAR) ? ORA_TYPE_NVARCHAR2 : ORA_TYPE_VARCHAR2;
				reply->cols[i-1]->val_size = charsize * 4 + 1;
				break;
			case SQLT_BLOB:
				/* BLOB */
				reply->cols[i-1]->oratype = ORA_TYPE_BLOB;
				/* for LOB columns, "val" will contain a pointer to the locator */
				reply->cols[i-1]->val_size = sizeof(OCILobLocator *);
				break;
			case SQLT_BFILE:
				/* BFILE */
				reply->cols[i-1]->oratype = ORA_TYPE_BFILE;
				/* for LOB columns, "val" will contain a pointer to the locator */
				reply->cols[i-1]->val_size = sizeof(OCILobLocator *);
				break;
			case SQLT_CLOB:
			case SQLT_CFILE:
				/* CLOB and CFILE */
				if (csfrm == SQLCS_NCHAR)
				{
					/*
					 * We don't support NCLOB because Oracle cannot
					 * transform it to the client character set automatically.
					 */
					reply->cols[i-1]->oratype = ORA_TYPE_OTHER;
					reply->cols[i-1]->val_size = 0;
				}
				else
				{
					reply->cols[i-1]->oratype = ORA_TYPE_CLOB;
					/* for LOB columns, "val" will contain a pointer to the locator */
					reply->cols[i-1]->val_size = sizeof(OCILobLocator *);
				}
				break;
			case SQLT_NUM:
				/* NUMBER */
				reply->cols[i-1]->oratype = ORA_TYPE_NUMBER;
				if (precision == 0)
					/* this should be big enough for unrestricted NUMBERs */
					reply->cols[i-1]->val_size = 140;
				else
					reply->cols[i-1]->val_size = ((-scale) > precision ? (-scale) : precision) + 5;
				break;
			case SQLT_FLT:
				/* FLOAT */
				reply->cols[i-1]->oratype = ORA_TYPE_NUMBER;
				reply->cols[i-1]->val_size = 140;
				break;
			case SQLT_IBFLOAT:
				/* BINARY_FLOAT */
				reply->cols[i-1]->oratype = ORA_TYPE_BINARYFLOAT;
				reply->cols[i-1]->val_size = 42;
				break;
			case SQLT_IBDOUBLE:
				/* BINARY_DOUBLE */
				reply->cols[i-1]->oratype = ORA_TYPE_BINARYDOUBLE;
				reply->cols[i-1]->val_size = 310;
				break;
			case SQLT_DAT:
				/* DATE */
				reply->cols[i-1]->oratype = ORA_TYPE_DATE;
				reply->cols[i-1]->val_size = 23;
				break;
			case SQLT_TIMESTAMP:
				/* TIMESTAMP */
				reply->cols[i-1]->oratype = ORA_TYPE_TIMESTAMP;
				reply->cols[i-1]->val_size = 34;
				break;
			case SQLT_TIMESTAMP_TZ:
				/* TIMESTAMP WITH TIMEZONE */
				reply->cols[i-1]->oratype = ORA_TYPE_TIMESTAMPTZ;
				reply->cols[i-1]->val_size = 40;
				break;
			case SQLT_TIMESTAMP_LTZ:
				/* TIMESTAMP WITH LOCAL TIMEZONE */
				reply->cols[i-1]->oratype = ORA_TYPE_TIMESTAMPLTZ;
				reply->cols[i-1]->val_size = 40;
				break;
			case SQLT_INTERVAL_YM:
				/* INTERVAL YEAR TO MONTH */
				reply->cols[i-1]->oratype = ORA_TYPE_INTERVALY2M;
				reply->cols[i-1]->val_size = precision + 5;
				break;
			case SQLT_INTERVAL_DS:
				/* INTERVAL DAY TO SECOND */
				reply->cols[i-1]->oratype = ORA_TYPE_INTERVALD2S;
				reply->cols[i-1]->val_size = precision + scale + 12;
				break;
			case SQLT_LBI:
				/* LONG RAW */
				reply->cols[i-1]->oratype = ORA_TYPE_LONGRAW;
				reply->cols[i-1]->val_size = max_long + 4;
				break;
			case SQLT_LNG:
				/* LONG */
				reply->cols[i-1]->oratype = ORA_TYPE_LONG;
				reply->cols[i-1]->val_size = max_long + 4;
				break;
			case SQLT_BIN:
				/* RAW(n) */
				reply->cols[i-1]->oratype = ORA_TYPE_RAW;
				/* use binary size for RAWs */
				reply->cols[i-1]->val_size = 2 * bin_size + 1;
				break;
			case SQLT_NTY:
				/* named type */
				if ((strcmp(type_schema, "MDSYS") == 0)
					&& (strcmp(type_name, "SDO_GEOMETRY") == 0))
				{
					reply->cols[i-1]->oratype = ORA_TYPE_GEOMETRY;
					reply->cols[i-1]->val_size = sizeof(ora_geometry);
					*has_geometry = 1;
					break;
				}

				if ((strcmp(type_schema, "SYS") == 0)
					&& (strcmp(type_name, "XMLTYPE") == 0))
				{
					reply->cols[i-1]->oratype = ORA_TYPE_XMLTYPE;
					/* will be converted to a LONG */
					reply->cols[i-1]->val_size = max_long + 4;
					break;
				}
				else
					/* fall through */
			default:
				reply->cols[i-1]->oratype = ORA_TYPE_OTHER;
				reply->cols[i-1]->val_size = 0;
		}
	}

	/* free statement handle, this takes care of the parameter handles */
	freeStmt(stmthp, session->connp, session->envp->errhp);

	return reply;
}

#define EXPLAIN_LINE_SIZE 1000

/*
 * oracleExplain
 * 		Returns the EXPLAIN PLAN for the query.
 * 		"plan" will contain "nrows" palloc'ed strings.
 */
void
oracleExplain(oracleSession *session, const char *query, int *nrows, char ***plan)
{
	char res[EXPLAIN_LINE_SIZE], *r=res;
	sb4 res_size = EXPLAIN_LINE_SIZE;
	ub2 res_len, res_type = SQLT_STR;
	sb2 res_ind;
	sword result;
	OCIStmt *stmthp;
	const char * const desc_query =
		"SELECT rtrim(lpad(' ',2*level-2)||operation||' '||options||' '||object_name||' '"
		"||CASE WHEN access_predicates IS NULL THEN NULL ELSE '(condition '||access_predicates||')' END"
		"||' '||CASE WHEN filter_predicates IS NULL THEN NULL ELSE '(filter '||filter_predicates||')' END)"
		" FROM v$sql_plan"
		" CONNECT BY prior id = parent_id AND prior sql_id = sql_id AND prior child_number = child_number"
		" START WITH id=0 AND sql_id=:sql_id and child_number=:child_number"
		" ORDER BY id";

	/* execute the query and get the first result row */
	stmthp = oracleQueryPlan(session, query, desc_query, 1, (dvoid **)&r, &res_size, &res_type, &res_len, &res_ind);

	*nrows = 0;
	do
	{
		/* increase result "array" */
		if (++(*nrows) == 1)
			*plan = (char **)oracleAlloc(sizeof(char *));
		else
			*plan = (char **)oracleRealloc(*plan, sizeof(char *) * (*nrows));

		/* add entry */
		(*plan)[(*nrows)-1] = oracleAlloc(strlen(res) + 1);
		strcpy((*plan)[(*nrows)-1], res);

		/* fetch next row */
		result = checkerr(
			OCIStmtFetch2(stmthp, session->envp->errhp, 1, OCI_FETCH_NEXT, 0, OCI_DEFAULT),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR);

		if (result != OCI_SUCCESS && result != OCI_NO_DATA)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error fetching result: OCIStmtFetch2 failed to fetch next result row",
				oraMessage);
		}
	}
	while (result != OCI_NO_DATA);

	/* close the statement */
	freeStmt(stmthp, session->connp, session->envp->errhp);
}

/*
 * oracleSetSavepoint
 * 		Set savepoints up to level "nest_level".
 */
void
oracleSetSavepoint(oracleSession *session, int nest_level)
{
	/* make sure there is no active statement */
	if (session->stmthp != NULL)
		oracleError(FDW_ERROR, "oracleSetSavepoint internal error: statement handle is not NULL");

	while (session->connp->xact_level < nest_level)
	{
		char query[40], message[50];

		++session->connp->xact_level;

		/*
		 * There is nothing else to do in read-only transactions, since Oracle
		 * has statement level rollback, so there is no need for savepoint
		 * processing in read-only transactions.
		 */
		if (readonly)
			continue;

		snprintf(message, 49, "oracle_fdw: set savepoint s%d", session->connp->xact_level);
		oracleDebug2(message);

		snprintf(query, 39, "SAVEPOINT s%d", session->connp->xact_level);

		/* prepare the query */
		if (checkerr(
			OCIStmtPrepare2(session->connp->svchp, &session->stmthp, session->envp->errhp,
				(text *)query, (ub4)strlen(query), (text *)NULL, (ub4)0,
				(ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error setting savepoint: OCIStmtPrepare2 failed to prepare savepoint statement",
				oraMessage);
		}

		/* register statement handle */
		registerStmt(session->stmthp, session->envp->envhp, session->connp);

		/* set savepoint */
		if (checkerr(
			OCIStmtExecute(session->connp->svchp, session->stmthp, session->envp->errhp, (ub4)1, (ub4)0,
				(CONST OCISnapshot *)NULL, (OCISnapshot *)NULL, OCI_DEFAULT),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error setting savepoint: OCIStmtExecute failed on SAVEPOINT",
				oraMessage);
		}

		oracleCloseStatement(session);
	}
}

/*
 * setOracleEnvironment
 * 		Set environment variables do that Oracle works as we want.
 *
 * 		NLS_LANG sets the language and client encoding
 * 		NLS_NCHAR is unset so that N* data types are converted to the
 * 		character set specified in NLS_LANG.
 *
 * 		The following variables are set to values that make Oracle convert
 * 		numeric and date/time values to strings PostgreSQL can parse:
 * 		NLS_DATE_FORMAT
 * 		NLS_TIMESTAMP_FORMAT
 * 		NLS_TIMESTAMP_TZ_FORMAT
 * 		NLS_NUMERIC_CHARACTERS
 * 		NLS_CALENDAR
 * 		ORA_SDTZ
 */
void
setOracleEnvironment(char *nls_lang, char *timezone)
{
	/* other environment variables that control Oracle formats */
	if (putenv("NLS_DATE_LANGUAGE=AMERICAN") != 0)
	{
		free(nls_lang);
		free(timezone);
		oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
			"error connecting to Oracle",
			"Environment variable NLS_DATE_LANGUAGE cannot be set.");
	}

	if (putenv("NLS_DATE_FORMAT=YYYY-MM-DD HH24:MI:SS BC") != 0)
	{
		free(nls_lang);
		free(timezone);
		oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
			"error connecting to Oracle",
			"Environment variable NLS_DATE_FORMAT cannot be set.");
	}

	if (putenv("NLS_TIMESTAMP_FORMAT=YYYY-MM-DD HH24:MI:SS.FF9 BC") != 0)
	{
		free(nls_lang);
		free(timezone);
		oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
			"error connecting to Oracle",
			"Environment variable NLS_TIMESTAMP_FORMAT cannot be set.");
	}

	if (putenv("NLS_TIMESTAMP_TZ_FORMAT=YYYY-MM-DD HH24:MI:SS.FF9TZH:TZM BC") != 0)
	{
		free(nls_lang);
		free(timezone);
		oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
			"error connecting to Oracle",
			"Environment variable NLS_TIMESTAMP_TZ_FORMAT cannot be set.");
	}

	if (putenv("NLS_NUMERIC_CHARACTERS=.,") != 0)
	{
		free(nls_lang);
		free(timezone);
		oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
			"error connecting to Oracle",
			"Environment variable NLS_NUMERIC_CHARACTERS cannot be set.");
	}

	if (putenv("NLS_CALENDAR=") != 0)
	{
		free(nls_lang);
		free(timezone);
		oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
			"error connecting to Oracle",
			"Environment variable NLS_CALENDAR cannot be set.");
	}

	if (putenv("NLS_NCHAR=") != 0)
	{
		free(nls_lang);
		free(timezone);
		oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
			"error connecting to Oracle",
			"Environment variable NLS_NCHAR cannot be set.");
	}

	/* empty string means "don't set" */
	if (timezone[0] != '\0' && putenv(timezone) != 0)
	{
		free(nls_lang);
		free(timezone);
		oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
			"error connecting to Oracle",
			"Environment variable ORA_SDTZ cannot be set.");
	}

	if (putenv(nls_lang) != 0)
	{
		if (timezone[0] != '\0')
			putenv("ORA_SDTZ=");
		free(nls_lang);
		free(timezone);
		oracleError_d(FDW_UNABLE_TO_ESTABLISH_CONNECTION,
			"error connecting to Oracle",
			"Environment variable NLS_LANG cannot be set.");
	}
}

/*
 * oracleQueryPlan
 * 		This is a utility function called by oracleExplain.
 * 		First the "query" is described explicitly.
 * 		This will parse the query and add it to the library cache.
 * 		Then a "desc_query" is executed against V$SQL_PLAN to extract
 * 		planner info. The Oracle statement is stored in "session" for further fetches.
 *
 * 		"nres" is the number of elements in the select list of "query",
 * 		"res_size" and "res_type" are arrays of length "nres" containing
 * 		result buffer size and Oracle type of the select list entries,
 * 		while the arrays "res", "res_len" and "res_ind" contain output parameters
 * 		for the result, the actual lenth of the result and NULL indicators.
 */
OCIStmt *
oracleQueryPlan(oracleSession *session, const char *query, const char *desc_query, int nres, dvoid **res, sb4 *res_size, ub2 *res_type, ub2 *res_len, sb2 *res_ind)
{
	int child_nr, i;
	const char * const sql_id_query = "SELECT sql_id, child_number FROM (SELECT sql_id, child_number FROM v$sql WHERE sql_text LIKE :sql ORDER BY last_active_time DESC) WHERE rownum=1";
	char sql_id[20], query_head[50], *p;
	OCIStmt *stmthp = NULL;
	OCIDefine *defnhp;
	OCIBind *bndhp;
	sb2 ind1, ind2, ind3;
	ub2 len1, len2;
	ub4 prefetch_rows = 50;

	/* prepare the query */
	if (checkerr(
		OCIStmtPrepare2(session->connp->svchp, &stmthp, session->envp->errhp,
			(text *)query, (ub4)strlen(query), (text *)NULL, (ub4)0,
			(ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT),
		(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{
		oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
			"error describing query: OCIStmtPrepare2 failed to prepare remote query",
			oraMessage);
	}

	/* register statement handle */
	registerStmt(stmthp, session->envp->envhp, session->connp);

	/* set prefetch options */
	if (checkerr(
		OCIAttrSet((dvoid *)stmthp, OCI_HTYPE_STMT, (dvoid *)&prefetch_rows, 0,
			OCI_ATTR_PREFETCH_ROWS, session->envp->errhp),
		(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{
		oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
			"error describing query: OCIAttrSet failed to set number of prefetched rows in statement handle",
			oraMessage);
	}

	/* parse and describe the query, store it in the library cache */
	if (checkerr(
		OCIStmtExecute(session->connp->svchp, stmthp, session->envp->errhp, (ub4)0, (ub4)0,
			(CONST OCISnapshot *)NULL, (OCISnapshot *)NULL, OCI_DESCRIBE_ONLY),
		(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{
		oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
			"error describing query: OCIStmtExecute failed to describe remote query",
			oraMessage);
	}

	freeStmt(stmthp, session->connp, session->envp->errhp);
	stmthp = NULL;

	/*
	 * Get the SQL_ID and CHILD_NUMBER from V$SQL.
	 */

	/* get the first part of the SQL query with '%' appended */
	if ((p = strchr(query + 7, ' ')) == NULL)
	{
		oracleError(FDW_ERROR, "oracleQueryPlan internal error: no space found in query");
	}
	strncpy(query_head, query, p-query);
	query_head[p-query] = '%';
	query_head[p-query+1] = '\0';

	/* prepare */
	if (checkerr(
		OCIStmtPrepare2(session->connp->svchp, &stmthp, session->envp->errhp,
			(text *)sql_id_query, (ub4)strlen(sql_id_query), (text *)NULL, (ub4)0,
			(ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT),
		(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{
		oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
			"error describing query: OCIStmtPrepare2 failed to prepare remote query for sql_id",
			oraMessage);
	}

	/* register statement handle */
	registerStmt(stmthp, session->envp->envhp, session->connp);

	/* bind */
	bndhp = NULL;
	ind3 = 0;
	if (checkerr(
		OCIBindByName(stmthp, &bndhp, session->envp->errhp, (text *)":sql",
			(sb4)4, (dvoid *)query_head, (sb4)(strlen(query_head) + 1),
			SQLT_STR, (dvoid *)&ind3,
			NULL, NULL, (ub4)0, NULL, OCI_DEFAULT),
		(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{
		oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
			"error describing query: OCIBindByName failed to bind parameter",
			oraMessage);
	}

	/* define result values */
	sql_id[19] = '\0';
	defnhp = NULL;
	if (checkerr(
		OCIDefineByPos(stmthp, &defnhp, session->envp->errhp, (ub4)1,
			(dvoid *)sql_id, (sb4)19,
			SQLT_STR, (dvoid *)&ind1,
			(ub2 *)&len1, NULL, OCI_DEFAULT),
		(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{
		oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
			"error describing query: OCIDefineByPos failed to define result value",
			oraMessage);
	}

	defnhp = NULL;
	if (checkerr(
		OCIDefineByPos(stmthp, &defnhp, session->envp->errhp, (ub4)2,
			(dvoid *)&child_nr, (sb4)sizeof(int),
			SQLT_INT, (dvoid *)&ind2,
			(ub2 *)&len2, NULL, OCI_DEFAULT),
		(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{
		oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
			"error describing query: OCIDefineByPos failed to define result value",
			oraMessage);
	}

	/* execute */
	if (checkerr(
		OCIStmtExecute(session->connp->svchp, stmthp, session->envp->errhp, (ub4)1, (ub4)0,
			(CONST OCISnapshot *)NULL, (OCISnapshot *)NULL, OCI_DEFAULT),
		(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{

		/* create a better message if we lack permissions on V$SQL */
		if (err_code == 942)
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"no SELECT privilege on V$SQL in the remote database",
				oraMessage);
		else
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error describing query: OCIStmtExecute failed to execute remote query for sql_id",
				oraMessage);
	}

	freeStmt(stmthp, session->connp, session->envp->errhp);
	stmthp = NULL;

	/*
	 * Run the final desc_query.
	 */

	/* prepare */
	if (checkerr(
		OCIStmtPrepare2(session->connp->svchp, &stmthp, session->envp->errhp,
			(text *)desc_query, (ub4)strlen(desc_query), (text *)NULL, (ub4)0,
			(ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT),
		(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{
		oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
			"error describing query: OCIStmtPrepare2 failed to prepare remote plan query",
			oraMessage);
	}

	/* register statement handle */
	registerStmt(stmthp, session->envp->envhp, session->connp);

	/* bind */
	bndhp = NULL;
	ind1 = 0;
	if (checkerr(
		OCIBindByName(stmthp, &bndhp, session->envp->errhp, (text *)":sql_id",
			(sb4)7, (dvoid *)sql_id, (sb4)strlen(sql_id) + 1,
			SQLT_STR, (dvoid *)&ind1,
			NULL, NULL, (ub4)0, NULL, OCI_DEFAULT),
		(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{
		oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
			"error describing query: OCIBindByName failed to bind parameter",
			oraMessage);
	}

	bndhp = NULL;
	ind2 = 0;
	if (checkerr(
		OCIBindByName(stmthp, &bndhp, session->envp->errhp, (text *)":child_number",
			(sb4)13, (dvoid *)&child_nr, (sb4)sizeof(int),
			SQLT_INT, (dvoid *)&ind2,
			NULL, NULL, (ub4)0, NULL, OCI_DEFAULT),
		(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{
		oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
			"error describing query: OCIBindByName failed to bind parameter",
			oraMessage);
	}

	/* define result values */
	for (i=0; i<nres; ++i)
	{
		defnhp = NULL;
		if (checkerr(
			OCIDefineByPos(stmthp, &defnhp, session->envp->errhp, (ub4)(i + 1),
				(dvoid *)res[i], res_size[i],
				res_type[i], (dvoid *)&res_ind[i],
				&res_len[i], NULL, OCI_DEFAULT),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error describing query: OCIDefineByPos failed to define result value",
				oraMessage);
		}
	}

	/* execute */
	if (checkerr(
		OCIStmtExecute(session->connp->svchp, stmthp, session->envp->errhp, (ub4)1, (ub4)0,
			(CONST OCISnapshot *)NULL, (OCISnapshot *)NULL, OCI_DEFAULT),
		(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{

		/* create a better message if we lack permissions on V$SQL_PLAN */
		if (err_code == 942)
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"no SELECT privilege on V$SQL_PLAN in the remote database",
				oraMessage);
		else
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error describing query: OCIStmtExecute failed to execute remote plan query",
				oraMessage);
	}

	return stmthp;
}

/*
 * oraclePrepareQuery
 * 		Prepares an SQL statement for execution.
 * 		This function should handle everything that has to be done only once
 * 		even if the statement is executed multiple times, that is:
 * 		- For SELECT statements, defines the result values to be stored in oraTable.
 * 		- For DML statements, allocates LOB locators for the RETURNING clause in oraTable.
 */
void
oraclePrepareQuery(oracleSession *session, const char *query, const struct oraTable *oraTable, unsigned int prefetch, unsigned int lob_prefetch)
{
	int i, j, col_pos, is_select;
	OCIDefine *defnhp;
	const ub1 nchar = SQLCS_NCHAR;
	const boolean is_true = TRUE;

	/* figure out if the query is FOR UPDATE */
	is_select = (strncmp(query, "SELECT", 6) == 0);

	/* make sure there is no statement handle stored in "session" */
	if (session->stmthp != NULL)
		oracleError(FDW_ERROR, "oraclePrepareQuery internal error: statement handle is not NULL");

	session->last_batch = 0;

	/* prepare the statement */
	if (checkerr(
		OCIStmtPrepare2(session->connp->svchp, &(session->stmthp), session->envp->errhp,
			(text *)query, (ub4)strlen(query), (text *)NULL, (ub4)0,
			(ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT),
		(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{
		oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
			"error executing query: OCIStmtPrepare2 failed to prepare remote query",
			oraMessage);
	}

	/* register statement handle */
	registerStmt(session->stmthp, session->envp->envhp, session->connp);

	/* loop through table columns */
	col_pos = 0;
	for (i=0; i<oraTable->ncols; ++i)
	{
		if (oraTable->cols[i]->used)
		{
			/*
			 * Unfortunately Oracle handles DML statements with a RETURNING clause
			 * quite different from SELECT statements.  In the latter, the result
			 * columns are "defined", i.e. bound to some storage space.
			 * This definition is only necessary once, even if the query is executed
			 * multiple times, so we do this here.
			 * RETURNING clause are handled in oracleExecuteQuery, here we only
			 * allocate locators for LOB columns in RETURNING clauses.
			 */
			if (is_select)
			{
				ub2 type;
				oraType oracle_type = oraTable->cols[i]->oratype;

				/* figure out in which format we want the results */
				type = getOraType(oraTable->cols[i]->oratype);
				if (oraTable->cols[i]->pgtype == UUIDOID)
					type = SQLT_STR;

				/* check if it is a LOB column */
				if (type == SQLT_BLOB || type == SQLT_BFILE || type == SQLT_CLOB)
				{
					/* allocate an array of LOB locators, store the pointers in "val" */
					for (j = 0; j < prefetch; ++j)
						allocLobLocator((OCILobLocator **)oraTable->cols[i]->val + j,
							session->stmthp, session->envp->envhp, session->connp,
							FDW_UNABLE_TO_CREATE_EXECUTION,
							"error executing query: OCIDescriptorAlloc failed to allocate LOB descriptor");
				}

				/* define result value */
				defnhp = NULL;
				if (checkerr(
					OCIDefineByPos(session->stmthp, &defnhp, session->envp->errhp, (ub4)++col_pos,
						(dvoid *)oraTable->cols[i]->val, (sb4)oraTable->cols[i]->val_size,
						type, (dvoid *)oraTable->cols[i]->val_null,
						(ub2 *)oraTable->cols[i]->val_len, NULL, OCI_DEFAULT),
					(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
				{
					oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
						"error executing query: OCIDefineByPos failed to define result value",
						oraMessage);
				}

				/* LOBs should be prefetched to save round trips */
				if (type == SQLT_BLOB || type == SQLT_BFILE || type == SQLT_CLOB)
				{
					if (checkerr(
						OCIAttrSet(defnhp, OCI_HTYPE_DEFINE, (void *)&lob_prefetch, (ub4)0,
							OCI_ATTR_LOBPREFETCH_SIZE, session->envp->errhp),
						(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
					{
						oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
							"error executing query: OCIAttrSet failed to set LOB prefetch size",
							oraMessage);
					}

					if (checkerr(
						OCIAttrSet(defnhp, OCI_HTYPE_DEFINE, (void *)&is_true, (ub4)0,
							OCI_ATTR_LOBPREFETCH_LENGTH, session->envp->errhp),
						(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
					{
						oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
							"error executing query: OCIAttrSet failed to set LOB length prefetch",
							oraMessage);
					}
				}

				/*
				 * Use the more expensive character set conversion only if requested and only
				 * for columns with a national character set.
				 */
				if (session->have_nchar
					&& (oracle_type == ORA_TYPE_NVARCHAR2 || oracle_type == ORA_TYPE_NCHAR)
					&& checkerr(
						OCIAttrSet((void *)defnhp, OCI_HTYPE_DEFINE, (void *)&nchar, 0,
							OCI_ATTR_CHARSET_FORM, session->envp->errhp),
						(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
				{
					oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
						"error executing query: OCIAttrSet failed to set charset form on result value",
						oraMessage);
				}

				if (oraTable->cols[i]->oratype == ORA_TYPE_GEOMETRY)
				{
					/*
					 * The actual object and indicator will be allocated when data are
					 * fetched and must be freed with OCIObjectFree().
					 */
					ora_geometry *geom = (ora_geometry *)oraTable->cols[i]->val;
					geom->geometry = NULL;
					geom->indicator = NULL;
					geom->num_elems = -1;
					geom->elem = NULL;
					geom->num_coords = -1;
					geom->coord = NULL;

					/* define the result for the named type */
					if (checkerr(
						OCIDefineObject(defnhp, session->envp->errhp, oracleGetGeometryType(session),
							(void **)&geom->geometry, 0, (void **)&geom->indicator, 0),
							session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
					{
						oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
							"error executing query: OCIDefineObject failed to define geometry",
							oraMessage);
					}

					/* set the column's indicator to NOT NULL for a later convertTuple */
					oraTable->cols[i]->val_null[0] = 0;
				}
			}
			else
			{
				ub2 type;

				/* for other statements, allocate LOB locators for RETURNING parameters */
				type = getOraType(oraTable->cols[i]->oratype);
				if (type == SQLT_BLOB || type == SQLT_BFILE || type == SQLT_CLOB)
				{
					/* allocate a LOB locator, store a pointer to it in "val" */
					allocLobLocator((OCILobLocator **)oraTable->cols[i]->val,
						session->stmthp, session->envp->envhp, session->connp,
						FDW_UNABLE_TO_CREATE_EXECUTION,
						"error executing query: OCIDescriptorAlloc failed to allocate LOB descriptor");
				}
			}
		}
	}

	if (is_select && col_pos == 0)
	{
		/*
		 * No columns selected (i.e., SELECT '1' FROM).
		 * Define dummy result columnn.
		 */
		sb4 dummy_size = 4;
		char *dummy = oracleAlloc(dummy_size * prefetch);
		sb2 *dummy_null = oracleAlloc(sizeof(sb2) * prefetch);

		defnhp = NULL;
		if (checkerr(
			OCIDefineByPos(session->stmthp, &defnhp, session->envp->errhp, (ub4)1,
				(dvoid *)dummy, dummy_size, SQLT_STR, (dvoid *)dummy_null,
				NULL, NULL, OCI_DEFAULT),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error executing query: OCIDefineByPos failed to define result value",
				oraMessage);
		}
	}
}

/*
 * oracleExecuteQuery
 * 		Execute a prepared statement and fetches the first result row.
 * 		The parameters ("bind variables") are filled from paramList.
 * 		Return the number of rows processed.
 * 		This can be called several times for a prepared SQL statement.
 */
unsigned int
oracleExecuteQuery(oracleSession *session, const struct oraTable *oraTable, struct paramDesc *paramList, unsigned int prefetch)
{
	sb2 *indicators;
	struct paramDesc *param;
	sword result;
	ub4 rowcount;
	int param_count = 0;
	const ub1 nchar = SQLCS_NCHAR;

	for (param=paramList; param; param=param->next)
		++param_count;

	/* allocate a temporary array of indicators */
	indicators = oracleAlloc(param_count * sizeof(sb2 *));

	/* bind the parameters */
	param_count = -1;
	for (param=paramList; param; param=param->next)
	{
		dvoid *value = NULL;         /* will contain the value as bound */
		sb4 value_len = 0;           /* length of "value" */
		ub2 value_type = SQLT_STR;   /* SQL_STR works for NULLs of all types */
		ub4 oci_mode = OCI_DEFAULT;  /* changed only for output parameters */
		OCINumber *number;
		char *num_format, *pos;

		++param_count;
		indicators[param_count] = (sb2)((param->value == NULL) ? -1 : 0);

		if (param->value != NULL)
			switch (param->bindType) {
				case BIND_NUMBER:
					/* allocate a new NUMBER */
					number = oracleAlloc(sizeof(OCINumber));

					/*
					 * Construct number format.
					 */
					value_len = strlen(param->value);
					num_format = oracleAlloc(value_len + 3);
					/* fill everything with '9' */
					memset(num_format, '9', value_len);
					num_format[value_len] = '\0';
					/* write 'D' in the decimal point position */
					if ((pos = strchr(param->value, '.')) != NULL)
						num_format[pos - param->value] = 'D';
					/* replace the scientific notation part with 'EEEE' */
					if ((pos = strchr(param->value, 'e')) != NULL)
					{
						memset(num_format + (pos - param->value), 'E', 4);
						num_format[(pos - param->value) + 4] = '\0';
					}

					/* convert parameter string to NUMBER */
					if (checkerr(
						OCINumberFromText(session->envp->errhp, (const OraText *)param->value,
							(ub4)value_len, (const OraText *)num_format, (ub4)strlen(num_format),
							(const OraText *)NULL, (ub4)0, number),
						(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
					{
						oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
							"error executing query: OCINumberFromText failed to convert parameter",
							oraMessage);
					}
					oracleFree(num_format);

					value = (dvoid *)number;
					value_len = sizeof(OCINumber);
					value_type = SQLT_VNU;
					break;
				case BIND_STRING:
					value = param->value;
					value_len = strlen(param->value)+1;
					value_type = SQLT_STR;
					break;
				case BIND_LONGRAW:
					value = param->value;
					value_len = *((sb4 *)param->value) + 4;
					value_type = SQLT_LVB;
					break;
				case BIND_LONG:
					value = param->value;
					value_len = *((sb4 *)param->value) + 4;
					value_type = SQLT_LVC;
					break;
				case BIND_GEOMETRY:
					value = param->value;
					value_len = 0;
					value_type = SQLT_NTY;
					break;
				case BIND_OUTPUT:
					value = NULL;
					value_len = oraTable->cols[param->colnum]->val_size;
					value_type = getOraType(oraTable->cols[param->colnum]->oratype);
					if (oraTable->cols[param->colnum]->pgtype == UUIDOID)
					{
						/* the type input function will interpret the string value correctly */
						value_type = SQLT_STR;
					}
					oci_mode = OCI_DATA_AT_EXEC;
					break;
			}

		/*
		 * Since this is a bit convoluted, here is a description of how different
		 * parameters are bound:
		 * - Input parameters of normal data types require just a simple OCIBindByName.
		 * - For named data type input parameters, a new object and its indicator structure
		 *   are allocated and filled, and in addition to OCIBindByName there is a call
		 *   to OCIBindObject that specifies the data type.
		 * - For all output parameters, OCIBindDynamic allocates two callback functions.
		 *   The "in" callback should just return a NULL values, and the "out" callback
		 *   returns a place where to store the retrieved data:
		 *   - For normal data types, a pointer to the column's "val" in the oraTable
		 *     is returned by the callback.
		 *   - For LOBs, the callback also returns the column's "val", which points to
		 *     a LOB locator allocated in oraclePrepareQuery.
		 *   - For named data types, the callback returns the adress of a pointer initialized
		 *     to NULL, and Oracle will allocate space for the object.  The indicator value
		 *     has to be retrieved with OCIObjectGetInd.
		 */

		/* bind geometry output parameters only the first time */
		if (param->bindType == BIND_OUTPUT
				&& oraTable->cols[param->colnum]->oratype == ORA_TYPE_GEOMETRY
				&& param->bindh != NULL)
			continue;

		/* bind the value to the parameter */
		if (checkerr(
			OCIBindByName(session->stmthp, (OCIBind **)&param->bindh, session->envp->errhp, (text *)param->name,
				(sb4)strlen(param->name), value, value_len, value_type,
				(dvoid *)&indicators[param_count], NULL, NULL, (ub4)0, NULL, oci_mode),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error executing query: OCIBindByName failed to bind parameter",
				oraMessage);
		}

		/*
		 * Use the expensive character conversion only if we are dealing with
		 * "national character sets" on the Oracle side.
		 * CLOBs have their own encoding, so we have to exclude them.
		 */
		if (session->have_nchar && value_type != SQLT_CLOB
			&& checkerr(
				OCIAttrSet((void *)param->bindh, OCI_HTYPE_BIND, (void *)&nchar, 0,
					OCI_ATTR_CHARSET_FORM, session->envp->errhp),
				(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error executing query: OCIAttrSet failed to set charset form on bind parameter",
				oraMessage);
		}

		/*
		 * for SDO_GEOMETRY parameters, bind the actual objects
		 * Note: these can only be output parameters or parameters in DML statements.
 		 */
		if (param->colnum > 0
			&& oraTable->cols[param->colnum]->oratype == ORA_TYPE_GEOMETRY)
		{
			ora_geometry *geom = (ora_geometry *)value;

			if (param->bindType == BIND_OUTPUT)
			{
				/* for output parameters, set the column to a NULL value */
				geom = (ora_geometry *)oraTable->cols[param->colnum]->val;
				setNullGeometry(session, geom);

				/* ... and initialize the other ora_geometry fields */
				geom->num_elems = -1;
				geom->elem = NULL;
				geom->num_coords = -1;
				geom->coord = NULL;
			}

			if (checkerr(
				OCIBindObject((OCIBind *)param->bindh, session->envp->errhp,
					oracleGetGeometryType(session), (void **)&geom->geometry, NULL, (void **)&geom->indicator, NULL),
				(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
			{
				oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
					"error executing query: OCIBindObject failed to bind geometry parameter",
					oraMessage);
			}
		}

		/* for output parameters, define callbacks that provide storage space */
		if (param->bindType == BIND_OUTPUT)
		{
			if (checkerr(
				OCIBindDynamic((OCIBind *)param->bindh, session->envp->errhp,
					oraTable->cols[param->colnum], &bind_in_callback,
					oraTable->cols[param->colnum], &bind_out_callback),
				(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
			{
				oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
					"error executing query: OCIBindDynamic failed to bind callback for parameter",
					oraMessage);
			}
		}
	}

	/* execute the query and get the first "prefetch" rows */
	result = checkerr(
		OCIStmtExecute(session->connp->svchp, session->stmthp, session->envp->errhp, (ub4)prefetch, (ub4)0,
			(CONST OCISnapshot *)NULL, (OCISnapshot *)NULL, OCI_DEFAULT),
		(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR);

	if (result != OCI_SUCCESS && result != OCI_NO_DATA)
	{
		oraError sqlstate;
		switch (err_code)
		{
			case 1:
				sqlstate = FDW_UNIQUE_VIOLATION; break;
			case 60:
				sqlstate = FDW_DEADLOCK_DETECTED; break;
			case 1400:
				sqlstate = FDW_NOT_NULL_VIOLATION; break;
			case 2290:
				sqlstate = FDW_CHECK_VIOLATION; break;
			case 2291:
			case 2292:
				sqlstate = FDW_FOREIGN_KEY_VIOLATION; break;
			case 8177:
				sqlstate = FDW_SERIALIZATION_FAILURE; break;
			default:
				sqlstate = FDW_UNABLE_TO_CREATE_EXECUTION;
		}

		/* use the correct SQLSTATE for serialization failures */
		oracleError_d(sqlstate,
			"error executing query: OCIStmtExecute failed to execute remote query",
			oraMessage);
	}

	/* free indicators */
	oracleFree(indicators);

	/* get the number of processed rows (important for DML) */
	if (checkerr(
		OCIAttrGet((dvoid *)session->stmthp, (ub4)OCI_HTYPE_STMT,
			(dvoid *)&rowcount, (ub4 *)0,
			(ub4)OCI_ATTR_ROW_COUNT, session->envp->errhp),
		(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{
		oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
			"error executing query: OCIAttrGet failed to get number of affected rows",
			oraMessage);
	}
	session->last_batch = (result == OCI_NO_DATA);
	session->fetched_rows = (unsigned int)rowcount;
	session->current_row = 0;

	/* post processing of output parameters */
	for (param=paramList; param; param=param->next)
		if (param->bindType == BIND_OUTPUT)
		{
			/*
			 * Store the received length in the table column.
			 * Should not lose any data in all possible cases
			 * since LONG and LONG RAW don't work with RETURNING anyway.
			 */
			oraTable->cols[param->colnum]->val_len[0] = (ub2)oraTable->cols[param->colnum]->val_len4;

			/* for geometry columns, we have to get the indicator */
			if (oraTable->cols[param->colnum]->oratype == ORA_TYPE_GEOMETRY)
			{
				ora_geometry *geom = (ora_geometry *)oraTable->cols[param->colnum]->val;

				/* atomic NULL values will just be NULL */
				if (geom->geometry != NULL)
				{
					if (checkerr(
						OCIObjectGetInd(session->envp->envhp, session->envp->errhp,
							geom->geometry, (void **)&geom->indicator),
						(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
					{
						oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
							"error executing query: OCIObjectGetInd failed to get indicator of returned geometry",
							oraMessage);
					}
				}
			}
		}

	return (unsigned int)rowcount;
}

/*
 * oracleFetchNext
 * 		Fetch the next result rows if the buffer is empty.
 * 		Return the position of the next result in the result buffer
 * 		if there is one, else 0.
 */
unsigned int
oracleFetchNext(oracleSession *session, unsigned int prefetch)
{
	sword result;
	ub4 rowcount;

	/* make sure there is a statement handle stored in "session" */
	if (session->stmthp == NULL)
		oracleError(FDW_ERROR, "oracleFetchNext internal error: statement handle is NULL");

	/* if there are still rows in the result set, return the next one */
	if (session->current_row < session->fetched_rows)
		return ++(session->current_row);

	/* return 0 if we are done */
	if (session->last_batch == 1)
		return 0;

	/* fetch the next result rows */
	result = checkerr(
		OCIStmtFetch2(session->stmthp, session->envp->errhp, (ub4)prefetch, OCI_FETCH_NEXT, 0, OCI_DEFAULT),
		(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR);

	if (result != OCI_SUCCESS && result != OCI_NO_DATA)
	{
		oracleError_d(err_code == 8177 ? FDW_SERIALIZATION_FAILURE : FDW_UNABLE_TO_CREATE_EXECUTION,
			"error fetching result: OCIStmtFetch2 failed to fetch next result rows",
			oraMessage);
	}

	/* get the number of rows fetched */
	if (checkerr(
		OCIAttrGet((dvoid *)session->stmthp, (ub4)OCI_HTYPE_STMT,
			(dvoid *)&rowcount, (ub4 *)0,
			(ub4)OCI_ATTR_ROWS_FETCHED, session->envp->errhp),
		(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{
		oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
			"error fetching result: OCIAttrGet failed to get number of affected rows",
			oraMessage);
	}
	session->last_batch = (result == OCI_NO_DATA);
	session->fetched_rows = (unsigned int)rowcount;
	session->current_row = (rowcount == 0) ? 0 : 1;

	return session->current_row;
}

/*
 * oracleExecuteCall
 * 		Execute an Oracle statement that has no results.
 */
void
oracleExecuteCall(oracleSession *session, char * const stmt)
{
	OCIStmt *stmthp = NULL;

	/* prepare the query */
	if (checkerr(
		OCIStmtPrepare2(session->connp->svchp, &stmthp, session->envp->errhp,
			(text *)stmt, (ub4)strlen(stmt), (text *)NULL, (ub4)0,
			(ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT),
		(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{
		oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
			"error executing statement: OCIStmtPrepare2 failed to prepare query",
			oraMessage);
	}

	/* register statement handle */
	registerStmt(stmthp, session->envp->envhp, session->connp);

	if (checkerr(
		OCIStmtExecute(session->connp->svchp, stmthp, session->envp->errhp, (ub4)1, (ub4)0,
			(CONST OCISnapshot *)NULL, (OCISnapshot *)NULL, OCI_DEFAULT),
		(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{
		if (err_code == 24374)
			oracleError(FDW_UNABLE_TO_CREATE_EXECUTION,
				"Oracle statement must not return a result");
		else
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error executing statement: OCIStmtExecute failed to execute query",
				oraMessage);
	}

	/* free the statement handle */
	freeStmt(stmthp, session->connp, session->envp->errhp);
}

/*
 * oracleGetLob
 * 		Get the LOB contents and store them in *value and *value_len.
 * 		If "trunc" is nonzero, it contains the number of bytes or characters to get.
 */
void
oracleGetLob(oracleSession *session, void *locptr, oraType type, char **value, long *value_len, unsigned long trunc)
{
	OCILobLocator *locp = *(OCILobLocator **)locptr;
	oraub8 amount_byte, amount_char, lobsize;
	sword result = OCI_SUCCESS;
	unsigned long chars_received = 0;

	/* initialize result buffer length */
	*value_len = 0;

	if (type == ORA_TYPE_BFILE)
	{
		/* BFILEs must be opened, LOBs not */
		if (checkerr(
			OCILobFileOpen(session->connp->svchp, session->envp->errhp, locp, OCI_FILE_READONLY),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error fetching result: OCILobFileOpen failed to open BFILE",
				oraMessage);
		}
	}

	/* get the size of the LOB */
	if (checkerr(
		OCILobGetLength2(session->connp->svchp, session->envp->errhp, locp, &lobsize),
		(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{
		oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
			"error fetching result: OCILobGetLength2 failed get the LOB size",
			oraMessage);
	}

	/* don't fetch more that "trunc" if it is non-zero */
	if (trunc != 0 && trunc < lobsize)
		lobsize = (oraub8)trunc;

	/*
	 * Read the LOB in chunks.
	 * We try to read the LOB in one go, but there is a catch: Oracle reports
	 * the length of a CLOB in characters.  So we hope that the length in bytes
	 * will not be more than the character length + LOB_CHUNK_SIZE.  At any rate,
	 * we have to be ready to repeat the read until we got the whole LOB.
	 */
	do
	{
		oraub8 lob_buf_size;

		/* extend result buffer */
		if (*value_len == 0)
		{
			lob_buf_size = lobsize + 1;
			if (type == ORA_TYPE_CLOB)
				lob_buf_size += LOB_CHUNK_SIZE;
			*value = oracleAlloc(lob_buf_size);
		}
		else
		{
			lob_buf_size = LOB_CHUNK_SIZE + 1;
			*value = oracleRealloc(*value, *value_len + lob_buf_size);
		}

		/*
		 * The first time round, tell OCILobRead to read the whole LOB.
		 * On subsequent reads, the amount_* parameters are ignored.
		 * After the call, "amount_byte" contains the number of bytes read.
		 */
		amount_byte = 0;
		amount_char = 0;
		result = checkerr(
			OCILobRead2(session->connp->svchp, session->envp->errhp, locp, &amount_byte, &amount_char,
				(oraub8)1, (dvoid *)(*value + *value_len), lob_buf_size,
				(result == OCI_NEED_DATA) ? OCI_NEXT_PIECE : OCI_FIRST_PIECE,
				NULL, NULL, (ub2)0, (ub1)0),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR);

		if (result == OCI_ERROR)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error fetching result: OCILobRead failed to read LOB chunk",
				oraMessage);
		}

		/* update LOB length */
		*value_len += (long)amount_byte;
		chars_received += amount_char;

		/* stop reading data if we got at least "trunc" bytes or characters */
		if (trunc != 0 &&
			((type == ORA_TYPE_CLOB && chars_received >= trunc) ||
			 *value_len >= trunc))
			break;
	}
	while (result == OCI_NEED_DATA);

	/* string end for CLOBs */
	(*value)[*value_len] = '\0';

	if (type == ORA_TYPE_BFILE)
	{
		/* close the BFILE */
		if (checkerr(
			OCILobFileClose(session->connp->svchp, session->envp->errhp, locp),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error fetching result: OCILobFileClose failed to close BFILE",
				oraMessage);
		}
	}
}

/*
 * oracleClientVersion
 * 		Returns the five components of the client version.
 */
void
oracleClientVersion(int *major, int *minor, int *update, int *patch, int *port_patch)
{
	OCIClientVersion(major, minor, update, patch, port_patch);
}

/*
 * oracleServerVersion
 * 		Returns the five components of the server version.
 */
void
oracleServerVersion(oracleSession *session, int *major, int *minor, int *update, int *patch, int *port_patch)
{
	*major = session->server_version[0];
	*minor = session->server_version[1];
	*update = session->server_version[2];
	*patch = session->server_version[3];
	*port_patch = session->server_version[4];
}

/*
 * getServerVersion
 * 		Retrieves the server version and stores it in the service handle cache.
 */
void
getServerVersion(struct srvEntry *srvp, OCIError *errhp)
{
	OraText version_text[1000];
	ub4 version;

	/* get version information from remote server */
	if (checkerr(
		OCIServerRelease(srvp->srvhp, errhp, version_text, 1000, OCI_HTYPE_SERVER, &version),
		(dvoid *)errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
	{
		oracleError_d(FDW_UNABLE_TO_CREATE_REPLY,
			"error getting server version: OCIServerRelease failed to retrieve version",
			oraMessage);
	}

	srvp->server_version[0] = (version >> 24) & 0x000000FF;
	srvp->server_version[1] = (version >> 20) & 0x0000000F;
	srvp->server_version[2] = (version >> 12) & 0x000000FF;
	srvp->server_version[3] = (version >> 8) & 0x0000000F;
	srvp->server_version[4] = (version >> 0) & 0x000000FF;
}

/*
 * oracleGetGeometryType
 * 		Get the MDSYS.DSO_GEOMETRY type.
 * 		The result is cached in the session' connEntry.
 */
void *oracleGetGeometryType(oracleSession *session)
{
	if (session->connp->geomtype == NULL)
	{
		/* type is not cached, get it */
		if (checkerr(
			OCITypeByName(session->envp->envhp, session->envp->errhp, session->connp->svchp,
				(const oratext *)"MDSYS", 5, (const oratext *)"SDO_GEOMETRY", 12, NULL, 0,
				OCI_DURATION_SESSION, OCI_TYPEGET_HEADER,
				&session->connp->geomtype), (dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"Error getting type MDSYS.SDO_GEOMETRY",
				oraMessage);
		}
	}

	return session->connp->geomtype;
}

/*
 * oracleGetImportColumn
 * 		Get the next element in the ordered list of tables and their columns for "schema".
 * 		Returns 0 if there are no more columns, -1 if the remote schema does not exist, else 1.
 */
int oracleGetImportColumn(oracleSession *session, char *dblink, char *schema, char *limit_to, char **tabname, char **colname, oraType *type, int *charlen, int *typeprec, int *typescale, int *nullable, int *key, int skip_tables, int skip_views, int skip_matviews)
{
	/* the static variables will contain data returned to the caller */
	static char s_tabname[129], s_colname[129];
	char typename[129] = { '\0' }, typeowner[129] = { '\0' }, isnull[2] = { '\0' };
	char object_types[40] = "";
	int count = 0;
	const char * const schema_query = "SELECT COUNT(*) FROM all_users WHERE username = :nsp";
	const char * const column_query_template =
		"SELECT col.table_name, col.column_name, col.data_type, col.data_type_owner,\n"
		"       col.char_length, col.data_precision, col.data_scale, col.nullable,\n"
		"       CASE WHEN primkey_col.position IS NOT NULL THEN 1 ELSE 0 END AS primary_key\n"
		"FROM all_tab_columns%s col,\n"
		"     (SELECT con.table_name, cons_col.column_name, cons_col.position\n"
		"      FROM all_constraints%s con, all_cons_columns%s cons_col\n"
		"      WHERE con.owner = cons_col.owner AND con.table_name = cons_col.table_name\n"
		"        AND con.constraint_name = cons_col.constraint_name\n"
		"        AND con.constraint_type = 'P' AND con.owner = :nsp) primkey_col,\n"
		"     (SELECT owner, object_name,\n"
		"             /* materialized views show up as TABLE and as MATERIALIZED VIEW */\n"
		"             min(object_type) AS object_type\n"
		"           FROM all_objects%s\n"
		"           WHERE object_type <> 'INDEX'\n"
		"           GROUP BY owner, object_name) obj\n"
		"WHERE col.table_name = primkey_col.table_name(+)\n"
		"  AND col.column_name = primkey_col.column_name(+)\n"
		"  AND col.owner = :nsp\n"
		"  AND col.table_name = obj.object_name AND obj.owner = :nsp\n"
		"  AND obj.object_type IN ('silly'%s)\n"
		"%s%s%sORDER BY col.table_name, col.column_id";
	char *column_query = NULL, *table_suffix = NULL;
	OCIBind *bndhp = NULL;
	sb2 ind = 0, ind_tabname, ind_colname, ind_typename, ind_typeowner = OCI_IND_NOTNULL,
		ind_charlen = OCI_IND_NOTNULL, ind_precision = OCI_IND_NOTNULL, ind_scale = OCI_IND_NOTNULL,
		ind_isnull, ind_key;
	OCIDefine *defnhp_tabname = NULL, *defnhp_colname = NULL, *defnhp_typename = NULL,
		*defnhp_typeowner = NULL, *defnhp_charlen = NULL, *defnhp_precision = NULL,
		*defnhp_scale = NULL, *defnhp_isnull = NULL, *defnhp_key = NULL, *defnhp_count = NULL;
	ub2 len_tabname, len_colname, len_typename, len_typeowner,
		len_charlen, len_precision, len_scale, len_isnull, len_key, len_count;
	ub4 prefetch_rows = 50;
	sword result;

	/* return a pointer to the static variables */
	*tabname = s_tabname;
	*colname = s_colname;

	/* when first called, check if the schema does exist */
	if (session->stmthp == NULL)
	{
		/* prepare the query */
		if (checkerr(
			OCIStmtPrepare2(session->connp->svchp, &(session->stmthp), session->envp->errhp,
				(text *)schema_query, (ub4)strlen(schema_query), (text *)NULL, (ub4)0,
				(ub4) OCI_NTV_SYNTAX, (ub4)OCI_DEFAULT),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error importing foreign schema: OCIStmtPrepare2 failed to prepare schema query",
				oraMessage);
		}

		/* register statement handle */
		registerStmt(session->stmthp, session->envp->envhp, session->connp);

		/* bind the parameter */
		if (checkerr(
			OCIBindByName(session->stmthp, &bndhp, session->envp->errhp, (text *)":nsp",
				(sb4)4, (dvoid *)schema, (sb4)(strlen(schema) + 1),
				SQLT_STR, (dvoid *)&ind,
				NULL, NULL, (ub4)0, NULL, OCI_DEFAULT),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error importing foreign schema: OCIBindByName failed to bind parameter",
				oraMessage);
		}

		/* define the result value */
		if (checkerr(
			OCIDefineByPos(session->stmthp, &defnhp_count, session->envp->errhp, (ub4)1,
				(dvoid *)&count, (sb4)sizeof(int),
				SQLT_INT, (dvoid *)&ind,
				(ub2 *)&len_count, NULL, OCI_DEFAULT),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error importing foreign schema: OCIDefineByPos failed to define result",
				oraMessage);
		}

		/* execute the query and get the first result row */
		if (checkerr(
			OCIStmtExecute(session->connp->svchp, session->stmthp, session->envp->errhp, (ub4)1, (ub4)0,
				(CONST OCISnapshot *)NULL, (OCISnapshot *)NULL, OCI_DEFAULT),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error importing foreign schema: OCIStmtExecute failed to execute schema query",
				oraMessage);
		}

		oracleCloseStatement(session);

		/* return -1 if the remote schema does not exist */
		if (count == 0)
			return -1;
	}

	/* when called the first time, execute the query for the table columns */
	if (session->stmthp == NULL)
	{
		if (dblink == NULL)
			table_suffix = "";
		else
		{
			/* we have to add the quoted database link */
			char *qdblink = copyOraText(dblink, strlen(dblink), 1);
			table_suffix = oracleAlloc(strlen(qdblink) + 2);
			table_suffix[0] = '@';
			table_suffix[1] = '\0';
			strcat(table_suffix, qdblink);
		}

		/* types of objects to import */
		if (!skip_tables)
			strcat(object_types, ", 'TABLE'");
		if (!skip_views)
			strcat(object_types, ", 'VIEW'");
		if (!skip_matviews)
			strcat(object_types, ", 'MATERIALIZED VIEW'");

		/* construct the query by appending the dblink to the catalog tables */
		column_query = oracleAlloc(1 + strlen(column_query_template) - 10
								   + 4 * strlen(table_suffix)
								   + strlen(object_types)
								   + (limit_to ? 34 + strlen(limit_to) : 0));
		sprintf(column_query, column_query_template,
				table_suffix, table_suffix, table_suffix, table_suffix,
				object_types,
				(limit_to ? "  AND upper(col.table_name) IN (" : ""),
				(limit_to ? limit_to : ""),
				(limit_to ? ")\n" : ""));

		/* prepare the query */
		if (checkerr(
			OCIStmtPrepare2(session->connp->svchp, &(session->stmthp), session->envp->errhp,
				(text *)column_query, (ub4)strlen(column_query), (text *)NULL, (ub4)0,
				(ub4) OCI_NTV_SYNTAX, (ub4) OCI_DEFAULT),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error importing foreign schema: OCIStmtPrepare2 failed to prepare remote query",
				oraMessage);
		}

		/* register statement handle */
		registerStmt(session->stmthp, session->envp->envhp, session->connp);

		/* set prefetch options */
		if (checkerr(
			OCIAttrSet((dvoid *)session->stmthp, OCI_HTYPE_STMT, (dvoid *)&prefetch_rows, 0,
				OCI_ATTR_PREFETCH_ROWS, session->envp->errhp),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error importing foreign schema: OCIAttrSet failed to set number of prefetched rows in statement handle",
				oraMessage);
		}

		oracleFree(column_query);

		/* bind the parameter */
		if (checkerr(
			OCIBindByName(session->stmthp, &bndhp, session->envp->errhp, (text *)":nsp",
				(sb4)4, (dvoid *)schema, (sb4)(strlen(schema) + 1),
				SQLT_STR, (dvoid *)&ind,
				NULL, NULL, (ub4)0, NULL, OCI_DEFAULT),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error importing foreign schema: OCIBindByName failed to bind parameter",
				oraMessage);
		}

		/* define result values */
		s_tabname[128] = '\0';
		if (checkerr(
			OCIDefineByPos(session->stmthp, &defnhp_tabname, session->envp->errhp, (ub4)1,
				(dvoid *)s_tabname, (sb4)129,
				SQLT_STR, (dvoid *)&ind_tabname,
				(ub2 *)&len_tabname, NULL, OCI_DEFAULT),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error importing foreign schema: OCIDefineByPos failed to define result for table name",
				oraMessage);
		}

		s_colname[128] = '\0';
		if (checkerr(
			OCIDefineByPos(session->stmthp, &defnhp_colname, session->envp->errhp, (ub4)2,
				(dvoid *)s_colname, (sb4)129,
				SQLT_STR, (dvoid *)&ind_colname,
				(ub2 *)&len_colname, NULL, OCI_DEFAULT),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error importing foreign schema: OCIDefineByPos failed to define result for column name",
				oraMessage);
		}

		if (checkerr(
			OCIDefineByPos(session->stmthp, &defnhp_typename, session->envp->errhp, (ub4)3,
				(dvoid *)typename, (sb4)129,
				SQLT_STR, (dvoid *)&ind_typename,
				(ub2 *)&len_typename, NULL, OCI_DEFAULT),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error importing foreign schema: OCIDefineByPos failed to define result for type name",
				oraMessage);
		}

		if (checkerr(
			OCIDefineByPos(session->stmthp, &defnhp_typeowner, session->envp->errhp, (ub4)4,
				(dvoid *)typeowner, (sb4)129,
				SQLT_STR, (dvoid *)&ind_typeowner,
				(ub2 *)&len_typeowner, NULL, OCI_DEFAULT),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error importing foreign schema: OCIDefineByPos failed to define result for type owner",
				oraMessage);
		}

		if (checkerr(
			OCIDefineByPos(session->stmthp, &defnhp_charlen, session->envp->errhp, (ub4)5,
				(dvoid *)charlen, (sb4)sizeof(int),
				SQLT_INT, (dvoid *)&ind_charlen,
				(ub2 *)&len_charlen, NULL, OCI_DEFAULT),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error importing foreign schema: OCIDefineByPos failed to define result for character length",
				oraMessage);
		}

		if (checkerr(
			OCIDefineByPos(session->stmthp, &defnhp_precision, session->envp->errhp, (ub4)6,
				(dvoid *)typeprec, (sb4)sizeof(int),
				SQLT_INT, (dvoid *)&ind_precision,
				(ub2 *)&len_precision, NULL, OCI_DEFAULT),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error importing foreign schema: OCIDefineByPos failed to define result for type precision",
				oraMessage);
		}

		if (checkerr(
			OCIDefineByPos(session->stmthp, &defnhp_scale, session->envp->errhp, (ub4)7,
				(dvoid *)typescale, (sb4)sizeof(int),
				SQLT_INT, (dvoid *)&ind_scale,
				(ub2 *)&len_scale, NULL, OCI_DEFAULT),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error importing foreign schema: OCIDefineByPos failed to define result for type scale",
				oraMessage);
		}

		if (checkerr(
			OCIDefineByPos(session->stmthp, &defnhp_isnull, session->envp->errhp, (ub4)8,
				(dvoid *)isnull, (sb4)2,
				SQLT_STR, (dvoid *)&ind_isnull,
				(ub2 *)&len_isnull, NULL, OCI_DEFAULT),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error importing foreign schema: OCIDefineByPos failed to define result for nullability",
				oraMessage);
		}

		if (checkerr(
			OCIDefineByPos(session->stmthp, &defnhp_key, session->envp->errhp, (ub4)9,
				(dvoid *)key, (sb4)sizeof(int),
				SQLT_INT, (dvoid *)&ind_key,
				(ub2 *)&len_key, NULL, OCI_DEFAULT),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error importing foreign schema: OCIDefineByPos failed to define result for primary key",
				oraMessage);
		}

		/* execute the query and get the first result row */
		result = checkerr(
			OCIStmtExecute(session->connp->svchp, session->stmthp, session->envp->errhp, (ub4)1, (ub4)0,
				(CONST OCISnapshot *)NULL, (OCISnapshot *)NULL, OCI_DEFAULT),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR);

		if (result != OCI_SUCCESS && result != OCI_NO_DATA)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error importing foreign schema: OCIStmtExecute failed to execute column query",
				oraMessage);
		}
	}
	else
	{
		/* fetch the next result row */
		result = checkerr(
			OCIStmtFetch2(session->stmthp, session->envp->errhp, 1, OCI_FETCH_NEXT, 0, OCI_DEFAULT),
			(dvoid *)session->envp->errhp, OCI_HTYPE_ERROR);

		if (result != OCI_SUCCESS && result != OCI_NO_DATA)
		{
			oracleError_d(FDW_UNABLE_TO_CREATE_EXECUTION,
				"error importing foreign schema: OCIStmtFetch2 failed to fetch next result row",
				oraMessage);
		}
	}

	if (result == OCI_NO_DATA)
	{
		oracleCloseStatement(session);
		return 0;
	}
	else
	{
		/* set nullable to 1 if isnull is 'Y', else 0 */
		*nullable = (isnull[0] == 'Y');

		/* figure out correct data type */
		if (strncmp(typename, "VARCHAR", 7) == 0)
			*type = ORA_TYPE_VARCHAR2;
		else if (strcmp(typename, "NUMBER") == 0)
			*type = ORA_TYPE_NUMBER;
		else if (strcmp(typename, "DATE") == 0)
			*type = ORA_TYPE_DATE;
		else if (strcmp(typename, "CHAR") == 0)
			*type = ORA_TYPE_CHAR;
		else if (strncmp(typename, "TIMESTAMP", 9) == 0)
		{
			if (strlen(typename) < 17)
				*type = ORA_TYPE_TIMESTAMP;
			else
				*type = ORA_TYPE_TIMESTAMPTZ;
		}
		else if (strcmp(typename, "RAW") == 0)
			*type = ORA_TYPE_RAW;
		else if (strcmp(typename, "BLOB") == 0)
			*type = ORA_TYPE_BLOB;
		else if (strcmp(typename, "CLOB") == 0)
			*type = ORA_TYPE_CLOB;
		else if (strcmp(typename, "BFILE") == 0)
			*type = ORA_TYPE_BFILE;
		else if (strcmp(typename, "LONG") == 0)
			*type = ORA_TYPE_LONG;
		else if (strcmp(typename, "LONG RAW") == 0)
			*type = ORA_TYPE_LONGRAW;
		else if (strcmp(typename, "SDO_GEOMETRY") == 0
				&& ind_typeowner == OCI_IND_NOTNULL && strcmp(typeowner, "MDSYS") == 0)
			*type = ORA_TYPE_GEOMETRY;
		else if (strcmp(typename, "XMLTYPE") == 0
				&& ind_typeowner == OCI_IND_NOTNULL
				&& (strcmp(typeowner, "PUBLIC") == 0 || strcmp(typeowner, "SYS") == 0))
			*type = ORA_TYPE_XMLTYPE;
		else if (strcmp(typename, "FLOAT") == 0)
			*type = ORA_TYPE_FLOAT;
		else if (strncmp(typename, "NVARCHAR", 8) == 0)
			*type = ORA_TYPE_NVARCHAR2;
		else if (strcmp(typename, "NCHAR") == 0)
			*type = ORA_TYPE_NCHAR;
		else if (strncmp(typename, "INTERVAL DAY", 12) == 0)
			*type = ORA_TYPE_INTERVALD2S;
		else if (strncmp(typename, "INTERVAL YEAR", 13) == 0)
			*type = ORA_TYPE_INTERVALY2M;
		else if (strcmp(typename, "BINARY_FLOAT") == 0)
			*type = ORA_TYPE_BINARYFLOAT;
		else if (strcmp(typename, "BINARY_DOUBLE") == 0)
			*type = ORA_TYPE_BINARYDOUBLE;
		else
			*type = ORA_TYPE_OTHER;

		/* set character length, precision and scale to 0 if it was a NULL value */
		if (ind_charlen != OCI_IND_NOTNULL)
			*charlen = 0;
		if (ind_precision != OCI_IND_NOTNULL)
			*typeprec = 0;
		if (ind_scale != OCI_IND_NOTNULL)
			*typescale = 0;
	}

	return 1;
}

/*
 * checkerr
 * 		Call OCIErrorGet to get error message and error code.
 */
sword
checkerr(sword status, dvoid *handle, ub4 handleType)
{
	int length;
	oraMessage[0] = '\0';

	if (status == OCI_SUCCESS_WITH_INFO || status == OCI_ERROR)
	{
		OCIErrorGet(handle, (ub4)1, NULL, &err_code,
			(text *)oraMessage, (ub4)ERRBUFSIZE, handleType);
		length = strlen(oraMessage);
		if (length > 0 && oraMessage[length-1] == '\n')
			oraMessage[length-1] = '\0';
	}

	if (status == OCI_SUCCESS_WITH_INFO)
		status = OCI_SUCCESS;

	if (status == OCI_NO_DATA)
	{
		strcpy(oraMessage, "ORA-00100: no data found");
		err_code = (sb4)100;
	}

	return status;
}

/*
 * copyOraText
 * 		Returns a palloc'ed string containing a (possibly quoted) copy of "string".
 * 		If the string starts with "(" and ends with ")", no quoting will take place
 * 		even if "quote" is true.
 */
char
*copyOraText(const char *string, int size, int quote)
{
	int resultsize = (quote ? size + 2 : size);
	register int i, j=-1;
	char *result;

	/* if "string" is parenthized, return a copy */
	if (string[0] == '(' && string[size-1] == ')')
	{
		result = oracleAlloc(size + 1);
		memcpy(result, string, size);
		result[size] = '\0';
		return result;
	}

	if (quote)
	{
		for (i=0; i<size; ++i)
		{
			if (string[i] == '"')
				++resultsize;
		}
	}

	result = oracleAlloc(resultsize + 1);
	if (quote)
		result[++j] = '"';
	for (i=0; i<size; ++i)
	{
		result[++j] = string[i];
		if (quote && string[i] == '"')
			result[++j] = '"';
	}
	if (quote)
		result[++j] = '"';
	result[j+1] = '\0';

	return result;
}

/*
 * closeSession
 * 		Close the session and remove it from the cache.
 * 		If "disconnect" is true, close the server connection when appropriate.
 */
void
closeSession(OCIEnv *envhp, OCIServer *srvhp, OCISession *userhp, int disconnect)
{
	struct envEntry *envp;
	struct srvEntry *srvp;
	struct connEntry *connp, *prevconnp = NULL;
	OCITrans *txnhp = NULL;

	/* search environment handle in cache */
	for (envp = envlist; envp != NULL; envp = envp->next)
	{
		if (envp->envhp == envhp)
			break;
	}

	if (envp == NULL)
	{
		if (silent)
			return;
		else
			oracleError(FDW_ERROR, "closeSession internal error: environment handle not found in cache");
	}

	/* search server handle in cache */
	for (srvp = envp->srvlist; srvp != NULL; srvp = srvp->next)
	{
		if (srvp->srvhp == srvhp)
			break;
	}

	if (srvp == NULL)
	{
		if (silent)
			return;
		else
			oracleError(FDW_ERROR, "closeSession internal error: server handle not found in cache");
	}

	/* search connection in cache */
	for (connp = srvp->connlist; connp != NULL; connp = connp->next)
	{
		if (connp->userhp == userhp)
			break;

		prevconnp = connp;
	}

	if (connp == NULL)
	{
		if (silent)
			return;
		else
			oracleError(FDW_ERROR, "closeSession internal error: user handle not found in cache");
	}

	/* terminate the session */
	if (checkerr(
		OCISessionEnd(connp->svchp, envp->errhp, connp->userhp, OCI_DEFAULT),
		(dvoid *)envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS && !silent)
	{
		oracleError_d(FDW_UNABLE_TO_CREATE_REPLY,
			"error closing session: OCISessionEnd failed to terminate session",
			oraMessage);
	}

	/* free the session handle */
	(void)OCIHandleFree((dvoid *)connp->userhp, OCI_HTYPE_SESSION);

	/* get the transaction handle */
	if (checkerr(
		OCIAttrGet((dvoid *)connp->svchp, (ub4)OCI_HTYPE_SVCCTX,
			(dvoid *)&txnhp, (ub4 *)0, (ub4)OCI_ATTR_TRANS, envp->errhp),
		(dvoid *)envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS && !silent)
	{
		oracleError_d(FDW_UNABLE_TO_CREATE_REPLY,
			"error closing session: OCIAttrGet failed to get transaction handle",
			oraMessage);
	}

	/* free the service handle */
	(void)OCIHandleFree((dvoid *)connp->svchp, OCI_HTYPE_SVCCTX);

	/* free the transaction handle */
	(void)OCIHandleFree((dvoid *)txnhp, OCI_HTYPE_TRANS);

	/* remove the session handle from the cache */
	if (prevconnp == NULL)
		srvp->connlist = connp->next;
	else
		prevconnp->next = connp->next;

	/* close the server session if desired and this is the last session */
	if (disconnect && srvp->connlist == NULL)
		disconnectServer(envhp, srvhp);

	/* unregister callback for rolled back transactions */
	oracleUnregisterCallback(connp);

	/* free the memory */
	free(connp->user);
	free(connp);
}

/*
 * disconnectServer
 * 		Disconnect from the server and remove it from the cache.
 */
void
disconnectServer(OCIEnv *envhp, OCIServer *srvhp)
{
	struct envEntry *envp;
	struct srvEntry *srvp, *prevsrvp = NULL;

	/* search environment handle in cache */
	for (envp = envlist; envp != NULL; envp = envp->next)
	{
		if (envp->envhp == envhp)
			break;
	}

	if (envp == NULL)
	{
		if (silent)
			return;
		else
			oracleError(FDW_ERROR, "disconnectServer internal error: environment handle not found in cache");
	}

	/* search server handle in cache */
	for (srvp = envp->srvlist; srvp != NULL; srvp = srvp->next)
	{
		if (srvp->srvhp == srvhp)
			break;

		prevsrvp = srvp;
	}

	if (srvp == NULL)
	{
		if (silent)
			return;
		else
			oracleError(FDW_ERROR, "disconnectServer internal error: server handle not found in cache");
	}

	/* disconnect server */
	if (checkerr(
		OCIServerDetach(srvp->srvhp, envp->errhp, OCI_DEFAULT),
		(dvoid *)envp->errhp, OCI_HTYPE_ERROR) != OCI_SUCCESS && !silent)
	{
		oracleError_d(FDW_UNABLE_TO_CREATE_REPLY,
			"error closing session: OCIServerDetach failed to detach from server",
			oraMessage);
	}

	/* free the server handle */
	(void)OCIHandleFree((dvoid *)srvp->srvhp, OCI_HTYPE_SERVER);

	/* remove server entry from the linked list */
	if (prevsrvp == NULL)
		envp->srvlist = srvp->next;
	else
		prevsrvp->next = srvp->next;

	/* free the memory */
	free(srvp->connectstring);
	free(srvp);
}

/*
 * removeEnvironment
 * 		Deallocate environment and error handle and remove cache entry.
 */
void
removeEnvironment(OCIEnv *envhp)
{
	struct envEntry *envp, *prevenvp = NULL;

	/* search environment handle in cache */
	for (envp = envlist; envp != NULL; envp = envp->next)
	{
		if (envp->envhp == envhp)
			break;

		prevenvp = envp;
	}

	if (envp == NULL)
	{
		if (silent)
			return;
		else
			oracleError(FDW_ERROR, "removeEnvironment internal error: environment handle not found in cache");
	}

	/* free the error handle */
	(void)OCIHandleFree((dvoid *)envp->errhp, OCI_HTYPE_ERROR);

	/* free the environment handle */
	(void)OCIHandleFree((dvoid *)envp->envhp, OCI_HTYPE_ENV);

	/* remove environment entry from the linked list */
	if (prevenvp == NULL)
		envlist = envp->next;
	else
		prevenvp->next = envp->next;

	/* free the memory */
	(void)putenv("NLS_LANG=");
	free(envp->nls_lang);
	if (envp->timezone[0] != '\0')
		(void)putenv("ORA_SDTZ=");
	free(envp->timezone);
	free(envp);
}

/*
 * registerStmt
 * 		Add the statement handle to the connection entry's linked list.
 */

void
registerStmt(OCIStmt *stmthp, OCIEnv *envhp, struct connEntry *connp)
{
	struct stmtHandleEntry *entry;

	/* create entry for the linked list */
	if ((entry = malloc(sizeof(struct stmtHandleEntry))) == NULL)
	{
		oracleError_i(FDW_OUT_OF_MEMORY,
			"error allocating handle: failed to allocate %d bytes of memory",
			sizeof(struct stmtHandleEntry));
	}

	/* add the handle to the linked list */
	entry->stmthp = stmthp;
	entry->loclist = NULL;
	entry->next = connp->stmtlist;
	connp->stmtlist = entry;
}

/*
 * allocLobLocator
 * 		Allocate an Oracle LOB locator, store it in the statement's linked list.
 */

void
allocLobLocator(OCILobLocator **locpp, OCIStmt *stmthp, OCIEnv *envhp, struct connEntry *connp, oraError error, const char *errmsg)
{
	struct stmtHandleEntry *entry;
	struct lobLocatorEntry *locentry;

	/* find the statement handle in the linked list */
	for (entry = connp->stmtlist; entry != NULL; entry = entry->next)
		if (entry->stmthp == stmthp)
			break;

	if (entry == NULL)
		oracleError(FDW_ERROR, "internal error allocating LOB locator: statement not found in list");

	/* create entry for the linked list */
	if ((locentry = malloc(sizeof(struct lobLocatorEntry))) == NULL)
	{
		oracleError_i(FDW_OUT_OF_MEMORY,
			"error allocating handle: failed to allocate %d bytes of memory",
			sizeof(struct lobLocatorEntry));
	}

	if (OCIDescriptorAlloc((const dvoid *)envhp, (void **)locpp, OCI_DTYPE_LOB, (size_t)0, NULL) != OCI_SUCCESS)
	{
		free(locentry);
		oracleError(error, errmsg);
	}

	/* add the handle to the linked list */
	locentry->lobloc = *locpp;
	locentry->next = entry->loclist;
	entry->loclist = locentry;
}

/*
 * freeStmt
 * 		Free an Oracle statement handle and all its LOB locators.
 */

void
freeStmt(OCIStmt *stmthp, struct connEntry *connp, OCIError *errhp)
{
	struct stmtHandleEntry *entry, *preventry = NULL;
	struct lobLocatorEntry *locentry;

	/* find it in the linked list */
	for (entry = connp->stmtlist; entry != NULL; entry = entry->next)
	{
		if (entry->stmthp == stmthp)
			break;

		preventry = entry;
	}

	if (entry == NULL)
		oracleError(FDW_ERROR, "internal error freeing statement handle: not found in list");

	/* free all the LOB descriptors */
	while (entry->loclist != NULL)
	{
		(void)OCIDescriptorFree(entry->loclist->lobloc, OCI_DTYPE_LOB);
		locentry = entry->loclist->next;
		free(entry->loclist);
		entry->loclist = locentry;
	}

	/* free the statement handle */
	(void)OCIStmtRelease(stmthp, errhp, (const OraText *)NULL, (ub4)0, OCI_DEFAULT);

	/* remove it */
	if (preventry == NULL)
		connp->stmtlist = entry->next;
	else
		preventry->next = entry->next;

	free(entry);
}

/*
 * getOraType
 * 		Find oracle's name for a given oraType.
 */

ub2
getOraType(oraType arg)
{
	switch (arg)
	{
		case ORA_TYPE_BLOB:
			return SQLT_BLOB;
		case ORA_TYPE_BFILE:
			return SQLT_BFILE;
		case ORA_TYPE_CLOB:
			return SQLT_CLOB;
		case ORA_TYPE_RAW:
			return SQLT_BIN;
		case ORA_TYPE_LONG:
			return SQLT_LVC;
		case ORA_TYPE_LONGRAW:
			return SQLT_LVB;
		case ORA_TYPE_GEOMETRY:
			return SQLT_NTY;
		default:
			/* all other columns are converted to strings */
			return SQLT_STR;
	}
}

/*
 * bind_out_callback
 * 		Point Oracle to where it should write the value for the output parameter.
 */

sb4
bind_out_callback(void *octxp, OCIBind *bindp, ub4 iter, ub4 index, void **bufpp, ub4 **alenp, ub1 *piecep, void **indp, ub2 **rcodep)
{
	struct oraColumn *column = (struct oraColumn *)octxp;

	if (column->oratype == ORA_TYPE_BLOB || column->oratype == ORA_TYPE_CLOB || column->oratype == ORA_TYPE_BFILE)
	{
		/* for LOBs, data should be written to the LOB locator */
		*bufpp = *((OCILobLocator **)column->val);
		*indp = column->val_null;
	}
	else if (column->oratype == ORA_TYPE_GEOMETRY)
	{
		ora_geometry *geom = (ora_geometry *)column->val;

		/* initialize pointers to NULL, memory will be allocated */
		geom->geometry = NULL;
		geom->indicator = NULL;
		*bufpp = &(geom->geometry);
		/* the indicator will be got from the object */
		*indp = NULL;
	}
	else
	{
		/* for other types, data should be written directly to the buffer */
		*bufpp = column->val;
		*indp = column->val_null;
	}
	column->val_len4 = (ub4)column->val_size;
	*alenp = &(column->val_len4);
	*rcodep = NULL;

	if (*piecep == OCI_ONE_PIECE)
		return OCI_CONTINUE;
	else
		return OCI_ERROR;
}

/*
 * bind_in_callback
 * 		Provide a NULL value.
 * 		This is necessary for output parameters to keep Oracle from crashing.
 */

sb4
bind_in_callback(void *ictxp, OCIBind *bindp, ub4 iter, ub4 index, void **bufpp, ub4 *alenp, ub1 *piecep, void **indpp)
{
	struct oraColumn *column = (struct oraColumn *)ictxp;

	*piecep = OCI_ONE_PIECE;

	if (column->oratype == ORA_TYPE_GEOMETRY)
	{
		ora_geometry *geom = (ora_geometry *)column->val;
		/* the column already contains a NULL value */
		*bufpp = geom->geometry;
		*indpp = geom->indicator;
	}
	else
	{
		column->val_null[0] = -1;
		*indpp = column->val_null;
	}

	return OCI_CONTINUE;
}

/*
 * setNullGeometry
 * 		Initialize the ora_geometry argument with a NULL value.
 */
void
setNullGeometry(oracleSession *session, ora_geometry *geom)
{
	/* allocate the cached NULL value if it is not set yet */
	if (null_geometry.geometry == NULL)
		oracleGeometryAlloc(session, &null_geometry);

	geom->geometry = null_geometry.geometry;
	geom->indicator = null_geometry.indicator;
}
