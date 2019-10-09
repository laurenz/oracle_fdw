#include "postgres.h"
#include "utils\elog.h"

#include "..\oracle_fdw.h"

#include <windows.h>

#pragma warning(push)
#pragma warning(disable: 4201)
#include <delayimp.h>
#pragma warning(pop)

#define ERRMSG_LIB "Oracle client library (oci.dll) not found"
#define ERRDETAIL_LIB "No Oracle client is installed, or your system is configured incorrectly."
#define ERRMSG_PROC "Incompatible version of Oracle client library (oci.dll) found"
#define ERRDETAIL_PROC "An exported function was not found in oci.dll."

FARPROC WINAPI
oracleDelayLoadFailureHook(unsigned dliNotify, PDelayLoadInfo pdli)
{
	if (dliNotify == dliFailLoadLib) {
#if defined(ORAFDW_INSECURE_DIAG)
		ereport(ERROR,
			(errcode(ERRCODE_SYSTEM_ERROR),
			errmsg(ERRMSG_LIB),
			errdetail(ERRDETAIL_LIB),
			errhint("The current PATH is: %s", getenv("PATH"))));
#else
		ereport(ERROR,
			(errcode(ERRCODE_SYSTEM_ERROR),
			errmsg(ERRMSG_LIB),
			errdetail(ERRDETAIL_LIB),
			errhint("Verify that the PATH variable includes the Oracle client.")));
#endif	/* ORAFDW_INSECURE_DIAG */
	}
	else if (dliNotify == dliFailGetProc) {
		/* There are no exports by ordinal yets. */
		if (pdli->dlp.fImportByName) {
			ereport(ERROR,
				(errcode(ERRCODE_SYSTEM_ERROR),
				errmsg(ERRMSG_PROC),
				errdetail(ERRDETAIL_PROC),
				errhint("Missing function: %s", pdli->dlp.szProcName)));
		}
		else {
			ereport(ERROR,
				(errcode(ERRCODE_SYSTEM_ERROR),
				errmsg(ERRMSG_PROC),
				errdetail(ERRDETAIL_PROC),
				errhint("Missing ordinal: #%u", pdli->dlp.dwOrdinal)));
		}
	}
	return 0;
}

#if _MSC_VER >= 1900
const
#endif
PfnDliHook __pfnDliFailureHook2 = oracleDelayLoadFailureHook;
