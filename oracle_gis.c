/*-------------------------------------------------------------------------
 *
 * oracle_gis.c
 * 		routines that convert between Oracle SDO_GEOMETRY and PostGIS EWKB
 *
 *-------------------------------------------------------------------------
 */

/*
 * The code relies heavilly on the PostGIS internal data stucture that is explained 
 * in g_serialized.txt in the PostGIS source code and implemented in liblwgeom.h.
 */

/* Oracle header */
#include <oci.h>

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

#include "oracle_fdw.h"

#define POINTTYPE			   1
#define LINETYPE			   2
#define POLYGONTYPE			   3
#define MULTIPOINTTYPE		   4
#define MULTILINETYPE		   5
#define MULTIPOLYGONTYPE	   6
#define COLLECTIONTYPE		   7
#define CIRCSTRINGTYPE		   8
#define COMPOUNDTYPE		   9
#define CURVEPOLYTYPE		  10
#define MULTICURVETYPE		  11
#define MULTISURFACETYPE	  12
#define POLYHEDRALSURFACETYPE 13
#define TRIANGLETYPE		  14
#define TINTYPE				  15

#define WKBSRIDFLAG 0x20000000
#define WKBZOFFSET  0x80000000
#define WKBMOFFSET  0x40000000

#define uintToNumber(errhp, intp, numberp) { \
	if (checkerr( \
			OCINumberFromInt((errhp), (dvoid *)(intp), sizeof(unsigned), OCI_NUMBER_UNSIGNED, (numberp)), \
			(errhp)) != OCI_SUCCESS) \
		oracleError_d(FDW_ERROR, "OCINumberFromInt failed to convert integer to NUMBER", oraMessage); \
}

#define numberToUint(errhp, numberp, intp) { \
	if (checkerr( \
			OCINumberToInt((errhp), (numberp), sizeof(unsigned), OCI_NUMBER_UNSIGNED, (dvoid *)(intp)), \
			(errhp)) != OCI_SUCCESS) \
		oracleError_d(FDW_ERROR, "OCINumberToInt failed to convert NUMBER to integer", oraMessage); \
}

#define doubleToNumber(errhp, doublep, numberp) { \
	if (checkerr( \
			OCINumberFromReal((errhp), (const dvoid *)(doublep), sizeof(double), (numberp)), \
			(errhp)) != OCI_SUCCESS) \
		oracleError_d(FDW_ERROR, "OCINumberFromReal failed to convert floating point number to NUMBER", oraMessage); \
}

#define numberToDouble(errhp, numberp, doublep) { \
	if (checkerr( \
			OCINumberToReal((errhp), (numberp), sizeof(double), (dvoid *)(doublep)), \
			(errhp)) != OCI_SUCCESS) \
		oracleError_d(FDW_ERROR, "OCINumberToReal failed to convert NUMBER to floating point number", oraMessage); \
}

/* file for mapping SRIDs in the "share" directory */
#define SRID_MAP_FILE "srid.map"

typedef struct
{
	unsigned from; /* != 0 for valid ones */
	unsigned to;
} mapEntry;

#define mapEntryValid(x) ((x)->from != 0)

/* maps Oracle SRIDs to PostGIS SRIDs */
static mapEntry *srid_map = NULL;

/* contains Oracle error messages, set by checkerr() */
#define ERRBUFSIZE 500
static char oraMessage[ERRBUFSIZE];
static sb4 err_code;

/*
 * Structures needed for managing the MDSYS.SDO_GEOMETRY Oracle type.
 * Most of these were generated with OTT.
 */
typedef struct
{
	OCINumber x;
	OCINumber y;
	OCINumber z;
} sdo_point_type;

typedef struct
{
	OCIInd _atomic;
	OCIInd x;
	OCIInd y;
	OCIInd z;
} sdo_point_type_ind;

typedef struct sdo_geometry
{
	OCINumber sdo_gtype;
	OCINumber sdo_srid;
	sdo_point_type sdo_point;
	OCIArray *sdo_elem_info;
	OCIArray *sdo_ordinates;
} sdo_geometry;

typedef struct sdo_geometry_ind
{
	OCIInd _atomic;
	OCIInd sdo_gtype;
	OCIInd sdo_srid;
	sdo_point_type_ind sdo_point;
	OCIInd sdo_elem_info;
	OCIInd sdo_ordinates;
} sdo_geometry_ind;

static unsigned ewkbType(oracleSession *session, ora_geometry *geom);
static unsigned ewkbDimension(oracleSession *session, ora_geometry *geom);
static unsigned ewkbSrid(oracleSession *session, ora_geometry *geom);
static unsigned numCoord(oracleSession *session, ora_geometry *geom);
static double coord(oracleSession *session, ora_geometry *geom, unsigned i);
static unsigned numElemInfo(oracleSession *session, ora_geometry *geom);
static unsigned elemInfo(oracleSession *session, ora_geometry *geom, unsigned i);
static void appendElemInfo(oracleSession *session, ora_geometry *geom, unsigned info );
static void appendCoord(oracleSession *session, ora_geometry *geom, double coord);
static char *doubleFill(double x, char * dest);
static char *unsignedFill(unsigned i, char * dest);
static sword checkerr(sword status, OCIError *handle);
static void initSRIDMap(void);
static unsigned epsgFromOracle(unsigned srid);
static unsigned epsgToOracle(unsigned srid);

/* All ...Fill() functions return a pointer to the end of the written zone
 */
static unsigned ewkbHeaderLen(oracleSession *session, ora_geometry *geom);
static char *ewkbHeaderFill(oracleSession *session, ora_geometry *geom, char * dest);
static unsigned ewkbPointLen(oracleSession *session, ora_geometry *geom);
static char *ewkbPointFill(oracleSession *session, ora_geometry *geom, char *dest);
static unsigned ewkbLineLen(oracleSession *session, ora_geometry *geom);
static char *ewkbLineFill(oracleSession *session, ora_geometry *geom, char * dest);
static unsigned ewkbPolygonLen(oracleSession *session, ora_geometry *geom);
static char *ewkbPolygonFill(oracleSession *session, ora_geometry *geom, char * dest);
static unsigned ewkbMultiPointLen(oracleSession *session, ora_geometry *geom);
static char *ewkbMultiPointFill(oracleSession *session, ora_geometry *geom, char *dest);
static unsigned ewkbMultiLineLen(oracleSession *session, ora_geometry *geom);
static char *ewkbMultiLineFill(oracleSession *session, ora_geometry *geom, char * dest);
static unsigned ewkbMultiPolygonLen(oracleSession *session, ora_geometry *geom);
static char *ewkbMultiPolygonFill(oracleSession *session, ora_geometry *geom, char * dest);

/*
 * All the set...() functions return a pointer that points to a position in the
 * input buffer right after the added data.
 * That way we can call several of them in a row and even nest them,
 * like in the case of a multipolygon composed of several polygons.
 */
static const char *setType(oracleSession *session, ora_geometry *geom, const char *data);
static const char *setSridAndFlags(oracleSession *session, ora_geometry *geom, const char *data);
static const char *setPoint(oracleSession *session, ora_geometry *geom, const char *data);
static const char *setLine(oracleSession *session, ora_geometry *geom, const char *data);
static const char *setPolygon(oracleSession *session, ora_geometry *geom, const char *data);
static const char *setMultiPoint(oracleSession *session, ora_geometry *geom, const char *data);
static const char *setMultiLine(oracleSession *session, ora_geometry *geom, const char *data);
static const char *setMultiPolygon(oracleSession *session, ora_geometry *geom, const char *data);

char *
doubleFill(double x, char * dest)
{
	memcpy(dest, &x, sizeof(double));
	dest += sizeof(double);
	return dest;
}

char *
unsignedFill(unsigned i, char * dest)
{
	memcpy(dest, &i, sizeof(unsigned));
	dest += sizeof(unsigned);
	return dest;
}

/*
 * initSRIDMap
 * 		Allocates "srid_map" and reads the SRID map file into it.
 */
void
initSRIDMap()
{
	char *mapFileName = NULL;
	FILE *mapFile;
	char line[20];
	unsigned long from, to;
	int count = 0, i, save_errno, c;

	mapFileName = oracleGetShareFileName(SRID_MAP_FILE);

	/* initialize "srid_map" with an invalid entry */
	srid_map = (mapEntry *)malloc(sizeof(mapEntry));
	if (srid_map == NULL)
		oracleError_i(FDW_ERROR, "failed to allocate %d bytes of memory", sizeof(mapEntry));
	srid_map[0].from = 0;

	/* from here on we must make sure that srid_map is reset to NULL if an error occurs */

	if ((mapFile = fopen(mapFileName, "r")) == NULL)
	{
		/* if the file does not exists, treat it as if it were empty */
		if (errno == ENOENT)
			return;

		/* other errors are reported */
		free(srid_map);
		srid_map = NULL;
		oracleError(FDW_ERROR, "cannot open file \"" SRID_MAP_FILE "\": %m");
	}

	/* from here on we must make sure that mapFile is closed if an error happens */
	oracleFree(mapFileName);

	do
	{
		/* read the next line into "line" */
		i = 0;
		do
		{
			c = fgetc(mapFile);

			if (c == '\n' || c == EOF)
			{
				line[i] = '\0';
			}
			else
			{
				if (i >= 19)
				{
					(void)fclose(mapFile);
					free(srid_map);
					srid_map = NULL;
					oracleError(FDW_ERROR,
						"syntax error in file \"" SRID_MAP_FILE "\": line too long");
				}
				line[i++] = c;
			}
		} while (c != '\n' && c != EOF);

		/* ignore empty lines */
		if (*line == '\0')
			continue;

		/* read two unsigned integers */
		i = sscanf(line, "%lu %lu", &from, &to);
		if (i == EOF)
		{
			save_errno = errno;
			(void)fclose(mapFile);
			errno = save_errno;
			free(srid_map);
			srid_map = NULL;
			oracleError(FDW_ERROR, "syntax error in file \"" SRID_MAP_FILE "\": %m");
		}
		if (i != 2)
		{
			(void)fclose(mapFile);
			free(srid_map);
			srid_map = NULL;
			oracleError(FDW_ERROR,
				"syntax error in file \"" SRID_MAP_FILE "\": line does not contain two numbers");
		}
		if (from == 0 || to == 0)
		{
			(void)fclose(mapFile);
			free(srid_map);
			srid_map = NULL;
			oracleError(FDW_ERROR,
				"syntax error in file \"" SRID_MAP_FILE "\": SRID cannot be zero");
		}
		if (from > 0xffffffff || to > 0xffffffff)
		{
			(void)fclose(mapFile);
			free(srid_map);
			srid_map = NULL;
			oracleError(FDW_ERROR,
				"syntax error in file \"" SRID_MAP_FILE "\": number too large");
		}

		/* add a new mapEntry to srid_map */
		srid_map = (mapEntry *)realloc(srid_map, sizeof(mapEntry) * (++count + 1));
		if (srid_map == NULL)
		{
			(void)fclose(mapFile);
			free(srid_map);
			srid_map = NULL;
			oracleError_i(FDW_ERROR, "failed to allocate %d bytes of memory", sizeof(mapEntry) * (count + 1));
		}

		srid_map[count - 1].from = (unsigned)from;
		srid_map[count - 1].to = (unsigned)to;
		srid_map[count].from = 0;
	} while (c != EOF);

	/* check for errors */
	save_errno = errno;
	(void)fclose(mapFile);
	errno = save_errno;

	if (errno)
	{
		free(srid_map);
		srid_map = NULL;
		oracleError(FDW_ERROR, "error reading from file \"" SRID_MAP_FILE "\": %m");
	}
}

unsigned
epsgFromOracle(unsigned srid)
{
	mapEntry *entry;

	if (srid_map == NULL)
		initSRIDMap();

	for (entry = srid_map; mapEntryValid(entry); ++entry)
		if (entry->from == srid)
			return entry->to;

	return srid;
}

unsigned
epsgToOracle(unsigned srid)
{
	mapEntry *entry;

	if (srid_map == NULL)
		initSRIDMap();

	for (entry = srid_map; mapEntryValid(entry); ++entry)
		if (entry->to == srid)
			return entry->from;

	return srid;
}

/*
 * oracleEWKBToGeom
 * 		Creates an Oracle SDO_GEOMETRY from a PostGIS EWKB.
 * 		The result is a partially palloc'ed structure.
 * 		Zero length or a NULL pointer for ewkb_data yield an atomically NULL object.
 */
ora_geometry *
oracleEWKBToGeom(oracleSession *session, unsigned ewkb_length, char *ewkb_data)
{
	const char *data = ewkb_data;
	unsigned type;

	ora_geometry *geom = (ora_geometry *)oracleAlloc(sizeof(ora_geometry));
	oracleGeometryAlloc(session, geom);

	/* for NULL data, return an object that is atomically NULL */
	if (data == NULL || ewkb_length == 0)
		return geom;
	else
		geom->indicator->_atomic = OCI_IND_NOTNULL;

	data = setSridAndFlags(session, geom, data);

	/*
	 * We don't move the data pointer after this call because 
	 * it will be moved after the following setTYPE functions
	 * and those functions expect the data pointer to be on the
	 * type and not after (see comment above about g_serialized.txt 
	 * and set... functions).
	 */
	setType(session, geom, data);

	type = ewkbType(session, geom);

	/* these will be NULL for points */
	geom->indicator->sdo_ordinates = (type == POINTTYPE) ? OCI_IND_NULL : OCI_IND_NOTNULL;
	geom->indicator->sdo_elem_info = (type == POINTTYPE) ? OCI_IND_NULL : OCI_IND_NOTNULL;

	switch (type)
	{
		case POINTTYPE:
			data = setPoint(session, geom, data);
			break;
		case LINETYPE:
			data = setLine(session, geom, data);
			break;
		case POLYGONTYPE:
			data = setPolygon(session, geom, data);
			break;
		case MULTIPOINTTYPE:
			data = setMultiPoint(session, geom, data);
			break;
		case MULTILINETYPE:
			data = setMultiLine(session, geom, data);
			break;
		case MULTIPOLYGONTYPE:
			data = setMultiPolygon(session, geom, data);
			break;
		default:
			oracleError_i(FDW_ERROR, "error converting SDO_GEOMETRY to geometry: unexpected geometry type %u", type);
	}

	/* check that we reached the end of input data */
	if (data - ewkb_data != ewkb_length)
		oracleError_ii(FDW_ERROR, "oracle_fdw internal error: number of bytes read %u is different from length %u", data - ewkb_data, ewkb_length);

	return geom;
}

/*
 * oracleGetEWKBLen
 * 		Returns the length in bytes needed to store an EWKB conversion of "geom".
 */
unsigned
oracleGetEWKBLen(oracleSession *session, ora_geometry *geom)
{
	unsigned type;

	/* return zero length for atomically NULL objects */
	if (geom->indicator->_atomic == OCI_IND_NULL)
		return 0;

	/* a first check for supported types is done in ewkbType */
	type = ewkbType(session, geom);

	/*
	 * If sdo_elem_info is NOT NULL, we go through and check that the
	 * type and interpretation are actually supported.
	 */
	if (geom->indicator->sdo_elem_info == OCI_IND_NOTNULL)
	{
		const unsigned n = numElemInfo(session, geom);
		unsigned i;
		for (i=0; i<n; i+=3){
			const unsigned etype = elemInfo(session, geom, i+1);
			const unsigned interpretation = elemInfo(session, geom, i+2);
			if (!((1 == etype && 1 == interpretation)
				||(2 == etype && 1 == interpretation)
				||(1003 == etype && 1 == interpretation)
				||(2003 == etype && 1 == interpretation)
				))
				oracleError_ii(FDW_ERROR, "error converting SDO_GEOMETRY to geometry: unsupported etype %u with interpretation %u in elem_info", etype, interpretation);
		}
	}

	switch (type)
	{
		case POINTTYPE:
			return ewkbHeaderLen(session, geom) + ewkbPointLen(session, geom);
		case LINETYPE:
			return ewkbHeaderLen(session, geom) + ewkbLineLen(session, geom);
		case POLYGONTYPE:
			return ewkbHeaderLen(session, geom) + ewkbPolygonLen(session, geom);
		case MULTIPOINTTYPE:
			return ewkbHeaderLen(session, geom) + ewkbMultiPointLen(session, geom);
		case MULTILINETYPE:
			return ewkbHeaderLen(session, geom) + ewkbMultiLineLen(session, geom);
		case MULTIPOLYGONTYPE:
			return ewkbHeaderLen(session, geom) + ewkbMultiPolygonLen(session, geom);
		default:
			oracleError_i(FDW_ERROR, "error converting SDO_GEOMETRY to geometry: unexpected geometry type %u", type);
			return 0;  /* unreachable, but keeps compiler happy */
	}
}

/*
 * oracleFillEWKB
 * 		Converts "geom" to an EWKB and stores the result in "dest".
 */
char *
oracleFillEWKB(oracleSession *session, ora_geometry *geom, unsigned size, char *dest)
{
	const char *orig = dest;
	unsigned type;

	dest = ewkbHeaderFill(session, geom, dest);
	switch ((type = ewkbType(session, geom)))
	{
		case POINTTYPE:
			dest = ewkbPointFill(session, geom, dest);
			break;
		case LINETYPE:
			dest = ewkbLineFill(session, geom, dest);
			break;
		case POLYGONTYPE:
			dest = ewkbPolygonFill(session, geom, dest);
			break;
		case MULTIPOINTTYPE:
			dest = ewkbMultiPointFill(session, geom, dest);
			break;
		case MULTILINETYPE:
			dest = ewkbMultiLineFill(session, geom, dest);
			break;
		case MULTIPOLYGONTYPE:
			dest = ewkbMultiPolygonFill(session, geom, dest);
			break;
		default:
			oracleError_i(FDW_ERROR, "error converting SDO_GEOMETRY to geometry: unexpected geometry type %u", type);
	}

	/* check that we have reached the end of the input buffer */
	if (dest - orig != size)
		oracleError_ii(FDW_ERROR, "oracle_fdw internal error: number of bytes written %u is different from size %u", dest - orig, size);

	return dest;
}

/*
 * oracleGeometryFree
 * 		Free the memory allocated with a geometry object.
 */
void
oracleGeometryFree(oracleSession *session, ora_geometry *geom)
{
	/*
	 * From the OCI documentation:
	 * If there is a top-level object (as with a non-atomically NULL object),
	 * then the indicator is freed when the top-level object is freed with OCIObjectFree().
	 * If the object is atomically null, then there is no top-level object,
	 * so the indicator must be freed separately.
	 */
	if (geom->geometry != NULL && geom->indicator->_atomic == OCI_IND_NOTNULL)
		(void)OCIObjectFree(session->envp->envhp, session->envp->errhp, geom->geometry, 0);
	else
		(void)OCIObjectFree(session->envp->envhp, session->envp->errhp, geom->indicator, 0);

	geom->geometry = NULL;
	geom->indicator = NULL;
}

/*
 * oracleGeometryAlloc
 * 		Allocate memory for a geometry object in the Oracle object cache.
 * 		The indicator is set to atomic NULL.
 */
void
oracleGeometryAlloc(oracleSession *session, ora_geometry *geom)
{
	/* allocate a SDO_GEOMETRY object */
	if (checkerr(
			OCIObjectNew(session->envp->envhp,
				 		session->envp->errhp,
				 		session->connp->svchp,
				 		OCI_TYPECODE_OBJECT,
				 		oracleGetGeometryType(session),
				 		(dvoid *)NULL,
				 		OCI_DURATION_TRANS,
				 		TRUE,
				 		(dvoid **)&geom->geometry),
			session->envp->errhp) != OCI_SUCCESS)
		oracleError_d(FDW_ERROR, "cannot allocate SDO_GEOMETRY object", oraMessage);

	/* get the NULL indicator */
	if (checkerr(
			OCIObjectGetInd(session->envp->envhp,
							session->envp->errhp,
							geom->geometry,
							(void **)&geom->indicator),
			session->envp->errhp) != OCI_SUCCESS)
		oracleError_d(FDW_ERROR, "cannot get indicator for new SDO_GEOMETRY object", oraMessage);
		
	/* initialize as atomic NULL */
	geom->indicator->_atomic = OCI_IND_NULL;
}

void
appendElemInfo(oracleSession *session, ora_geometry *geom, unsigned info)
{
	OCINumber n;

	uintToNumber(session->envp->errhp, &info, &n);

	if (checkerr(
			OCICollAppend(session->envp->envhp,
				   		session->envp->errhp,
				   		(CONST dvoid*) &n,
				   		NULL,
				   		geom->geometry->sdo_elem_info),
			session->envp->errhp) != OCI_SUCCESS)
		oracleError_d(FDW_ERROR, "cannot append to element info collection", oraMessage);
}

void
appendCoord(oracleSession *session, ora_geometry *geom, double coord)
{
	OCINumber n;

	doubleToNumber(session->envp->errhp, &coord, &n);
	if (checkerr(
			OCICollAppend(session->envp->envhp,
				   		session->envp->errhp,
				   		(CONST dvoid*) &n,
				   		NULL,
				   		geom->geometry->sdo_ordinates),
			session->envp->errhp) != OCI_SUCCESS)
		oracleError_d(FDW_ERROR, "cannot append to ordinate collection", oraMessage);
}

unsigned
ewkbType(oracleSession *session, ora_geometry *geom)
{
	unsigned gtype = 0;
	if (geom->indicator->sdo_gtype == OCI_IND_NULL)
		oracleError(FDW_ERROR, "error converting SDO_GEOMETRY to geometry: geometry type cannot be NULL");

	numberToUint(session->envp->errhp, &(geom->geometry->sdo_gtype), &gtype);

	switch (gtype%1000)
	{
		case 1:
			return POINTTYPE;
		case 2:
			return LINETYPE;
		case 3:
			return POLYGONTYPE;
		case 4:
			oracleError(FDW_ERROR, "error converting SDO_GEOMETRY to geometry: geometry type COLLECTION not supported");
		case 5:
			return MULTIPOINTTYPE;
		case 6:
			return MULTILINETYPE;
		case 7:
			return MULTIPOLYGONTYPE;
		case 8:
			oracleError(FDW_ERROR, "error converting SDO_GEOMETRY to geometry: geometry type SOLID not supported");
		case 9:
			oracleError(FDW_ERROR, "error converting SDO_GEOMETRY to geometry: geometry type MULTISOLID not supported");
		default:
			oracleError_i(FDW_ERROR, "error converting SDO_GEOMETRY to geometry: unknown geometry type %u", gtype);
	}
	return 0;  /* unreachable, but keeps compiler happy */
}

const char *
setType(oracleSession *session, ora_geometry *geom, const char *data)
{
	const ub4 wkbType = *((ub4 *)data);
	unsigned gtype;

	numberToUint(session->envp->errhp, &(geom->geometry->sdo_gtype), &gtype);

	data += sizeof(ub4);

	switch (wkbType)
	{
		case POINTTYPE:
			gtype += 1;
			break;
		case LINETYPE:
			gtype += 2;
			break;
		case POLYGONTYPE:
			gtype += 3;
			break;
		case MULTIPOINTTYPE:
			gtype += 5;
			break;
		case MULTILINETYPE:
			gtype += 6;
			break;
		case MULTIPOLYGONTYPE:
			gtype += 7;
			break;
#define UNSUPPORTED_TYPE( T ) case T ## TYPE: oracleError(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: geometry type "#T" not supported");
		UNSUPPORTED_TYPE(COLLECTION)
		UNSUPPORTED_TYPE(CIRCSTRING)
		UNSUPPORTED_TYPE(COMPOUND)
		UNSUPPORTED_TYPE(CURVEPOLY)
		UNSUPPORTED_TYPE(MULTICURVE)
		UNSUPPORTED_TYPE(MULTISURFACE)
		UNSUPPORTED_TYPE(POLYHEDRALSURFACE)
		UNSUPPORTED_TYPE(TRIANGLE)
		UNSUPPORTED_TYPE(TIN)
#undef UNSUPPORTED_TYPE
		default:
			oracleError_i(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: unknown geometry type %u", wkbType);
	}

	geom->indicator->sdo_gtype = OCI_IND_NOTNULL;
	uintToNumber(session->envp->errhp, &gtype, &(geom->geometry->sdo_gtype));

	return data;
}

/*
 * Header contains:
 * - srid : 3 bytes
 * - flags : 1 byte
 */
unsigned
ewkbHeaderLen(oracleSession *session, ora_geometry *geom)
{
	return 4;
}

char *
ewkbHeaderFill(oracleSession *session, ora_geometry *geom, char * dest)
{
	const unsigned srid = ewkbSrid(session, geom);
	const ub1 flags = ((3 == ewkbDimension(session, geom)) ? 0x01 : 0x00 );
	ub1 s[3];

	s[0] = (srid & 0x001F0000) >> 16;
	s[1] = (srid & 0x0000FF00) >> 8;
	s[2] = (srid & 0x000000FF);

	memcpy(dest, s, 3);
	dest += 3;
	memcpy(dest, &flags, 1);
	dest += 1;
	return dest;
}


unsigned
ewkbDimension(oracleSession *session, ora_geometry *geom)
{
	unsigned gtype = 0;
	if (geom->indicator->sdo_gtype == OCI_IND_NOTNULL)
		numberToUint(session->envp->errhp, &(geom->geometry->sdo_gtype), &gtype);
	return gtype / 1000;
}

unsigned
ewkbSrid(oracleSession *session, ora_geometry *geom)
{
	unsigned srid = 0;
	if (geom->indicator->sdo_srid == OCI_IND_NOTNULL)
		numberToUint(session->envp->errhp, &(geom->geometry->sdo_srid), &srid);

	/* convert PostGIS->Oracle SRID when needed */
	return epsgFromOracle(srid);
}

const char *
setSridAndFlags(oracleSession *session, ora_geometry *geom, const char *data)
{
	unsigned srid = 0;
	unsigned gtype = 0;

	srid |= ((ub1)data[0]) << 16;
	srid |= ((ub1)data[1]) << 8;
	srid |= ((ub1)data[2]);
	/*
	 * Only the first 21 bits are set. Slide up and back to pull
	 * the negative bits down, if we need them.
	 */
	srid = (srid<<11)>>11;

	data += 3;

	/* convert Oracle->PostGIS SRID when needed */
	srid = epsgToOracle(srid);

	geom->indicator->sdo_srid = (srid == 0) ? OCI_IND_NULL : OCI_IND_NOTNULL;

	if (geom->indicator->sdo_srid == OCI_IND_NOTNULL)
		uintToNumber(session->envp->errhp, &srid, &(geom->geometry->sdo_srid));

	gtype = (((ub1)data[0]) & 0x01 ) ? 3000 : 2000; /* 3d/2d */
	if (data[0] & 0x02)
		oracleError(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: measure dimension not supported");
	if (data[0] & 0x08)
		oracleError(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: geodetic not supported");

	if (((ub1)data[0]) & 0x04) /* has bbox, offsets */
	{
		data += 1 + 2*(((ub1)data[0]) & 0x01 ? 3 : 2)*sizeof(float);
	}
	else
	{
		data += 1;
	}

	geom->indicator->sdo_gtype = OCI_IND_NOTNULL;

	uintToNumber(session->envp->errhp, &gtype, &(geom->geometry->sdo_gtype));

	return data;
}

unsigned
ewkbPointLen(oracleSession *session, ora_geometry *geom)
{
	return 2*sizeof(unsigned) + sizeof(double)*ewkbDimension(session, geom);
}

char *
ewkbPointFill(oracleSession *session, ora_geometry *geom, char *dest)
{

	if (geom->indicator->sdo_point.x == OCI_IND_NULL
		|| geom->indicator->sdo_point.y == OCI_IND_NULL 
		|| (ewkbDimension(session, geom) == 3 
			&& geom->indicator->sdo_point.z == OCI_IND_NULL) ) 
	{
		oracleError(FDW_ERROR, "error converting SDO_GEOMETRY to geometry: null point coordinates not supported");
	}

	dest = unsignedFill(POINTTYPE, dest);
	dest = unsignedFill(1, dest);

	numberToDouble(session->envp->errhp, &(geom->geometry->sdo_point.x), dest);
	dest += sizeof(double);
	numberToDouble(session->envp->errhp, &(geom->geometry->sdo_point.y), dest);
	dest += sizeof(double);
	if (3 == ewkbDimension(session, geom))
	{
		numberToDouble(session->envp->errhp, &(geom->geometry->sdo_point.z), dest);
		dest += sizeof(double);
	}

	return dest;
}

const char *
setPoint(oracleSession *session, ora_geometry *geom, const char *data)
{
	if (*((unsigned *)data) != POINTTYPE)
		oracleError_i(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: expected point, got type %u", *((unsigned *)data));
	data += sizeof(unsigned);
	if (*((unsigned *)data ) != 1)
		oracleError(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: empty point is not supported");
	data += sizeof(unsigned);

	geom->indicator->sdo_point._atomic = OCI_IND_NOTNULL;

	geom->indicator->sdo_point.x = OCI_IND_NOTNULL;
	doubleToNumber(session->envp->errhp, data, &(geom->geometry->sdo_point.x));
	data += sizeof(double);
	geom->indicator->sdo_point.y = OCI_IND_NOTNULL;
	doubleToNumber(session->envp->errhp, data, &(geom->geometry->sdo_point.y));
	data += sizeof(double);
	if (3 == ewkbDimension(session, geom))
	{
		geom->indicator->sdo_point.z = OCI_IND_NOTNULL;
		doubleToNumber(session->envp->errhp, data, &(geom->geometry->sdo_point.z));
		data += sizeof(double);
	}
	return data;
}

unsigned
ewkbLineLen(oracleSession *session, ora_geometry *geom)
{
	return 2*sizeof(unsigned) + sizeof(double)*numCoord(session, geom);
}

char *
ewkbLineFill(oracleSession *session, ora_geometry *geom, char * dest)
{
	unsigned i;
	const unsigned numC = numCoord(session, geom);
	const unsigned numPoints = numC / ewkbDimension(session, geom);
	dest = unsignedFill(LINETYPE, dest);
	dest = unsignedFill(numPoints, dest);
	for (i=0; i<numC; i++) dest = doubleFill(coord(session, geom, i), dest);
	return dest;
}

const char *
setLine(oracleSession *session, ora_geometry *geom, const char *data)
{
	unsigned i, n;

	if (*((unsigned *)data) != LINETYPE)
		oracleError_i(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: expected line, got type %u", *((unsigned *)data));
	data += sizeof(unsigned);
	n = *((unsigned *)data) * ewkbDimension(session, geom);
	data += sizeof(unsigned);

	if (!n)
		oracleError(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: empty line is not supported");

	appendElemInfo(session, geom, numCoord(session, geom) + 1); /* start index + 1 */
	appendElemInfo(session, geom, 2); /* SDO_ETYPE linestring */
	appendElemInfo(session, geom, 1); /* SDO_INTERPRETATION straight line segments */

	for (i=0; i<n; i++)
	{
		appendCoord(session, geom, *((double *)data));
		data += sizeof(double);
	}
	return data;
}

unsigned
ewkbPolygonLen(oracleSession *session, ora_geometry *geom)
{
	const unsigned numRings = numElemInfo(session, geom)/3;
	/* there is the number of rings, and, for each ring the number of points
	 * numRings%2 is there for padding
	 */
	return (numRings+2+numRings%2)*sizeof(unsigned)
			+ sizeof(double)*numCoord(session, geom);
}

const char *
setPolygon(oracleSession *session, ora_geometry *geom, const char *data)
{
	unsigned r, i, numRings;
	const unsigned dimension = ewkbDimension(session, geom);
	const char * ringSizeData;

	if (*((unsigned *)data) != POLYGONTYPE)
		oracleError_i(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: expected polygon, got type %u", *((unsigned *)data));
	data += sizeof(unsigned);

	numRings = *((unsigned *)data);
	data += sizeof(unsigned);

	if (!numRings)
		oracleError(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: empty polygon is not supported");

	ringSizeData = data;
	data += (numRings+numRings%2)*sizeof(unsigned);
	for (r=0; r<numRings; r++)
	{
		const unsigned n= *((unsigned *)ringSizeData) * dimension;
		ringSizeData += sizeof(unsigned);

		appendElemInfo(session, geom, numCoord(session, geom) + 1); /* start index + 1 */
		appendElemInfo(session, geom, r == 0 ? 1003 : 2003); /* SDO_ETYPE ext ring or int ring */
		appendElemInfo(session, geom, 1); /* SDO_INTERPRETATION straight line segments */

		for (i=0; i<n; i++)
		{
			appendCoord(session, geom, *((double *)data));
			data += sizeof(double);
		}
	}

	return data;
}

char *
ewkbPolygonFill(oracleSession *session, ora_geometry *geom, char * dest)
{
	const unsigned dimension = ewkbDimension(session, geom);
	const unsigned numRings = numElemInfo(session, geom)/3;
	const unsigned numC = numCoord(session, geom);
	unsigned i;
	dest = unsignedFill(POLYGONTYPE, dest);

	dest = unsignedFill(numRings, dest);

	for (i=0; i<numRings; i++)
	{
		const unsigned coord_b = elemInfo(session, geom, i*3) - 1;
		const unsigned coord_e = i+1 == numRings
								 ? numC
								 : elemInfo(session, geom, (i+1)*3) - 1;
		const unsigned numPoints = (coord_e - coord_b) / dimension;
		dest = unsignedFill(numPoints, dest);
	}

	/* padding */
	if ( numRings % 2 != 0 ) dest = unsignedFill(0, dest);

	for (i=0; i<numC; i++) dest = doubleFill(coord(session, geom, i), dest);

	return dest;
}

unsigned
ewkbMultiPointLen(oracleSession *session, ora_geometry *geom)
{
	const unsigned numC = numCoord(session, geom);
	const unsigned numPoints = numC / ewkbDimension(session, geom);
	return 2*sizeof(unsigned) + (2*sizeof(unsigned)*numPoints) + sizeof(double)*numC;
}

char *
ewkbMultiPointFill(oracleSession *session, ora_geometry *geom, char * dest)
{
	unsigned i;
	const unsigned dim = ewkbDimension(session, geom);
	const unsigned numPoints = numCoord(session, geom) / dim;
	dest = unsignedFill(MULTIPOINTTYPE, dest);
	dest = unsignedFill(numPoints, dest);
	for (i=0; i<numPoints; i++)
	{
		unsigned j;
		dest = unsignedFill(POINTTYPE, dest);
		dest = unsignedFill(1, dest);
		for (j=0; j<dim; j++) dest = doubleFill(coord(session, geom, i*dim+j), dest);
	}

	return dest;
}

const char *
setMultiPoint(oracleSession *session, ora_geometry *geom, const char *data)
{
	unsigned i, j, numPoints;
	const unsigned dimension = ewkbDimension(session, geom);

	if (*((unsigned *)data) != MULTIPOINTTYPE)
		oracleError_i(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: expected multipoint, got type %u", *((unsigned *)data));
	data += sizeof(unsigned);
	numPoints = *((unsigned *)data);
	data += sizeof(unsigned);

	if (!numPoints)
		oracleError(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: empty multipoint is not supported");

	for (i=0; i<numPoints; i++)
	{
		if (*((unsigned *)data) != POINTTYPE)
			oracleError_i(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: expected point in multipoint, got type %u", *((unsigned *)data));
		data += sizeof(unsigned);
		if (*((unsigned *)data ) != 1)
			oracleError(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: empty point in multipoint is not supported");
		data += sizeof(unsigned);
		for (j=0; j<dimension; j++)
		{
			appendCoord(session, geom, *((double *)data));
			data += sizeof(double);
		}
	}
	appendElemInfo(session, geom, 1); /* start index + 1 */
	appendElemInfo(session, geom, 1); /* SDO_ETYPE point */
	appendElemInfo(session, geom, 1); /* SDO_INTERPRETATION no orientation */
	return data;
}

unsigned
ewkbMultiLineLen(oracleSession *session, ora_geometry *geom)
{
	const unsigned numLines = numElemInfo(session, geom)/3;
	return 2*sizeof(unsigned) + 2*sizeof(unsigned)*numLines + sizeof(double)*numCoord(session, geom);
}

char *
ewkbMultiLineFill(oracleSession *session, ora_geometry *geom, char * dest)
{
	unsigned i;
	const unsigned numC = numCoord(session, geom);
	const unsigned dimension = ewkbDimension(session, geom);
	const unsigned numLines = numElemInfo(session, geom)/3;
	dest = unsignedFill(MULTILINETYPE, dest);
	dest = unsignedFill(numLines, dest);
	for (i=0; i<numLines; i++)
	{
		unsigned j;
		const unsigned coord_b = elemInfo(session, geom, i*3) - 1;
		const unsigned coord_e = i+1 == numLines
								 ? numC
								 : elemInfo(session, geom, (i+1)*3) - 1;
		const unsigned numPoints = (coord_e - coord_b) / dimension;
		dest = unsignedFill(LINETYPE, dest);
		dest = unsignedFill(numPoints, dest);
		for (j=coord_b; j<coord_e; j++) dest = doubleFill(coord(session, geom, j), dest);
	}

	return dest;
}

const char *
setMultiLine(oracleSession *session, ora_geometry *geom, const char *data)
{
	unsigned r, numLines;
	if (*((unsigned *)data) != MULTILINETYPE)
		oracleError_i(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: expected multiline, got type %u", *((unsigned *)data));
	data += sizeof(unsigned);
	numLines = *((unsigned *)data);
	data += sizeof(unsigned);

	if (!numLines)
		oracleError(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: empty multiline is not supported");

	for (r=0; r<numLines; r++) data = setLine(session, geom, data);

	return data;
}

unsigned
ewkbMultiPolygonLen(oracleSession *session, ora_geometry *geom)
{
	/* polygons are padded, so the size detremination is a bit trickier */
	const unsigned totalNumRings = numElemInfo(session, geom)/3;
	unsigned numPolygon = 0;
	unsigned i, j;
	unsigned padding = 0;

	for (i = 0; i<totalNumRings; i++)
		numPolygon += elemInfo(session, geom, i*3+1) == 1003 ;

	for (i=0, j=0; i < numPolygon; i++)
	{
		unsigned numRings = 1;
		/* move j to the next ext ring, or the end */
		for (j++; j < totalNumRings && elemInfo(session, geom, j*3+1) != 1003; j++, numRings++);
		padding += numRings%2;

	}

	/* there is the type and the number of polygons, for each polygon the type
	 * and number of rings and
	 * for each ring the number of points and the padding
	 */
	return 2*sizeof(unsigned) +
		   + numPolygon*(2*sizeof(unsigned))
		   + (totalNumRings + padding)*(sizeof(unsigned))
		   + sizeof(double)*numCoord(session, geom);
}


char *
ewkbMultiPolygonFill(oracleSession *session, ora_geometry *geom, char * dest)
{
	const unsigned dimension = ewkbDimension(session, geom);
	const unsigned numC = numCoord(session, geom);
	const unsigned totalNumRings = numElemInfo(session, geom)/3;
	unsigned numPolygon = 0;
	unsigned i, j;

	for (i = 0; i<totalNumRings; i++)
		numPolygon += elemInfo(session, geom, i*3+1) == 1003 ;

	dest = unsignedFill(MULTIPOLYGONTYPE, dest);
	dest = unsignedFill(numPolygon, dest);

	for (i=0, j=0; i < numPolygon; i++)
	{
		unsigned end, k;
		unsigned numRings = 1;
		/* move j to the next ext ring, or the end */
		for (j++; j < totalNumRings && elemInfo(session, geom, j*3+1) != 1003; j++, numRings++);
		dest = unsignedFill(POLYGONTYPE, dest);
		dest = unsignedFill(numRings, dest);

		/*
		 * Reset j to be on the exterior ring of the current polygon
		 * and output rings number of points.
		 */
		for (end = j, j -= numRings; j<end; j++)
		{
			const unsigned coord_b = elemInfo(session, geom, j*3) - 1;
			const unsigned coord_e = j+1 == totalNumRings
									 ? numC
									 : elemInfo(session, geom, (j+1)*3) - 1;
			const unsigned numPoints = (coord_e - coord_b) / dimension;
			dest = unsignedFill(numPoints, dest);
		}

		if (numRings%2) dest = unsignedFill(0, dest); /* padding */

		for (end = j, j -= numRings; j<end; j++)
		{
			const unsigned coord_b = elemInfo(session, geom, j*3) - 1;
			const unsigned coord_e = j+1 == totalNumRings
									 ? numC
									 : elemInfo(session, geom, (j+1)*3) - 1;

			for (k=coord_b; k<coord_e; k++) dest = doubleFill(coord(session, geom, k), dest);
		}
	}
	return dest;
}

const char *
setMultiPolygon(oracleSession *session, ora_geometry *geom, const char *data)
{
	unsigned p, numPolygons;

	if (*((unsigned *)data) != MULTIPOLYGONTYPE)
		oracleError_i(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: expected multipolygon, got type %u", *((unsigned *)data));
	data += sizeof(unsigned);
	numPolygons = *((unsigned *)data);
	data += sizeof(unsigned);

	if (!numPolygons)
		oracleError(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: empty multipolygon is not supported");

	for (p=0; p<numPolygons; p++) data = setPolygon(session, geom, data);

	return data;
}

unsigned
numCoord(oracleSession *session, ora_geometry *geom)
{
	int n;

	if (checkerr(
			OCICollSize(session->envp->envhp,
						session->envp->errhp,
						(OCIColl *)(geom->geometry->sdo_ordinates),
						&n),
			session->envp->errhp) != OCI_SUCCESS)
		oracleError_d(FDW_ERROR, "cannot get size of ordinate collection", oraMessage);
	return n;
}

double
coord(oracleSession *session, ora_geometry *geom, unsigned i)
{
	double coord;
	boolean exists;
	OCINumber *oci_number;
	OCIInd *indicator;

	if (checkerr(
			OCICollGetElem(session->envp->envhp, session->envp->errhp,
				   		(OCIColl *)(geom->geometry->sdo_ordinates),
				   		(sb4)i,
				   		&exists,
				   		(dvoid **)&oci_number,
				   		(dvoid **)&indicator),
			session->envp->errhp) != OCI_SUCCESS)
		oracleError_d(FDW_ERROR, "error fetching element from ordinate collection", oraMessage);
	if (! exists)
		oracleError_i(FDW_ERROR, "element %u of ordinate collection does not exist", i);
	if (*indicator == OCI_IND_NULL)
		oracleError_i(FDW_ERROR, "element %u of ordinate collection is NULL", i);
	/* convert the element to double */
	numberToDouble(session->envp->errhp, oci_number, &coord);

	return coord;
}

unsigned
numElemInfo(oracleSession *session, ora_geometry *geom)
{
	int n;
	if (checkerr(
			OCICollSize (session->envp->envhp,
				 		session->envp->errhp,
				 		(OCIColl *)(geom->geometry->sdo_elem_info),
				 		&n),
			session->envp->errhp) != OCI_SUCCESS)
		oracleError_d(FDW_ERROR, "cannot get size of element info collection", oraMessage);
	return n;
}

unsigned
elemInfo(oracleSession *session, ora_geometry *geom, unsigned i)
{
	unsigned info;
	boolean exists;
	OCINumber *oci_number;
	OCIInd *indicator;

	if (checkerr(
			OCICollGetElem(session->envp->envhp, session->envp->errhp,
				   		(OCIColl *)(geom->geometry->sdo_elem_info),
				   		(sb4)i,
				   		&exists,
				   		(dvoid **)&oci_number,
				   		(dvoid **)&indicator),
			session->envp->errhp) != OCI_SUCCESS)
		oracleError_d(FDW_ERROR, "error fetching element from element info collection", oraMessage);
	if (! exists)
		oracleError_i(FDW_ERROR, "element %u of element info collection does not exist", i);
	if (*indicator == OCI_IND_NULL)
		oracleError_i(FDW_ERROR, "element %u of element info collection is NULL", i);
	numberToUint(session->envp->errhp, oci_number, &info);
	return info;
}

/*
 * checkerr
 * 		Call OCIErrorGet to get error message and error code.
 */
sword
checkerr(sword status, OCIError *handle)
{
	unsigned length;
	oraMessage[0] = '\0';

	if (status == OCI_SUCCESS_WITH_INFO || status == OCI_ERROR)
	{
		OCIErrorGet(handle, (ub4)1, NULL, &err_code,
			(text *)oraMessage, (ub4)ERRBUFSIZE, OCI_HTYPE_ERROR);
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
