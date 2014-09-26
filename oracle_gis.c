/*-------------------------------------------------------------------------
 *
 * oracle_gis.c
 * 		routines that convert between Oracle SDO_GEOMETRY and PostGIS EWKB
 *
 *-------------------------------------------------------------------------
 */

/* Oracle header */
#include <oci.h>

#include <string.h>
#include <stdint.h>
#include <stdio.h>

#include "oracle_fdw.h"

#define UNKNOWNTYPE			   0
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

#define intToNumber(errhp, intp, flag, numberp) { \
	if (checkerr( \
			OCINumberFromInt((errhp), (dvoid *)(intp), sizeof(int), (flag), (numberp)), \
			(errhp)) != OCI_SUCCESS) \
		oracleError_i(FDW_ERROR, "OCINumberFromInt failed to convert integer %d to NUMBER", *(intp)); \
}

#define numberToInt(errhp, numberp, flag, intp) { \
	if (checkerr( \
			OCINumberToInt((errhp), (numberp), sizeof (int), (flag), (dvoid *)(intp)), \
			(errhp)) != OCI_SUCCESS) \
		oracleError(FDW_ERROR, "OCINumberToInt failed to convert NUMBER to integer"); \
}

#define doubleToNumber(errhp, doublep, numberp) { \
	if (checkerr( \
			OCINumberFromReal((errhp), (const dvoid *)(doublep), sizeof(double), (numberp)), \
			(errhp)) != OCI_SUCCESS) \
		oracleError(FDW_ERROR, "OCINumberFromReal failed to convert floating point number to NUMBER"); \
}

#define numberToDouble(errhp, numberp, doublep) { \
	if (checkerr( \
			OCINumberToReal((errhp), (numberp), sizeof(double), (dvoid *)(doublep)), \
			(errhp)) != OCI_SUCCESS) \
		oracleError(FDW_ERROR, "OCINumberToReal failed to convert NUMBER to floating point number"); \
}

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

/* All ...Fill() functions return a pointer to the end of the written zone */
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

static const char *setType(oracleSession *session, ora_geometry *geom, const char *data);
static const char *setSridAndFlags(oracleSession *session, ora_geometry *geom, const char *data);
static const char *setPoint(oracleSession *session, ora_geometry *geom, const char *data);
static const char *setLine(oracleSession *session, ora_geometry *geom, const char *data);
static const char *setPolygon(oracleSession *session, ora_geometry *geom, const char *data);
static const char *setMultiPoint(oracleSession *session, ora_geometry *geom, const char *data);
static const char *setMultiLine(oracleSession *session, ora_geometry *geom, const char *data);
static const char *setMultiPolygon(oracleSession *session, ora_geometry *geom, const char *data);
static void appendElemInfo(oracleSession *session, ora_geometry *geom, int info );
static void appendCoord(oracleSession *session,  ora_geometry *geom, double coord);
static char *doubleFill(double x, char * dest);
static char *unsignedFill(unsigned i, char * dest);
static sword checkerr(sword status, OCIError *handle);
unsigned epsgFromOracle( unsigned srid );

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

unsigned epsgFromOracle( unsigned srid )
{
    /* Oracle SRID, EPSG GCS/PCS Code */
    switch (srid)
    {
    case 8192: return 4326; // WGS84
    case 8306: return 4322; // WGS72
    case 8267: return 4269; // NAD83
    case 8274: return 4277; // OSGB 36
    case 81989: return 27700; // UK National Grid
    }
    return srid;

};


/*
 * oracleEWKBToGeom
 * 		Creates an Oracle SDO_GEOMETRY from a PostGIS EWKB.
 * 		The result is a partially palloc'ed structure.
 * 		Zero length or a NULL pointer for ewkb_data yield an atomically NULL object.
 */
ora_geometry *
oracleEWKBToGeom(oracleSession *session, unsigned int ewkb_length, char *ewkb_data)
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
	 * We don't move the data pointer, so we can check it in
	 * set functions and reuse the same function
	 * for collections (i.e. multi*)
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
			oracleError(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: unknown geometry type");
	}

	/* check that we reached the end of input data */
	{
		char msg[1000];
		sprintf( msg, " data - ewkb_data = %lu, ewkb_length = %u ",
			data - ewkb_data, ewkb_length);
		oracleDebug2(msg);
	}

	if (data - ewkb_data != ewkb_length)
		oracleError_i(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: number of bytes read is different from length %u", ewkb_length);

	return geom;
}

/*
 * oracleGetEWKBLen
 * 		Returns the length in bytes needed to store an EWKB conversion of "geom".
 */
unsigned int
oracleGetEWKBLen(oracleSession *session, ora_geometry *geom)
{
	unsigned int type;

	/* return zero length for atomically NULL objects */
	if (geom->indicator->_atomic == OCI_IND_NULL)
		return 0;

	switch ((type = ewkbType(session, geom)))
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
			oracleError_i(FDW_ERROR, "error converting SDO_GEOMETRY to geometry: unknown type %u", type);
			return 0;  /* unreachable, but keeps compiler happy */
	}
}

/*
 * oracleFillEWKB
 * 		Converts "geom" to an EWKB and stores the result in "dest".
 */
char *
oracleFillEWKB(oracleSession *session, ora_geometry *geom, unsigned int size, char *dest)
{
	const char *orig = dest;
	unsigned int type;

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
			oracleError_i(FDW_ERROR, "error converting SDO_GEOMETRY to geometry: unknown type %u", type);
	}

	{
		char msg[1000];
		sprintf( msg, " dest - orig = %lu, len = %u ",
			dest - orig, size );
		oracleDebug2(msg);
	}

	if (dest - orig != size)
		oracleError_i(FDW_ERROR, "error converting SDO_GEOMETRY to geometry: number of bytes written is different from size %u", size);

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
		oracleError(FDW_ERROR, "cannot allocate SDO_GEOMETRY object");

	/* get the NULL indicator */
	if (checkerr(
			OCIObjectGetInd(session->envp->envhp,
							session->envp->errhp,
							geom->geometry,
							(void **)&geom->indicator),
			session->envp->errhp) != OCI_SUCCESS)
		oracleError(FDW_ERROR, "cannot get indicator for new SDO_GEOMETRY object");
		
	/* initialize as atomic NULL */
	geom->indicator->_atomic = OCI_IND_NULL;
}

void
appendElemInfo(oracleSession *session, ora_geometry *geom, int info)
{
	OCINumber n;
	intToNumber(session->envp->errhp, &info, OCI_NUMBER_SIGNED, &n);
	{
		char msg[1000];
		sprintf(msg, "elem info %d", info);
		oracleDebug2(msg);
	}

	if (checkerr(
			OCICollAppend(session->envp->envhp,
				   		session->envp->errhp,
				   		(CONST dvoid*) &n,
				   		NULL,
				   		geom->geometry->sdo_elem_info),
			session->envp->errhp) != OCI_SUCCESS)
		oracleError(FDW_ERROR, "cannot append to element info collection");
}

void
appendCoord(oracleSession *session, ora_geometry *geom, double coord)
{
	OCINumber n;

	doubleToNumber(session->envp->errhp, &coord, &n);
	{
		char msg[1000];
		sprintf(msg, "coord %f", coord);
		oracleDebug2(msg);
	}
	if (checkerr(
			OCICollAppend(session->envp->envhp,
				   		session->envp->errhp,
				   		(CONST dvoid*) &n,
				   		NULL,
				   		geom->geometry->sdo_ordinates),
			session->envp->errhp) != OCI_SUCCESS)
		oracleError(FDW_ERROR, "cannot append to ordinate collection");
}

unsigned int
ewkbType(oracleSession *session, ora_geometry *geom)
{
	int gtype = 0;
	if (geom->indicator->sdo_gtype == OCI_IND_NOTNULL)
		numberToInt(session->envp->errhp, &(geom->geometry->sdo_gtype), OCI_NUMBER_SIGNED, &gtype);

	switch (gtype%1000)
	{
		case 1:
			return POINTTYPE;
		case 2:
			return LINETYPE;
		case 3:
			return POLYGONTYPE;
		case 4:
			return COLLECTIONTYPE;
		case 5:
			return MULTIPOINTTYPE;
		case 6:
			return MULTILINETYPE;
		case 7:
			return MULTIPOLYGONTYPE;
                case 8:
                        oracleError(FDW_ERROR, "error converting SDO_GEOMETRY to geometry: oracle SOLID not supported");
                case 9:
                        oracleError(FDW_ERROR, "error converting SDO_GEOMETRY to geometry: oracle MULTISOLID not supported");
		default:
			return UNKNOWNTYPE;
	}
}

const char *
setType(oracleSession *session, ora_geometry *geom, const char * data)
{
	const unsigned wkbType =  *((unsigned *)data);
	unsigned gtype;

	numberToInt(session->envp->errhp, &(geom->geometry->sdo_gtype), OCI_NUMBER_SIGNED, &gtype);

	data += sizeof(unsigned);

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
		case COLLECTIONTYPE:
			gtype += 4;
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
		default:
			oracleError_i(FDW_ERROR, "error converting SDO_GEOMETRY to geometry: unknown geometry type %u", wkbType);
	}

	geom->indicator->sdo_gtype = OCI_IND_NOTNULL;
	intToNumber(session->envp->errhp, &gtype, OCI_NUMBER_UNSIGNED, &(geom->geometry->sdo_gtype));

	return data;
}



/*
 * Header contains:
 * - srid : 3 bytes
 * - flags : 1 byte
 */
unsigned int
ewkbHeaderLen(oracleSession *session, ora_geometry *geom)
{
	return 4;
}

char *
ewkbHeaderFill(oracleSession *session, ora_geometry *geom, char * dest)
{
	const unsigned int srid = ewkbSrid(session, geom);
	const uint8_t flags = ((3 == ewkbDimension(session, geom)) ? 0x01 : 0x00 );
	uint8_t s[3];

	s[0] = (srid & 0x001F0000) >> 16;
	s[1] = (srid & 0x0000FF00) >> 8;
	s[2] = (srid & 0x000000FF);

	memcpy(dest, s, 3);
	dest += 3;
	memcpy(dest, &flags, 1);
	dest += 1;
	return dest;
}


unsigned int
ewkbDimension(oracleSession *session, ora_geometry *geom)
{
	int gtype = 0;
	if (geom->indicator->sdo_gtype == OCI_IND_NOTNULL)
		numberToInt(session->envp->errhp, &(geom->geometry->sdo_gtype), OCI_NUMBER_SIGNED, &gtype);
	return gtype / 1000;
}

unsigned int
ewkbSrid(oracleSession *session, ora_geometry *geom)
{
	int srid = 0;
	if (geom->indicator->sdo_srid == OCI_IND_NOTNULL)
		numberToInt(session->envp->errhp, &(geom->geometry->sdo_srid), OCI_NUMBER_SIGNED, &srid);

	return epsgFromOracle(srid);
}

const char *
setSridAndFlags(oracleSession *session, ora_geometry *geom, const char *data)
{
	unsigned int srid = 0;
	unsigned int gtype = 0;

	srid |= ((uint8_t)data[0]) << 16;
	srid |= ((uint8_t)data[1]) << 8;
	srid |= ((uint8_t)data[2]);
	/*
	 * Only the first 21 bits are set. Slide up and back to pull
	 * the negative bits down, if we need them.
	 */
	srid = (srid<<11)>>11;

	data += 3;

	/* TODO convert oracle->postgis SRID when needed */

	geom->indicator->sdo_srid = (srid == 0) ? OCI_IND_NULL : OCI_IND_NOTNULL;

	if (geom->indicator->sdo_srid == OCI_IND_NOTNULL)
		intToNumber(session->envp->errhp, &srid, OCI_NUMBER_UNSIGNED, &(geom->geometry->sdo_srid));

	gtype = (((uint8_t)data[0]) & 0x01 ) ? 3000 : 2000; /* 3d/2d */
	if (data[0] & 0x02)
		oracleError(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: measure dimension not supported");
	if (data[0] & 0x08)
		oracleError(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: geodetic not supported");

	if (((uint8_t)data[0]) & 0x04) /* has bbox, offsets */
	{
		oracleDebug2("geometry has bounding box");
		data += 1 + 2*(((uint8_t)data[0]) & 0x01 ? 3 : 2)*sizeof(float);
	}
	else
	{
		data += 1;
	}

	geom->indicator->sdo_gtype = OCI_IND_NOTNULL;

	intToNumber(session->envp->errhp, &gtype, OCI_NUMBER_UNSIGNED, &(geom->geometry->sdo_gtype));

	return data;
}

unsigned int
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
	if (*((unsigned int *)data) != POINTTYPE)
		oracleError_i(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: expected point, got type %u", *((unsigned int *)data));
	data += sizeof(unsigned);
	if (*((unsigned int *)data ) != 1)
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

unsigned int
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

unsigned int
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
	ringSizeData = data;
	data += (numRings+numRings%2)*sizeof(unsigned);
	for (r=0; r<numRings; r++)
	{
		const unsigned n= *((unsigned *)ringSizeData) * dimension;
		ringSizeData += sizeof(unsigned);
		{
			char msg[1000];
			sprintf(msg, "output ring with %d points",*((unsigned *)(ringSizeData-sizeof(unsigned))));
			oracleDebug2(msg);
		}


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

unsigned int
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
	const unsigned dim =  ewkbDimension(session, geom);
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

	for (i=0; i<numPoints; i++)
	{
		if (*((unsigned *)data) != POINTTYPE)
			oracleError_i(FDW_ERROR, "error converting geometry to SDO_GEOMETRY: expected point in multipoint, got type %u", *((unsigned *)data));
		data += sizeof(unsigned);
		if (*((unsigned int *)data ) != 1)
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

unsigned int
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

	for (r=0; r<numLines; r++) data = setLine(session, geom, data);

	return data;
}

unsigned int
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

	for (p=0; p<numPolygons; p++) data = setPolygon(session, geom, data);

	return data;
}

unsigned int
numCoord(oracleSession *session, ora_geometry *geom)
{
	int n;

	if (checkerr(
			OCICollSize(session->envp->envhp,
						session->envp->errhp,
						(OCIColl *)(geom->geometry->sdo_ordinates),
						&n),
			session->envp->errhp) != OCI_SUCCESS)
		oracleError(FDW_ERROR, "cannot get size of ordinate collection");
	return n;
}

double
coord(oracleSession *session, ora_geometry *geom, unsigned i)
{
	double coord;
	boolean exists;
	OCINumber *oci_number;

	if (checkerr(
			OCICollGetElem(session->envp->envhp, session->envp->errhp,
				   		(OCIColl *)(geom->geometry->sdo_ordinates),
				   		(sb4)i,
				   		&exists,
				   		(dvoid **)&oci_number,
				   		(dvoid **)0),
			session->envp->errhp) != OCI_SUCCESS)
		oracleError_i(FDW_ERROR, "cannot get element %u from ordinate collection", i);
	if (! exists)
		oracleError_i(FDW_ERROR, "element %u of ordinate collection does not exist", i);
	/* convert the element to double */
	numberToDouble(session->envp->errhp, oci_number, &coord);
	{
		char msg[1000];
		sprintf(msg, "coord from geom %d %f", i, coord);
		oracleDebug2(msg);
	}
	return coord;
}

unsigned int
numElemInfo(oracleSession *session, ora_geometry *geom)
{
	int n;
	if (checkerr(
			OCICollSize (session->envp->envhp,
				 		session->envp->errhp,
				 		(OCIColl *)(geom->geometry->sdo_elem_info),
				 		&n),
			session->envp->errhp) != OCI_SUCCESS)
		oracleError(FDW_ERROR, "cannot get size of element info collection");
	return n;
}

unsigned int
elemInfo(oracleSession *session, ora_geometry *geom, unsigned i)
{
	unsigned info;
	boolean exists;
	OCINumber *oci_number;
	if (checkerr(
			OCICollGetElem(session->envp->envhp, session->envp->errhp,
				   		(OCIColl *)(geom->geometry->sdo_elem_info),
				   		(sb4)i,
				   		&exists,
				   		(dvoid **)&oci_number,
				   		(dvoid **)0),
			session->envp->errhp) != OCI_SUCCESS)
		oracleError_i(FDW_ERROR, "cannot get element %u from element info collection", i);
	if (! exists)
		oracleError_i(FDW_ERROR, "element %u of element info collection does not exist", i);
	numberToInt(session->envp->errhp, oci_number, OCI_NUMBER_UNSIGNED, &info);
	return info;
}

/*
 * checkerr
 * 		Call OCIErrorGet to get error message and error code.
 */
sword
checkerr(sword status, OCIError *handle)
{
	int length;
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
