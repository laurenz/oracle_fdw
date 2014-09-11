/*-------------------------------------------------------------------------
 *
 * oracle_gis.c
 * 		routines that convert between Oracle SDO_GEOMETRY and PostGIS EWKB
 *
 *-------------------------------------------------------------------------
 */

/* Oracle header */
#include <oci.h>

#include "oracle_fdw.h"

#include <string.h>

#define	UNKNOWNTYPE              0
#define	POINTTYPE                1
#define	LINETYPE                 2
#define	POLYGONTYPE              3
#define	MULTIPOINTTYPE           4
#define	MULTILINETYPE            5
#define	MULTIPOLYGONTYPE         6
#define	COLLECTIONTYPE           7
#define CIRCSTRINGTYPE           8
#define COMPOUNDTYPE             9
#define CURVEPOLYTYPE           10
#define MULTICURVETYPE          11
#define MULTISURFACETYPE        12
#define POLYHEDRALSURFACETYPE   13
#define TRIANGLETYPE            14
#define TINTYPE                 15

/* generated with OTT */
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

typedef struct
{
   OCINumber sdo_gtype;
   OCINumber sdo_srid;
   sdo_point_type sdo_point;
   OCIArray *sdo_elem_info;
   OCIArray *sdo_ordinates;
} sdo_geometry;

typedef struct
{
   OCIInd _atomic;
   OCIInd sdo_gtype;
   OCIInd sdo_srid;
   sdo_point_type_ind sdo_point;
   OCIInd sdo_elem_info;
   OCIInd sdo_ordinates;
} sdo_geometry_ind;

struct ora_geometry
{
	sdo_geometry geometry;
	sdo_geometry_ind indicator;
};

struct envEntry
{
	char *nls_lang;
	OCIEnv *envhp;
	OCIError *errhp;
	struct envEntry *next;
	struct srvEntry *srvlist;
};

unsigned ewkbType(oracleSession *session, ora_geometry *geom);
unsigned ewkbDimension(oracleSession *session, ora_geometry *geom);
unsigned ewkbSrid(oracleSession *session, ora_geometry *geom);
unsigned numCoord(oracleSession *session, ora_geometry *geom);
double coord(oracleSession *session, ora_geometry *geom, unsigned i);
unsigned numElemInfo(oracleSession *session, ora_geometry *geom);
unsigned elemInfo(oracleSession *session, ora_geometry *geom, unsigned i);
unsigned ewkbHeaderLen(oracleSession *session, ora_geometry *geom);
void ewkbHeaderFill(oracleSession *session, ora_geometry *geom, char * dest);
unsigned ewkbPointLen(oracleSession *session, ora_geometry *geom);
void ewkbPointFill(oracleSession *session, ora_geometry *geom, char *dest);
unsigned ewkbLineLen(oracleSession *session, ora_geometry *geom);
void ewkbLineFill(oracleSession *session, ora_geometry *geom, char * dest);
unsigned ewkbPolygonLen(oracleSession *session, ora_geometry *geom);
void ewkbPolygonFill(oracleSession *session, ora_geometry *geom, char * dest);
unsigned ewkbGeomLen(oracleSession *session, ora_geometry *geom);
void ewkbGeomFill(oracleSession *session, ora_geometry *geom, char * dest);

/*
 * ewkbToGeom
 * 		Creates an Oracle SDO_GEOMETRY from a PostGIS EWKB.
 * 		The result is a palloc'ed structure.
 */
ora_geometry *ewkbToGeom(oracleSession *session, ewkb *postgis_geom)
{
	return NULL;
}

/*
 * geomToEwkb
 * 		Creates a PostGIS EWKB from an Oracle SDO_GEOMETRY.
 * 		The result is a palloc'ed structure.
 */
ewkb *geomToEwkb(oracleSession *session, ora_geometry *geom)
{
#ifdef HEX_ENCODE
        const char *hexchr = "0123456789ABCDEF";
        unsigned numBytes = ewkbGeomLen(session, geom);
        unsigned i;
        char * data = (char *)oracleAlloc( 2*numBytes+1 );
        ewkbGeomFill(session, geom, data);
        data[2*numBytes] = '\0';
        for (i=numBytes-1; i>=0; i--)
        {
            data[2*i] = hexchr[(uint8_t)data[i] >> 4];
            data[2*i+1] = hexchr[(uint8_t)data[i] & 0x0F];
        }
        return (ewkb *)data;
#else
        char * data = (char *)oracleAlloc( ewkbGeomLen(session, geom) );
        ewkbGeomFill(session, geom, data);
        return (ewkb *)data;
#endif
}

unsigned ewkbType(oracleSession *session, ora_geometry *geom)
{
    int gtype = 0;
    if (geom->indicator.sdo_gtype == OCI_IND_NOTNULL) {
        OCINumberToInt (
            session->envp->errhp,
            &(geom->geometry.sdo_gtype),
            (uword) sizeof (int),
            OCI_NUMBER_SIGNED,
            (dvoid *) &gtype);
    }
    switch (gtype%1000)
    {
    case 1: return POINTTYPE;
    case 2: return LINETYPE;
    case 3: return POLYGONTYPE;
    case 4: return COLLECTIONTYPE;
    case 5: return MULTIPOINTTYPE;
    case 6: return MULTILINETYPE;
    case 7: return MULTIPOLYGONTYPE;
    default: return UNKNOWNTYPE;
    }
}


/* Header contains:
 * - char indianess 0/1 -> big/little
 * - unsigned type, with additionnal flag to know if srid is specified
 * - unsigned srid, IF NEEDED
 */
unsigned ewkbHeaderLen(oracleSession *session, ora_geometry *geom)
{
    return 1 + sizeof(unsigned) + (ewkbSrid(session, geom) != 0 ? sizeof(unsigned) : 0 );
}

void ewkbHeaderFill(oracleSession *session, ora_geometry *geom, char * dest)
{
    unsigned wkbType = ewkbType(session, geom);
    unsigned srid = ewkbSrid(session, geom);
    if (srid) wkbType |= 0x20000000;
    if (3 == ewkbDimension(session, geom)) wkbType |= 0x80000000;

#ifdef BIG_INDIAN
    dest[0] = 0 ;
#else
    dest[0] = 1 ;
#endif
    memcpy(dest+1, &wkbType, sizeof(unsigned));
    if (srid) memcpy(dest+1+sizeof(unsigned), &srid, sizeof(unsigned));
}


unsigned ewkbDimension(oracleSession *session, ora_geometry *geom)
{
    int gtype = 0;
    if (geom->indicator.sdo_gtype == OCI_IND_NOTNULL) {
        OCINumberToInt (
            session->envp->errhp,
            &(geom->geometry.sdo_gtype),
            (uword) sizeof (int),
            OCI_NUMBER_SIGNED,
            (dvoid *) &gtype);
    }
    return gtype / 1000;
}

unsigned ewkbSrid(oracleSession *session, ora_geometry *geom)
{
    int srid = 0;
    if (geom->indicator.sdo_srid == OCI_IND_NOTNULL) {
        OCINumberToInt (
            session->envp->errhp,
            &(geom->geometry.sdo_srid),
            (uword) sizeof (int),
            OCI_NUMBER_SIGNED,
            (dvoid *) & srid);
    }
    /* TODO convert oracle->postgis SRID when needed*/
    return srid;
}

unsigned ewkbPointLen(oracleSession *session, ora_geometry *geom)
{
    return sizeof(double)*ewkbDimension(session, geom);
}

void ewkbPointFill(oracleSession *session, ora_geometry *geom, char *dest)
{
    if (geom->indicator.sdo_point.x == OCI_IND_NOTNULL)
        OCINumberToReal( session->envp->errhp,
                         &(geom->geometry.sdo_point.x),
                         (uword)sizeof(double),
                         (dvoid *)&dest[0]);
    if (geom->indicator.sdo_point.y == OCI_IND_NOTNULL)
        OCINumberToReal( session->envp->errhp,
                         &(geom->geometry.sdo_point.y),
                         (uword)sizeof(double),
                         (dvoid *)&dest[sizeof(double)]);
    if (3 == ewkbDimension(session, geom))
    {
        if (geom->indicator.sdo_point.z == OCI_IND_NOTNULL)
            OCINumberToReal( session->envp->errhp,
                             &(geom->geometry.sdo_point.z),
                             (uword)sizeof(double),
                             (dvoid *)&dest[2*sizeof(double)]);
    }
}

unsigned ewkbLineLen(oracleSession *session, ora_geometry *geom)
{
    return sizeof(double)*numCoord(session, geom);
}

void ewkbLineFill(oracleSession *session, ora_geometry *geom, char * dest)
{
    const unsigned num = numCoord(session, geom);
    unsigned i;
    memcpy(dest, &num, sizeof(unsigned));
    dest += sizeof(unsigned);
    for (i=0; i<num; i++)
    {
        const double x = coord(session, geom, i);
        memcpy(dest, &x, sizeof(double));
        dest += sizeof(double);
    }
}

unsigned ewkbPolygonLen(oracleSession *session, ora_geometry *geom)
{
    const unsigned numRings = numElemInfo(session, geom)/3;
    return (numRings+1)*sizeof(unsigned)
        + sizeof(double)*numCoord(session, geom);
}

void ewkbPolygonFill(oracleSession *session, ora_geometry *geom, char * dest)
{
    const unsigned numRings = numElemInfo(session, geom)/3;
    const unsigned dimension = ewkbDimension(session, geom);
    unsigned i;
    memcpy(dest, &numRings, sizeof(unsigned));
    dest += sizeof(unsigned);
    for (i=0; i<numRings; i++)
    {
        /* elem_info index start at 1, so -1 at the end*/
        const unsigned coord_b = elemInfo(session, geom, i*3) - 1; 
        const unsigned coord_e = i+1 == numRings
            ? numCoord(session, geom)
            : elemInfo(session, geom, (i+1)*3) - 1;
        const unsigned numPoints = (coord_e - coord_b) / dimension;
        unsigned j;

        memcpy(dest, &numPoints, sizeof(unsigned));
        dest += sizeof(unsigned);

        for (j = coord_b; j != coord_e; j++)
        {
            const double x = coord(session, geom, j);
            memcpy(dest, &x, sizeof(double));
            dest += sizeof(double);
        }
    }
}

unsigned numCoord(oracleSession *session, ora_geometry *geom)
{
    int n;
    OCICollSize(session->envp->envhp, 
                session->envp->errhp,
                (OCIColl *)(geom->geometry.sdo_ordinates), 
                &n);
    return n;
}

double coord(oracleSession *session, ora_geometry *geom, unsigned i)
{
    double coord;
    boolean exists;
    OCINumber *oci_number;
    OCICollGetElem(session->envp->envhp, session->envp->errhp,
                   (OCIColl *) (geom->geometry.sdo_ordinates),
                   (sb4)       i,
                   &exists,
                   (dvoid **)  &oci_number,
                   (dvoid **)  0);
    /* convert the element to double */
    OCINumberToReal(session->envp->errhp, oci_number,
                    (uword)sizeof(double),
                    (dvoid *)&coord);
    return coord;
}

unsigned numElemInfo(oracleSession *session, ora_geometry *geom)
{
    int n;
    OCICollSize (session->envp->envhp, 
                 session->envp->errhp, 
                 (OCIColl *)(geom->geometry.sdo_elem_info), 
                 &n);
    return n;
}

unsigned elemInfo(oracleSession *session, ora_geometry *geom, unsigned i)
{
    unsigned info;
    boolean exists;
    OCINumber *oci_number;
    OCICollGetElem(session->envp->envhp, session->envp->errhp,
                   (OCIColl *) (geom->geometry.sdo_elem_info),
                   (sb4)       i,
                   (boolean *) &exists,
                   (dvoid **)  &oci_number,
                   (dvoid **)  0);
    OCINumberToInt(session->envp->errhp, oci_number,
                   (uword)sizeof(int),
                   OCI_NUMBER_UNSIGNED,
                   (dvoid *)&info);
    return info;
}


unsigned ewkbGeomLen(oracleSession *session, ora_geometry *geom)
{
    switch (ewkbType(session, geom)) 
    {
    case POINTTYPE:              return ewkbHeaderLen(session, geom) + ewkbPointLen(session, geom);
    case LINETYPE:               return ewkbHeaderLen(session, geom) + ewkbLineLen(session, geom);
    case POLYGONTYPE:            return ewkbHeaderLen(session, geom) + ewkbPolygonLen(session, geom);
    //case MULTIPOINTTYPE:         return ewkbHeaderLen(session, geom) + ewkbMultiPointLen(session, geom);
    //case MULTILINETYPE:          return ewkbHeaderLen(session, geom) + ewkbMultiLineLen(session, geom);
    //case MULTIPOLYGONTYPE:       return ewkbHeaderLen(session, geom) + ewkbMultiPolygonLen(session, geom);
    default: return 0;
    }
}

void ewkbGeomFill(oracleSession *session, ora_geometry *geom, char * dest)
{
    const unsigned headerLength = ewkbHeaderLen(session, geom);
    ewkbHeaderFill(session, geom, dest);
    switch (ewkbType(session, geom)) 
    {
    case POINTTYPE: ewkbPointFill(session, geom, dest+headerLength); break;
    case LINETYPE: ewkbLineFill(session, geom, dest+headerLength); break;
    case POLYGONTYPE: ewkbPolygonFill(session, geom, dest+headerLength); break;
    //case MULTIPOINTTYPE: ewkbMultiPointFill(session, geom, dest+headerLength); break;
    //case MULTILINETYPE: ewkbMultiLineFill(session, geom, dest+headerLength); break;
    //case MULTIPOLYGONTYPE: ewkbMultiPolygonFill(session, geom, dest+headerLength); break;
    }
}
