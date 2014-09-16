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
#include <assert.h>

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

#define WKBSRIDFLAG 0x20000000
#define WKBZOFFSET  0x80000000
#define WKBMOFFSET  0x40000000

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
char *ringFill(oracleSession *session, ora_geometry *geom, char * dest, unsigned ringIdx);

/* All ...Fill() functions return a pointer to the end of the written zone */
unsigned ewkbHeaderLen(oracleSession *session, ora_geometry *geom);
char *ewkbHeaderFill(oracleSession *session, ora_geometry *geom, char * dest);
unsigned ewkbPointLen(oracleSession *session, ora_geometry *geom);
char *ewkbPointFill(oracleSession *session, ora_geometry *geom, char *dest);
unsigned ewkbLineLen(oracleSession *session, ora_geometry *geom);
char *ewkbLineFill(oracleSession *session, ora_geometry *geom, char * dest);
unsigned ewkbPolygonLen(oracleSession *session, ora_geometry *geom);
char *ewkbPolygonFill(oracleSession *session, ora_geometry *geom, char * dest);
unsigned ewkbMultiPointLen(oracleSession *session, ora_geometry *geom);
char *ewkbMultiPointFill(oracleSession *session, ora_geometry *geom, char *dest);
unsigned ewkbMultiLineLen(oracleSession *session, ora_geometry *geom);
char *ewkbMultiLineFill(oracleSession *session, ora_geometry *geom, char * dest);
unsigned ewkbMultiPolygonLen(oracleSession *session, ora_geometry *geom);
char *ewkbMultiPolygonFill(oracleSession *session, ora_geometry *geom, char * dest);
unsigned ewkbGeomLen(oracleSession *session, ora_geometry *geom);
char *ewkbGeomFill(oracleSession *session, ora_geometry *geom, char * dest);

const char *setType(oracleSession *session, ora_geometry *geom, const char *data);
const char *setSrid(oracleSession *session, ora_geometry *geom, const char *data);
const char *setPoint(oracleSession *session, ora_geometry *geom, const char *data);
const char *setLine(oracleSession *session, ora_geometry *geom, const char *data);
const char *setPolygon(oracleSession *session, ora_geometry *geom, const char *data);
void appendElemInfo(oracleSession *session, ora_geometry *geom, int info );
void appendCoord(oracleSession *session,  ora_geometry *geom, double coord);

/*
 * ewkbToGeom
 * 		Creates an Oracle SDO_GEOMETRY from a PostGIS EWKB.
 * 		The result is a palloc'ed structure.
 */
ora_geometry *ewkbToGeom(oracleSession *session, ewkb *postgis_geom)
{
    const char * data = postgis_geom->data;
    ora_geometry *geom = (ora_geometry *)oracleAlloc(sizeof(ora_geometry));
    
#ifdef BIG_INDIAN
    assert( *data == 0 );
#else
    assert( *data == 1 );
#endif
    ++data;

    data = setType(session, geom, data);
    data = setSrid(session, geom, data);

    switch ( ewkbType(session, geom) ) 
    {
    case POINTTYPE:        data = setPoint(session, geom, data); break;
    case LINETYPE:         data = setLine(session, geom, data); break;
    case POLYGONTYPE:      data = setPolygon(session, geom, data); break;
    //case MULTIPOINTTYPE:   data = setMultiPoint(session, geom, data); break;
    //case MULTILINETYPE:    data = setMultiLine(session, geom, data); break;
    //case MULTIPOLYGONTYPE: data = setMultiPolygon(session, geom, data); break;
    default: assert(0);
    }

    // check that we reached the end of input data
    assert( data - postgis_geom->data == postgis_geom->length );

    return geom;
}

/*
 * geomToEwkb
 * 		Creates a PostGIS EWKB from an Oracle SDO_GEOMETRY.
 * 		The result is a palloc'ed structure.
 */
ewkb *geomToEwkb(oracleSession *session, ora_geometry *geom)
{
    char * data = (char *)oracleAlloc( ewkbGeomLen(session, geom) );
    ewkbGeomFill(session, geom, data);
    return (ewkb *)data;
}

void appendElemInfo(oracleSession *session, ora_geometry *geom, int info )
{
    OCINumber n;
    OCINumberFromInt (
        session->envp->errhp,
        (CONST dvoid *) &info,
        (uword) sizeof(int),
        OCI_NUMBER_SIGNED,
        &n);

    OCICollAppend( session->envp->envhp, 
                   session->envp->errhp,
                  (CONST dvoid*) &n, 
                  NULL,
                  geom->geometry.sdo_elem_info );
}

void appendCoord(oracleSession *session, ora_geometry *geom, double coord )
{
    OCINumber n;
    OCINumberFromReal (
        session->envp->errhp,
        (const dvoid*)&coord,
        (uword) sizeof(double),
        &n);

    OCICollAppend( session->envp->envhp, 
                   session->envp->errhp,
                   (CONST dvoid*) &n, 
                   NULL,
                   geom->geometry.sdo_ordinates );
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

// srid indicator is also set
const char *setType(oracleSession *session, ora_geometry *geom, const char * data)
{
    const unsigned wkbType =  *((unsigned *)data) & 0x0FFFFFFF;
    int gtype = *((unsigned *)data) & WKBZOFFSET ? 3000 : 2000;
    assert(! (*((unsigned *)data) & WKBMOFFSET )); // M not supported
    switch (wkbType)
    {
    case POINTTYPE:        gtype += 1; break;
    case LINETYPE:         gtype += 2; break;
    case POLYGONTYPE:      gtype += 3; break;
    case COLLECTIONTYPE:   gtype += 4; break;
    case MULTIPOINTTYPE:   gtype += 5; break;
    case MULTILINETYPE:    gtype += 6; break;
    case MULTIPOLYGONTYPE: gtype += 7; break;
    default: assert(0);
    }

    geom->indicator.sdo_gtype = OCI_IND_NOTNULL;
    OCINumberFromInt (
        session->envp->errhp,
        (dvoid *) &gtype,
        (uword) sizeof (int),
        OCI_NUMBER_SIGNED,
        &(geom->geometry.sdo_gtype));

    geom->indicator.sdo_srid =  *((unsigned *)data) &  WKBSRIDFLAG
        ? OCI_IND_NOTNULL
        : OCI_IND_NULL;

    data += sizeof(unsigned);
    return data;
}



/* Header contains:
 * - char indianess 0/1 -> big/little
 * - unsigned type, with additionnal flag to know if srid is specified
 * - unsigned srid, IF NEEDED
 */
unsigned ewkbHeaderLen(oracleSession *session, ora_geometry *geom)
{
    return 1 + sizeof(unsigned) + ( 0 != ewkbSrid(session, geom) ? sizeof(unsigned) : 0 );
}

char *ewkbHeaderFill(oracleSession *session, ora_geometry *geom, char * dest)
{
    unsigned wkbType = ewkbType(session, geom);
    unsigned srid = ewkbSrid(session, geom);
    if (srid) wkbType |= WKBSRIDFLAG;
    if (3 == ewkbDimension(session, geom)) wkbType |= WKBZOFFSET;

#ifdef BIG_INDIAN
    dest[0] = 0 ;
#else
    dest[0] = 1 ;
#endif
    dest += 1;
    memcpy(dest, &wkbType, sizeof(unsigned));
    dest += sizeof(unsigned);

    if ( 0 != srid ) 
    {
        memcpy(dest, &srid, sizeof(unsigned));
        dest += sizeof(unsigned);
    }
    return dest;
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

const char *setSrid(oracleSession *session, ora_geometry *geom, const char *data)
{
    /* TODO convert oracle->postgis SRID when needed*/
    const int srid = *((unsigned *)data);
    if (geom->indicator.sdo_srid == OCI_IND_NOTNULL)
    {
        OCINumberFromInt (
            session->envp->errhp,
            (dvoid *)&srid,
            (uword) sizeof (unsigned),
            OCI_NUMBER_SIGNED,
            &(geom->geometry.sdo_srid));
        data += sizeof(unsigned);
    }
    return data;
}

unsigned ewkbPointLen(oracleSession *session, ora_geometry *geom)
{
    return sizeof(double)*ewkbDimension(session, geom);
}

char *ewkbPointFill(oracleSession *session, ora_geometry *geom, char *dest)
{
    if (geom->indicator.sdo_point.x == OCI_IND_NOTNULL)
        OCINumberToReal( session->envp->errhp,
                         &(geom->geometry.sdo_point.x),
                         (uword)sizeof(double),
                         (dvoid *)dest);
    dest += sizeof(double);
    if (geom->indicator.sdo_point.y == OCI_IND_NOTNULL)
        OCINumberToReal( session->envp->errhp,
                         &(geom->geometry.sdo_point.y),
                         (uword)sizeof(double),
                         (dvoid *)dest);
    dest += sizeof(double);
    if (3 == ewkbDimension(session, geom))
    {
        if (geom->indicator.sdo_point.z == OCI_IND_NOTNULL)
            OCINumberToReal( session->envp->errhp,
                             &(geom->geometry.sdo_point.z),
                             (uword)sizeof(double),
                             (dvoid *)dest);
        dest += sizeof(double);
    }
    return dest;
}

const char *setPoint(oracleSession *session, ora_geometry *geom, const char *data)
{
    geom->indicator.sdo_point.x = OCI_IND_NOTNULL;
    OCINumberFromReal( session->envp->errhp,
                     (dvoid *)data,
                     (uword)sizeof(double),
                     &(geom->geometry.sdo_point.x));
    data += sizeof(double);
    geom->indicator.sdo_point.y = OCI_IND_NOTNULL;
    OCINumberFromReal( session->envp->errhp,
                     (dvoid *)data,
                     (uword)sizeof(double),
                     &(geom->geometry.sdo_point.y));
    data += sizeof(double);
    if (3 == ewkbDimension(session, geom))
    {
        geom->indicator.sdo_point.z = OCI_IND_NOTNULL;
        OCINumberFromReal( session->envp->errhp,
                         (dvoid *)data,
                         (uword)sizeof(double),
                         &(geom->geometry.sdo_point.z));
        data += sizeof(double);
    }
    return data;
}

unsigned ewkbLineLen(oracleSession *session, ora_geometry *geom)
{
    return sizeof(unsigned) + sizeof(double)*numCoord(session, geom);
}

char *ewkbLineFill(oracleSession *session, ora_geometry *geom, char * dest)
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
    return dest;
}

const char *setLine(oracleSession *session, ora_geometry *geom, const char *data)
{
    unsigned i;
    const unsigned n = *((unsigned *)data) * ewkbDimension(session, geom);
    data += sizeof(unsigned);
    for (i=0; i<n; i++)
    {
        appendCoord(session, geom, *((double *)data));
        data += sizeof(double);
    }
    appendElemInfo(session, geom, 1); // start index + 1
    appendElemInfo(session, geom, 2); // SDO_ETYPE linestring
    appendElemInfo(session, geom, 1); // SDO_INTERPRETATION straight line segments
    return data;
}
unsigned ewkbPolygonLen(oracleSession *session, ora_geometry *geom)
{
    const unsigned numRings = numElemInfo(session, geom)/3;
    /* there is the number of rings, and, for each ring the number of points */ 
    return (numRings+1)*sizeof(unsigned)
        + sizeof(double)*numCoord(session, geom);
}

char *ringFill(oracleSession *session, ora_geometry *geom, char * dest, unsigned ringIdx)
{
    /* elem_info index start at 1, so -1 at the end*/
    const unsigned numRings = numElemInfo(session, geom)/3;
    const unsigned dimension = ewkbDimension(session, geom);
    const unsigned coord_b = elemInfo(session, geom, ringIdx*3) - 1; 
    const unsigned coord_e = ringIdx+1 == numRings
        ? numCoord(session, geom)
        : elemInfo(session, geom, (ringIdx+1)*3) - 1;
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
    return dest;
}

const char *setPolygon(oracleSession *session, ora_geometry *geom, const char *data)
{
    unsigned r, i;
    const unsigned dim = ewkbDimension(session, geom);
    const unsigned numRings = *((unsigned *)data);
    data += sizeof(unsigned);
    for (r=0; r<numRings; r++)
    {
        const unsigned n= *((unsigned *)data) * dim;
        data += sizeof(unsigned);

        appendElemInfo(session, geom, numCoord(session, geom) + 1); // start index + 1
        appendElemInfo(session, geom, r == 0 ? 1003 : 2003); // SDO_ETYPE ext ring or int ring
        appendElemInfo(session, geom, 1); // SDO_INTERPRETATION straight line segments
        
        for (i=0; i<n; i++)
        {
            appendCoord(session, geom, *((double *)data));
            data += sizeof(double);
        }
    }

    return data;
}

char *ewkbPolygonFill(oracleSession *session, ora_geometry *geom, char * dest)
{
    const unsigned numRings = numElemInfo(session, geom)/3;
    unsigned i;
    memcpy(dest, &numRings, sizeof(unsigned));
    dest += sizeof(unsigned);
    for (i=0; i<numRings; i++)
        dest = ringFill(session, geom, dest, i);
    return dest;
}

unsigned ewkbMultiPointLen(oracleSession *session, ora_geometry *geom)
{
    /* same code as Line */
    return ewkbLineLen(session, geom);
}

char *ewkbMultiPointFill(oracleSession *session, ora_geometry *geom, char * dest)
{
    /* same code as Line */
    return ewkbLineFill(session, geom, dest);
}

unsigned ewkbMultiLineLen(oracleSession *session, ora_geometry *geom)
{
    /* same code as Line */
    return ewkbPolygonLen(session, geom);
}

char *ewkbMultiLineFill(oracleSession *session, ora_geometry *geom, char * dest)
{
    /* same code as Line */
    return ewkbPolygonFill(session, geom, dest);
}

unsigned ewkbMultiPolygonLen(oracleSession *session, ora_geometry *geom)
{
    const unsigned numRings = numElemInfo(session, geom)/3;
    unsigned numPolygon = 0;
    unsigned i;
    for (i = 0; i<numRings; i++)
        numPolygon += elemInfo(session, geom, i*3+1) == 1003 ;

    /* there is the number of polygons, for each polygon the number of rings and
     * for each ring the number of points */
    return (1+numPolygon+numRings)*sizeof(unsigned)
        + sizeof(double)*numCoord(session, geom);
}


char *ewkbMultiPolygonFill(oracleSession *session, ora_geometry *geom, char * dest)
{
    const unsigned totalNumRings = numElemInfo(session, geom)/3;
    unsigned numPolygon = 0;
    unsigned i,j;
    for (i = 0; i<totalNumRings; i++)
        numPolygon += elemInfo(session, geom, i*3+1) == 1003 ;
    memcpy(dest, &numPolygon, sizeof(unsigned));
    dest += sizeof(unsigned);

    for (i=0, j=0; i < numPolygon; i++)
    {
        unsigned numRings = 1;
        /* move j to the next ext ring, or the end */
        for (j++; j < totalNumRings && elemInfo(session, geom, j*3+1) != 1003; j++, numRings++);
        memcpy(dest, &numRings, sizeof(unsigned));
        dest += sizeof(unsigned);

        /* reset j to be on the exterior ring of the current polygon 
         * and output rings */
        j -= numRings; 

        dest = ringFill(session, geom, dest, j);
        for (j++; j < totalNumRings && elemInfo(session, geom, j*3+1) != 1003; j++)
            dest = ringFill(session, geom, dest, j);
    }
    return dest;
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
                   &exists,
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
    case MULTIPOINTTYPE:         return ewkbHeaderLen(session, geom) + ewkbMultiPointLen(session, geom);
    case MULTILINETYPE:          return ewkbHeaderLen(session, geom) + ewkbMultiLineLen(session, geom);
    case MULTIPOLYGONTYPE:       return ewkbHeaderLen(session, geom) + ewkbMultiPolygonLen(session, geom);
    default: return 0;
    }
}

char* ewkbGeomFill(oracleSession *session, ora_geometry *geom, char * dest)
{
    dest = ewkbHeaderFill(session, geom, dest);
    switch (ewkbType(session, geom)) 
    {
    case POINTTYPE: ewkbPointFill(session, geom, dest); break;
    case LINETYPE: ewkbLineFill(session, geom, dest); break;
    case POLYGONTYPE: ewkbPolygonFill(session, geom, dest); break;
    case MULTIPOINTTYPE: ewkbMultiPointFill(session, geom, dest); break;
    case MULTILINETYPE: ewkbMultiLineFill(session, geom, dest); break;
    case MULTIPOLYGONTYPE: ewkbMultiPolygonFill(session, geom, dest); break;
    }
    return dest;
}
