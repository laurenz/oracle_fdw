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

#include "oracle_fdw.h"

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

#define _QUOTE(x) #x
#define QUOTE(x) _QUOTE(x)
#ifndef NDEBUG
#   define ORA_ASSERT( expr ) \
    ((expr) ? (void)0 : (void)assertionFailed( __FILE__":"QUOTE(__LINE__)" assertion ("#expr") failed" ) )
#else
#   define ORA_ASSERT( expr ) ((void(0))
#endif


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

struct sdo_geometry
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
static int assertionFailed(const char * msg);
char *doubleFill(double x, char * dest);
char *unsignedFill(unsigned i, char * dest);
void oracleAllocOrdinatesAndElemInfo(oracleSession *session, ora_geometry *geom);


char *doubleFill(double x, char * dest)
{
    memcpy(dest, &x, sizeof(double));
    dest += sizeof(double);
    return dest;
}

char *unsignedFill(unsigned i, char * dest)
{
    memcpy(dest, &i, sizeof(unsigned));
    dest += sizeof(unsigned);
    return dest;
}

int assertionFailed(const char * msg)
{
    oracleError( FDW_ERROR, msg );
    return 0;
}

void oracleAllocOrdinatesAndElemInfo(oracleSession *session, ora_geometry *geom)
{
    OCIType * tdo;
    OCITypeByName(session->envp->envhp, 
                  session->envp->errhp, 
                  session->connp->svchp, 
                  (const oratext *)"MDSYS",
                  strlen("MDSYS"), 
                  (const oratext *)"SDO_ORDINATE_ARRAY", 
                  strlen("SDO_ORDINATE_ARRAY"), 
                  NULL,
                  0,
                  OCI_DURATION_TRANS,
                  OCI_TYPEGET_ALL,
                  &tdo );
    OCIObjectNew( session->envp->envhp, 
                  session->envp->errhp, 
                  session->connp->svchp, 
                  OCI_TYPECODE_VARRAY,
                  tdo, 
                  (dvoid *)NULL, 
                  OCI_DURATION_TRANS,
                  FALSE, 
                  (dvoid **)geom->geometry->sdo_ordinates);

    OCITypeByName(session->envp->envhp, 
                  session->envp->errhp, 
                  session->connp->svchp, 
                  (const oratext *)"MDSYS",
                  strlen("MDSYS"), 
                  (const oratext *)"SDO_ELEM_INFO_ARRAY", 
                  strlen("SDO_ELEM_INFO_ARRAY"), 
                  NULL,
                  0,
                  OCI_DURATION_TRANS,
                  OCI_TYPEGET_ALL,
                  &tdo );
    OCIObjectNew( session->envp->envhp, 
                  session->envp->errhp, 
                  session->connp->svchp, 
                  OCI_TYPECODE_VARRAY,
                  tdo, 
                  (dvoid *)NULL, 
                  OCI_DURATION_TRANS,
                  FALSE, 
                  (dvoid **)geom->geometry->sdo_elem_info);

    geom->indicator->sdo_ordinates = OCI_IND_NOTNULL;
    geom->indicator->sdo_elem_info = OCI_IND_NOTNULL;
}

/*
 * ewkbToGeom
 * 		Creates an Oracle SDO_GEOMETRY from a PostGIS EWKB.
 * 		The result is a partially palloc'ed structure.
 */
ora_geometry *ewkbToGeom(oracleSession *session, unsigned int ewkb_length, char *ewkb_data)
{
    const char *data = ewkb_data;
    unsigned type;

    ora_geometry *geom = (ora_geometry *)oracleAlloc(sizeof(ora_geometry));
    geom->geometry->sdo_ordinates = NULL;
    geom->geometry->sdo_elem_info = NULL;
    geom->indicator->sdo_ordinates = OCI_IND_NULL;
    geom->indicator->sdo_elem_info = OCI_IND_NULL;

    data = setSridAndFlags(session, geom, data);

    /* we don't move the data pointer, so we can check it in
     * set functions and reuse the same function 
     * for collections (i.e. multi*) 
     */
    setType(session, geom, data);

    type = ewkbType(session, geom);

    /* we allocate array except for points */
    if ( type != POINTTYPE ) oracleAllocOrdinatesAndElemInfo(session, geom);

    switch ( type ) 
    {
    case POINTTYPE:        data = setPoint(session, geom, data); break;
    case LINETYPE:         data = setLine(session, geom, data); break;
    case POLYGONTYPE:      data = setPolygon(session, geom, data); break;
    case MULTIPOINTTYPE:   data = setMultiPoint(session, geom, data); break;
    case MULTILINETYPE:    data = setMultiLine(session, geom, data); break;
    case MULTIPOLYGONTYPE: data = setMultiPolygon(session, geom, data); break;
    default: ORA_ASSERT(0);
    }

    // check that we reached the end of input data
    ORA_ASSERT( data - ewkb_data == ewkb_length );

    return geom;
}

/*
 * oracleGetEWKBLen
 * 		Returns the length in bytes needed to store an EWKB conversion of "geom".
 */
unsigned int
oracleGetEWKBLen(oracleSession *session, ora_geometry *geom)
{
    oracleDebug2("oracle_fdw: oracleGetEWKBLen");
    switch (ewkbType(session, geom)) 
    {
    	case POINTTYPE:              return ewkbHeaderLen(session, geom) + ewkbPointLen(session, geom);
    	case LINETYPE:               return ewkbHeaderLen(session, geom) + ewkbLineLen(session, geom);
    	case POLYGONTYPE:            return ewkbHeaderLen(session, geom) + ewkbPolygonLen(session, geom);
    	case MULTIPOINTTYPE:         return ewkbHeaderLen(session, geom) + ewkbMultiPointLen(session, geom);
    	case MULTILINETYPE:          return ewkbHeaderLen(session, geom) + ewkbMultiLineLen(session, geom);
    	case MULTIPOLYGONTYPE:       return ewkbHeaderLen(session, geom) + ewkbMultiPolygonLen(session, geom);
    	default: ORA_ASSERT(0); return 0;
    }
}

/*
 * oracleFillEWKB
 * 		Converts "geom" to an EWKB and stores the result in "dest".
 */
char *
oracleFillEWKB(oracleSession *session, ora_geometry *geom, char *dest)
{
    const char * orig = dest;
    oracleDebug2("oracle_fdw: oracleFillEWKB");
    dest = ewkbHeaderFill(session, geom, dest);
    switch (ewkbType(session, geom)) 
    {
    	case POINTTYPE: ewkbPointFill(session, geom, dest); break;
    	case LINETYPE: ewkbLineFill(session, geom, dest); break;
    	case POLYGONTYPE: ewkbPolygonFill(session, geom, dest); break;
    	case MULTIPOINTTYPE: ewkbMultiPointFill(session, geom, dest); break;
    	case MULTILINETYPE: ewkbMultiLineFill(session, geom, dest); break;
    	case MULTIPOLYGONTYPE: ewkbMultiPolygonFill(session, geom, dest); break;
    	default: ORA_ASSERT(0);
    }

    {
        const char *hexchr = "0123456789ABCDEF";
        unsigned numBytes = oracleGetEWKBLen(session, geom);
        
        //ORA_ASSERT( dest - orig == numBytes );

        unsigned i;
        char * data = (char *)oracleAlloc( 2*numBytes+1 );
        oracleDebug2("oracle_fdw: hex encode for debug");
        data[2*numBytes] = '\0';
        for (i=0; i<numBytes; i++)
        {
            const unsigned idx2 = ((uint8_t)orig[i]) & 0x0F;
            const unsigned idx1 = ((uint8_t)orig[i]) >> 4;
            ORA_ASSERT(idx1 < 16);
            ORA_ASSERT(idx2 < 16);
            data[2*i+1] = hexchr[idx2];
            data[2*i] = hexchr[idx1];
        }
        oracleDebug2(data);
        oracleFree( data );
    }
    return dest;
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
                  geom->geometry->sdo_elem_info );
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
                   geom->geometry->sdo_ordinates );
}

unsigned ewkbType(oracleSession *session, ora_geometry *geom)
{
    int gtype = 0;
    if (geom->indicator->sdo_gtype == OCI_IND_NOTNULL) {
        OCINumberToInt (
            session->envp->errhp,
            &(geom->geometry->sdo_gtype),
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

const char *setType(oracleSession *session, ora_geometry *geom, const char * data)
{
    const unsigned wkbType =  *((unsigned *)data);
    unsigned gtype = ewkbType(session, geom);

    data += sizeof(unsigned);

    switch (wkbType)
    {
    case POINTTYPE:        gtype += 1; break;
    case LINETYPE:         gtype += 2; break;
    case POLYGONTYPE:      gtype += 3; break;
    case COLLECTIONTYPE:   gtype += 4; break;
    case MULTIPOINTTYPE:   gtype += 5; break;
    case MULTILINETYPE:    gtype += 6; break;
    case MULTIPOLYGONTYPE: gtype += 7; break;
    default: ORA_ASSERT(0);
    }

    geom->indicator->sdo_gtype = OCI_IND_NOTNULL;
    OCINumberFromInt (
        session->envp->errhp,
        (dvoid *) &gtype,
        (uword) sizeof (int),
        OCI_NUMBER_SIGNED,
        &(geom->geometry->sdo_gtype));

    return data;
}



/* Header contains:
 * - srid : 3 bytes
 * - flags : 1 byte
 */
unsigned ewkbHeaderLen(oracleSession *session, ora_geometry *geom)
{
    return sizeof(unsigned);
}

char *ewkbHeaderFill(oracleSession *session, ora_geometry *geom, char * dest)
{
    const unsigned srid= ewkbSrid(session, geom);
    const uint8_t flags = ((3 == ewkbDimension(session, geom) ) ? 0x01 : 0x00 );
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


unsigned ewkbDimension(oracleSession *session, ora_geometry *geom)
{
    int gtype = 0;
    if (geom->indicator->sdo_gtype == OCI_IND_NOTNULL) {
        OCINumberToInt (
            session->envp->errhp,
            &(geom->geometry->sdo_gtype),
            (uword) sizeof (int),
            OCI_NUMBER_SIGNED,
            (dvoid *) &gtype);
    }
    return gtype / 1000;
}

unsigned ewkbSrid(oracleSession *session, ora_geometry *geom)
{
    int srid = 0;
    if (geom->indicator->sdo_srid == OCI_IND_NOTNULL) {
        OCINumberToInt (
            session->envp->errhp,
            &(geom->geometry->sdo_srid),
            (uword) sizeof (int),
            OCI_NUMBER_SIGNED,
            (dvoid *) & srid);
    }
    /* TODO convert oracle->postgis SRID when needed*/
    return srid;
}

const char *setSridAndFlags(oracleSession *session, ora_geometry *geom, const char *data)
{
    unsigned srid = 0;
    unsigned gtype = 0;

    srid |= ((uint8_t)data[0]) << 16;
    srid |= ((uint8_t)data[1]) << 8;
    srid |= ((uint8_t)data[2]);
    /* Only the first 21 bits are set. Slide up and back to pull
       the negative bits down, if we need them. */
    srid = (srid<<11)>>11;

    data += 3;

    /* TODO convert oracle->postgis SRID when needed*/
    

    geom->indicator->sdo_srid =  srid !=0 ? OCI_IND_NOTNULL : OCI_IND_NULL;
    
    if (geom->indicator->sdo_srid == OCI_IND_NOTNULL)
    {
        OCINumberFromInt (
            session->envp->errhp,
            (dvoid *)&srid,
            (uword) sizeof (unsigned),
            OCI_NUMBER_SIGNED,
            &(geom->geometry->sdo_srid));
    }

    gtype = (((uint8_t)data[0]) & 0x01 ) ? 3000 : 2000; /* 3d/2d */
    ORA_ASSERT(! (((uint8_t)data[0]) & 0x02 )); // M not supported

    data += 1;

    geom->indicator->sdo_gtype = OCI_IND_NOTNULL;

    OCINumberFromInt (
        session->envp->errhp,
        (dvoid *) &gtype,
        (uword) sizeof (int),
        OCI_NUMBER_SIGNED,
        &(geom->geometry->sdo_gtype));

    return data;
}

unsigned ewkbPointLen(oracleSession *session, ora_geometry *geom)
{
    return 2*sizeof(unsigned) + sizeof(double)*ewkbDimension(session, geom);
}

char *ewkbPointFill(oracleSession *session, ora_geometry *geom, char *dest)
{
    const unsigned numPoints = 
        (geom->indicator->sdo_point.x == OCI_IND_NULL
        && geom->indicator->sdo_point.y == OCI_IND_NULL
        && geom->indicator->sdo_point.z == OCI_IND_NULL) ? 0 : 1;

    dest = unsignedFill(POINTTYPE, dest);
    dest = unsignedFill(numPoints, dest);

    oracleDebug2("oracle_fdw: ewkbPointFill");
    if (geom->indicator->sdo_point.x == OCI_IND_NOTNULL)
        OCINumberToReal( session->envp->errhp,
                         &(geom->geometry->sdo_point.x),
                         (uword)sizeof(double),
                         (dvoid *)dest);
    dest += sizeof(double);
    if (geom->indicator->sdo_point.y == OCI_IND_NOTNULL)
        OCINumberToReal( session->envp->errhp,
                         &(geom->geometry->sdo_point.y),
                         (uword)sizeof(double),
                         (dvoid *)dest);
    dest += sizeof(double);
    if (3 == ewkbDimension(session, geom))
    {
        if (geom->indicator->sdo_point.z == OCI_IND_NOTNULL)
            OCINumberToReal( session->envp->errhp,
                             &(geom->geometry->sdo_point.z),
                             (uword)sizeof(double),
                             (dvoid *)dest);
        dest += sizeof(double);
    }

    return dest;
}

const char *setPoint(oracleSession *session, ora_geometry *geom, const char *data)
{
    ORA_ASSERT( *((unsigned *)data) == POINTTYPE );
    data += sizeof(unsigned);
    ORA_ASSERT( *( (unsigned *)data ) == 1 );
    data += sizeof(unsigned);

    geom->indicator->sdo_point.x = OCI_IND_NOTNULL;
    OCINumberFromReal( session->envp->errhp,
                     (dvoid *)data,
                     (uword)sizeof(double),
                     &(geom->geometry->sdo_point.x));
    data += sizeof(double);
    geom->indicator->sdo_point.y = OCI_IND_NOTNULL;
    OCINumberFromReal( session->envp->errhp,
                     (dvoid *)data,
                     (uword)sizeof(double),
                     &(geom->geometry->sdo_point.y));
    data += sizeof(double);
    if (3 == ewkbDimension(session, geom))
    {
        geom->indicator->sdo_point.z = OCI_IND_NOTNULL;
        OCINumberFromReal( session->envp->errhp,
                         (dvoid *)data,
                         (uword)sizeof(double),
                         &(geom->geometry->sdo_point.z));
        data += sizeof(double);
    }
    return data;
}

unsigned ewkbLineLen(oracleSession *session, ora_geometry *geom)
{
    return 2*sizeof(unsigned) + sizeof(double)*numCoord(session, geom);
}

char *ewkbLineFill(oracleSession *session, ora_geometry *geom, char * dest)
{
    unsigned i;
    const unsigned numC = numCoord(session, geom);
    const unsigned numPoints = numC / ewkbDimension(session, geom);
    dest = unsignedFill(LINETYPE, dest);
    dest = unsignedFill(numPoints, dest);
    for (i=0; i<numC; i++) dest = doubleFill(coord(session, geom, i), dest);
    return dest;
}

const char *setLine(oracleSession *session, ora_geometry *geom, const char *data)
{
    unsigned i, n;
    ORA_ASSERT( *((unsigned *)data) == LINETYPE );
    data += sizeof(unsigned);
    n = *((unsigned *)data) * ewkbDimension(session, geom);
    data += sizeof(unsigned);
    
    appendElemInfo(session, geom, numCoord(session, geom) + 1); // start index + 1
    appendElemInfo(session, geom, 2); // SDO_ETYPE linestring
    appendElemInfo(session, geom, 1); // SDO_INTERPRETATION straight line segments

    for (i=0; i<n; i++)
    {
        appendCoord(session, geom, *((double *)data));
        data += sizeof(double);
    }
    return data;
}

unsigned ewkbPolygonLen(oracleSession *session, ora_geometry *geom)
{
    const unsigned numRings = numElemInfo(session, geom)/3;
    /* there is the number of rings, and, for each ring the number of points */
    /* numRings%2 is there for padding */
    return (numRings+2+numRings%2)*sizeof(unsigned)
        + sizeof(double)*numCoord(session, geom);
}

const char *setPolygon(oracleSession *session, ora_geometry *geom, const char *data)
{
    unsigned r, i, numRings;
    const unsigned dimension = ewkbDimension(session, geom);
    const char * ringSizeData;

    ORA_ASSERT( *((unsigned *)data) == POLYGONTYPE );
    data += sizeof(unsigned);

    ringSizeData = data;
    numRings = *((unsigned *)data);
    data += (numRings+1+numRings%2)*sizeof(unsigned);
    for (r=0; r<numRings; r++)
    {
        const unsigned n= *((unsigned *)ringSizeData) * dimension;
        ringSizeData += sizeof(unsigned);

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

unsigned ewkbMultiPointLen(oracleSession *session, ora_geometry *geom)
{
    const unsigned numC = numCoord(session, geom);
    const unsigned numPoints = numC / ewkbDimension(session, geom);
    oracleDebug2("multipoint size");
    return 2*sizeof(unsigned) + (2*sizeof(unsigned)*numPoints) + sizeof(double)*numC;
}

char *ewkbMultiPointFill(oracleSession *session, ora_geometry *geom, char * dest)
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
        oracleDebug2("point in multi");
        for (j=0; j<dim; j++) dest = doubleFill(coord(session, geom, i*3+j), dest);
    }

    return dest;
}

const char *setMultiPoint(oracleSession *session, ora_geometry *geom, const char *data)
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
    appendElemInfo(session, geom, 1); // SDO_ETYPE point
    appendElemInfo(session, geom, 1); // SDO_INTERPRETATION no orientation
    return data;
}

unsigned ewkbMultiLineLen(oracleSession *session, ora_geometry *geom)
{
    const unsigned numLines = numElemInfo(session, geom)/3;
    return 2*sizeof(unsigned) + 2*sizeof(unsigned)*numLines + sizeof(double)*numCoord(session, geom);
}

char *ewkbMultiLineFill(oracleSession *session, ora_geometry *geom, char * dest)
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

const char *setMultiLine(oracleSession *session, ora_geometry *geom, const char *data)
{
    unsigned r, numLines;
    ORA_ASSERT( *((unsigned *)data) == MULTILINETYPE );
    data += sizeof(unsigned);
    numLines = *((unsigned *)data);
    data += sizeof(unsigned);

    for (r=0; r<numLines; r++) data = setLine(session, geom, data);

    return data;
}

unsigned ewkbMultiPolygonLen(oracleSession *session, ora_geometry *geom)
{
    /* polygons are padded, so the size detremination is a bit trickier */
    const unsigned numRings = numElemInfo(session, geom)/3;
    unsigned numPolygon = 0;
    unsigned i, numRingOfCurrentPoly;
    unsigned padding = 0;
    for (i=0, numRingOfCurrentPoly=0; i<numRings; i++, numRingOfCurrentPoly++)
    {
        if (elemInfo(session, geom, i*3+1) == 1003)
        {
            if (numRingOfCurrentPoly%2) ++padding;
            ++numPolygon;
            numRingOfCurrentPoly = 0;
        }
    }
    if (numRingOfCurrentPoly%2) ++padding;
        

    /* there is the number of polygons, for each polygon the type
     * and number of rings and
     * for each ring the number of points and the padding*/
    return sizeof(unsigned) +
        + numPolygon*(2*sizeof(unsigned))
        + (numRings + padding)*(sizeof(unsigned))
        + sizeof(double)*numCoord(session, geom);
}


char *ewkbMultiPolygonFill(oracleSession *session, ora_geometry *geom, char * dest)
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

        /* reset j to be on the exterior ring of the current polygon 
         * and output rings number of points */
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

const char *setMultiPolygon(oracleSession *session, ora_geometry *geom, const char *data)
{
    unsigned p, numPolygons;

    ORA_ASSERT( *((unsigned *)data) == MULTIPOLYGONTYPE );
    data += sizeof(unsigned);
    numPolygons = *((unsigned *)data);
    data += sizeof(unsigned);

    for (p=0; p<numPolygons; p++) data = setPolygon(session, geom, data);

    return data;
}

unsigned numCoord(oracleSession *session, ora_geometry *geom)
{
    int n;
    OCICollSize(session->envp->envhp, 
                session->envp->errhp,
                (OCIColl *)(geom->geometry->sdo_ordinates), 
                &n);
    return n;
}

double coord(oracleSession *session, ora_geometry *geom, unsigned i)
{
    double coord;
    boolean exists;
    OCINumber *oci_number;
    OCICollGetElem(session->envp->envhp, session->envp->errhp,
                   (OCIColl *) (geom->geometry->sdo_ordinates),
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
                 (OCIColl *)(geom->geometry->sdo_elem_info), 
                 &n);
    return n;
}

unsigned elemInfo(oracleSession *session, ora_geometry *geom, unsigned i)
{
    unsigned info;
    boolean exists;
    OCINumber *oci_number;
    OCICollGetElem(session->envp->envhp, session->envp->errhp,
                   (OCIColl *) (geom->geometry->sdo_elem_info),
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
