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

/*
 * ewkbToGeom
 * 		Creates an Oracle SDO_GEOMETRY from a PostGIS EWKB.
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
ewkb *geomToEwkb(oracleSession *session, ora_geometry *oracle_geom)
{
        const char * emptyPoint = "01040000206A08000000000000";
        void * p = oracleAlloc(strlen(emptyPoint)+1);
        memcpy(p, emptyPoint, strlen(emptyPoint)+1);
        return p;
}
