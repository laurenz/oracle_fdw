#!/bin/bash


echo "DROP FOREIGN TABLE IF EXISTS gis CASCADE;"
echo "CREATE FOREIGN TABLE gis (gid int OPTIONS (key 'true') NOT NULL, geom GEOMETRY) SERVER oradb OPTIONS (schema 'C##OSLANDIA', table 'GIS');"

n=0
while read line; do
    n=$(($n+1))

    echo "INSERT INTO gis VALUES ($n,'SRID=8307;$line'::geometry);"
done < test_geometrie.wkt

echo "SELECT 'result',gid, ST_SRID(geom), ST_AsText(geom) FROM gis;"
