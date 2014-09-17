#!/bin/bash

n=0
while read line; do
    n=$(($n+1))
    oraTab="geom_test_"$n
    echo "DROP FOREIGN TABLE IF EXISTS geom_test_$n CASCADE;"
    echo "CREATE FOREIGN TABLE geom_test_$n (geom GEOMETRY) SERVER orcl OPTIONS (schema 'OSLANDIA', table '${oraTab^^}');"
    echo "SELECT 'result $n '||ST_AsText(geom) FROM geom_test_$n;"
done < test_geometries.wkt
