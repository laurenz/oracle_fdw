#!/bin/bash

n=0
while read line; do
    n=$(($n+1))
    echo "create or replace view geom_test_$n as select SDO_UTIL.FROM_WKTGEOMETRY('$line') as geom from dual;"
done < test_geometries.wkt
