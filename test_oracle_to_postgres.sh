#!/bin/bash

rm /tmp/ora_to_pg.ref
n=0
while read line; do
    n=$(($n+1))
    echo " result $n $line" >> /tmp/ora_to_pg.ref
done < test_geometries.wkt

./create_postgres_test_tables.sh |psql oracle 2>&1|grep result > /tmp/ora_to_pg.res

diff /tmp/ora_to_pg.ref /tmp/ora_to_pg.res
