\pset border 1
\pset linestyle ascii
\set VERBOSITY terse
SET client_min_messages = INFO;

/* analyze table for reliable output */
ANALYZE typetest1;

/*
 * Cases that should be pushed down.
 */
-- inner join two tables
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1, typetest1  t2 WHERE t1.c = t2.c ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id FROM typetest1  t1, typetest1  t2 WHERE t1.c = t2.c ORDER BY t1.id, t2.id;
EXPLAIN (COSTS off)
SELECT length(t1.lb), length(t2.lc) FROM typetest1  t1 JOIN typetest1  t2 ON (t1.id + t2.id = 2) ORDER BY t1.id, t2.id;
SELECT length(t1.lb), length(t2.lc) FROM typetest1  t1 JOIN typetest1  t2 ON (t1.id + t2.id = 2) ORDER BY t1.id, t2.id;
-- inner join two tables with ORDER BY clause, but ORDER BY does not get pushed down
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 JOIN typetest1  t2 USING (ts, num) ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id FROM typetest1  t1 JOIN typetest1  t2 USING (ts, num) ORDER BY t1.id, t2.id;
-- natural join two tables
EXPLAIN (COSTS off)
SELECT id FROM typetest1  NATURAL JOIN shorty  ORDER BY id;
SELECT id FROM typetest1  NATURAL JOIN shorty  ORDER BY id;
-- table with column that does not exist in Oracle (should become NULL)
EXPLAIN (COSTS off)
SELECT t1.id, t2.x FROM typetest1  t1 JOIN longy t2  ON t1.c = t2.c ORDER BY t1.id, t2.x;
SELECT t1.id, t2.x FROM typetest1  t1 JOIN longy t2  ON t1.c = t2.c ORDER BY t1.id, t2.x;
-- left outer join two tables
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d ORDER BY t1.id, t2.id;
-- right outer join two tables
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d ORDER BY t1.id, t2.id;
-- full outer join two tables
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d ORDER BY t1.id, t2.id;
-- joins with filter conditions
---- inner join with WHERE clause
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON t1.d = t2.d WHERE t1.id > 1 ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON t1.d = t2.d WHERE t1.id > 1 ORDER BY t1.id, t2.id;
---- left outer join with WHERE clause
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d WHERE t1.id > 1 ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d WHERE t1.id > 1 ORDER BY t1.id, t2.id;
---- right outer join with WHERE clause
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d WHERE t1.id > 1 ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d WHERE t1.id > 1 ORDER BY t1.id, t2.id;
---- full outer join with WHERE clause
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d WHERE t1.id > 1 ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d WHERE t1.id > 1 ORDER BY t1.id, t2.id;

/*
 * Cases that should not be pushed down.
 */
-- join expression cannot be pushed down
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1, typetest1  t2 WHERE t1.lc = t2.lc ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id FROM typetest1  t1, typetest1  t2 WHERE t1.lc = t2.lc ORDER BY t1.id, t2.id;
-- only one join condition cannot be pushed down
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 JOIN typetest1  t2 ON t1.vc = t2.vc AND t1.lb = t2.lb ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id FROM typetest1  t1 JOIN typetest1  t2 ON t1.vc = t2.vc AND t1.lb = t2.lb ORDER BY t1.id, t2.id;
-- condition on one table needs to be evaluated locally
EXPLAIN (COSTS off)
SELECT max(t1.id), min(t2.id) FROM typetest1  t1 JOIN typetest1  t2 ON t1.fl = t2.fl WHERE t1.vc || 'x' = 'shortx' ORDER BY 1, 2;
SELECT max(t1.id), min(t2.id) FROM typetest1  t1 JOIN typetest1  t2 ON t1.fl = t2.fl WHERE t1.vc || 'x' = 'shortx' ORDER BY 1, 2;
EXPLAIN (COSTS off)
SELECT t1.c, t2.nc FROM typetest1  t1 JOIN (SELECT * FROM typetest1)  t2 ON (t1.id = t2.id AND t1.c >= t2.c) ORDER BY t1.id, t2.nc;
SELECT t1.c, t2.nc FROM typetest1  t1 JOIN (SELECT * FROM typetest1)  t2 ON (t1.id = t2.id AND t1.c >= t2.c) ORDER BY t1.id, t2.nc;
EXPLAIN (COSTS off)
SELECT t1.c, t2.nc FROM typetest1  t1 LEFT JOIN (SELECT * FROM typetest1)  t2 ON (t1.id = t2.id AND t1.c >= t2.c) ORDER BY t1.id, t2.nc;
SELECT t1.c, t2.nc FROM typetest1  t1 LEFT JOIN (SELECT * FROM typetest1)  t2 ON (t1.id = t2.id AND t1.c >= t2.c) ORDER BY t1.id, t2.nc;
-- subquery with where clause cannnot be pushed down in full outer join query
EXPLAIN (COSTS off)
SELECT t1.c, t2.nc FROM typetest1  t1 FULL JOIN (SELECT * FROM typetest1  WHERE id > 1) t2 USING (id) ORDER BY t1.id, t2.nc;
SELECT t1.c, t2.nc FROM typetest1  t1 FULL JOIN (SELECT * FROM typetest1  WHERE id > 1) t2 USING (id) ORDER BY t1.id, t2.nc;
-- left outer join with placeholder, not pushed down
EXPLAIN (COSTS off)
SELECT t1.id, sq1.x, sq1.y
FROM typetest1  t1 LEFT OUTER JOIN (SELECT id AS x, 99 AS y FROM typetest1  t2 WHERE id > 1) sq1 ON t1.id = sq1.x ORDER BY t1.id, sq1.x;
SELECT t1.id, sq1.x, sq1.y
FROM typetest1  t1 LEFT OUTER JOIN (SELECT id AS x, 99 AS y FROM typetest1  t2 WHERE id > 1) sq1 ON t1.id = sq1.x ORDER BY t1.id, sq1.x;
-- inner join with placeholder, not pushed down
EXPLAIN (COSTS off)
SELECT subq2.c3
FROM typetest1
RIGHT JOIN (SELECT c AS c1 FROM typetest1)  AS subq1 ON TRUE
LEFT JOIN  (SELECT ref1.nc AS c2, 10 AS c3 FROM typetest1  AS ref1
            INNER JOIN typetest1  AS ref2 ON ref1.fl = ref2.fl) AS subq2
ON subq1.c1 = subq2.c2 ORDER BY subq2.c3;
SELECT subq2.c3
FROM typetest1
RIGHT JOIN (SELECT c AS c1 FROM typetest1)  AS subq1 ON TRUE
LEFT JOIN  (SELECT ref1.nc AS c2, 10 AS c3 FROM typetest1  AS ref1
            INNER JOIN typetest1  AS ref2 ON ref1.fl = ref2.fl) AS subq2
ON subq1.c1 = subq2.c2 ORDER BY subq2.c3;
-- inner rel is false, not pushed down
EXPLAIN (COSTS off)
SELECT 1 FROM (SELECT 1 FROM typetest1  WHERE false) AS subq1 RIGHT JOIN typetest1  AS ref1 ON NULL ORDER BY ref1.id;
SELECT 1 FROM (SELECT 1 FROM typetest1  WHERE false) AS subq1 RIGHT JOIN typetest1  AS ref1 ON NULL ORDER BY ref1.id;
-- semi-join, not pushed down
EXPLAIN (COSTS off)
SELECT t1.id FROM typetest1  t1 WHERE EXISTS (SELECT 1 FROM typetest1  t2 WHERE t1.d = t2.d) ORDER BY t1.id;
SELECT t1.id FROM typetest1  t1 WHERE EXISTS (SELECT 1 FROM typetest1  t2 WHERE t1.d = t2.d) ORDER BY t1.id;
-- anti-join, not pushed down
EXPLAIN (COSTS off)
SELECT t1.id FROM typetest1  t1 WHERE NOT EXISTS (SELECT 1 FROM typetest1  t2 WHERE t1.d = t2.d) ORDER BY t1.id;
SELECT t1.id FROM typetest1  t1 WHERE NOT EXISTS (SELECT 1 FROM typetest1  t2 WHERE t1.d = t2.d) ORDER BY t1.id;
-- cross join, not pushed down
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 CROSS JOIN typetest1  t2 ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id FROM typetest1  t1 CROSS JOIN typetest1  t2 ORDER BY t1.id, t2.id;
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON true ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON true ORDER BY t1.id, t2.id;
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON true ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON true ORDER BY t1.id, t2.id;
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON true ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON true ORDER BY t1.id, t2.id;
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON true ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON true ORDER BY t1.id, t2.id;
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 CROSS JOIN (SELECT * FROM typetest1  WHERE vc = 'short') t2 ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id FROM typetest1  t1 CROSS JOIN (SELECT * FROM typetest1  WHERE vc = 'short') t2 ORDER BY t1.id, t2.id;
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 INNER JOIN (SELECT * FROM typetest1  WHERE vc = 'short') t2 ON true ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id FROM typetest1  t1 INNER JOIN (SELECT * FROM typetest1  WHERE vc = 'short') t2 ON true ORDER BY t1.id, t2.id;
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 LEFT  JOIN (SELECT * FROM typetest1  WHERE vc = 'short') t2 ON true ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id FROM typetest1  t1 LEFT  JOIN (SELECT * FROM typetest1  WHERE vc = 'short') t2 ON true ORDER BY t1.id, t2.id;
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 RIGHT JOIN (SELECT * FROM typetest1  WHERE vc = 'short') t2 ON true ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id FROM typetest1  t1 RIGHT JOIN (SELECT * FROM typetest1  WHERE vc = 'short') t2 ON true ORDER BY t1.id, t2.id;
EXPLAIN (COSTS off)
SELECT t1.id, t2.id FROM typetest1  t1 FULL  JOIN (SELECT * FROM typetest1  WHERE vc = 'short') t2 ON true ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id FROM typetest1  t1 FULL  JOIN (SELECT * FROM typetest1  WHERE vc = 'short') t2 ON true ORDER BY t1.id, t2.id;
-- update statement, not pushed down
EXPLAIN (COSTS off) UPDATE typetest1 t1 SET c = NULL FROM typetest1 t2 WHERE t1.vc = t2.vc AND t2.num = 3.14159;
-- join with FOR UPDATE, not pushed down
EXPLAIN (COSTS off) SELECT t1.id FROM typetest1 t1, typetest1 t2 WHERE t1.id = t2.id FOR UPDATE;
-- join in CTE
WITH t (t1_id, t2_id) AS (SELECT t1.id, t2.id FROM typetest1  t1 JOIN typetest1  t2 ON t1.d = t2.d) SELECT t1_id, t2_id FROM t ORDER BY t1_id, t2_id;
-- whole-row and system columns, not pushed down
EXPLAIN (COSTS off)
SELECT t1, t1.ctid FROM shorty t1 INNER JOIN longy t2 ON t1.id = t2.id ORDER BY t1.id;
SELECT t1, t1.ctid FROM shorty t1 INNER JOIN longy t2 ON t1.id = t2.id ORDER BY t1.id;
EXPLAIN (COSTS off)
SELECT t1, t1.ctid FROM shorty t1 LEFT  JOIN longy t2 ON t1.id = t2.id ORDER BY t1.id;
SELECT t1, t1.ctid FROM shorty t1 LEFT  JOIN longy t2 ON t1.id = t2.id ORDER BY t1.id;
EXPLAIN (COSTS off)
SELECT t1, t1.ctid FROM shorty t1 RIGHT JOIN longy t2 ON t1.id = t2.id ORDER BY t1.id;
SELECT t1, t1.ctid FROM shorty t1 RIGHT JOIN longy t2 ON t1.id = t2.id ORDER BY t1.id;
EXPLAIN (COSTS off)
SELECT t1, t1.ctid FROM shorty t1 FULL  JOIN longy t2 ON t1.id = t2.id ORDER BY t1.id;
SELECT t1, t1.ctid FROM shorty t1 FULL  JOIN longy t2 ON t1.id = t2.id ORDER BY t1.id;
EXPLAIN (COSTS off)
SELECT t1, t1.ctid FROM shorty t1 CROSS JOIN longy t2 ORDER BY t1.id;
SELECT t1, t1.ctid FROM shorty t1 CROSS JOIN longy t2 ORDER BY t1.id;
-- only part of a three-way join will be pushed down
---- inner join three tables
EXPLAIN (COSTS off)
SELECT t1.id, t3.id FROM typetest1  t1 JOIN typetest1  t2 USING (nvc) JOIN typetest1  t3 ON t2.db = t3.db ORDER BY t1.id, t3.id;
SELECT t1.id, t3.id FROM typetest1  t1 JOIN typetest1  t2 USING (nvc) JOIN typetest1  t3 ON t2.db = t3.db ORDER BY t1.id, t3.id;
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON t1.d = t2.d INNER JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON t1.d = t2.d INNER JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- inner outer join + left outer join
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON t1.d = t2.d LEFT  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON t1.d = t2.d LEFT  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- inner outer join + right outer join
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON t1.d = t2.d RIGHT JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON t1.d = t2.d RIGHT JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- inner outer join + full outer join
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON t1.d = t2.d FULL  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 INNER JOIN typetest1  t2 ON t1.d = t2.d FULL  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- left outer join three tables
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d LEFT  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d LEFT  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- left outer join + inner outer join
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d INNER JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d INNER JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- left outer join + right outer join
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d RIGHT JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d RIGHT JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- left outer join + full outer join
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d FULL  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 LEFT  JOIN typetest1  t2 ON t1.d = t2.d FULL  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- right outer join three tables
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d RIGHT JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d RIGHT JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- right outer join + inner outer join
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d INNER JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d INNER JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- right outer join + left outer join
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d LEFT  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d LEFT  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- right outer join + full outer join
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d FULL  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 RIGHT JOIN typetest1  t2 ON t1.d = t2.d FULL  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- full outer join three tables
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d FULL  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d FULL  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- full outer join + inner join
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d INNER JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d INNER JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- full outer join + left outer join
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d LEFT  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d LEFT  JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
---- full outer join + right outer join
EXPLAIN (COSTS off)
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d RIGHT JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id, t3.id FROM typetest1  t1 FULL  JOIN typetest1  t2 ON t1.d = t2.d RIGHT JOIN typetest1  t3 ON t2.d = t3.d ORDER BY t1.id, t2.id;
-- join with LATERAL reference
EXPLAIN (COSTS off)
SELECT t1.id, sl.c FROM typetest1  t1, LATERAL (SELECT DISTINCT s.c FROM shorty s,   longy l WHERE s.id = l.id AND l.c = t1.c) sl ORDER BY t1.id, sl.c;
SELECT t1.id, sl.c FROM typetest1  t1, LATERAL (SELECT DISTINCT s.c FROM shorty s,   longy l WHERE s.id = l.id AND l.c = t1.c) sl ORDER BY t1.id, sl.c;
-- test for bug #279 fixed with 839b125e1bdc63b71220ccd675fa852c028de9ea
SELECT 1
FROM typetest1 a
   LEFT JOIN typetest1 b ON (a.id IS NOT NULL)
WHERE (a.c = a.vc) = (b.id IS NOT NULL);

/*
 * Cost estimates.
 */
-- delete statistics
DELETE FROM pg_statistic WHERE starelid = 'typetest1'::regclass;
UPDATE pg_class SET relpages = 0, reltuples = 0.0 WHERE oid = 'typetest1'::regclass;
-- default costs
EXPLAIN SELECT t1.id, t2.id FROM typetest1 t1, typetest1 t2 WHERE t1.c = t2.c;
-- gather statistics
ANALYZE typetest1;
-- costs with statistics
EXPLAIN SELECT t1.id, t2.id FROM typetest1 t1, typetest1 t2 WHERE t1.c = t2.c;
EXPLAIN SELECT t1.id, t2.id FROM typetest1 t1, typetest1 t2 WHERE t1.c <> t2.c;
