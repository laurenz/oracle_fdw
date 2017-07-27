/*
 * Test JOIN pushdown

Note: Create typetest2, typetest3, costtest1 and costtest2 on Oracle before executing this regression test.

CREATE TABLE scott.typetest2 (
   id  NUMBER(5)
      CONSTRAINT typetest2_pkey PRIMARY KEY,
   c   CHAR(10 CHAR),
   nc  NCHAR(10),
   vc  VARCHAR2(10 CHAR),
   nvc NVARCHAR2(10),
   lc  CLOB,
   r   RAW(10),
   u   RAW(16),
   lb  BLOB,
   lr  LONG RAW,
   b   NUMBER(1),
   num NUMBER(7,5),
   fl  BINARY_FLOAT,
   db  BINARY_DOUBLE,
   d   DATE,
   ts  TIMESTAMP WITH TIME ZONE,
   ids INTERVAL DAY TO SECOND,
   iym INTERVAL YEAR TO MONTH
) SEGMENT CREATION IMMEDIATE;

CREATE TABLE scott.typetest3 (
   id  NUMBER(5)
      CONSTRAINT typetest3_pkey PRIMARY KEY,
   c   CHAR(10 CHAR),
   nc  NCHAR(10),
   vc  VARCHAR2(10 CHAR),
   nvc NVARCHAR2(10),
   lc  CLOB,
   r   RAW(10),
   u   RAW(16),
   lb  BLOB,
   lr  LONG RAW,
   b   NUMBER(1),
   num NUMBER(7,5),
   fl  BINARY_FLOAT,
   db  BINARY_DOUBLE,
   d   DATE,
   ts  TIMESTAMP WITH TIME ZONE,
   ids INTERVAL DAY TO SECOND,
   iym INTERVAL YEAR TO MONTH
) SEGMENT CREATION IMMEDIATE;

CREATE TABLE COSTTEST1(
   id integer,
   val integer,
   primary key(id)
) SEGMENT CREATION IMMEDIATE;

CREATE TABLE COSTTEST2(
   id integer,
   val integer,
   primary key(id)
) SEGMENT CREATION IMMEDIATE;
 */

SET client_min_messages = WARNING;

CREATE FOREIGN TABLE typetest2 (
   id  integer OPTIONS (key 'yes') NOT NULL,
   c   character(10),
   nc  character(10),
   vc  character varying(10),
   nvc character varying(10),
   lc  text,
   r   bytea,
   u   uuid,
   lb  bytea,
   lr  bytea,
   b   boolean,
   num numeric(7,5),
   fl  float,
   db  double precision,
   d   date,
   ts  timestamp with time zone,
   ids interval,
   iym interval
) SERVER oracle OPTIONS (table 'TYPETEST2');

INSERT INTO typetest2 SELECT * FROM typetest1;
INSERT INTO typetest2 (id, c) VALUES (2, NULL);

\x
SELECT id, c, nc, vc, nvc, r, u, lb, lr, b, num, fl, db, d, ts, ids, iym FROM typetest1 order by id;
SELECT id, c, nc, vc, nvc, r, u, lb, lr, b, num, fl, db, d, ts, ids, iym FROM typetest2 order by id;
\x

/* Pushdown: OK */
/* Inner join */
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.id = t2.id;
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.c = t2.c;
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.nc = t2.nc;
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.vc = t2.vc;
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.nvc = t2.nvc;
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.num = t2.num;
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.fl = t2.fl;
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.db = t2.db;
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.d = t2.d;
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.ts = t2.ts;
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1 JOIN typetest2 t2 on (t1.id + t2.id = 2);
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1 JOIN typetest2 t2 on t1.id = t2.id AND t1.num = t2.num;

SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.id = t2.id;
SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.c = t2.c;
SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.nc = t2.nc;
SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.vc = t2.vc;
SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.nvc = t2.nvc;
SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.num = t2.num;
SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.fl = t2.fl;
SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.db = t2.db;
SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.d = t2.d;
SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.ts = t2.ts;
SELECT t1.id, t2.id FROM typetest1 t1 JOIN typetest2 t2 on (t1.id + t2.id = 2);
SELECT t1.id, t2.id FROM typetest1 t1 JOIN typetest2 t2 on t1.id = t2.id AND t1.num = t2.num;

/* Pushdown: NG */
/* Inner join */
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.lc = t2.lc;
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.r = t2.r;
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.u = t2.u;
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.lb = t2.lb;
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.lr = t2.lr;
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.b = t2.b;
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.ids = t2.ids;
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.iym = t2.iym;
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1 JOIN typetest2 t2 on t1.id = t2.id AND t1.lb = t2.lb;

SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.lc = t2.lc;
SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.r = t2.r;
SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.u = t2.u;
SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.lb = t2.lb;
SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.lr = t2.lr;
SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.b = t2.b;
SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.ids = t2.ids;
SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 WHERE t1.iym = t2.iym;
SELECT t1.id, t2.id FROM typetest1 t1 JOIN typetest2 t2 on t1.id = t2.id AND t1.lb = t2.lb;

/* Outer join */
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1 LEFT OUTER JOIN typetest2 t2 ON t1.id = t2.id;
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1 RIGHT OUTER JOIN typetest2 t2 ON t1.id = t2.id;

SELECT t1.id, t2.id FROM typetest1 t1 LEFT OUTER JOIN typetest2 t2 ON t1.id = t2.id;
SELECT t1.id, t2.id FROM typetest1 t1 RIGHT OUTER JOIN typetest2 t2 ON t1.id = t2.id;

/* Full join */
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1 FULL OUTER JOIN typetest2 t2 ON t1.id = t2.id;
SELECT t1.id, t2.id FROM typetest1 t1 FULL OUTER JOIN typetest2 t2 ON t1.id = t2.id;

/* Semi join */
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id FROM typetest1 t1 WHERE EXISTS (SELECT t2.id FROM typetest2 t2 WHERE t1.id > 1);
SELECT t1.id FROM typetest1 t1 WHERE EXISTS (SELECT t2.id FROM typetest2 t2 WHERE t1.id > 1);

/* Anti join */
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id FROM typetest1 t1 WHERE NOT EXISTS (SELECT t2.id FROM typetest2 t2 WHERE t1.id = t2.id);
SELECT t1.id FROM typetest1 t1 WHERE NOT EXISTS (SELECT t2.id FROM typetest2 t2 WHERE t1.id = t2.id);

/* Cross join */
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 ORDER BY t1.id, t2.id;
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1 JOIN typetest2 t2 ON true ORDER BY t1.id, t2.id;

SELECT t1.id, t2.id FROM typetest1 t1, typetest2 t2 ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id FROM typetest1 t1 JOIN typetest2 t2 ON true ORDER BY t1.id, t2.id;

/* Natural join */
EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id FROM typetest1 t1 NATURAL JOIN typetest2 t2 ORDER BY t1.id, t2.id;
SELECT t1.id, t2.id FROM typetest1 t1 NATURAL JOIN typetest2 t2 ORDER BY t1.id, t2.id;

/* 3-way join */
CREATE FOREIGN TABLE typetest3 (
   id  integer OPTIONS (key 'yes') NOT NULL,
   c   character(10),
   nc  character(10),
   vc  character varying(10),
   nvc character varying(10),
   lc  text,
   r   bytea,
   u   uuid,
   lb  bytea,
   lr  bytea,
   b   boolean,
   num numeric(7,5),
   fl  float,
   db  double precision,
   d   date,
   ts  timestamp with time zone,
   ids interval,
   iym interval
) SERVER oracle OPTIONS (table 'TYPETEST3');

INSERT INTO typetest3 SELECT * FROM typetest1;
INSERT INTO typetest3 (id, c) VALUES (2, NULL);

EXPLAIN (VERBOSE on, COSTS off) SELECT t1.id, t2.id, t3.id FROM typetest1 t1, typetest2 t2, typetest3 t3 WHERE t1.id = t2.id AND t2.id = t3.id;
SELECT t1.id, t2.id, t3.id FROM typetest1 t1, typetest2 t2, typetest3 t3 WHERE t1.id = t2.id AND t2.id = t3.id;

/* Clean up */
DELETE FROM typetest1;
DELETE FROM typetest2;
DELETE FROM typetest3;
DROP FOREIGN TABLE typetest1;
DROP FOREIGN TABLE typetest2;
DROP FOREIGN TABLE typetest3;


/* Test for local cost estimation */
/* Setup */
SET client_min_messages = WARNING;
set enable_material to off;

CREATE FOREIGN TABLE costtest1 (
    id  integer OPTIONS (key 'yes'),
    val integer
) SERVER oracle OPTIONS (table 'COSTTEST1');

CREATE FOREIGN TABLE costtest2 (
    id  integer OPTIONS (key 'yes'),
    val integer
) SERVER oracle OPTIONS (table 'COSTTEST2');


/*---------- High cardinality ----------*/
INSERT INTO costtest1(id, val) SELECT i, i from GENERATE_SERIES(1, 10000 ) as i;
INSERT INTO costtest2(id, val) SELECT * FROM costtest1;

SELECT * FROM costtest1 order by 1 LIMIT 10;
SELECT * FROM costtest2 order by 1 LIMIT 10;

/** Test by using no statistics **/
select relname, relpages, reltuples from pg_class where relname like 'costtest%';

EXPLAIN (verbose, costs off) SELECT T1.val FROM costtest1 as T1, costtest2 as T2 WHERE T1.val = T2.val;
EXPLAIN (verbose, costs off) SELECT T1.val FROM costtest1 as T1, costtest2 as T2 WHERE T1.val = T2.val and t1.val = 1;

/** Test by using statistics **/
analyze costtest1;
analyze costtest2;
select relname, relpages, reltuples from pg_class where relname like 'costtest%';

EXPLAIN (verbose, costs off) SELECT T1.val FROM costtest1 as T1, costtest2 as T2 WHERE T1.val = T2.val;
EXPLAIN (verbose, costs off) SELECT T1.val FROM costtest1 as T1, costtest2 as T2 WHERE T1.val = T2.val and t1.val = 1;


/* Clean up */
DELETE FROM costtest1;
DELETE FROM costtest2;
DROP FOREIGN TABLE costtest1;
DROP FOREIGN TABLE costtest2;


/* Setup */
CREATE FOREIGN TABLE costtest1 (
    id  integer OPTIONS (key 'yes'),
    val integer
) SERVER oracle OPTIONS (table 'COSTTEST1');

CREATE FOREIGN TABLE costtest2 (
    id  integer OPTIONS (key 'yes'),
    val integer
) SERVER oracle OPTIONS (table 'COSTTEST2');


/*---------- Low cardinality ----------*/
INSERT INTO costtest1(id, val) SELECT i, i/100 from GENERATE_SERIES(1, 10000 ) as i;
INSERT INTO costtest2(id, val) SELECT * FROM costtest1;

SELECT * FROM costtest1 order by 1 LIMIT 10;
SELECT * FROM costtest2 order by 1 LIMIT 10;

/** Test by using no statistics **/
select relname, relpages, reltuples from pg_class where relname like 'costtest%';

EXPLAIN (verbose, costs off) SELECT T1.val FROM costtest1 as T1, costtest2 as T2 WHERE T1.val = T2.val;
EXPLAIN (verbose, costs off) SELECT T1.val FROM costtest1 as T1, costtest2 as T2 WHERE T1.val = T2.val and t1.val = 1;

/** Test by using statistics **/
analyze costtest1;
analyze costtest2;
select relname, relpages, reltuples from pg_class where relname like 'costtest%';

EXPLAIN (verbose, costs off) SELECT T1.val FROM costtest1 as T1, costtest2 as T2 WHERE T1.val = T2.val;
EXPLAIN (verbose, costs off) SELECT T1.val FROM costtest1 as T1, costtest2 as T2 WHERE T1.val = T2.val and t1.val = 1;


/* Clean up */
DELETE FROM costtest1;
DELETE FROM costtest2;
DROP FOREIGN TABLE costtest1;
DROP FOREIGN TABLE costtest2;

