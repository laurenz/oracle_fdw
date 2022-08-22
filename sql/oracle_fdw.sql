/*
 * Install the extension and define the tables.
 * All the foreign tables defined refer to the same Oracle table.
 */

SET client_min_messages = WARNING;

CREATE EXTENSION oracle_fdw;

-- TWO_TASK or ORACLE_HOME and ORACLE_SID must be set in the server's environment for this to work
CREATE SERVER oracle FOREIGN DATA WRAPPER oracle_fdw OPTIONS (dbserver '', isolation_level 'read_committed', nchar 'true');

CREATE USER MAPPING FOR CURRENT_ROLE SERVER oracle OPTIONS (user 'SCOTT', password 'tiger');

-- drop the Oracle tables if they exist
DO
$$BEGIN
   SELECT oracle_execute('oracle', 'DROP TABLE scott.typetest1 PURGE');
EXCEPTION
   WHEN OTHERS THEN
      NULL;
END;$$;

DO
$$BEGIN
   SELECT oracle_execute('oracle', 'DROP TABLE scott.typetest2 PURGE');
EXCEPTION
   WHEN OTHERS THEN
      NULL;
END;$$;

DO
$$BEGIN
   SELECT oracle_execute('oracle', 'DROP TABLE scott.gis PURGE');
EXCEPTION
   WHEN OTHERS THEN
      NULL;
END;$$;

SELECT oracle_execute(
          'oracle',
          E'CREATE TABLE scott.typetest1 (\n'
          '   id  NUMBER(5)\n'
          '      CONSTRAINT typetest1_pkey PRIMARY KEY,\n'
          '   c   CHAR(10 CHAR),\n'
          '   nc  NCHAR(10),\n'
          '   vc  VARCHAR2(10 CHAR),\n'
          '   nvc NVARCHAR2(10),\n'
          '   lc  CLOB,\n'
          '   r   RAW(10),\n'
          '   u   RAW(16),\n'
          '   lb  BLOB,\n'
          '   lr  LONG RAW,\n'
          '   b   NUMBER(1),\n'
          '   num NUMBER(7,5),\n'
          '   fl  BINARY_FLOAT,\n'
          '   db  BINARY_DOUBLE,\n'
          '   d   DATE,\n'
          '   ts  TIMESTAMP WITH TIME ZONE,\n'
          '   ids INTERVAL DAY TO SECOND,\n'
          '   iym INTERVAL YEAR TO MONTH\n'
          ') SEGMENT CREATION IMMEDIATE'
       );

SELECT oracle_execute(
          'oracle',
          E'CREATE TABLE scott.typetest2 (\n'
          '   id  NUMBER(5)\n'
          '     CONSTRAINT typetest2_pkey PRIMARY KEY,\n'
          '   ts1 TIMESTAMP WITH LOCAL TIME ZONE,\n'
          '   ts2 TIMESTAMP WITH LOCAL TIME ZONE,\n'
          '   ts3 TIMESTAMP WITH LOCAL TIME ZONE\n'
          ') SEGMENT CREATION IMMEDIATE'
       );

SELECT oracle_execute(
          'oracle',
          E'CREATE TABLE scott.gis (\n'
          '   id  NUMBER(5) PRIMARY KEY,\n'
          '   g   MDSYS.SDO_GEOMETRY\n'
          ') SEGMENT CREATION IMMEDIATE'
       );

-- gather statistics
SELECT oracle_execute(
          'oracle',
          E'BEGIN\n'
          '   DBMS_STATS.GATHER_TABLE_STATS (''SCOTT'', ''TYPETEST1'', NULL, 100);\n'
          'END;'
       );

SELECT oracle_execute(
          'oracle',
          E'BEGIN\n'
          '   DBMS_STATS.GATHER_TABLE_STATS (''SCOTT'', ''TYPETEST2'', NULL, 100);\n'
          'END;'
       );

SELECT oracle_execute(
          'oracle',
          E'BEGIN\n'
          '   DBMS_STATS.GATHER_TABLE_STATS (''SCOTT'', ''GIS'', NULL, 100);\n'
          'END;'
       );

-- initial data for typetest2
SELECT oracle_execute(
          'oracle',
          E'INSERT INTO scott.typetest2 (id, ts1, ts2, ts3) VALUES (\n'
          '   1,\n'
          '   ''2022-08-01 00:00:00 AD'',\n'
          '   ''2022-08-01 00:00:00 AD'',\n'
          '   ''2022-08-01 00:00:00 AD''\n'
          ')'
       );

-- create the foreign tables
CREATE FOREIGN TABLE typetest1 (
   id  integer OPTIONS (key 'yes') NOT NULL,
   q   double precision,
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
) SERVER oracle OPTIONS (table 'TYPETEST1');
ALTER FOREIGN TABLE typetest1 DROP q;

-- a table that is missing some fields
CREATE FOREIGN TABLE shorty (
   id  integer OPTIONS (key 'yes') NOT NULL,
   c   character(10)
) SERVER oracle OPTIONS (table 'TYPETEST1');

-- a table that has some extra fields
CREATE FOREIGN TABLE longy (
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
   iym interval,
   x   integer
) SERVER oracle OPTIONS (table 'TYPETEST1');

CREATE FOREIGN TABLE typetest2 (
   id  integer OPTIONS (key 'yes') NOT NULL,
   ts1 timestamp with time zone,
   ts2 timestamp without time zone,
   ts3 date
) SERVER oracle OPTIONS (table 'TYPETEST2');

/*
 * INSERT some rows into "typetest1".
 */

-- will fail with a read-only transaction
ALTER SERVER oracle OPTIONS (SET isolation_level 'read_only');
SELECT oracle_close_connections();
DELETE FROM typetest1;

-- use the default SERIALIZABLE isolation level from now on
ALTER SERVER oracle OPTIONS (DROP isolation_level);
SELECT oracle_close_connections();
DELETE FROM typetest1;

INSERT INTO typetest1 (id, c, nc, vc, nvc, lc, r, u, lb, lr, b, num, fl, db, d, ts, ids, iym) VALUES (
   1,
   'fixed char',
   'nat''l char',
   'varlena',
   'nat''l var',
   'character large object',
   bytea('\xDEADBEEF'),
   uuid('055e26fa-f1d8-771f-e053-1645990add93'),
   bytea('\xDEADBEEF'),
   bytea('\xDEADBEEF'),
   TRUE,
   3.14159,
   3.14159,
   3.14159,
   '1968-10-20',
   '2009-01-26 15:02:54.893532 PST',
   '1 day 2 hours 30 seconds 1 microsecond',
   '-6 months'
);

-- change the "boolean" in Oracle to "2"
SELECT oracle_execute('oracle', 'UPDATE typetest1 SET b = 2 WHERE id = 1');

INSERT INTO shorty (id, c) VALUES (2, NULL);

INSERT INTO typetest1 (id, c, nc, vc, nvc, lc, r, u, lb, lr, b, num, fl, db, d, ts, ids, iym) VALUES (
   3,
   E'a\u001B\u0007\u000D\u007Fb',
   E'a\u001B\u0007\u000D\u007Fb',
   E'a\u001B\u0007\u000D\u007Fb',
   E'a\u001B\u0007\u000D\u007Fb',
   E'a\u001B\u0007\u000D\u007Fb ABC' || repeat('X', 9000),
   bytea('\xDEADF00D'),
   uuid('055f3b32-a02c-4532-e053-1645990a6db2'),
   bytea('\xDEADF00DDEADF00DDEADF00D'),
   bytea('\xDEADF00DDEADF00DDEADF00D'),
   FALSE,
   -2.71828,
   -2.71828,
   -2.71828,
   '0044-03-15 BC',
   '0044-03-15 12:00:00 BC',
   '-2 days -12 hours -30 minutes',
   '-2 years -6 months'
);

INSERT INTO typetest1 (id, c, nc, vc, nvc, lc, r, u, lb, lr, b, num, fl, db, d, ts, ids, iym) VALUES (
   4,
   'short',
   'short',
   'short',
   'short',
   'short',
   bytea('\xDEADF00D'),
   uuid('0560ee34-2ef9-1137-e053-1645990ac874'),
   bytea('\xDEADF00D'),
   bytea('\xDEADF00D'),
   NULL,
   0,
   0,
   0,
   NULL,
   NULL,
   '23:59:59.999999',
   '3 years'
);

/*
 * Test SELECT, UPDATE ... RETURNING, DELETE and transactions.
 */

-- simple SELECT
SELECT id, c, nc, vc, nvc, length(lc), r, u, length(lb), length(lr), b, num, fl, db, d, ts, ids, iym, x FROM longy ORDER BY id;
-- mass UPDATE
WITH upd (id, c, lb, d, ts) AS
   (UPDATE longy SET c = substr(c, 1, 9) || 'u',
                    lb = lb || bytea('\x00'),
                    lr = lr || bytea('\x00'),
                     d = d + 1,
                    ts = ts + '1 day'
   WHERE id < 3 RETURNING id + 1, c, lb, d, ts)
SELECT * FROM upd ORDER BY id;
-- transactions
BEGIN;
DELETE FROM shorty WHERE id = 2;
SAVEPOINT one;
-- will cause an error
INSERT INTO shorty (id, c) VALUES (1, 'c');
ROLLBACK TO one;
INSERT INTO shorty (id, c) VALUES (2, 'c');
ROLLBACK TO one;
COMMIT;
-- see if the correct data are in the table
SELECT id, c FROM typetest1 ORDER BY id;
-- try to update the nonexistant column (should cause an error)
UPDATE longy SET x = NULL WHERE id = 1;
-- check that UPDATES work with "date" in Oracle and "timestamp" in PostgreSQL
BEGIN;
ALTER FOREIGN TABLE typetest1 ALTER COLUMN d TYPE timestamp(0) without time zone;
UPDATE typetest1 SET d = '1968-10-10 12:00:00' WHERE id = 1 RETURNING d;
ROLLBACK;
-- test if "IN" or "= ANY" expressions are pushed down correctly
SELECT id FROM typetest1 WHERE vc = ANY (ARRAY['short', (SELECT 'varlena'::varchar)]) ORDER BY id;
EXPLAIN (COSTS off) SELECT id FROM typetest1 WHERE vc = ANY (ARRAY['short', (SELECT 'varlena'::varchar)]) ORDER BY id;
-- test modifications that need no foreign scan scan (bug #295)
DELETE FROM typetest1 WHERE FALSE;
UPDATE shorty SET c = NULL WHERE FALSE RETURNING *;
-- test deparsing of ScalarArrayOpExpr where the RHS has different element type than the LHS
SELECT id FROM typetest1 WHERE vc = ANY ('{zzzzz}'::name[]);

/*
 * Test "strip_zeros" column option.
 */

SELECT oracle_execute(
          'oracle',
          'INSERT INTO typetest1 (id, vc) VALUES (5, ''has'' || chr(0) || ''zeros'')'
       );

SELECT vc FROM typetest1 WHERE id = 5;  -- should fail
ALTER FOREIGN TABLE typetest1 ALTER vc OPTIONS (ADD strip_zeros 'yes');
SELECT vc FROM typetest1 WHERE id = 5;  -- should work
ALTER FOREIGN TABLE typetest1 ALTER vc OPTIONS (DROP strip_zeros);

DELETE FROM typetest1 WHERE id = 5;

/*
 * Test EXPLAIN support.
 */

EXPLAIN (COSTS off) UPDATE typetest1 SET lc = current_timestamp WHERE id < 4 RETURNING id + 1;
EXPLAIN (VERBOSE on, COSTS off) SELECT * FROM shorty;
-- this should fetch all columns from the foreign table
EXPLAIN (COSTS off) SELECT typetest1 FROM typetest1;

/*
 * Test parameters.
 */

PREPARE stmt(integer, date, timestamp) AS SELECT d FROM typetest1 WHERE id = $1 AND d < $2 AND ts < $3;
-- six executions to switch to generic plan
EXECUTE stmt(1, '2011-03-09', '2011-03-09 05:00:00');
EXECUTE stmt(1, '2011-03-09', '2011-03-09 05:00:00');
EXECUTE stmt(1, '2011-03-09', '2011-03-09 05:00:00');
EXECUTE stmt(1, '2011-03-09', '2011-03-09 05:00:00');
EXECUTE stmt(1, '2011-03-09', '2011-03-09 05:00:00');
EXPLAIN (COSTS off) EXECUTE stmt(1, '2011-03-09', '2011-03-09 05:00:00');
EXECUTE stmt(1, '2011-03-09', '2011-03-09 05:00:00');
DEALLOCATE stmt;
-- test NULL parameters
SELECT id FROM typetest1 WHERE vc = (SELECT NULL::text);

/*
 * Test current_timestamp.
 */
SELECT id FROM typetest1
   WHERE d < current_date
     AND ts < now()
     AND ts < current_timestamp
     AND ts < 'now'::timestamp
ORDER BY id;

/*
 * Test foreign table based on SELECT statement.
 */

CREATE FOREIGN TABLE qtest (
   id  integer OPTIONS (key 'yes') NOT NULL,
   vc  character varying(10),
   num numeric(7,5)
) SERVER oracle OPTIONS (table '(SELECT id, vc, num FROM typetest1)');

-- INSERT works with simple "view"
INSERT INTO qtest (id, vc, num) VALUES (5, 'via query', -12.5);

ALTER FOREIGN TABLE qtest OPTIONS (SET table '(SELECT id, SUBSTR(vc, 1, 3), num FROM typetest1)');

-- SELECT and DELETE should also work with derived columns
SELECT * FROM qtest ORDER BY id;
DELETE FROM qtest WHERE id = 5;

/*
 * Test COPY
 */

BEGIN;
COPY typetest1 FROM STDIN;
666	cöpy	variation	dynamo	ünicode	Not very long	DEADF00D	9a0cf1eb-02e2-4b1f-bbe0-449fa4a99969	\\x01020304	\\xFFFF	\N	0.11111	0.43211	0.01010	2100-01-29	2050-04-01 19:30:00	12 hours	0 years
777	fdjkl	r89809rew	^ß[]#~	\N	Das also ist des Pudels Kern.	00	fe288446-05f6-4074-9e9e-6ee41af7b377	\\x00	\\x00	FALSE	10	1002	1003	2019-05-01	2019-05-01 0:00:00	0 seconds	1 year
\.
ROLLBACK;

/*
 * Test foreign table as a partition.
 */

CREATE TABLE party (LIKE typetest1) PARTITION BY RANGE (id);
CREATE TABLE defpart PARTITION OF party DEFAULT;
ALTER TABLE party ATTACH PARTITION typetest1 FOR VALUES FROM (1) TO (MAXVALUE);
BEGIN;
COPY party FROM STDIN;
666	cöpy	variation	dynamo	ünicode	Not very long	DEADF00D	9a0cf1eb-02e2-4b1f-bbe0-449fa4a99969	\\x01020304	\\xFFFF	\N	0.11111	0.43211	0.01010	2100-01-29	2050-04-01 19:30:00	12 hours	0 years
777	fdjkl	r89809rew	^ß[]#~	\N	Das also ist des Pudels Kern.	00	fe288446-05f6-4074-9e9e-6ee41af7b377	\\x00	\\x00	FALSE	10	1002	1003	2019-05-01	2019-05-01 0:00:00	0 seconds	1 year
\.
INSERT INTO party (id, lc, lr, lb)
   VALUES (12, 'very long character', '\x0001020304', '\xFFFEFDFC');
SELECT id, lr, lb, c FROM typetest1 ORDER BY id;
ROLLBACK;

BEGIN;
CREATE TABLE shortpart (
   id integer NOT NULL,
   c  character(10)
) PARTITION BY LIST (id);
ALTER TABLE shortpart ATTACH PARTITION shorty FOR VALUES IN (1, 2, 3, 4, 5, 6, 7, 8, 9);
INSERT INTO shortpart (id, c) VALUES (6, 'returnme') RETURNING *;
ROLLBACK;

/*
 * Test triggers on foreign tables.
 */

-- trigger function
CREATE FUNCTION shorttrig() RETURNS trigger LANGUAGE plpgsql AS
$$BEGIN
   IF TG_OP IN ('UPDATE', 'DELETE') THEN
      RAISE WARNING 'trigger % % OLD row: id = %, c = %', TG_WHEN, TG_OP, OLD.id, OLD.c;
   END IF;
   IF TG_OP IN ('INSERT', 'UPDATE') THEN
      RAISE WARNING 'trigger % % NEW row: id = %, c = %', TG_WHEN, TG_OP, NEW.id, NEW.c;
   END IF;
   RETURN NEW;
END;$$;

-- test BEFORE trigger
CREATE TRIGGER shorttrig BEFORE UPDATE ON shorty FOR EACH ROW EXECUTE PROCEDURE shorttrig();
BEGIN;
UPDATE shorty SET id = id + 1 WHERE id = 4;
ROLLBACK;

-- test AFTER trigger
DROP TRIGGER shorttrig ON shorty;
CREATE TRIGGER shorttrig AFTER UPDATE ON shorty FOR EACH ROW EXECUTE PROCEDURE shorttrig();
BEGIN;
UPDATE shorty SET id = id + 1 WHERE id = 4;
ROLLBACK;

-- test AFTER INSERT trigger with COPY
DROP TRIGGER shorttrig ON shorty;
CREATE TRIGGER shorttrig AFTER INSERT ON shorty FOR EACH ROW EXECUTE PROCEDURE shorttrig();
BEGIN;
COPY shorty FROM STDIN;
42	hammer
753	rom
0	\N
\.
ROLLBACK;

/*
 * Test ORDER BY pushdown.
 */

-- don't push down string data types
EXPLAIN (COSTS off) SELECT id FROM typetest1 ORDER BY id, vc;
-- push down complicated expressions
EXPLAIN (COSTS off) SELECT id FROM typetest1 ORDER BY length(vc), CASE WHEN vc IS NULL THEN 0 ELSE 1 END, ts DESC NULLS FIRST FOR UPDATE;
SELECT id FROM typetest1 ORDER BY length(vc), CASE WHEN vc IS NULL THEN 0 ELSE 1 END, ts DESC NULLS FIRST FOR UPDATE;

/*
 * Test that incorrect type mapping throws an error.
 */

-- create table with bad type matches
CREATE FOREIGN TABLE badtypes (
   id  integer OPTIONS (key 'yes') NOT NULL,
   c   xml,
   nc  xml
) SERVER oracle OPTIONS (table 'TYPETEST1');
-- should fail for column "nc", as "c" is not used
SELECT id, nc FROM badtypes WHERE id = 1;
-- this will fail for inserting a NULL in column "c"
INSERT INTO badtypes (id, nc) VALUES (42, XML '<empty/>');
-- remove foreign table
DROP FOREIGN TABLE badtypes;

/*
 * Test subplans (initplans)
 */

-- testcase for bug #364
SELECT id FROM typetest1
WHERE vc NOT IN (SELECT * FROM (VALUES ('short'), ('other')) AS q)
ORDER BY id;

/*
 * Test type coerced array parameters (bug #452)
 */

PREPARE stmt(varchar[]) AS SELECT id FROM typetest1 WHERE vc = ANY ($1);
EXECUTE stmt('{varlena,nonsense}');
EXECUTE stmt('{varlena,nonsense}');
EXECUTE stmt('{varlena,nonsense}');
EXECUTE stmt('{varlena,nonsense}');
EXECUTE stmt('{varlena,nonsense}');
EXECUTE stmt('{varlena,nonsense}');
DEALLOCATE stmt;


/*
 * Test push-down of the LIMIT clause.
 */

-- the LIMIT clause is pushed down with and without ORDER BY
EXPLAIN (COSTS off) SELECT d FROM typetest1 LIMIT 2;
SELECT d FROM typetest1 LIMIT 2;
EXPLAIN (COSTS off) SELECT d FROM typetest1 ORDER BY d LIMIT 2;
SELECT d FROM typetest1 ORDER BY d LIMIT 2;
-- the LIMIT clause is not pushed down because the ORDER BY is not
EXPLAIN (COSTS off) SELECT d FROM typetest1 ORDER BY lc LIMIT 2;
-- with an OFFSET clause, the offset value is added to the limit
EXPLAIN (COSTS off) SELECT * FROM qtest LIMIT 1 OFFSET 2;
SELECT * FROM qtest LIMIT 1 OFFSET 2;
-- no LIMIT push-down if there is a GROUP BY clause
EXPLAIN (COSTS off) SELECT d, count(*) FROM typetest1 GROUP BY d LIMIT 2;
SELECT d, count(*) FROM typetest1 GROUP BY d LIMIT 2;
-- no LIMIT push-down if there is an aggregate function
EXPLAIN (COSTS off) SELECT 12 - count(*) FROM typetest1 LIMIT 1;
SELECT 12 - count(*) FROM typetest1 LIMIT 1;
-- no LIMIT push-down if there is a local WHERE condition
EXPLAIN (COSTS OFF) SELECT id FROM typetest1 WHERE vc < 'u' LIMIT 1;
SELECT id FROM typetest1 WHERE vc < 'u' LIMIT 1;

/* test ANALYZE */

ANALYZE typetest1;
ANALYZE longy;
-- bug reported by Jan
ANALYZE shorty;

/* test if views and SECURITY DEFINER functions use the correct user mapping */

CREATE ROLE duff LOGIN;
GRANT SELECT ON typetest1 TO PUBLIC;

CREATE VIEW v_typetest1 AS SELECT id FROM typetest1;
GRANT SELECT ON v_typetest1 TO PUBLIC;

CREATE VIEW v_join AS
   SELECT id, a.vc, b.c
   FROM typetest1 AS a
      JOIN typetest1 AS b USING (id);
GRANT SELECT ON v_join TO PUBLIC;

CREATE FUNCTION f_typetest1() RETURNS TABLE (id integer)
   LANGUAGE sql SECURITY DEFINER AS
'SELECT id FROM public.typetest1';

SET SESSION AUTHORIZATION duff;
-- this should fail
SELECT id FROM typetest1 ORDER BY id;
-- these should succeed
SELECT id FROM v_typetest1 ORDER BY id;
SELECT c FROM v_join WHERE vc = 'short';
SELECT id FROM f_typetest1() ORDER BY id;
-- clean up
RESET SESSION AUTHORIZATION;
DROP ROLE duff;

/* test TIMESTAMP WITH LOCAL TIME ZONE */
INSERT INTO typetest2 (id, ts1, ts2, ts3) VALUES (
   2,
   '2020-12-31 00:00:00 UTC',
   '2020-12-31 00:00:00',
   '2020-12-31'
);
SELECT * FROM typetest2;
SET timezone = 'Asia/Kolkata';
INSERT INTO typetest2 (id, ts1, ts2, ts3) VALUES (
   3,
   '2020-12-31 00:00:00 UTC',
   '2020-12-31 00:00:00',
   '2020-12-31'
);
SELECT * FROM typetest2;
RESET timezone;
