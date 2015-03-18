/*
 * Install the extension and define the tables.
 * All the foreign tables defined refer to the same Oracle table.
 */

SET client_min_messages = WARNING;

CREATE EXTENSION oracle_fdw;

-- TWO_TASK or ORACLE_HOME and ORACLE_SID must be set in the server's environment for this to work
CREATE SERVER oracle FOREIGN DATA WRAPPER oracle_fdw OPTIONS (dbserver '');
CREATE SERVER another_server FOREIGN DATA WRAPPER oracle_fdw OPTIONS (dbserver '');

CREATE USER MAPPING FOR PUBLIC SERVER oracle OPTIONS (user 'SCOTT', password 'tiger');
CREATE USER MAPPING FOR PUBLIC SERVER another_server OPTIONS (user 'SCOTT', password 'tiger');

CREATE USER another_user;

-- Oracle table TYPETEST1 must be created for this one
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

-- a table for join tests
CREATE FOREIGN TABLE shorty2 (
   id  integer OPTIONS (key 'yes') NOT NULL,
   c   character(10)
) SERVER oracle OPTIONS (table 'TYPETEST2');
CREATE FOREIGN TABLE shorty3 (
   id  integer OPTIONS (key 'yes') NOT NULL,
   c   character(10)
) SERVER another_server OPTIONS (table 'TYPETEST2');
GRANT SELECT ON shorty2 TO another_user;
CREATE VIEW v_shorty2 AS SELECT * FROM shorty2;
ALTER VIEW v_shorty2 OWNER TO another_user;
GRANT ALL ON v_shorty2 TO PUBLIC;

/*
 * Empty the table and INSERT some samples.
 */

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
   '2009-01-26 22:30:00 PST',
   '1 day 2 hours 30 seconds 1 microsecond',
   '-6 months'
);

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
   '2014-11-25',
   '2014-11-25 15:02:54.893532 PST',
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

-- generate data for join tests
BEGIN;
DELETE FROM shorty2;
INSERT INTO shorty2 (id, c) VALUES (1, '2-1');
INSERT INTO shorty2 (id, c) VALUES (2, '2-2');
INSERT INTO shorty2 (id, c) VALUES (4, '2-4');
INSERT INTO shorty2 (id, c) VALUES (5, '2-5');
COMMIT;

/*
 * Test SELECT, UPDATE ... RETURNING, DELETE and transactions.
 */

-- simple SELECT
SELECT id, c, nc, vc, nvc, length(lc), r, u, length(lb), length(lr), b, num, fl, db, d, ts, ids, iym, x FROM longy ORDER BY id;
-- mass UPDATE
WITH upd (id, c, lb) AS
   (UPDATE longy SET c = substr(c, 1, 9) || 'u',
                    lb = lb || bytea('\x00'),
                    lr = lr || bytea('\x00')
   WHERE id < 3 RETURNING id + 1, c, lb)
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

/*
 * Test EXPLAIN support.
 */

EXPLAIN (COSTS off) UPDATE typetest1 SET lc = current_timestamp WHERE id < 4 RETURNING id + 1;
EXPLAIN (VERBOSE on, COSTS off) SELECT * FROM shorty;

/*
 * join SELECT
 */
-- query with ORDER BY causes MergeJoin even cost is not cheapest, so supress it.
SET enable_mergejoin = off;
-- simple join
EXPLAIN (COSTS false)
SELECT s1.id, s1.c, s2.id, s2.c FROM shorty s1 JOIN shorty2 s2 ON s1.id = s2.id ORDER BY 1, 3;
SELECT s1.id, s1.c, s2.id, s2.c FROM shorty s1 JOIN shorty2 s2 ON s1.id = s2.id ORDER BY 1, 3;
-- simple join in CTE
EXPLAIN (COSTS false)
WITH s AS (SELECT s1.id, s1.c, s2.id, s2.c FROM shorty s1 JOIN shorty2 s2 ON s1.id = s2.id) SELECT * FROM s ORDER BY 1, 3;
WITH s AS (SELECT s1.id, s1.c, s2.id, s2.c FROM shorty s1 JOIN shorty2 s2 ON s1.id = s2.id) SELECT * FROM s ORDER BY 1, 3;
-- left outer join
EXPLAIN (COSTS false)
SELECT s1.id, s1.c, s2.id, s2.c FROM shorty s1 LEFT JOIN shorty2 s2 ON s1.id = s2.id ORDER BY 1, 3;
SELECT s1.id, s1.c, s2.id, s2.c FROM shorty s1 LEFT JOIN shorty2 s2 ON s1.id = s2.id ORDER BY 1, 3;
-- right outer join
EXPLAIN (COSTS false)
SELECT s1.id, s1.c, s2.id, s2.c FROM shorty s1 RIGHT JOIN shorty2 s2 ON s1.id = s2.id ORDER BY 1, 3;
SELECT s1.id, s1.c, s2.id, s2.c FROM shorty s1 RIGHT JOIN shorty2 s2 ON s1.id = s2.id ORDER BY 1, 3;
-- full outer join
EXPLAIN (COSTS false)
SELECT s1.id, s1.c, s2.id, s2.c FROM shorty s1 FULL JOIN shorty2 s2 ON s1.id = s2.id ORDER BY 1, 3;
SELECT s1.id, s1.c, s2.id, s2.c FROM shorty s1 FULL JOIN shorty2 s2 ON s1.id = s2.id ORDER BY 1, 3;
-- semi join (no push-down)
EXPLAIN (COSTS false)
SELECT s1.id, s1.c FROM shorty s1 WHERE EXISTS (SELECT 1 FROM shorty2 s2 WHERE s1.id = s2.id) ORDER BY 1;
SELECT s1.id, s1.c FROM shorty s1 WHERE EXISTS (SELECT 1 FROM shorty2 s2 WHERE s1.id = s2.id) ORDER BY 1;
-- anti join (no push-down)
EXPLAIN (COSTS false)
SELECT s1.id, s1.c FROM shorty s1 WHERE NOT EXISTS (SELECT 1 FROM shorty2 s2 WHERE s1.id = s2.id) ORDER BY 1;
SELECT s1.id, s1.c FROM shorty s1 WHERE NOT EXISTS (SELECT 1 FROM shorty2 s2 WHERE s1.id = s2.id) ORDER BY 1;
-- cross join (no push-down)
EXPLAIN (COSTS false)
SELECT * FROM shorty s1 CROSS JOIN shorty2 s2 ORDER BY 1, 3;
SELECT * FROM shorty s1 CROSS JOIN shorty2 s2 ORDER BY 1, 3;
-- UPDATE (no push-down)
EXPLAIN (COSTS false)
UPDATE shorty s1 SET c = s2.c FROM shorty2 s2 WHERE s1.id = s2.id;
UPDATE shorty s1 SET c = s2.c FROM shorty2 s2 WHERE s1.id = s2.id;
-- DELETE (no push-down)
EXPLAIN (COSTS false)
DELETE FROM shorty s1 USING shorty2 s2 WHERE s1.id = s2.id;
DELETE FROM shorty s1 USING shorty2 s2 WHERE s1.id = s2.id;
-- different server (no push-down)
EXPLAIN (COSTS false)
SELECT s1.id, s1.c, s2.id, s2.c FROM shorty s1 JOIN shorty3 s2 ON s1.id = s2.id ORDER BY 1, 3;
SELECT s1.id, s1.c, s2.id, s2.c FROM shorty s1 JOIN shorty3 s2 ON s1.id = s2.id ORDER BY 1, 3;
-- different checkAsUser
EXPLAIN (COSTS false)
SELECT s1.id, s1.c, s2.id, s2.c FROM shorty s1 JOIN v_shorty2 s2 ON s1.id = s2.id ORDER BY 1, 3;
SELECT s1.id, s1.c, s2.id, s2.c FROM shorty s1 JOIN v_shorty2 s2 ON s1.id = s2.id ORDER BY 1, 3;
-- unsafe join conditions (no push-down)
EXPLAIN (COSTS false)
SELECT s1.id, s1.c, s2.id, s2.c FROM shorty s1 JOIN shorty2 s2 ON s1.c = s2.c || now()::text ORDER BY 1, 3;
SELECT s1.id, s1.c, s2.id, s2.c FROM shorty s1 JOIN shorty2 s2 ON s1.c = s2.c || now()::text ORDER BY 1, 3;
-- with local filter (no push-down)
EXPLAIN (COSTS false)
SELECT s1.id, s1.c, s2.id, s2.c FROM shorty s1 JOIN shorty2 s2 ON s1.c = s2.c WHERE s1.c = now()::text ORDER BY 1, 3;
SELECT s1.id, s1.c, s2.id, s2.c FROM shorty s1 JOIN shorty2 s2 ON s1.c = s2.c WHERE s1.c = now()::text ORDER BY 1, 3;

/*
 * Test parameters.
 */

PREPARE stmt(integer) AS SELECT d FROM typetest1 WHERE id = $1;
-- six executions to switch to generic plan
EXECUTE stmt(1);
EXECUTE stmt(1);
EXECUTE stmt(1);
EXECUTE stmt(1);
EXECUTE stmt(1);
EXPLAIN EXECUTE stmt(1);
EXECUTE stmt(1);
DEALLOCATE stmt;

/*
 * Cleanup
 */
DROP OWNED BY another_user;
DROP USER another_user;
