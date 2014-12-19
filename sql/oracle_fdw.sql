/*
 * Install the extension and define the tables.
 * All the foreign tables defined refer to the same Oracle table.
 */

SET client_min_messages = WARNING;

CREATE EXTENSION oracle_fdw;

-- TWO_TASK or ORACLE_HOME and ORACLE_SID must be set in the server's environment for this to work
CREATE SERVER oracle FOREIGN DATA WRAPPER oracle_fdw OPTIONS (dbserver '');

CREATE USER MAPPING FOR PUBLIC SERVER oracle OPTIONS (user 'SCOTT', password 'tiger');

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
