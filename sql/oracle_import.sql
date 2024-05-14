SET client_min_messages = WARNING;

CREATE SCHEMA import;

/* first, import only a table and a materialized view */
IMPORT FOREIGN SCHEMA "SCOTT"
   LIMIT TO ("typetest1", ttv, mattest2)
   FROM SERVER oracle
   INTO import
   OPTIONS (case 'lower', collation 'C', skip_views 'true');

SELECT t.relname, fs.srvname, ft.ftoptions
FROM pg_foreign_table ft
     JOIN pg_class t ON ft.ftrelid = t.oid
     JOIN pg_foreign_server fs ON ft.ftserver = fs.oid
WHERE relnamespace = 'import'::regnamespace
ORDER BY t.relname;

/* then import only a view */
IMPORT FOREIGN SCHEMA "SCOTT"
   LIMIT TO ("typetest1", ttv, mattest2)
   FROM SERVER oracle
   INTO import
   OPTIONS (case 'lower', skip_tables 'true', skip_matviews 'true');

SELECT t.relname, fs.srvname, ft.ftoptions
FROM pg_foreign_table ft
     JOIN pg_class t ON ft.ftrelid = t.oid
     JOIN pg_foreign_server fs ON ft.ftserver = fs.oid
WHERE relnamespace = 'import'::regnamespace
ORDER BY t.relname;

SELECT t.relname, a.attname, a.atttypid::regtype, a.attfdwoptions
FROM pg_attribute AS a
   JOIN pg_class AS t ON t.oid = a.attrelid
WHERE t.relname IN ('typetest1', 'ttv', 'mattest2')
  AND a.attnum > 0
  AND t.relnamespace = 'import'::regnamespace
  AND NOT a.attisdropped
ORDER BY t.relname, a.attnum;
