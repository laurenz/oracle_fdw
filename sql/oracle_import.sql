SET client_min_messages = WARNING;

CREATE SCHEMA import;

/* first, import only the table */
IMPORT FOREIGN SCHEMA "SCOTT"
   LIMIT TO ("typetest1", ttv)
   FROM SERVER oracle
   INTO import
   OPTIONS (case 'lower', collation 'C', skip_views 'true');

SELECT t.relname, fs.srvname, ft.ftoptions
FROM pg_foreign_table ft
     JOIN pg_class t ON ft.ftrelid = t.oid
     JOIN pg_foreign_server fs ON ft.ftserver = fs.oid
WHERE relnamespace = 'import'::regnamespace;

/* then import only the view */
IMPORT FOREIGN SCHEMA "SCOTT"
   LIMIT TO ("typetest1", ttv)
   FROM SERVER oracle
   INTO import
   OPTIONS (case 'lower', collation 'C', skip_tables 'true');

SELECT t.relname, fs.srvname, ft.ftoptions
FROM pg_foreign_table ft
     JOIN pg_class t ON ft.ftrelid = t.oid
     JOIN pg_foreign_server fs ON ft.ftserver = fs.oid
WHERE relnamespace = 'import'::regnamespace;

SELECT attname, atttypid::regtype, attfdwoptions
FROM pg_attribute
WHERE attrelid = 'typetest1'::regclass
  AND attnum > 0
  AND NOT attisdropped
ORDER BY attnum;
