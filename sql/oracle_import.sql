SET client_min_messages = WARNING;

CREATE SCHEMA import;

IMPORT FOREIGN SCHEMA "SCOTT" LIMIT TO ("typetest1") FROM SERVER oracle INTO import OPTIONS (case 'lower');

\det import.*

\d import.typetest1
