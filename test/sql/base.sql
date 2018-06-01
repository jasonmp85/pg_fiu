\set ECHO 0
BEGIN;
\i sql/pg_fiu.sql
\set ECHO all

-- You should write your tests

SELECT pg_fiu('test');

ROLLBACK;
