/*
 * Author: Jason Petersen <jasonmp85@gmail.com>
 * Created at: 2018-05-30 05:57:58 +0000
 *
 */

--
-- This is a example code genereted automaticaly
-- by pgxn-utils.

-- This is how you define a C function in PostgreSQL.
CREATE OR REPLACE FUNCTION pg_fiu(text)
RETURNS text
AS 'pg_fiu'
LANGUAGE C IMMUTABLE STRICT;

-- See more: http://www.postgresql.org/docs/current/static/xfunc-c.html
