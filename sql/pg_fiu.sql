CREATE OR REPLACE FUNCTION add_failure_point("name" text, fail_retval int4,
                                             failinfo_retval anyelement,
                                             fail_once boolean DEFAULT false)
RETURNS void
AS 'MODULE_PATHNAME', 'add_failure_point'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION add_random_failure_point("name" text, fail_retval int4,
                                                    failinfo_retval anyelement,
                                                    probability real,
                                                    fail_once boolean DEFAULT false)
RETURNS void
AS 'MODULE_PATHNAME', 'add_random_failure_point'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION add_external_failure_point("name" text, fail_retval int4,
                                                      failinfo_retval anyelement,
                                                      predicate text,
                                                      fail_once boolean DEFAULT false)
RETURNS void
AS 'MODULE_PATHNAME', 'add_external_failure_point'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION add_stack_failure_point("name" text, fail_retval int4,
                                                   failinfo_retval anyelement,
                                                   function_name text,
                                                   fail_once boolean DEFAULT false)
RETURNS void
AS 'MODULE_PATHNAME', 'add_stack_failure_point'
LANGUAGE C IMMUTABLE STRICT;

CREATE OR REPLACE FUNCTION failure_points(OUT "name" text, OUT fail_retval int4,
                                          OUT failinfo_desc text, OUT fail_once boolean,
                                          OUT method_desc text)
RETURNS SETOF record
AS 'MODULE_PATHNAME', 'failure_points'
LANGUAGE C IMMUTABLE STRICT;
