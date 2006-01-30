CREATE SCHEMA dbi_link;

COMMENT ON SCHEMA dbi_link IS
$$
This schema holds all the functions needed for using dbi-link.
$$;

SET search_path TO dbi_link;

CREATE TABLE dbi_connection (
  data_source_id SERIAL PRIMARY KEY
, data_source TEXT NOT NULL
, user_name TEXT
, auth TEXT
, dbh_attr TEXT
, remote_schema TEXT
, remote_catalog TEXT
, local_schema TEXT
, UNIQUE(data_source, user_name)
);

COMMENT ON TABLE dbi_connection IS
$$
This table contains the necessary connection information for a DBI
connection.  For now, dbh_attr is a TEXT representation of the DBI
database handle attributes, as it allows maximum flexibility.

$$;

--------------------------------------
--                                  --
--  PL/PerlU Interface to DBI. :)   --
--                                  --
--------------------------------------
CREATE OR REPLACE FUNCTION available_drivers()
RETURNS SETOF TEXT
LANGUAGE plperlu
AS $$
    require 5.8;
    use DBI;
    return \@{[ DBI->available_drivers ]};
$$;

COMMENT ON FUNCTION available_drivers() IS $$
This is a wrapper around the DBI function of the same name which
returns a list (SETOF TEXT) of DBD:: drivers available through DBI on
your machine.  This is used internally and is unlikely to be called
directly.
$$;

CREATE OR REPLACE FUNCTION data_sources(TEXT)
RETURNS SETOF TEXT
LANGUAGE plperlu
AS $$
    require 5.8;
    use DBI;
    return \@{[ DBI->data_sources($_[0]) ]};
$$;

COMMENT ON FUNCTION data_sources(TEXT) IS $$
This is a wrapper around the DBI function of the same name.  It takes
as input one of the rows from available_drivers() and returns known
data sources for that driver.  You will probably not call this
function, but it's there just in case.
$$;

\i remote_query.sql
\i make_connection.sql
SET search_path TO DEFAULT;
