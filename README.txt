Here is how to use what you have:

As database superuser (often postgres, but check for your system), do
the following:

Install DBI-Link Software:

1. Create or choose a previously created database where DBI-Link will operate.
For the rest of this document, that database's name is 'outreach'.  To create
a new database from the shell as the postgres user, do:

    createdb outreach

You can find more documents on createdb here:
http://www.postgresql.org/docs/current/static/app-createdb.html

2. As postgres, install the PL/PerlU language into that database.

    createlang plperlu outreach

You can find more documents on createlang here:
http://www.postgresql.org/docs/current/static/app-createlang.html

3.  Load dbi_link.sql, which will make the underlying methods aka functions
available.

    psql -f dbi_link.sql outreach

Add Remote Database Connection

Do the following, with the appropriate parameters.  "Appropriate parameters"
come from the perldoc of the appropriate DBD, in this case, DBD::mysql, except
for "local schema," which you must supply.  "local schema" must not yet exist.

/* 
 * Data source:     dbi:mysql:database=world;host=localhost
 * User:            root
 * Password:        foobar
 * dbh attributes:  {AutoCommit => 1, RaiseError => 1}
 * dbh environment: NULL
 * remote schema:   NULL
 * remote catalog:  NULL
 * local schema:    world
 */

UPDATE
    pg_catalog.pg_settings
SET
    setting =
        CASE WHEN 'dbi_link' = ANY(string_to_array(setting, ','))
        THEN setting
        ELSE 'dbi_link,' || setting
        END
WHERE
    name = 'search_path'
;

SELECT make_accessor_functions(
    'dbi:mysql:database=sakila;host=localhost',
    'root',
    'foobar',
    '---
AutoCommit: 1
RaiseError: 1
',
    NULL,
    NULL,
    NULL,
    'sakila'
);


USING THE REMOTE DB CONNECTION

SELECT
    initcap(title)
FROM
    sakila.film;

REFRESHING THE REMOTE DB SCHEMA

Any time the DDL of the remote database changes, drop its local schema and
call dbi_link.refresh_schema.  Here's an example:

DROP SCHEMA sakila CASCADE;

SELECT
    dbi_link.refresh_schema(data_source_id)
FROM
    dbi_link.dbi_connection
WHERE
    local_schema = 'sakila';

