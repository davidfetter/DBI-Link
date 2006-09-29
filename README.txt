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

Add Foreign Database Connection

Do the following, with the appropriate parameters.  "Appropriate parameters"
come from the perldoc of the appropriate DBD, in this case, DBD::mysql, except
for "local schema," which you must supply.  "local schema" must not yet exist.

/* 
 * Data source:     dbi:mysql:database=world;host=localhost
 * User:            root
 * Password:        NULL
 * dbh attributes:  {
 *                      AutoCommit => 1,
 *                      RaiseError => 1,
 *                      FetchHashKeyName => "NAME_lc"
 *                  }
 * remote schema:   NULL
 * remote catalog:  NULL
 * local schema:    world
 */

UPDATE
    pg_catalog.pg_settings
SET
    setting =
        CASE WHEN setting ~ 'dbi_link'
        THEN setting
        ELSE 'dbi_link,' || setting
        END
WHERE
    name = 'search_path'
;

SELECT make_accessor_functions(
    'dbi:mysql:database=world;host=localhost',
    'root',
    'foobar',
    '---
AutoCommit: 1
RaiseError: 1
FetchHashKeyName: NAME_lc
',                           -- This is YAML.
    NULL,
    NULL,
    'world'
);

USING THE FOREIGN DB CONNECTION

UPDATE world.country
SET code2 = lower(code2)
WHERE id < 10;

