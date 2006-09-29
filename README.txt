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
come from the perldoc of the appropriate DBD::Pg, except for "local schema,"
which you must supply.  "local schema" must not yet exist.

/* 
 * Data source:     dbi:Pg:dbname=neil;host=localhost;port=5432
 * User:            neil
 * Password:        NULL
 * dbh attributes:  {AutoCommit => 1, RaiseError => 1}
 * remote schema:   public
 * remote catalog:  NULL
 * local schema:    neil
 */

UPDATE
    pg_catalog.pg_settings
SET
    setting = 'dbi_link,' || setting
WHERE
    name = 'search_path'
;

SELECT make_accessor_functions(
  'dbi:Pg:dbname=neil;host=localhost;port=5432'
, 'neil'
, NULL
, '{AutoCommit => 1, RaiseError => 1}'
, 'public'
, NULL
, 'neil'
);

Congratulations!  You can now access anything in the 'neil' schema for both
read and write.

USING THE FOREIGN DB CONNECTION

DELETE FROM neil.person
WHERE id < 10;

