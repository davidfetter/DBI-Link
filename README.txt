Here is how to use what you have:

As database superuser (often postgres, but check for your system), do
the following:

INSTALLATION

1.  Load PL/Perlu into your database.  See the createlang documents
for details on how to do this.

2.  Load dbi_link.sql, which will make the underlying methods aka functions
available.

ADDING FOREIGN DB CONNECTION

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

SELECT set_config(
  'search_path'
, 'dbi_link,' || current_setting('search_path')
, false
);

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

