/* 
 * Data source:     dbi:Pg:dbname=neil;host=localhost;port=5432
 * User:            neil
 * Password:        NULL
 * dbh attributes:  {AutoCommit => 1, RaiseError => 1}
 * local schema:    neil
 */

SELECT make_accessor_functions(
  'dbi:Pg:dbname=neil;host=localhost;port=5432'
, 'neil'
, NULL
, '{AutoCommit => 1, RaiseError => 1}'
, neil
);
