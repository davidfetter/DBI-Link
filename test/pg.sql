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
