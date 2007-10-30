/* 
 * Data source:     dbi:Pg:dbname=neil;host=localhost;port=5432
 * User:            neil
 * Password:        NULL
 * dbh attributes:  {AutoCommit => 1, RaiseError => 1}
 * dbh environment: NULL
 * remote schema:   public
 * remote catalog:  NULL
 * local schema:    neil
 */

SELECT dbi_link.prepend_to_search_path('dbi_link');

SELECT make_accessor_functions(
    'dbi:Pg:dbname=neil;host=localhost;port=5432',
    'neil',
    NULL,
    '---
AutoCommit: 1
RaiseError: 1
',
    NULL,
    'public',
    NULL,
    'neil'
);

