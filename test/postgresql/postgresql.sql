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
    setting =
        CASE WHEN setting ~ 'dbi_link'
        THEN setting
        ELSE 'dbi_link,' || setting
        END
WHERE
    name = 'search_path'
;

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

