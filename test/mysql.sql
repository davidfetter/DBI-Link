/* 
 * Data source:     dbi:mysql:database=world;host=localhost
 * User:            root
 * Password:        NULL
 * dbh attributes:  {AutoCommit => 1, RaiseError => 1, FetchHashKeyName => "NAME_lc"}
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
',
    NULL,
    NULL,
    'world'
);
