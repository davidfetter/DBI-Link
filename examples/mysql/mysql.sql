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

SELECT dbi_link.make_accessor_functions(
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
