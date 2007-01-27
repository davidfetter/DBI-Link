/* 
 * Data source:     dbi:Oracle:hr;host=localhost;sid=xe
 * User:            hr
 * Password:        foobar
 * dbh attributes:  {AutoCommit => 1, RaiseError => 1}
 * dbh environment: NULL
 * remote schema:   NULL
 * remote catalog:  NULL
 * local schema:    hr
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
    'dbi:Oracle:hr;host=localhost;sid=xe',
    'hr',
    'foobar',
    '---
AutoCommit: 1
RaiseError: 1
',
    NULL,
    NULL,
    NULL,
    'hr'
);

