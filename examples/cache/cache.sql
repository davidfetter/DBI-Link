/*
 * Data source:     dbi:ODBC:Cache
 * User:            LIVE:DAVIDB
 * Password:        NULL
 * dbh attributes:  NULL
 * dbh environment: NULL
 * remote schema:   NULL
 * remote catalog:  NULL
 * local schema:    cache
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
    'dbi:ODBC:Cache',
    'LIVE:DAVIDB',
    NULL,
    NULL,
    NULL,
    NULL,
    NULL,
    'cache'
);

