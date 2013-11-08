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

SELECT dbi_link.ensure_in_search_path('dbi_link');

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

