/* 
 * Data source:     dbi:Excel:file=settings.xls
 * User:            NULL
 * Password:        NULL
 * dbh attributes:  NULL
 * remote schema:   NULL
 * remote catalog:  NULL
 * local schema:    excel
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
  'dbi:Excel:file=settings.xls'
, NULL
, NULL
, NULL
, NULL
, NULL
, NULL
, 'excel'
);
