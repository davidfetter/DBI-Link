/* 
 * Data source:     dbi:Excel:file=settings.xls
 * User:            NULL
 * Password:        NULL
 * dbh attributes:  NULL
 * remote schema:   NULL
 * remote catalog:  NULL
 * local schema:    excel
 */

SELECT set_config(
  'search_path'
, 'dbi_link,' || current_setting('search_path')
, false
);

SELECT make_accessor_functions(
  'dbi:Excel:file=settings.xls'
, NULL
, NULL
, NULL
, NULL
, NULL
, 'excel'
);
