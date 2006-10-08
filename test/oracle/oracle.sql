/* 
 * Data source:     dbi:Oracle:hr;host=localhost;sid=xe
 * User:            hr
 * Password:        foobar
 * dbh attributes:  {AutoCommit => 1, RaiseError => 1}
 * dbh environment: See below
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
    $$---
env_action: overwrite
env_name: NLS_LANG
env_value: AMERICAN_AMERICA.AL32UTF8
---
env_action: overwrite
env_name: ORACLE_HOME
env_value: /usr/lib/oracle/xe/app/oracle/product/10.2.0/client
---
env_action: overwrite
env_name: SQLPATH
env_value:
/usr/lib/oracle/xe/app/oracle/product/10.2.0/client/sqlplus
---
env_action: prepend
env_name: PATH
env_value:
'/usr/lib/oracle/xe/app/oracle/product/10.2.0/client/bin:'
---
env_action: overwrite
env_name: LD_LIBRARY_PATH
env_value:
'/usr/lib/oracle/xe/app/oracle/product/10.2.0/client/lib:'
    $$,
    NULL,
    NULL,
    'hr'
);

