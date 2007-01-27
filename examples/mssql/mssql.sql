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
    'dbi:Sybase:NiftyDB'::dbi_link.data_source, 
    'user'::text,
    'secret_password'::text,
    '---
AutoCommit: 1
RaiseError: 1
'::dbi_link.yaml,
    NULL::dbi_link.yaml,
    NULL::text,
    NULL::text,
    'nifty_mssql'::text
);
