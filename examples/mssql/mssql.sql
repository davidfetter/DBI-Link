SELECT dbi_link.prepend_to_search_path('dbi_link');

SELECT dbi_link.make_accessor_functions(
    'dbi:Sybase:NiftyDB;host=mssql.host.com;port=1433'::dbi_link.data_source, 
    'user'::text,
    'secret_password'::text,
    '{"AutoCommit":1,"RaiseError":1}'::dbi_link.json,
    NULL::dbi_link.json,
    NULL::text,
    NULL::text,
    'nifty_mssql'::text
);
