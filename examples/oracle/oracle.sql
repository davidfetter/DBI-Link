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

SELECT dbi_link.prepend_to_search_path('dbi_link');

SELECT make_accessor_functions(
    'dbi:Oracle:hr;host=localhost;sid=xe',
    'hr',
    'foobar',
    '---
AutoCommit: 1
RaiseError: 1
ora_array_chunk_size: 65536
',
    NULL,
    NULL,
    NULL,
    'hr'
);

