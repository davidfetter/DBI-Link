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

SELECT dbi_link.ensure_in_search_path('dbi_link');

SELECT dbi_link.make_accessor_functions(
    'dbi:mysql:database=sakila;host=localhost',
    'root',
    'foobar',
    '{"AutoCommit": 1, "RaiseError": 1}',
    NULL,
    NULL,
    NULL,
    'sakila'
);
