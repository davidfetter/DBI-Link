/* 
 * Database type:                Pg
 * Host:                         localhost
 * Port:                         5432
 * Database:                     neil
 * User:                         neil
 * Password:                     NULL
 * Remote Schema:                public
 * Remote Catalog:               NULL
 * Schema with accessor methods: neil
 */

SELECT make_accessor_functions('Pg','localhost',5432,'neil','neil',NULL,'public',NULL,'neil');
