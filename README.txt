If you've ever wanted to join PostgreSQL tables from other data you
can access via Perl's DBI, this is your project.

DBI-Link requires PostgreSQL 8.0 or better, and has been tested with Perl
5.8.5.  Backports to older versions of PostgreSQL are unlikely, and earlier
versions of Perl only if there is an excellent reason.

The first milestone, which has working prototype code, is a user-visible
function that takes a set of connection parameters to pass to DBI and a string
of SQL. On success, it returns a SETOF RECORD [doc ref here].

The second milestone, tagged with version 1.0, takes a set of parameters for
connecting to a remote data source.  It queries the data source, creates a new
schema for it, and creates VIEWs, shadow TABLEs, TYPEs and accessor FUNCTIONs
for each TABLE and VIEW it finds by creating VIEWs and shadow TABLEs for each
TABLE.

The third milestone, in design phase, will handle JOINs with remote data
sources.  The current idea is to use VIEW-like objects which are stored as
above.
