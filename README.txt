If you've ever wanted to join PostgreSQL tables from other data you
can access via Perl's DBI, this is your project.  The code, though
slow and resources-intensive for the moment, is very small and very
flexible.

The first milestone, which has working prototype code, is a
user-visible function that takes a set of connection parameters to
pass to DBI and a string of SQL. On success, it returns a SETOF RECORD
[doc ref here].

The second milestone, now with working prototype code, takes a set of
parameters for connecting to a remote data source.  It queries the data source
and creates TYPEs and accessor FUNCTIONs for each TABLE and VIEW it finds.

The third milestone, in design phase, will handle JOINs with remote data
sources.  The current idea is to use VIEW-like objects which are stored as
above.
