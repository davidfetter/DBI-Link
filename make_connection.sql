CREATE OR REPLACE FUNCTION make_accessor_functions (
  driver TEXT
, host TEXT
, port INTEGER
, database TEXT
, db_user TEXT
, db_password TEXT
, schema_name TEXT
, catalog_name TEXT
, local_schema TEXT
)
RETURNS BOOLEAN
LANGUAGE plperlu
AS $$
use DBI;
my $dbh;
my ($driver, $host, $port, $database, $db_user, $db_password,
    $schema_name, $catalog_name, $local_schema) = @_;
$schema_name =~ s/'/''/g;
$local_schema =~ s/'/''/g;
check_connection(
  driver => $driver
, host => $host
, port => $port
, database => $database
, db_user => $db_user
, db_password => $db_password
);

create_schema(
  local_schema => $local_schema
);

create_accessor_methods(
  schema_name => $schema_name
, local_schema => $local_schema
);

return TRUE;

sub check_connection {
    my %parms = (
      driver => undef
    , host => undef
    , port => undef
    , database => undef
    , db_user => undef
    , db_password => undef
    , @_
    );
    $parms{'driver'} =~ s/'/''/g;
    my $dtsql = <<SQL;
SELECT ad
FROM available_drivers() AS "ad"
WHERE ad = '$parms{"driver"}'
SQL
    elog NOTICE, $dtsql;
    my $driver_there = spi_exec_query($dtsql);
    if ($driver_there->{rows}[0] == 0) {
        elog ERROR, "Driver $parms{'driver'} is not available.  Can't look at database."
    }
    my $db_label = ($driver eq 'Pg')?'dbname':'database';
    $host = (length $host)?$host:'localhost';
    $dbh = DBI->connect(
      "dbi:$parms{'driver'}:$db_label=$parms{'database'};host=$parms{'host'};port=$parms{'port'}"
    , $parms{'db_user'}
    , $parms{'db_password'}
      { RaiseError => 0
      , PrintError => 0
      , AutoCommit => 0
      }
    );
    if ($DBI::errstr) {
        elog ERROR, <<ERR;
Could not connect to database
type: $parms{'driver'}
host: $parms{'host'}
database: $parms{'database'}
user: $parms{'db_user'}
password: $parms{'db_password'}
                                                                            
$DBI::errstr
ERR
    }
}

sub create_schema {
    my %parms = (
      local_schema => undef
    , @_
    );
    elog ERROR, "Must have a local_schema!" unless $parms{'local_schema'} =~ /\S/;
    my $sql_check_for_schema = <<SQL;
SELECT COUNT(*) AS "cnt"
FROM pg_namespace
WHERE nspname = '$parms{"local_schema"}'
SQL
    elog NOTICE, "Attempting\n$sql_check_for_schema\n";
    my $schema_there = spi_exec_query($sql_check_for_schema);
    if ($schema_there->{rows}[0]->{'cnt'} != 0) {
        elog ERROR, "Schema $parms{'local_schema'} already exists.  1st row was ".$schema_there->{rows}[0]->{'cnt'};
    } else {
        my $sql_create_schema = "CREATE SCHEMA $parms{'local_schema'}";
        my $rv = spi_exec_query($sql_create_schema);
        if ($rv->{status} eq 'SPI_OK_UTILITY') {
            elog NOTICE, "Created schema $parms{'local_schema'}."
        } else {
            elog ERROR, "Could not create schema $parms{'local_schema'}.  Status was\n$rv->{status}";
        }
    }
}

sub create_accessor_methods {
    my %parms = (
      schema_name => undef
    , local_schema  => undef
    , @_
    );
    my $types = "'TABLE','VIEW'";
    my $sth = $dbh->table_info($catalog_name, $parms{'schema_name'}, '%', $types);
    my $db_label = ($driver eq 'Pg')?'dbname':'database';
    my $f_host = (defined $host)?$host:'localhost';
    while(my $table = $sth->fetchrow_hashref) {
        my $type_name = $parms{'local_schema'}.'.'.$table->{TABLE_NAME}.'_type';
        my @cols;
        my $sth2 = $dbh->column_info(undef, $schema_name, $table->{TABLE_NAME}, '%');
        while(my $column = $sth2->fetchrow_hashref) {
            my $line = $column->{COLUMN_NAME};
            if ( $column->{TYPE_NAME} =~ /integer/i ) {
                $line .= ' INTEGER';
            } else {
                $line .= ' TEXT';
            }
            push @cols, $line;
        }
        $sth2->finish;
        my $sql = "CREATE TYPE $type_name AS (\n  "
                . join("\n, ", @cols)
                . "\n)"
                ;
        elog NOTICE, "Trying to create type\n$sql\n";
        my $rv = spi_exec_query($sql);
        if ($rv->{status} eq 'SPI_OK_UTILITY') {
            elog NOTICE, "Created type $type_name."
        } else {
            elog ERROR, "Could not create type $type_name.  $rv->{status}";
        }
        my $method_name = "$parms{'local_schema'}.$table->{TABLE_NAME}";
        $sql = <<SQL;
CREATE OR REPLACE FUNCTION $method_name ()
RETURNS SETOF $type_name
LANGUAGE plperlu
AS \$\$
use DBI;

my \$dbh = DBI->connect(
  "dbi:$driver:$db_label=$database;host=$f_host;port=$port"
, '$db_user'
, '$db_password'
, {
    RaiseError => 0
  , PrintError => 0
  , AutoCommit => 0
  }
);

if (\$DBI::errstr) {
    elog ERROR, "
Could not connect to database
type: $driver
host: $f_host
database: $database
user: $db_user
password: $db_password
error: \$DBI::errstr
";
}

elog NOTICE, "Connected to database";

my \$sql = 'SELECT * FROM $table->{TABLE_NAME}';

elog NOTICE, "sql is\n\$sql";

my \$sth = \$dbh->prepare(\$sql);
if (\$DBI::errstr) {
    elog ERROR, "
Cannot prepare

\$sql

\$DBI::errstr
";
}

elog NOTICE, "Prepared query";

my \$rowset;
\$sth->execute;

elog NOTICE, "Started executing query";

while(my \$row = \$sth->fetchrow_hashref) {
    push \@\$rowset, \$row;
}

elog NOTICE, "Finished executing query";

\$sth->finish;
\$dbh->disconnect;
return \$rowset;
\$\$;
SQL
        elog NOTICE, "Trying to create method $method_name\n";
        my $rv = spi_exec_query($sql);
        if ($rv->{status} eq 'SPI_OK_UTILITY') {
            elog NOTICE, "Created method $method_name."
        } else {
            elog ERROR, "Could not create method $method_name.  $rv->{status}";
        }
    }
    $sth->finish;
}

$$;
