CREATE OR REPLACE FUNCTION make_accessor_functions (
  driver TEXT
, host TEXT
, port INTEGER
, database TEXT
, db_user TEXT
, db_password TEXT
, schema_name TEXT
, catalog_name TEXT
)
RETURNS BOOLEAN
LANGUAGE plperlu
AS $$
use DBI;
my $dbh;
my ($driver, $host, $port, $database, $db_user, $db_password,
    $schema_name, $catalog_name) = @_;
$schema_name =~ s/'/''/g;
check_connection(
  driver => $driver
, host => $host
, port => $port
, database => $database
, db_user => $db_user
, db_password => $db_password
);
create_schema(schema_name => $schema_name);
create_accessor_methods(schema_name => $schema_name);
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
      schema_name => undef
    , @_
    );
    my $sql_check_for_schema = <<SQL;
SELECT COUNT(*)
FROM pg_namespace
WHERE nspname = '$parms{"schema_name"}'
SQL
    my $schema_there = spi_exec_query($sql_check_for_schema);
    if ($schema_there->{rows}[0] != 0) {
        elog NOTICE, "Schema $parms{'schema_name'} already exists.";
    } else {
        my $sql_create_schema = "CREATE SCHEMA $schema";
        my $rv = spi_exec_query($sql_create_schema);
        if ($rv->{status} eq 'SPI_OK_UTILITY') {
            elog NOTICE, "Created schema $parms{'schema_name'}."
        } else {
            elog ERROR, "Could not create schema $parms{'schema_name'}.  $rv->{status}";
        }
    }
}

sub create_accessor_methods {
    my %parms = (
      schema_name => undef
    , @_
    );
    my $types = "'TABLE','VIEW'";
    my $sth = $dbh->table_info($catalog_name, $schema_name, '%', $types);
    while(my $row = $sth->fetchrow_hashref) {
        my $type_name = "$parms{'schema_name'}.$table->{TABLE_NAME}_type";
        my $sql = "CREATE TYPE $type_name(\n";
        my @cols;
        my $sth2 = $dbh->column_info(undef, $schema_name, $table->{TABLE_NAME}, '%');
        while(my $column = $sth2->fetchrow_hashref) {
            push @cols, $column->{COLUMN_NAME}
              .($column->{TYPE_NAME}=~/integer/i)?' INTEGER':' TEXT';
        }
        $sth2->finish;
        $sql .= '  '. join("\n, ", @cols)."\n)";
        elog NOTICE, "Trying to create type\n$sql\n";
        my $rv = spi_exec_query($sql);
        if ($rv->{status} eq 'SPI_OK_UTILITY') {
            elog NOTICE, "Created type $type_name."
        } else {
            elog ERROR, "Could not create type $type_name.  $rv->{status}";
        }
    }
    $sth->finish;
}

$$;
