CREATE OR REPLACE FUNCTION make_accessor_functions (
  data_source TEXT
, db_user TEXT
, db_password TEXT
, dbh_attributes TEXT
)
RETURNS BOOLEAN
LANGUAGE plperlu
AS $$
use DBI;
my $dbh;
my ($data_source, $db_user, $db_password, $dbh_attributes) = @_;
$schema_name =~ s/'/\\'/g;
$local_schema =~ s/'/\\'/g;
my $data_source_id = check_connection(
  data_source => $data_source 
, db_user => $db_user
, db_password => $db_password
, dbh_attributes => $dbh_attributes
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
      data_source => undef
    , db_user => undef
    , db_password => undef
    , dbh_attributes => undef
    , @_
    );
    my $driver = $parms{data_source};
    $driver =~ s/^dbi:([^:]+):.*/$1/;
    my $dtsql = <<SQL;
SELECT ad
FROM dbi_link.available_drivers() AS "ad"
WHERE ad = quote_literal($driver)
SQL
    elog NOTICE, $dtsql;
    my $driver_there = spi_exec_query($dtsql);
    if ($driver_there->{processed} == 0) {
        elog ERROR, "Driver $driver is not available.  Can't look at database."
    }
###################################################################
#                                                                 #
# $attr_ref is a hash reference plugged into the database handle. #
# a typical $attr_ref might be:                                   #
# {                                                               #
#     AutoCommit => 0,                                            #
#     RaiseError => 0,                                            #
#     PrintError => 0                                             #
# }                                                               #
#                                                                 #
###################################################################
    my $attr_href = eval{$parms{dbh_attributes}};
    my $dbh = DBI->connect(
      $parms{data_source}
    , $parms{db_user}
    , $parms{db_password}
    , $attr_href
    );
    if ($DBI::errstr) {
        elog ERROR, <<ERR;
Could not connect to database
data source: $parms{data_source}
user: $parms{db_user}
password: $parms{db_password}
dbh attributes:
$parms{dbh_attributes}
                                                                            
$DBI::errstr
ERR
    } else {
        my $sql = <<SQL;
INSERT INTO dbi_link.dbi_connection (data_source, user_name, auth, dbh_attr)
VALUES (
  quote_literal($parms{data_source})
, quote_literal($parms{db_user})
, quote_literal($parms{db_password})
, quote_literal($parms{dbh_attributes})
)
SQL
        my $result = spi_exec_query($sql);
        if ($result->{status} eq 'SPI_OK_INSERT') {
            elog NOTICE, "Stashed connection info.";
            $sql = <<SQL;
SELECT currval('dbi_link.dbi_connection_data_source_id_seq') AS "the_val"
SQL
            $result = spi_exec_query($sql);
            if ($result->{processed} == 0) {
                elog ERROR, "Couldn't retrieve the dbi connection id via currval()!";
            } elsif ($result->{processed} != 1) {
                elog ERROR, "Got >$result->{processed}< results, not 1.  This can't happen!";
            } else {
                return $result->{rows}[0]->{the_val};
            }
        } else {
            elog ERROR, "Could not do\n$sql\n$result->{status}";
        }
    }
}

sub create_schema {
    my %parms = (
      local_schema => undef
    , @_
    );
    elog ERROR, "Must have a local_schema!" unless $parms{'local_schema'} =~ /\S/;
    my $sql_check_for_schema = <<SQL;
SELECT COUNT(*) AS "the_count"
FROM pg_namespace
WHERE nspname = '$parms{local_schema}'
SQL
    elog NOTICE, "Attempting\n$sql_check_for_schema\n";
    my $schema_there = spi_exec_query($sql_check_for_schema);
    if ($schema_there->{rows}[0]->{'the_count'} != 0) {
        elog ERROR, "Schema $parms{'local_schema'} already exists.";
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
    my $DEBUG = 1; # Set this to 1 for more wordiness, 0 for less.
    my %parms = (
      schema_name => undef
    , local_schema  => undef
    , @_
    );
    my $types = "'TABLE','VIEW'";
    my $sth = $dbh->table_info(undef, $parms{'schema_name'}, '%', $types);
    my $quote = '$'x 2;
    my $set_search = <<SQL;
SELECT set_config(
  'search_path'
, '$parms{local_schema},' || current_setting('search_path')
, false
)
SQL
    my $rv = spi_exec_query($set_search);
    if ($rv->{status} eq 'SPI_OK_SELECT') {
        elog NOTICE, "Fixed search_path." if $DEBUG==1;
    } else {
        elog ERROR, "Could not fix search path.  $rv->{status}";
    }
    while(my $table = $sth->fetchrow_hashref) {
        my $base_name = $table->{TABLE_NAME};
        my $type_name = join('_',$base_name,'type');
        my @cols;
        my %comments = ();
        my $sth2 = $dbh->column_info(undef, $schema_name, $table->{TABLE_NAME}, '%');
######################################################################
#                                                                    #
# This part should probably refer to a whole mapping between foreign #
# database column types and PostgreSQL ones.  Meanwhile, it turns    #
# integer-looking things into INTEGERs, everything else into TEXT.   #
#                                                                    #
######################################################################
        while(my $column = $sth2->fetchrow_hashref) {
            my $line = $column->{COLUMN_NAME};
            $comments{ $column->{COLUMN_NAME} } =
                ($DEBUG==1)
              ? join("\n", map {"$_: $column->{$_}"} sort keys %$column)
              : $column->{TYPE_NAME}
              ;
            if ( $column->{TYPE_NAME} =~ /integer/i ) {
                $line .= ' INTEGER'
            } else {
                $line .= ' TEXT '
            }
            push @cols, $line;
        }
        $sth2->finish;
        my $sql = <<SQL;
CREATE TYPE $type_name AS (
  @{[join("\n, ", @cols)]}
)
SQL
        elog NOTICE, "Trying to create type\n$sql\n" if $DEBUG==1;
        $rv = spi_exec_query($sql);
        if ($rv->{status} eq 'SPI_OK_UTILITY') {
            elog NOTICE, "Created type $type_name." if $DEBUG==1;
        } else {
            elog ERROR, "Could not create type $type_name.  $rv->{status}";
        }
        foreach my $comment (keys %comments) {
            $sql = <<SQL;
COMMENT ON COLUMN $type_name.$comment IS $quote
$comments{$comment}
$quote
SQL
            elog NOTICE, $sql if $DEBUG==1;
            $rv = spi_exec_query($sql);
            if ($rv->{status} eq 'SPI_OK_UTILITY') {
                elog NOTICE, "Created comment on $table_name.$comment" if $DEBUG==1;
            } else {
                elog ERROR, "Could not create comment on $table_name.$comment  $rv->{status}";
            }
        }
        my $method_name = join('_', $base_name, 'sel');
        $sql = <<SQL;
CREATE OR REPLACE FUNCTION $method_name ()
RETURNS SETOF $type_name
LANGUAGE plperlu
AS $quote
use DBI;

my \$dbh = DBI->connect(
  $parms{data_source}
, $parms{db_user}
, $parms{db_password}
, {
    RaiseError => 0
  , PrintError => 0
  , AutoCommit => 0
  }
);

if (\$DBI::errstr) {
    elog ERROR, "
Could not connect to database
data source: \$data_source
user: \$db_user
password: \$db_password
error: \$DBI::errstr
";
}

# elog NOTICE, "Connected to database";

my \$sql = 'SELECT * FROM $base_name';

# elog NOTICE, "sql is\n\$sql";

my \$sth = \$dbh->prepare(\$sql);
if (\$DBI::errstr) {
    elog ERROR, "
Cannot prepare

\$sql

\$DBI::errstr
";
}

my \$rowset;
\@\$rowset = ();
\$sth->execute;

while(my \$row = \$sth->fetchrow_hashref) {
    push \@\$rowset, \$row;
}

\$sth->finish;
\$dbh->disconnect;
return \$rowset;
$quote;
SQL
        # elog NOTICE, "Trying to create method $method_name\n";
        my $rv = spi_exec_query($sql);
        if ($rv->{status} eq 'SPI_OK_UTILITY') {
            elog NOTICE, "Created method $method_name."
        } else {
            elog ERROR, "Could not create method $method_name.  $rv->{status}";
        }
        $sql = <<SQL;
CREATE VIEW $base_name AS
SELECT * FROM $method_name ()
SQL
        my $rv = spi_exec_query($sql);
        if ($rv->{status} eq 'SPI_OK_UTILITY') {
            elog NOTICE, "Created view $base_name."
        } else {
            elog ERROR, "Could not create view $base_name.  $rv->{status}";
        }
#########################################################################
#                                                                       #
# This section does INSERTs, UPDATEs and DELETEs by INSERTing into a    #
# shadow table with an action marker.  There is a TRIGGER on the shadow #
# table that Does The Right Thing(TM).                                  #
#                                                                       #
#########################################################################
        my $shadow_table = join('_', $base_name, 'shadow');
        $sql = <<SQL;
CREATE TABLE $shadow_table AS (
  iud_action CHAR(1) CHECK(iud_action IN ('I', 'U', 'D') )
, @{[ join("\n, ", map {"old_$_"} @cols) ]}
, @{[ join("\n, ", map {"new_$_"} @cols) ]}
)
SQL
        elog NOTICE, "Trying to create shadow table $shadow_table\n$sql\n" if $DEBUG==1;
        $rv = spi_exec_query($sql);
        if ($rv->{status} eq 'SPI_OK_UTILITY') {
            elog NOTICE, "Created shadow table $shadow_table." if $DEBUG==1;
        } else {
            elog ERROR, "Could not create shadow table $shadow_table.  $rv->{status}";
        }
        $sql = <<SQL;
CREATE TRIGGER ${shadow_table}_trg
    BEFORE INSERT ON test
    FOR EACH ROW EXECUTE PROCEDURE dbi_link.shadow_trigger_func($data_source_id)
SQL
        elog NOTICE, "Trying to create trigger on shadow table\n$sql\n" if $DEBUG==1;
        $rv = spi_exec_query($sql);
        if ($rv->{status} eq 'SPI_OK_UTILITY') {
            elog NOTICE, "Created trigger on shadow table $shadow_table." if $DEBUG==1;
        } else {
            elog ERROR, "Could not create trigger on shadow table $shadow_table.  $rv->{status}";
        }
        $sql = <<SQL;
CREATE RULE ${base_name}_insert AS
ON INSERT TO $base_name DO INSTEAD
INSERT INTO $shadow_table ('I', OLD.*, NEW.*)
SQL
        my $rv = spi_exec_query($sql);
        if ($rv->{status} eq 'SPI_OK_UTILITY') {
            elog NOTICE, "Created INSERT rule on VIEW $base_name."
        } else {
            elog ERROR, "Could not create INSERT rule on VIEW $base_name.  $rv->{status}";
        }
        $sql = <<SQL;
CREATE RULE ${base_name}_update AS
ON UPDATE TO $base_name DO INSTEAD
INSERT INTO ${base_name}_shadow ('U', OLD.*, NEW.*)
SQL
        my $rv = spi_exec_query($sql);
        if ($rv->{status} eq 'SPI_OK_UTILITY') {
            elog NOTICE, "Created UPDATE rule on VIEW $base_name."
        } else {
            elog ERROR, "Could not create UPDATE rule on VIEW $base_name.  $rv->{status}";
        }
        $sql = <<SQL;
CREATE RULE ${base_name}_delete AS
ON DELETE TO $base_name DO INSTEAD
INSERT INTO ${base_name}_shadow ('D', OLD.*, NEW.*)
SQL
        my $rv = spi_exec_query($sql);
        if ($rv->{status} eq 'SPI_OK_UTILITY') {
            elog NOTICE, "Created DELETE rule on VIEW $base_name."
        } else {
            elog ERROR, "Could not create DELETE rule on VIEW $base_name.  $rv->{status}";
        }
    }
    $sth->finish;
}

$$;
