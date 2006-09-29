CREATE SCHEMA dbi_link;

COMMENT ON SCHEMA dbi_link IS $$
This schema holds all the functionality needed for using dbi-link.
$$;

UPDATE
    pg_catalog.pg_settings
SET
    setting = CASE
              WHEN
                  setting LIKE '%dbi_link%'
              THEN
                  setting
              ELSE 'dbi_link,' || setting
              END
WHERE
    name = 'search_path';

CREATE OR REPLACE FUNCTION version_integer()
RETURNS INTEGER
STRICT
LANGUAGE sql
AS $$
SELECT
    sum(
        pg_catalog.substring(
            pg_catalog.split_part(
                pg_catalog.current_setting(
                    'server_version'
                ),
                '.',
                i
            ),
            '^[[:digit:]]+'
        )::NUMERIC * 10^(6-i*2)
    )::INTEGER AS server_version_integer
FROM
    generate_series(1,3) AS s(i);
$$;

COMMENT ON FUNCTION version_integer() IS $$
This gets the integer version number e.g. 80200.  It will be used for
turning on/off PostgreSQL version-specific goodies.  Thanks to Andrew
of Supernews for this.
$$;

CREATE TABLE min_pg_version (
    min_pg_version INTEGER NOT NULL
);

INSERT INTO min_pg_version (min_pg_version)
VALUES (80200);

CREATE RULE min_pg_version_no_insert AS
    ON INSERT TO min_pg_version
    DO INSTEAD NOTHING;

CREATE RULE min_pg_version_no_update AS
    ON UPDATE TO min_pg_version
    DO INSTEAD NOTHING;

CREATE RULE min_pg_version_no_delete AS
    ON DELETE TO min_pg_version
    DO INSTEAD NOTHING;

COMMENT ON TABLE min_pg_version IS
$$This table contains exactly one row: the minimum version of
PostgreSQL required to use this version of DBI-Link.$$;

CREATE OR REPLACE FUNCTION is_yaml(TEXT)
RETURNS boolean
STRICT
LANGUAGE plperlu
AS $$
use YAML;

eval {
    my $hashref = Load($_[0]);
};

if ($@) {
    return 0;
}
return 1;
$$;

COMMENT ON FUNCTION is_yaml(TEXT) IS $$
Pretty self-explanatory ;)
$$;

CREATE DOMAIN yaml AS TEXT
    CHECK (
        dbi_link.is_yaml(VALUE)
    );

COMMENT ON DOMAIN yaml IS $$
Pretty self-explanatory ;)
$$;

CREATE OR REPLACE FUNCTION is_data_source(TEXT)
RETURNS boolean
STRICT
LANGUAGE plperlu
AS $$
use DBI;
my @args = DBI->parse_dsn($_[0]);
if (defined @args) {
    return 1;
}
return 0;
$$;

COMMENT ON FUNCTION is_data_source(TEXT) IS $$
Pretty self-explanatory ;)
$$;

CREATE DOMAIN data_source AS TEXT
    CHECK (
        dbi_link.is_data_source(VALUE)
    );

COMMENT ON DOMAIN data_source IS $$
Pretty self-explanatory ;)
$$;

CREATE TABLE dbi_connection (
    data_source_id SERIAL PRIMARY KEY,
    data_source DATA_SOURCE NOT NULL,
    user_name TEXT,
    auth TEXT,
    dbh_attr YAML,
    environment YAML,
    remote_schema TEXT,
    remote_catalog TEXT,
    local_schema TEXT,
    UNIQUE(data_source, user_name)
);

COMMENT ON TABLE dbi_connection IS
$$This table contains the necessary connection information for a DBI
connection.  For now, dbh_attr is a YAML <http://www.yaml.org>
representation of the DBI database handle attributes, as it allows
maximum flexibility while ensuring some modicum of safety.$$;

--------------------------------------
--                                  --
--  PL/PerlU Interface to DBI. :)   --
--                                  --
--------------------------------------
CREATE OR REPLACE FUNCTION available_drivers()
RETURNS SETOF TEXT
LANGUAGE plperlu
AS $$
    require 5.8.3;
    use DBI;
    return \@{[ DBI->available_drivers ]};
$$;

COMMENT ON FUNCTION available_drivers() IS $$
This is a wrapper around the DBI function of the same name which
returns a list (SETOF TEXT) of DBD:: drivers available through DBI on
your machine.  This is used internally and is unlikely to be called
directly.
$$;

CREATE OR REPLACE FUNCTION data_sources(TEXT)
RETURNS SETOF TEXT
LANGUAGE plperlu
AS $$
    require 5.8.3;
    use DBI;
    return \@{[ DBI->data_sources($_[0]) ]};
$$;

COMMENT ON FUNCTION data_sources(TEXT) IS $$
This is a wrapper around the DBI function of the same name.  It takes
as input one of the rows from available_drivers() and returns known
data sources for that driver.  You will probably not call this
function, but it is there just in case.
$$;

CREATE OR REPLACE FUNCTION dbi_link.dbi_link_init()
RETURNS VOID
LANGUAGE plperlu
AS $$
$_SHARED{debug} = 1;

my $shared = populate_hashref();

foreach my $sub (keys %$shared) {
    my $ref = ref($_SHARED{$sub});
    # elog NOTICE, $ref;
    if ($ref eq 'CODE') {
        # elog NOTICE, "$sub already set.";
    }
    else {
        # elog NOTICE, "Setting $sub in \%_SHARED hash.";
        $_SHARED{$sub} = $shared->{$sub};
    }
}

undef $shared;

sub populate_hashref {
    return {
bail => sub {
my ($params) = @_;
elog ERROR, join("\n",
    map{$params->{$_}} grep {
        $params->{$_} =~ /\S/
    } qw(header message error));
},

get_connection_info => sub {
    my ($args) = @_;
    elog NOTICE, "Entering get_connection_info" if $_SHARED{debug};
    elog NOTICE, 'ref($args) is '.ref($args)."\n".Dump($args) if $_SHARED{debug};
    unless (defined $args->{data_source_id}) {
        elog ERROR, "In get_connection_info, must provide a data_source_id"
    }
    unless ($args->{data_source_id} =~ /^\d+$/) {
        elog ERROR, "In get_connection_info, must provide an integer data_source_id";
    }
    my $sql = <<SQL;
SELECT
    data_source,
    user_name,
    auth,
    dbh_attr
FROM
    dbi_link.dbi_connection
WHERE
    data_source_id = $args->{data_source_id}
SQL
    my $rv = spi_exec_query($sql);
    if ($rv->{processed} != 1) {
        elog ERROR, "Should have gotten 1 row back.  Got $rv->{processed} instead.";
    }
    else {
        # Do nothing
        # elog NOTICE, "Got 1 row back" if $_SHARED{debug};
    }
    # elog NOTICE, Dump($rv->{rows}[0]) if $_SHARED{debug};
    elog NOTICE, "Leaving get_connection_info" if $_SHARED{debug};
    return $rv->{rows}[0];
},

get_dbh => sub {
    use YAML;
    use DBI;
    my ($connection_info) = @_;
    my $attribute_hashref;
    elog NOTICE, "In get_dbh, input connection info is\n".Dump($connection_info);
    ##################################################
    #                                                #
    # Here, we get the raw connection info as input. #
    #                                                #
    ##################################################
    unless (
        defined $connection_info->{data_source} &&  # NOT NULL
        exists $connection_info->{user_name}    &&
        exists $connection_info->{auth}         &&
        exists $connection_info->{dbh_attr}
    ) {
        elog ERROR, "You must provide all of data_source, user_name, auth and dbh_attr to get a database handle.";
    }

    $attribute_hashref = Load(
        $connection_info->{dbh_attr}
    );
    my $dbh = DBI->connect(
        $connection_info->{data_source},
        $connection_info->{user_name},
        $connection_info->{auth},
        $attribute_hashref,
    );
    if ($DBI::errstr) {
        elog ERROR, <<ERROR;
$DBI::errstr
Could not connect with parameters
data_source: $connection_info->{data_source}
user_name: $connection_info->{user_name}
auth: $connection_info->{auth}
dbh_attr: $connection_info->{dbh_attr}
ERROR
    }
    return $dbh;
},

remote_exec_dbh => sub {
    use DBI;
    my @errors;
    my ($params) = @_;
    push @errors, 'You must supply a database handle.'
        unless defined $params->{dbh};
    push @errors, 'You must supply a query.'
        unless defined $params->{query};
    push @errors, 'You must tell whether your query returns rows.'
        unless (
            lc($params->{returns_rows}) eq 't' ||
            lc($params->{returns_rows}) eq 'f'
        );
    if (scalar @errors > 0) {
        $_SHARED{bail}->({
            header => 'In $_SHARED{remote_exec_dbh}',
            error => join("\n", @errors),
        });
    }

    my $sth = $params->{dbh}->prepare(
        $params->{query}
    );
    if ($DBI::errstr) {
        $_SHARED{bail}->({
            header  => 'Cannot prepare',
            message => $params->{query},
            error   => $DBI::errstr,
        });
    }

    $sth->execute();
    if ($DBI::errstr) {
        $_SHARED{bail}-> ({
            header  => 'Cannot execute',
            message => $params->{query},
            error   => $DBI::errstr,
        });
    }
    
    if (lc($params->{returns_rows}) eq 't') {
        while(my $row = $sth->fetchrow_hashref) {
            return_next($row);
        }
        $sth->finish;
    }
    return;
},
};
}

$$;

COMMENT ON FUNCTION dbi_link.dbi_link_init() IS $$
This function sets up all the common perl-callable functions in
$_SHARED.

bail:
    This takes a hashref with (all optional) keys header, message and
    error, and raises an ERROR with an informative message of all
    that appear.

get_connection_info:
    Input:
        data_source_id INTEGER NOT NULL
    Output:
        data_source TEXT
        user_name TEXT
        auth TEXT
        dbh_attr YAML

get_dbh:
    This takes the output of get_connection_info or equivalent data
    structure, returns a database handle.  Used to populate
    $_SHARED->{dbh}.  Also used in remote_select in the case where you
    set the connection at run time.
    Input:
        data_source TEXT
        user_name TEXT
        auth TEXT
        dbh_attr YAML
    Output:
        a dbh (perl structure)

remote_exec_dbh:
    This takes a database handle, a query and a bool telling whether
    the query returns rows, then does as told.  Beware telling it
    something untrue.
    Input:
        dbh database handle NOT NULL
        query TEXT NOT NULL
        returns_rows BOOLEAN NOT NULL
    Output:
        VOID

$$;

CREATE OR REPLACE FUNCTION cache_connection(
    in_data_source_id INTEGER
)
RETURNS VOID
LANGUAGE plperlU
AS $$
use YAML;
spi_exec_query('SELECT dbi_link.dbi_link_init()');

return if (defined $_SHARED{dbh}{ $_[0] } );

elog NOTICE, "In cache_connection, there's no shared dbh $_[0]";

my $info = $_SHARED{get_connection_info}->({
    data_source_id => $_[0]
});
elog NOTICE, Dump($info);

$_SHARED{dbh}{ $_[0] } = $_SHARED{get_dbh}->(
    $info
);
return;
$$;

CREATE OR REPLACE FUNCTION remote_select (
  data_source_id INTEGER,
  query TEXT
)
RETURNS SETOF RECORD
STRICT
LANGUAGE plperlu AS $$

# use warnings;
spi_exec_query('SELECT dbi_link.dbi_link_init()');

##########################################################
#                                                        #
# This is safe because we already know it is an integer. #
#                                                        #
##########################################################
my $query = "SELECT cache_connection( $_[0] )";
elog NOTICE, $query;
my $rv = spi_exec_query($query);

$_SHARED{remote_exec_dbh}->({
    dbh => $_SHARED{dbh}{ $_[0] },
    query => $_[1],
    returns_rows => 't',
});

return;

$$;

COMMENT ON FUNCTION remote_select (
  data_source_id INTEGER,
  query TEXT
) IS $$
This function does SELECTs on a remote data source stored in
dbi_link.data_sources.
$$;

CREATE OR REPLACE FUNCTION remote_select (
    data_source TEXT,
    user_name TEXT,
    auth TEXT,
    dbh_attr YAML,
    query TEXT
)
RETURNS SETOF RECORD
LANGUAGE plperlu AS $$
#################################
#                               #
# Get common code into %_SHARED #
#                               #
#################################
spi_exec_query('SELECT dbi_link.dbi_link_init()');

my ($params) = @_;

#########################################################################
#                                                                       #
# Sanity checks: must have a query, and it must be row-returning query. #
# TODO: check for multiple queries.                                     #
#                                                                       #
#########################################################################
if (length($params->{query}) == 0) {
    elog ERROR, 'Must issue a query!';
}

my $dbh = $_SHARED{get_dbh}->({
    data_source => $_[0],
    user_name => $_[1],
    auth => $_[2],
    dbh_attr => $_[3],
});

my $sth = $dbh->prepare($params->{query});
$sth->execute();
while(my $row = $sth->fetchrow_hashref) {
    return_next($row);
}
$sth->finish;
$dbh->disconnect;
return;
$$;

COMMENT ON FUNCTION remote_select (
  data_source TEXT,
  user_name TEXT,
  auth TEXT,
  dbh_attr YAML,
  query TEXT
) IS $$
This function does SELECTs on a remote data source de novo.
$$;

CREATE OR REPLACE FUNCTION remote_execute (
  data_source_id INTEGER,
  query TEXT
)
RETURNS VOID
STRICT
LANGUAGE plperlu AS $$
#################################
#                               #
# Get common code into %_SHARED #
#                               #
#################################
spi_exec_query('SELECT dbi_link.dbi_link_init()');

spi_exec_query("SELECT cache_connection($_[0])");

$_SHARED{remote_exec_dbh}->({
    dbh => $_SHARED{dbh}{ $_[0] },
    query => $_[1],
    returns_rows => 'f',
});

return;
$$;

COMMENT ON FUNCTION remote_execute (
  data_source_id INTEGER,
  query TEXT
) IS $$
This function executes non-row-returning queries on a remote data
source stored in dbi_link.data_sources.
$$;

CREATE OR REPLACE FUNCTION shadow_trigger_func()
RETURNS TRIGGER
LANGUAGE plperlu
AS $$
require 5.8.3;
######################################################
#                                                    #
# Immediately reject anything that is not an INSERT. #
#                                                    #
######################################################
if ($_TD->{event} ne 'INSERT') {
    return "SKIP";
}

use DBI;

my $data_source_id = $_[0];
my $dbh;
if ( defined $_SHARED{dbh}{$data_source_id} ) {
    $dbh = $_SHARED{dbh}{$data_source_id}
}
else {
    ##################################################
    #                                                #
    # Is the named driver available on this machine? #
    #                                                #
    ##################################################
    my $sql = <<SQL;
    SELECT data_source, user_name, auth, dbh_attr
    FROM dbi_link.dbi_connection
    WHERE data_source_id = $data_source_id
SQL
    my ($data_source, $user_name, $auth, $dbh_attr);
    my $driver_there = spi_exec_query($sql);
    my $nrows = $driver_there->{processed};
    if ($nrows == 0) {
        $_SHARED{bail}->({
            message => "No such database connection as $data_source_id!",
        });
    }
    elsif ($nrows != 1) {
        $_SHARED{bail}->({
            message => "This can't happen!  data_source_id = $data_source_id is a primary key!",
        });
    }
    else {
        $data_source = $driver_there->{rows}[0]{data_source};
        $user_name = $driver_there->{rows}[0]{user_name};
        $auth = $driver_there->{rows}[0]{auth};
        $dbh_attr = $driver_there->{rows}[0]{dbh_attr};
    }

    my $attr = undef;
    if (length($dbh_attr) > 0 ) {
        $attr = Load($dbh_attr);
    }

    $dbh = DBI->connect(
      $data_source,
      $user_name,
      $auth,
      $attr,
    );

    if ($DBI::errstr) {
        $_SHARED{bail}->({
            header => "Could not connect to database",
            message => (<<MESSAGE),
    $data_source
    user: $user_name
    password: $auth
    attributes:
    $dbh_attr
MESSAGE
            error => $DBI::errstr,
        });
    }
}

#######################################################################
#                                                                     #
# We're only INSERTing into the shadow table, so to distinguish OLD.* #
# from NEW.*, we do the following based on the prefix of the column   #
# name.                                                               #
#                                                                     #
#######################################################################
my ($old, $new);
foreach my $key (keys %{ $_TD->{'new'} }) {
    next unless $key =~ /^(old|new)_(.*)/;
    if ($1 eq 'old') {
        if (defined $_TD->{'new'}->{$_}) {
            $old->{$2} = $dbh->quote($_TD->{'new'}->{$key});
        }
        else {
            $old->{$2} = 'NULL';
        }
    }
    else {
        if (defined $_TD->{'new'}->{$_}) {
            $new->{$2} = $dbh->quote($_TD->{'new'}->{$key});
        }
        else {
            $new->{$2} = 'NULL';
        }
    }
}

my $iud = (
  I => \&insert,
  U => \&update,
  D => \&delete,
);

my $table = $_TD->{relname};

if ($iud->{ $_TD->{'new'}{iud_action} }) {
    $iud->{ $_TD->{'new'}{iud_action} }->();
}
else {
    elog ERROR, "Trigger event was $_TD->{'new'}{iud_action}<, but should have been one of I, U or D!"
}

return 'SKIP';

sub insert {
    my $sql = <<SQL;
INSERT INTO $table (
  @{[join("\n, ", sort keys %$new) ]}
) VALUES (
  @{[join(", ", map { '?' } sort keys %$new) ]}
)
SQL
    my $sth = $dbh->prepare($sql);
    $sth->execute($sql, map {$new->{$_}} sort keys %$new);
}

sub update {
    my $sql = <<SQL;
UPDATE $table
SET
  @{[ join("\n, ", map { "$_ = $new->{$_}" } sort keys %$new) ]}
WHERE
  @{[
        join(
            "\nAND ",
            map {
                my $connector = ($old->{$_} ne 'NULL')?'=':'IS';
                "$_ $connector $old->{$_}"
            } sort keys %$old
        )
    ]}
SQL
    my $sth = $dbh->prepare($sql);
    $sth->execute($sql);
}

sub delete {
    my $sql = <<SQL;
DELETE FROM $table
WHERE
  @{[
        join(
            "\nAND ",
            map {
                my $connector = ($old->{$_} ne 'NULL')?'=':'IS';
                "$_ $connector $old->{$_}"
            } sort keys %$old
        )
    ]}
SQL
    my $sth = $dbh->prepare($sql);
    $sth->execute($sql);
}

$$;

CREATE OR REPLACE FUNCTION make_accessor_functions (
  data_source TEXT,
  user_name TEXT,
  auth TEXT,
  dbh_attributes TEXT,
  remote_schema TEXT,
  remote_catalog TEXT,
  local_schema TEXT
)
RETURNS BOOLEAN
LANGUAGE plperlu
AS $$
use strict;
use DBI;
use YAML;

my $dbh;
my ($data_source, $user_name, $auth, $dbh_attributes, $remote_schema, $remote_catalog, $local_schema) = @_;
my $data_source_id = check_connection({
  data_source => $data_source,
  user_name => $user_name,
  auth => $auth,
  dbh_attributes => $dbh_attributes,
  remote_schema => $remote_schema,
  remote_catalog => $remote_catalog,
  local_schema => $local_schema
});

create_schema({
  local_schema => $local_schema
});

create_accessor_methods({
  local_schema => $local_schema,
  remote_schema => $remote_schema,
  remote_catalog => $remote_catalog,
  data_source => $data_source,
  user_name => $user_name,
  auth => $auth,
  data_source_id => $data_source_id,
});

return 'TRUE';

sub check_connection {
    my ($params) = @_;
    my $driver = $params->{data_source};
    my $sql = <<'SQL';
SELECT count(*) AS "driver_there"
FROM dbi_link.available_drivers()
WHERE available_drivers = $1
SQL
    elog NOTICE, $sql;
    my $sth = spi_prepare($sql, 'TEXT');
    my $driver_there = spi_exec_prepared($sth, $driver);
    if ($driver_there->{processed} == 0) {
        elog ERROR, "Driver $driver is not available.  Can't look at database."
    }
    my $attr_href = Load($params->{dbh_attributes});
    $dbh = DBI->connect(
      $params->{data_source},
      $params->{user_name},
      $params->{auth},
      $attr_href
    );
    if ($DBI::errstr) {
        elog ERROR, <<ERR;
Could not connect to database
data source: $params->{data_source}
user: $params->{user_name}
password: $params->{auth}
dbh attributes:
$params->{dbh_attributes}

$DBI::errstr
ERR
    }
    my @methods = qw(table_info column_info quote);
    foreach my $method (@methods) {
        elog NOTICE, "Checking whether $driver has $method...";
        if ($dbh->can($method)) {
            elog NOTICE, "$driver has $method :)";
        }
        else {
            elog ERROR, (<<ERR);
DBD driver $driver does not have the $method method, which is required
for DBI-Link to work.  Exiting.
ERR
        }
    }

    my $sql = <<'SQL';
INSERT INTO dbi_link.dbi_connection (
    data_source,
    user_name,
    auth,
    dbh_attr,
    remote_schema,
    remote_catalog,
    local_schema
) VALUES (
    $1,
    $2,
    $3,
    $4,
    $5,
    $6,
    $7
)
SQL
    elog NOTICE, $sql;
    my $sth = spi_prepare(
        $sql,
        'TEXT',
        'TEXT',
        'TEXT',
        'TEXT',
        'TEXT',
        'TEXT',
        'TEXT',
    );
    spi_exec_prepared(
        $sth,
        $params->{data_source},
        $params->{user_name},
        $params->{auth},
        $params->{dbh_attributes},
        $params->{remote_schema},
        $params->{remote_catalog},
        $params->{local_schema}
    );
            $sql = <<SQL;
SELECT
    currval(pg_get_serial_sequence(
        'dbi_link.dbi_connection',
        'data_source_id'
    )) AS "the_val"
SQL
    my $result = spi_exec_query($sql);
    if ($result->{processed} == 0) {
        elog ERROR, "Couldn't retrieve the dbi connection id via currval()!";
    }
    elsif ($result->{processed} != 1) {
        elog ERROR, "Got >$result->{processed}< results, not 1.  This can't happen!";
    }
    else {
        return $result->{rows}[0]->{the_val};
    }
}

sub create_schema {
    my ($params) = @_;
    elog ERROR, "Must have a local_schema!" unless $params->{'local_schema'} =~ /\S/;
    my $sql_check_for_schema = <<SQL;
SELECT COUNT(*) AS "the_count"
FROM pg_namespace
WHERE nspname = '$params->{local_schema}'
SQL
    elog NOTICE, "Attempting\n$sql_check_for_schema\n";
    my $schema_there = spi_exec_query($sql_check_for_schema);
    if ($schema_there->{rows}[0]->{'the_count'} != 0) {
        elog ERROR, "Schema $params->{'local_schema'} already exists.";
    }
    else {
        my $sql_create_schema = "CREATE SCHEMA $params->{'local_schema'}";
        my $rv = spi_exec_query($sql_create_schema);
        if ($rv->{status} eq 'SPI_OK_UTILITY') {
            elog NOTICE, "Created schema $params->{'local_schema'}."
        }
        else {
            elog ERROR, "Could not create schema $params->{'local_schema'}.  Status was\n$rv->{status}";
        }
    }
}

sub create_accessor_methods {
    my ($params) = @_;
    my $types = "'TABLE','VIEW'";
    my $sth = $dbh->table_info($params->{remote_catalog}, $params->{remote_schema}, '%', $types);
    my $quote = '$'x 2;
    my $set_search = <<SQL;
UPDATE
    pg_catalog.pg_settings
SET
    setting = '$local_schema,' || setting
WHERE name = 'search_path'
SQL
    elog NOTICE, $set_search if $_SHARED{debug};
    my $rv = spi_exec_query($set_search);
    while(my $table = $sth->fetchrow_hashref('NAME_lc')) {
        my $base_name = $table->{table_name};
        my $type_name = join('_',$base_name,'rowtype');
        my @cols;
        my @types;
        my %comments = ();
        my $sth2 = $dbh->column_info(undef, $params->{remote_schema}, $table->{table_name}, '%');
######################################################################
#                                                                    #
# This part should probably refer to a whole mapping between foreign #
# database column types and PostgreSQL ones.  Meanwhile, it turns    #
# integer-looking things into INTEGERs, everything else into TEXT.   #
#                                                                    #
######################################################################
        while(my $column = $sth2->fetchrow_hashref('NAME_lc')) {
            push @cols, $column->{column_name};
            $comments{ $column->{column_name} } =
                $column->{type_name}
            ;
            if ( $column->{type_name} =~ /integer/i ) {
                push @types, 'INTEGER';
            }
            else {
                push @types, 'TEXT';
            }
        }
        $sth2->finish;
        my $sql = <<SQL;
CREATE TYPE $type_name AS (
    @{[
        join(
            ",\n    ",
            map {
                "$cols[$_] $types[$_]"
            } (0..$#cols)
        )
    ]}
)
SQL
        elog NOTICE, "Trying to create type\n$sql\n" if $_SHARED{debug};
        $rv = spi_exec_query($sql);
        if ($rv->{status} eq 'SPI_OK_UTILITY') {
            elog NOTICE, "Created type $type_name." if $_SHARED{debug};
        }
        else {
            elog ERROR, "Could not create type $type_name.  $rv->{status}";
        }
        foreach my $comment (keys %comments) {
            $sql = <<SQL;
COMMENT ON COLUMN $type_name.$comment IS $quote
$comments{$comment}
$quote
SQL
            elog NOTICE, $sql if $_SHARED{debug};
            $rv = spi_exec_query($sql);
            if ($rv->{status} eq 'SPI_OK_UTILITY') {
                elog NOTICE, "Created comment on $type_name.$comment" if $_SHARED{debug};
            }
            else {
                elog ERROR, "Could not create comment on $type_name.$comment  $rv->{status}";
            }
        }
        $sql = <<SQL;
CREATE VIEW $base_name AS
SELECT * FROM dbi_link.remote_select(
    $params->{data_source_id},
    'SELECT * FROM $base_name'
)
AS (
    @{[join(",\n    ", map {"$cols[$_] $types[$_]"} (0..$#cols)) ]}
)
SQL
        elog NOTICE, $sql;
        my $rv = spi_exec_query($sql);
        if ($rv->{status} eq 'SPI_OK_UTILITY') {
            elog NOTICE, "Created view $base_name."
        }
        else {
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
CREATE TABLE $shadow_table (
    iud_action CHAR(1),
    @{[ join(",\n    ", map {"old_$cols[$_] $types[$_]"} (0..$#cols) ) ]},
    @{[ join(",\n    ", map {"new_$cols[$_] $types[$_]"} (0..$#cols) ) ]}
)
SQL
        elog NOTICE, "Trying to create shadow table $shadow_table\n$sql\n" if $_SHARED{debug};
        $rv = spi_exec_query($sql);
        if ($rv->{status} eq 'SPI_OK_UTILITY') {
            elog NOTICE, "Created shadow table $shadow_table." if $_SHARED{debug};
        }
        else {
            elog ERROR, "Could not create shadow table $shadow_table.  $rv->{status}";
        }
        $sql = <<SQL;
CREATE TRIGGER ${shadow_table}_trg
    BEFORE INSERT ON $shadow_table
    FOR EACH ROW
    EXECUTE PROCEDURE dbi_link.shadow_trigger_func($data_source_id)
SQL
        elog NOTICE, "Trying to create trigger on shadow table\n$sql\n" if $_SHARED{debug};
        $rv = spi_exec_query($sql);
        if ($rv->{status} eq 'SPI_OK_UTILITY') {
            elog NOTICE, "Created trigger on shadow table $shadow_table." if $_SHARED{debug};
        }
        else {
            elog ERROR, "Could not create trigger on shadow table $shadow_table.  $rv->{status}";
        }
        my $nulls = join(", ", map {'NULL'} 1..scalar(@cols));
        $sql = <<SQL;
CREATE RULE ${base_name}_insert AS
ON INSERT TO $base_name DO INSTEAD
INSERT INTO $shadow_table VALUES ('I', $nulls, NEW.*)
SQL
        my $rv = spi_exec_query($sql);
        if ($rv->{status} eq 'SPI_OK_UTILITY') {
            elog NOTICE, "Created INSERT rule on VIEW $base_name."
        }
        else {
            elog ERROR, "Could not create INSERT rule on VIEW $base_name.  $rv->{status}";
        }
        $sql = <<SQL;
CREATE RULE ${base_name}_update AS
ON UPDATE TO $base_name DO INSTEAD
INSERT INTO ${base_name}_shadow VALUES ('U', OLD.*, NEW.*)
SQL
        my $rv = spi_exec_query($sql);
        if ($rv->{status} eq 'SPI_OK_UTILITY') {
            elog NOTICE, "Created UPDATE rule on VIEW $base_name."
        }
        else {
            elog ERROR, "Could not create UPDATE rule on VIEW $base_name.  $rv->{status}";
        }
        $sql = <<SQL;
CREATE RULE ${base_name}_delete AS
ON DELETE TO $base_name DO INSTEAD
INSERT INTO ${base_name}_shadow VALUES ('D', OLD.*, $nulls)
SQL
        my $rv = spi_exec_query($sql);
        if ($rv->{status} eq 'SPI_OK_UTILITY') {
            elog NOTICE, "Created DELETE rule on VIEW $base_name."
        }
        else {
            elog ERROR, "Could not create DELETE rule on VIEW $base_name.  $rv->{status}";
        }
    }
    $sth->finish;
}

$$;

SET search_path TO DEFAULT;
