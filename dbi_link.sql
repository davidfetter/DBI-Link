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

CREATE OR REPLACE FUNCTION dbi_link.version_integer()
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

COMMENT ON FUNCTION dbi_link.version_integer() IS $$
This gets the integer version number e.g. 80200.  It will be used for
turning on/off PostgreSQL version-specific goodies.  Thanks to Andrew
of Supernews for this.
$$;

CREATE TABLE dbi_link.min_pg_version (
    min_pg_version INTEGER NOT NULL
);

INSERT INTO dbi_link.min_pg_version (min_pg_version)
VALUES (80104);

CREATE RULE min_pg_version_no_insert AS
    ON INSERT TO dbi_link.min_pg_version
    DO INSTEAD NOTHING;

CREATE RULE min_pg_version_no_update AS
    ON UPDATE TO dbi_link.min_pg_version
    DO INSTEAD NOTHING;

CREATE RULE min_pg_version_no_delete AS
    ON DELETE TO dbi_link.min_pg_version
    DO INSTEAD NOTHING;

COMMENT ON TABLE dbi_link.min_pg_version IS
$$This table contains exactly one row: the minimum version of
PostgreSQL required to use this version of DBI-Link.$$;

CREATE OR REPLACE FUNCTION dbi_link.is_yaml(TEXT)
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

COMMENT ON FUNCTION dbi_link.is_yaml(TEXT) IS $$Pretty self-explanatory ;)$$;

CREATE DOMAIN yaml AS TEXT
    CHECK (
        dbi_link.is_yaml(VALUE)
    );

COMMENT ON DOMAIN dbi_link.yaml IS $$Pretty self-explanatory ;)$$;

CREATE OR REPLACE FUNCTION dbi_link.is_data_source(TEXT)
RETURNS boolean
STRICT
LANGUAGE plperlu
AS $$
use DBI 1.43;
my @args = DBI->parse_dsn($_[0]);
if (defined @args) {
    return 1;
}
return 0;
$$;

COMMENT ON FUNCTION dbi_link.is_data_source(TEXT) IS $$
Pretty self-explanatory ;)
$$;

CREATE DOMAIN dbi_link.data_source AS TEXT
    CHECK (
        dbi_link.is_data_source(VALUE)
    );

COMMENT ON DOMAIN dbi_link.data_source IS $$
Pretty self-explanatory ;)
$$;

CREATE TABLE dbi_link.dbi_connection (
    data_source_id SERIAL PRIMARY KEY,
    data_source DATA_SOURCE NOT NULL,
    user_name TEXT,
    auth TEXT,
    dbh_attributes YAML,
    remote_schema TEXT,
    remote_catalog TEXT,
    local_schema TEXT,
    UNIQUE(data_source, user_name)
);

COMMENT ON TABLE dbi_link.dbi_connection IS
$$This table contains the necessary connection information for a DBI
connection.  The dbh_attributes is a YAML <http://www.yaml.org>
representation of the DBI database handle attributes which allows
maximum flexibility while ensuring some modicum of safety.$$;

CREATE TABLE dbi_link.dbi_connection_environment (
    data_source_id INTEGER NOT NULL
                   REFERENCES dbi_link.dbi_connection(data_source_id)
                   ON DELETE CASCADE
                   ON UPDATE CASCADE,
    env_name   TEXT NOT NULL,
    env_value  TEXT NOT NULL,
    env_action TEXT NOT NULL CHECK(
        env_action IN (
            'overwrite',  -- Set the envonment variable to this.
            'prepend',    -- Prepend this to the environment variable with a ':' separator if it is not already there.
            'append'      -- Append this to the environment variable with a ':' separator if it is not already there.
        )
    )
);

COMMENT ON TABLE dbi_link.dbi_connection_environment
IS $$Environment settings for a $dbh$$;

CREATE OR REPLACE FUNCTION dbi_link.add_dbi_connection_environment(
    in_data_source_id BIGINT,
    in_settings YAML
)
RETURNS VOID
LANGUAGE plperlU
AS $$
my ($data_source_id, $settings_yaml) = @_;

return unless (defined  $settings_yaml);

my $settings = Load($settings_yaml);
warn Dump($settings) if $_SHARED{debug};
die "In dbi_link.add_dbi_connection_environment, settings is a >@{[
    ref($settings)
]}<, not an array reference"
    unless (ref($settings) eq 'ARRAY');
my $count = 0;
foreach my $setting (@$settings) {
    die "In dbi_link.add_dbi_connection_environment, setting $count is not even a hash reference"
        unless (ref($setting) eq 'HASH');
    die "In dbi_link.add_dbi_connection_environment, setting $count does have the proper components"
        unless (
            exists $setting->{env_name} &&
            exists $setting->{env_value} &&
            exists $setting->{env_action}
        );
    die "In dbi_link.add_dbi_connection_environment, setting $count does have the proper right-hand sides"
        if (
            ref($setting->{env_name}) ||
            ref($setting->{env_value}) ||
            ref($setting->{env_action})
        );
    foreach my $sub_setting (qw(env_name env_value env_action)) {
        if (defined $setting->{$sub_setting}) {
            $setting->{$sub_setting} = $_SHARED{quote_literal}->(
                $setting->{$sub_setting}
            );
        }
        else {
            $setting->{$sub_setting} = 'NULL';
        }
    }
    my $sql = <<SQL;
INSERT INTO dbi_link.dbi_connection_environment (
    data_source_id,
    env_name,
    env_value,
    env_action
)
VALUES (
    $data_source_id,
    $setting->{env_name},
    $setting->{env_value},
    $setting->{env_action}
)
SQL
    warn "In dbi_link.add_dbi_connection_environment, executing:\n$sql";
    my $rv = spi_exec_query($sql);
    if ($rv->{status} ne 'SPI_OK_INSERT') {
        die "In dbi_link.add_dbi_connection_environment, could not insert into dbi_link.dbi_connection_environment: $rv->{status}";
    }
}
return;
$$;

CREATE OR REPLACE FUNCTION dbi_link.yaml_result_set(in_query TEXT)
RETURNS YAML
LANGUAGE plperlU
AS $$
use YAML;
my $rv = spi_exec_query($_[0]);
if ($rv->{processed} > 0) {
    return Dump($rv->{rows});
}
else {
    return undef;
}
$$;

COMMENT ON FUNCTION dbi_link.yaml_result_set(in_query TEXT)
IS $$
This takes a query as input and returns yaml that rolls up all the
records.
$$;

CREATE VIEW dbi_link.dbi_all_connection_info AS
SELECT
    c.data_source_id,
    c.data_source,
    c.user_name,
    c.auth,
    c.dbh_attributes,
    c.remote_schema,
    c.remote_catalog,
    c.local_schema,
    dbi_link.yaml_result_set(
        'SELECT
            env_name, env_value, env_action
        FROM
            dbi_link.dbi_connection_environment
        WHERE
            data_source_id = ' || c.data_source_id
    ) AS dbi_connection_environment
FROM
    dbi_link.dbi_connection c
;

COMMENT ON VIEW dbi_link.dbi_all_connection_info IS
$$Rollup of the whole connection_info object.$$;

CREATE RULE dbi_all_connection_info_insert AS
    ON INSERT TO dbi_link.dbi_all_connection_info
    DO INSTEAD (
        INSERT INTO dbi_link.dbi_connection (
            data_source,
            user_name,
            auth,
            dbh_attributes,
            remote_schema,
            remote_catalog,
            local_schema
        )
        VALUES (
            NEW.data_source,
            NEW.user_name,
            NEW.auth,
            NEW.dbh_attributes,
            NEW.remote_schema,
            NEW.remote_catalog,
            NEW.local_schema
        );
        SELECT dbi_link.add_dbi_connection_environment(
            pg_catalog.currval(
                pg_catalog.pg_get_serial_sequence(
                    'dbi_link.dbi_connection',
                    'data_source_id'
                )
            ),
            NEW.dbi_connection_environment
        )
    );

--------------------------------------
--                                  --
--  PL/PerlU Interface to DBI. :)   --
--                                  --
--------------------------------------
CREATE OR REPLACE FUNCTION dbi_link.available_drivers()
RETURNS SETOF TEXT
LANGUAGE plperlu
AS $$
    require 5.8.3;
    use DBI;
    return \@{[ DBI->available_drivers ]};
$$;

COMMENT ON FUNCTION dbi_link.available_drivers() IS $$
This is a wrapper around the DBI function of the same name which
returns a list (SETOF TEXT) of DBD:: drivers available through DBI on
your machine.  This is used internally and is unlikely to be called
directly.
$$;

CREATE OR REPLACE FUNCTION dbi_link.data_sources(TEXT)
RETURNS SETOF TEXT
LANGUAGE plperlu
AS $$
    require 5.8.3;
    use DBI;
    return \@{[ DBI->data_sources($_[0]) ]};
$$;

COMMENT ON FUNCTION dbi_link.data_sources(TEXT) IS $$
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
$_SHARED{min_pg_version} = spi_exec_query('SELECT min_pg_version FROM dbi_link.min_pg_version')
    ->{rows}
    ->[0]
    ->{min_pg_version };
$_SHARED{server_version} = spi_exec_query('SELECT dbi_link.version_integer()')
    ->{rows}
    ->[0]
    ->{version_integer};

if ( $_SHARED{server_version} < $_SHARED{min_pg_version} ) {
    die "Server version is $_SHARED{server_version}.  You need at least $_SHARED{min_pg_version} to run DBI-Link.";
}
my $shared = populate_hashref();

foreach my $sub (keys %$shared) {
    my $ref = ref($_SHARED{$sub});
    # warn $ref if $_SHARED{debug};
    if ($ref eq 'CODE') {
        # Do nothing.
        # warn "$sub already set." if $_SHARED{debug};
    }
    else {
        warn "Setting $sub in \%_SHARED hash." if $_SHARED{debug};
        $_SHARED{$sub} = $shared->{$sub};
    }
}

undef $shared;

sub populate_hashref {
    return {
bail => sub {
my ($params) = @_;
die join("\n",
    map{$params->{$_}} grep {
        $params->{$_} =~ /\S/
    } qw(header message error));
},

get_connection_info => sub {
    my ($args) = @_;
    warn "Entering get_connection_info" if $_SHARED{debug};
    warn 'ref($args) is '.ref($args)."\n".Dump($args) if $_SHARED{debug};
    unless (defined $args->{data_source_id}) {
        die "In get_connection_info, must provide a data_source_id";
    }
    unless ($args->{data_source_id} =~ /^\d+$/) {
        die "In get_connection_info, must provide an integer data_source_id";
    }
    my $sql = <<SQL;
SELECT
    data_source,
    user_name,
    auth,
    dbh_attributes,
    remote_schema,
    remote_catalog,
    local_schema,
    dbi_connection_environment
FROM
    dbi_link.dbi_all_connection_info
WHERE
    data_source_id = $args->{data_source_id}
SQL
    my $rv = spi_exec_query($sql);
    if ($rv->{processed} != 1) {
        die "Should have gotten 1 row back.  Got $rv->{processed} instead.";
    }
    else {
        # Do nothing
        # warn "Got 1 row back" if $_SHARED{debug};
    }
    # warn Dump($rv->{rows}[0]) if $_SHARED{debug};
    warn "Leaving get_connection_info" if $_SHARED{debug};
    return $rv->{rows}[0];
},

get_dbh => sub {
    use YAML;
    use DBI;
    local %ENV;
    my ($connection_info) = @_;
    my $attribute_hashref;
    warn "In get_dbh, input connection info is\n".Dump($connection_info) if $_SHARED{debug};
    ##################################################
    #                                                #
    # Here, we get the raw connection info as input. #
    #                                                #
    ##################################################
    unless (
        defined $connection_info->{data_source} &&  # NOT NULL
        exists $connection_info->{user_name}    &&
        exists $connection_info->{auth}         &&
        exists $connection_info->{dbh_attributes}
    ) {
        die "You must provide all of data_source, user_name, auth and dbh_attributes to get a database handle.";
    }

    if (defined $connection_info->{dbi_connection_environment}) {
        my $parsed_env = Load($connection_info->{dbi_connection_environment});
        die "In get_dbh, dbi_connection_environment must be an array reference."
            unless (ref($parsed_env) eq 'ARRAY');
        foreach my $setting (@$parsed_env) {
            foreach my $key (qw(env_name env_value env_action)) {
                die "In get_dbh, missing key $key"
                    unless (defined $setting->{$key});
            }
            if ($setting->{env_action} eq 'overwrite') {
                $ENV{ $setting->{env_name} } = $setting->{env_value};
            }
            elsif (
                $setting->{env_action} eq 'prepend'
            ) {
                if ($ENV{ $setting->{env_name} } !~ /$setting->{env_value}/) {
                    $ENV{ $setting->{env_name} } =
                        $setting->{env_value} .
                        $ENV{ $setting->{env_name} }
                        ;
                }
            }
            elsif (
                $setting->{env_action} eq 'append'
            ) {
                if ($ENV{ $setting->{env_name} } !~ /$setting->{env_value}/) {
                    $ENV{ $setting->{env_name} } .= $setting->{env_value};
                }
            }
            else {
                die "In get_dbh, env_action may only be one of {overwrite, prepend, append}.";
            }
        }
    }

    $attribute_hashref = Load(
        $connection_info->{dbh_attributes}
    );
    my $dbh = DBI->connect(
        $connection_info->{data_source},
        $connection_info->{user_name},
        $connection_info->{auth},
        $attribute_hashref,
    );
    if ($DBI::errstr) {
        die <<ERROR;
$DBI::errstr
Could not connect with parameters
data_source: $connection_info->{data_source}
user_name: $connection_info->{user_name}
auth: $connection_info->{auth}
dbh_attributes: $connection_info->{dbh_attributes}
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

quote_ident => sub {
    $_[0] =~ s/'/''/g; # Just in case
    return spi_exec_query(
        "SELECT pg_catalog.quote_ident('$_[0]') AS foo",
        1
    )->{rows}[0]{foo};
},

quote_literal => sub {
    $_[0] =~ s/'/''/g; # Just in case
    return spi_exec_query(
        "SELECT pg_catalog.quote_literal('$_[0]') AS foo",
        1
    )->{rows}[0]{foo};
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
        dbh_attributes YAML
        remote_schema TEXT
        remote_catalog TEXT
        local_schema TEXT

get_dbh:
    This takes the output of get_connection_info or equivalent data
    structure, returns a database handle.  Used to populate
    $_SHARED->{dbh}.  Also used in remote_select in the case where you
    set the connection at run time.
    Input:
        data_source TEXT
        user_name TEXT
        auth TEXT
        dbh_attributes YAML
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
        undef

quote_ident:
    Perl function which wraps SQL function of the same name.
    Input:
        raw_identifier TEXT
    Output:
        quoted_identifier TEXT

quote_literal:
    Perl function which wraps SQL function of the same name.
    Input:
        raw_literal TEXT
    Output:
        quoted_literal TEXT

$$;

CREATE OR REPLACE FUNCTION dbi_link.cache_connection(
    in_data_source_id INTEGER
)
RETURNS VOID
LANGUAGE plperlU
AS $$
use YAML;
spi_exec_query('SELECT dbi_link.dbi_link_init()');

return if (defined $_SHARED{dbh}{ $_[0] } );

warn "In cache_connection, there's no shared dbh $_[0]";

my $info = $_SHARED{get_connection_info}->({
    data_source_id => $_[0]
});
warn Dump($info) if $_SHARED{debug};

$_SHARED{dbh}{ $_[0] } = $_SHARED{get_dbh}->(
    $info
);
return;
$$;

CREATE OR REPLACE FUNCTION dbi_link.remote_select (
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
my $query = "SELECT dbi_link.cache_connection( $_[0] )";
warn $query if $_SHARED{debug};
my $rv = spi_exec_query($query);

$_SHARED{remote_exec_dbh}->({
    dbh => $_SHARED{dbh}{ $_[0] },
    query => $_[1],
    returns_rows => 't',
});

return;

$$;

COMMENT ON FUNCTION dbi_link.remote_select (
    data_source_id INTEGER,
    query TEXT
) IS $$
This function does SELECTs on a remote data source stored in
dbi_link.data_sources.
$$;

CREATE OR REPLACE FUNCTION dbi_link.remote_select (
    data_source TEXT,
    user_name TEXT,
    auth TEXT,
    dbh_attributes YAML,
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
    die 'Must issue a query!';
}

my $dbh = $_SHARED{get_dbh}->({
    data_source => $_[0],
    user_name => $_[1],
    auth => $_[2],
    dbh_attributes => $_[3],
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

COMMENT ON FUNCTION dbi_link.remote_select (
    data_source TEXT,
    user_name TEXT,
    auth TEXT,
    dbh_attributes YAML,
    query TEXT
) IS $$
This function does SELECTs on a remote data source de novo.
$$;

CREATE OR REPLACE FUNCTION dbi_link.remote_execute (
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

spi_exec_query("SELECT dbi_link.cache_connection($_[0])");

$_SHARED{remote_exec_dbh}->({
    dbh => $_SHARED{dbh}{ $_[0] },
    query => $_[1],
    returns_rows => 'f',
});

return;
$$;

COMMENT ON FUNCTION dbi_link.remote_execute (
    data_source_id INTEGER,
    query TEXT
) IS $$
This function executes non-row-returning queries on a remote data
source stored in dbi_link.data_sources.
$$;

CREATE OR REPLACE FUNCTION dbi_link.shadow_trigger_func()
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

spi_exec_query('SELECT dbi_link.dbi_link_init()');
my $data_source_id = shift;
die "In shadow_trigger_function, data_source_id must be an integer"
    unless ($data_source_id =~ /^\d+$/);
my $query = "SELECT dbi_link.cache_connection( $data_source_id )";
warn "In shadow_trigger_function, calling\n    $query" if $_SHARED{debug};
warn "In shadow_trigger_function, the trigger payload is\n". Dump(\$_TD) if $_SHARED{debug};
my $rv = spi_exec_query($query);

my $table = $_TD->{relname};
warn "Raw table name is $table";
warn "In trigger on $table, action is $_TD->{new}{iud_action}" if $_SHARED{debug};
$table =~ s{
    \A                  # Beginning of string.
    (.*)                # Actual table name.
    _shadow             # Strip off shadow.
    \z                  # End of string.
}
{$1}sx;
warn "Cooked table name is $table";

my $iud = {
    I => \&do_insert,
    U => \&do_update,
    D => \&do_delete,
};

if ($iud->{ $_TD->{new}{iud_action} }) {
    $iud->{ $_TD->{new}{iud_action} }->({
        payload => $_TD->{new}
    });
}
else {
    die "Trigger event was $_TD->{new}{iud_action}<, but should have been one of I, U or D!"
}

return 'SKIP';

sub do_insert {
    my ($params) = @_;
    die "In do_insert, must pass a payload!"
        unless (defined $params->{payload});
    die "In do_insert, payload must be a hash reference!"
        unless (ref $params->{payload} eq 'HASH');
    my (@keys, @values);
    foreach my $key (sort keys %{ $params->{payload} } ) {
        next unless $key =~ /^.?new_(.*)/;
        my $real_key = $1;
        push @keys, $real_key;
        push @values, $_SHARED{dbh}{ $data_source_id }->quote(
            $params->{payload}{$key}
        );
    }
    my $sql = <<SQL;
INSERT INTO $table (
    @{[
        join(
            ",\n    ",
            @keys
        )
    ]}
)
VALUES (
    @{[
        join(
            ",\n    ",
            @values
        )
    ]}
)
SQL
    warn "SQL is\n$sql" if $_SHARED{debug};
    $_SHARED{dbh}{ $data_source_id }->do($sql);
}

sub do_update {
    my ($params) = @_;
    die "In do_update, must pass a payload!"
        unless (defined $params->{payload});
    die "In do_update, payload must be a hash reference!"
        unless (ref $params->{payload} eq 'HASH');
    my $sql = <<SQL;
UPDATE $table
SET
    @{[ make_pairs({
        payload => $params->{payload},
        which => 'new',
        joiner => ",\n    ",
    }) ]}
WHERE
    @{[ make_pairs({
        payload => $params->{payload},
        which => 'old',
        joiner => "\nAND ",
        transform_null => 'true'
    }) ]}
SQL
    warn "SQL is\n$sql" if $_SHARED{debug};
    $_SHARED{dbh}{ $data_source_id }->do($sql);
}

sub do_delete {
    my ($params) = @_;
    die "In do_delete, must pass a payload!"
        unless (defined $params->{payload});
    die "In do_delete, payload must be a hash reference!"
        unless (ref $params->{payload} eq 'HASH');
    my $sql = <<SQL;
DELETE FROM $table
WHERE
    @{[ make_pairs({
        payload => $params->{payload},
        which => 'old',
        joiner => "\nAND ",
        transform_null => 'true'
    }) ]}
SQL
    warn "SQL is\n$sql" if $_SHARED{debug};
    $_SHARED{dbh}{ $data_source_id }->do($sql);
}

sub make_pairs {
    my ($params) = @_;
    die "In make_pairs, must pass a payload!"
        unless (defined $params->{payload});
    die "In make_pairs, payload must be a hash reference!"
        unless (ref $params->{payload} eq 'HASH');
    warn "In make_pairs, parameters are:\n". Dump($params) if $_SHARED{debug};
    my @pairs;
    foreach my $key (
        keys %{ $params->{payload} }
    ) {
        next unless $key =~ m/^(.?)$params->{which}_(.*)/;
        my $left = "$1$2";
        warn "In make_pairs, raw key is $key, cooked key is $left" if $_SHARED{debug};
        if (
            defined $params->{transform_null} &&  # In a WHERE clause,
           !defined $params->{payload}{$key}      # turn undef into IS NULL
        ) {
            push @pairs, "$left IS NULL";
        }
        else {
            push @pairs, "$left = " . $_SHARED{dbh}{ $data_source_id }->quote(
                $params->{payload}{$key}
            );
        }
    }
    my $ret = 
        join (
            $params->{joiner},
            @pairs,
        );
    warn "In make_pairs, the pairs are:\n". Dump(\@pairs) if $_SHARED{debug};
    return $ret;
}

$$;

CREATE OR REPLACE FUNCTION set_up_connection (
    data_source DATA_SOURCE,
    user_name TEXT,
    auth TEXT,
    dbh_attributes YAML,
    dbi_connection_environment YAML,
    remote_schema TEXT,
    remote_catalog TEXT,
    local_schema TEXT
)
RETURNS INTEGER
LANGUAGE plperlu
AS $$

spi_exec_query('SELECT dbi_link.dbi_link_init()');

my ($params, $quoted);
foreach my $param (qw(data_source user_name auth dbh_attributes
    dbi_connection_environment remote_schema remote_catalog
    local_schema)) {
    $params->{ $param } = shift;
    if ( defined $params->{ $param } ) {
        $quoted->{ $param } = $_SHARED{quote_literal}->(
            $params->{ $param }
        );
    }
    else {
        $quoted->{ $param } = 'NULL';
    }
}

my $driver = $_SHARED{quote_literal}->(
    $params->{data_source}
);

my $sql = <<SQL;
SELECT count(*) AS "driver_there"
FROM dbi_link.available_drivers()
WHERE available_drivers = $driver
SQL

warn $sql if $_SHARED{debug};
my $driver_there = spi_exec_query($sql);
if ($driver_there->{processed} == 0) {
    die "Driver $driver is not available.  Can't look at database."
}

my $attr_href = Load($params->{dbh_attributes});
my $dbh = DBI->connect(
    $params->{data_source},
    $params->{user_name},
    $params->{auth},
    $attr_href,
);

if ($DBI::errstr) {
    die <<ERR;
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
    warn "Checking whether $driver has $method..." if $_SHARED{debug};
    if ($dbh->can($method)) {
        warn "$driver has $method :)" if $_SHARED{debug};
    }
    else {
        die (<<ERR);
DBD driver $driver does not have the $method method, which is required
for DBI-Link to work.  Exiting.
ERR
    }
}

my $sql = <<SQL;
INSERT INTO dbi_link.dbi_all_connection_info (
    data_source,
    user_name,
    auth,
    dbh_attributes,
    dbi_connection_environment,
    remote_schema,
    remote_catalog,
    local_schema
) VALUES (
    $quoted->{data_source},
    $quoted->{user_name},
    $quoted->{auth},
    $quoted->{dbh_attributes},
    $quoted->{dbi_connection_environment},
    $quoted->{remote_schema},
    $quoted->{remote_catalog},
    $quoted->{local_schema}
)
SQL

warn $sql if $_SHARED{debug};
my $rv = spi_exec_query(
    $sql
);

$sql = <<SQL;
SELECT
    pg_catalog.currval(
        pg_catalog.pg_get_serial_sequence(
            'dbi_link.dbi_connection',
            'data_source_id'
        )
    ) AS "the_val"
SQL
warn $sql if $_SHARED{debug};
my $result = spi_exec_query($sql);
if ($result->{processed} == 0) {
    die "Couldn't retrieve the dbi connection id via currval()!";
}
else {
    return $result->{rows}[0]{the_val};
}
$$;

CREATE OR REPLACE FUNCTION dbi_link.create_schema (local_schema TEXT)
RETURNS VOID
LANGUAGE plperlU
AS $$

my $local_schema = shift;

die "Must have a local_schema!" unless $local_schema =~ /\S/;
my $literal_local_schema = $_SHARED{quote_literal}->(
    $local_schema
);
my $identifier_local_schema = $_SHARED{quote_ident}->(
    $local_schema
);

my $sql_check_for_schema = <<SQL;
SELECT 1
FROM pg_namespace
WHERE nspname = $literal_local_schema
SQL

warn "In create_schema, attempting\n$sql_check_for_schema\n" if $_SHARED{debug};

my $result = spi_exec_query($sql_check_for_schema);

if ($result->{processed} != 0) {
    die "Schema $local_schema already exists.";
}
else {
    my $sql_create_schema = "CREATE SCHEMA $identifier_local_schema";
    my $rv = spi_exec_query($sql_create_schema);
    if ($rv->{status} eq 'SPI_OK_UTILITY') {
        warn "Created schema $local_schema." if $_SHARED{debug}
    }
    else {
        die "Could not create schema $local_schema.  Status was\n$rv->{status}";
    }
}
return;
$$;

CREATE OR REPLACE FUNCTION dbi_link.create_accessor_methods (
    local_schema TEXT,
    remote_schema TEXT,
    remote_catalog TEXT,
    data_source DATA_SOURCE,
    user_name TEXT,
    auth TEXT,
    data_source_id INTEGER
)
RETURNS VOID
LANGUAGE plperlU
AS $$

spi_exec_query('SELECT dbi_link.dbi_link_init()');

my $params;

foreach my $param (qw(local_schema remote_schema remote_catalog
        data_source user_name auth data_source_id)) {
    $params->{$param} = shift;
}

my $quote = '$'x 2;
my $identifier_local_schema = $_SHARED{quote_ident}->(
    $params->{local_schema}
);
my $set_search = <<SQL;
UPDATE
    pg_catalog.pg_settings
SET
    setting = CASE
        WHEN
            '$identifier_local_schema' = ANY(string_to_array(setting, ','))
        THEN
            setting
        ELSE
            '$identifier_local_schema,' || setting
        END
WHERE name = 'search_path'
SQL

warn $set_search if $_SHARED{debug};
my $rv = spi_exec_query($set_search);
my $types = "'TABLE','VIEW'";


spi_exec_query("SELECT dbi_link.cache_connection( $params->{data_source_id} )");

my $sth = $_SHARED{dbh}{ $params->{data_source_id} }->table_info(
    $params->{remote_catalog},
    $params->{remote_schema},
    '%',
    $types
);

my %dup_tabs = (); # Fix for Oracle/Linux/x86_64 issue.

while(my $table = $sth->fetchrow_hashref) {
    next if exists $dup_tabs{ $table->{TABLE_NAME} };
    ++$dup_tabs{ $table->{TABLE_NAME} };
    my $base_name = $_SHARED{quote_ident}->(
        $table->{TABLE_NAME}
    );
    my $type_name = join(
        '.',
        map { $_SHARED{quote_ident}->($_) }
        $identifier_local_schema,
        join(
            '_',
            $table->{TABLE_NAME},
            'rowtype'
        )
    );
    my (@raw_cols, @cols, @types);
    my %comments = ();
    warn "Getting column info for >$table->{TABLE_NAME}<" if $_SHARED{debug};
    my $sth2 = $_SHARED{dbh}{ $params->{data_source_id} }->column_info(
        undef,
        $params->{remote_schema},
        $table->{TABLE_NAME},
        '%'
    );
######################################################################
#                                                                    #
# This part should probably refer to a whole mapping between foreign #
# database column types and PostgreSQL ones.  Meanwhile, it turns    #
# integer-looking things into INTEGERs, everything else into TEXT.   #
#                                                                    #
######################################################################
    my %dup_cols = (); # Fix for Oracle/Linux/x86_64 issue.
    while(my $column = $sth2->fetchrow_hashref) {
        next if exists $dup_cols{ $column->{COLUMN_NAME} };
        ++$dup_cols{ $column->{COLUMN_NAME} };

        ###############################################################
        #                                                             #
        # The following uses ORDINAL_POSITION to order columns.  You  #
        # should not be depending on column order in the first place, #
        # but it is here as a courtesy ;)                             #
        #                                                             #
        ###############################################################
        $raw_cols[ $column->{ORDINAL_POSITION} - 1 ] = $column->{COLUMN_NAME};

        my $cn = $_SHARED{quote_ident}->(
            $column->{COLUMN_NAME}
        );
        $cols [ $column->{ORDINAL_POSITION} - 1 ] = $cn;
        $comments{ $cn } = $column->{TYPE_NAME};
        warn "Adding column $cn to table $table->{TABLE_NAME}"
            if $_SHARED{debug};
        if ( $column->{TYPE_NAME} =~ /integer/i ) {
            push $types[ $column->{ORDINAL_POSITION} - 1 ] = 'INTEGER';
        }
        else {
            push $types[ $column->{ORDINAL_POSITION} - 1 ] = 'TEXT';
        }
    }
    $sth2->finish;

    my $sql = <<SQL;
CREATE VIEW $identifier_local_schema.$base_name AS
SELECT * FROM dbi_link.remote_select(
    $params->{data_source_id},
    'SELECT * FROM $table->{TABLE_NAME}'
)
AS (
    @{[join(",\n    ", map {"$cols[$_] $types[$_]"} (0..$#cols)) ]}
)
SQL
    warn $sql;
    my $rv = spi_exec_query($sql);
    if ($rv->{status} eq 'SPI_OK_UTILITY') {
        warn "Created view $base_name."
    }
    else {
        die "Could not create view $base_name.  $rv->{status}";
    }

    foreach my $comment (keys %comments) {
        $sql = <<SQL;
COMMENT ON COLUMN $identifier_local_schema.$base_name.$comment IS $quote
$comments{$comment}
$quote
SQL
        warn $sql if $_SHARED{debug};
        $rv = spi_exec_query($sql);
        if ($rv->{status} eq 'SPI_OK_UTILITY') {
            warn "Created comment on $type_name.$comment" if $_SHARED{debug};
        }
        else {
            die "Could not create comment on $type_name.$comment  $rv->{status}";
        }
    }

#########################################################################
#                                                                       #
# This section does INSERTs, UPDATEs and DELETEs by INSERTing into a    #
# shadow table with an action marker.  There is a TRIGGER on the shadow #
# table that Does The Right Thing(TM).                                  #
#                                                                       #
#########################################################################
    my $shadow_table = $_SHARED{quote_ident}->(
        join('_', $table->{TABLE_NAME}, 'shadow')
    );
    my $shadow_columns;
    foreach my $age (qw(old new)) {
        my $at = join('_', $age, 'type');
        foreach my $i (0..$#raw_cols) {
            my $sc_name = $_SHARED{quote_ident}->(
                join('_', $age, $raw_cols[$i])
            );
            push @{ $shadow_columns->{ $age } },
                $sc_name;
            push @{ $shadow_columns->{ $at } },
                $sc_name . " $types[$i]";
        }
    }

    $sql = <<SQL;
CREATE TABLE $identifier_local_schema.$shadow_table (
    @{[
        join(
            ",\n    ",
            'iud_action CHAR(1)',
            @{ $shadow_columns->{old_type} },
            @{ $shadow_columns->{new_type} },
        )
    ]}
)
SQL
    warn "Trying to create shadow table $shadow_table\n$sql\n" if $_SHARED{debug};
    $rv = spi_exec_query($sql);
    if ($rv->{status} eq 'SPI_OK_UTILITY') {
        warn "Created shadow table $shadow_table." if $_SHARED{debug};
    }
    else {
        die "Could not create shadow table $shadow_table.  $rv->{status}";
    }
    my $shadow_trigger_name = $_SHARED{quote_ident}->(
        join('_', $table->{TABLE_NAME}, 'shadow','trg')
    );

        $sql = <<SQL;
CREATE TRIGGER $shadow_trigger_name
    BEFORE INSERT ON $shadow_table
    FOR EACH ROW
    EXECUTE PROCEDURE dbi_link.shadow_trigger_func($params->{data_source_id})
SQL
    warn "Trying to create trigger on shadow table\n$sql\n" if $_SHARED{debug};
    $rv = spi_exec_query($sql);
    if ($rv->{status} eq 'SPI_OK_UTILITY') {
        warn "Created trigger on shadow table $shadow_table." if $_SHARED{debug};
    }
    else {
        die "Could not create trigger on shadow table $shadow_table.  $rv->{status}";
    }
    my $insert_rule_name = $_SHARED{quote_ident}->(
        join('_', $table->{TABLE_NAME}, 'insert')
    );

    $sql = <<SQL;
CREATE RULE $insert_rule_name AS
ON INSERT TO $identifier_local_schema.$base_name DO INSTEAD
INSERT INTO $identifier_local_schema.$shadow_table (
    @{[
        join(
            "\n,    ",
            'iud_action',
            @{ $shadow_columns->{new} }
        )
    ]}
)
VALUES (
    'I',
    NEW.@{[join(",\n    NEW.", @cols)]}
)
SQL
    my $rv = spi_exec_query($sql);
    if ($rv->{status} eq 'SPI_OK_UTILITY') {
        warn "Created INSERT rule on VIEW $base_name."
    }
    else {
        die "Could not create INSERT rule on VIEW $base_name.  $rv->{status}";
    }
    my $update_rule_name = $_SHARED{quote_ident}->(
        join('_', $table->{TABLE_NAME}, 'update')
    );

    $sql = <<SQL;
CREATE RULE $update_rule_name AS
ON UPDATE TO $identifier_local_schema.$base_name DO INSTEAD
INSERT INTO $identifier_local_schema.$shadow_table (
    @{[
        join(
            "\n,    ",
            'iud_action',
            @{ $shadow_columns->{old} },
            @{ $shadow_columns->{new} },
        )
    ]}
)
VALUES (
    'U',
    OLD.@{[join(",\n    OLD.", @cols)]},
    NEW.@{[join(",\n    NEW.", @cols)]}
)
SQL
    my $rv = spi_exec_query($sql);
    if ($rv->{status} eq 'SPI_OK_UTILITY') {
        warn "Created UPDATE rule on VIEW $base_name."
    }
    else {
        die "Could not create UPDATE rule on VIEW $base_name.  $rv->{status}";
    }
    my $delete_rule_name = $_SHARED{quote_ident}->(
        join('_', $table->{TABLE_NAME}, 'delete')
    );

    $sql = <<SQL;
CREATE RULE $delete_rule_name AS
ON DELETE TO $identifier_local_schema.$base_name DO INSTEAD
INSERT INTO $identifier_local_schema.$shadow_table (
    @{[
        join(
            "\n,    ",
            'iud_action',
            @{ $shadow_columns->{old} },
        )
    ]}
)
VALUES (
    'D',
    OLD.@{[join(",\n    OLD.", @cols)]}
)
SQL
    my $rv = spi_exec_query($sql);
    if ($rv->{status} eq 'SPI_OK_UTILITY') {
        warn "Created DELETE rule on VIEW $base_name."
    }
    else {
        die "Could not create DELETE rule on VIEW $base_name.  $rv->{status}";
    }
}
$sth->finish;

return;
$$;

CREATE OR REPLACE FUNCTION dbi_link.make_accessor_functions (
    data_source DATA_SOURCE,
    user_name TEXT,
    auth TEXT,
    dbh_attributes YAML,
    dbi_connection_environment YAML,
    remote_schema TEXT,
    remote_catalog TEXT,
    local_schema TEXT
)
RETURNS VOID
LANGUAGE plperlu
AS $$

spi_exec_query('SELECT dbi_link.dbi_link_init()');

my (
    $data_source,
    $user_name,
    $auth,
    $dbh_attributes,
    $dbi_connection_environment,
    $remote_schema,
    $remote_catalog,
    $local_schema,
) = map {
    defined $_                    ?
    $_SHARED{quote_literal}->($_) :
    'NULL'
} @_;

my $sql = <<SQL;
SELECT dbi_link.set_up_connection(
    $data_source,
    $user_name,
    $auth,
    $dbh_attributes,
    $dbi_connection_environment,
    $remote_schema,
    $remote_catalog,
    $local_schema
)
SQL

warn $sql if $_SHARED{debug};

my $data_source_id = spi_exec_query($sql)->{rows}[0]{set_up_connection};

spi_exec_query("SELECT dbi_link.create_schema($local_schema)");

$sql = <<SQL;
SELECT dbi_link.create_accessor_methods(
    $local_schema,
    $remote_schema,
    $remote_catalog,
    $data_source,
    $user_name,
    $auth,
    $data_source_id
)
SQL

warn $sql if $_SHARED{debug};

spi_exec_query($sql);

return;
$$;

COMMENT ON FUNCTION dbi_link.make_accessor_functions (
    data_source DATA_SOURCE,
    user_name TEXT,
    auth TEXT,
    dbh_attributes YAML,
    dbi_connection_environment YAML,
    remote_schema TEXT,
    remote_catalog TEXT,
    local_schema TEXT
) IS $$
Called by end users.
This is where the automagic happens :)
$$;

CREATE OR REPLACE FUNCTION dbi_link.refresh_schema(
    data_source_id INTEGER
)
RETURNS VOID
STRICT
LANGUAGE plperlU
AS $$

spi_exec_query('SELECT dbi_link.dbi_link_init()');

my $data_source_id = shift;
my $connection_info = $_SHARED{get_connection_info}->({
    data_source_id => $data_source_id,
});

warn "connection info is\n".Dump($connection_info)
    if $_SHARED{debug};

my $literal =  $_SHARED{quote_literal}->(
    $connection_info->{local_schema}
);

my $sql_check_for_schema = <<SQL;
SELECT 1
FROM pg_namespace
WHERE nspname = $literal
SQL

warn "Attempting\n$sql_check_for_schema\n" if $_SHARED{debug};

my $result = spi_exec_query($sql_check_for_schema);

my $identifier = $_SHARED{quote_ident}->(
    $connection_info->{local_schema}
);

if ($result->{processed} == 1) {
    warn "Found schema $identifier"
        if $_SHARED{debug};
    my $search_path = join(
        ',',
        grep { !/$identifier/ }
            split (/,/, spi_exec_query(
                    'SHOW search_path'
                )->{rows}[0]{search_path}
            )
        );
    spi_exec_query(<<SQL);
UPDATE pg_catalog.pg_settings
SET setting = '$search_path'
WHERE name = 'search_path'
SQL

    spi_exec_query("DROP SCHEMA $identifier CASCADE");
}

spi_exec_query("SELECT dbi_link.create_schema($literal)");

my $quoted;
foreach my $key (keys %$connection_info) {
    if (defined $connection_info->{$key}) {
        $quoted->{$key} = $_SHARED{quote_literal}->(
            $connection_info->{$key}
        );
    }
    else {
        $quoted->{$key} = 'NULL';
    }
}

my $sql = <<SQL;
SELECT dbi_link.create_accessor_methods(
    $quoted->{local_schema},
    $quoted->{remote_schema},
    $quoted->{remote_catalog},
    $quoted->{data_source},
    $quoted->{user_name},
    $quoted->{auth},
    $data_source_id
)
SQL

warn $sql if $_SHARED{debug};

spi_exec_query($sql);

return;
$$;

COMMENT ON FUNCTION dbi_link.refresh_schema(
    data_source_id INTEGER
) IS $$
Drops the schema for the data source ID and re-creates it from scratch.
This can be handy when you have made schema changes on the remote side.
$$;

CREATE OR REPLACE FUNCTION dbi_link.reset_connection(
    data_source_id INTEGER
)
RETURNS VOID
STRICT
LANGUAGE plperlU
AS $$
spi_exec_query('SELECT dbi_link.dbi_link_init()');
eval {
    $_SHARED{dbh}{ $_[0] }->disconnect();
};
warn $@ if $@;
delete $_SHARED{dbh}{ $_[0] }; # Need to make it go away before it can revive.
spi_exec_query("SELECT dbi_link.cache_connection( $_[0] )");
return;
$$;

COMMENT ON FUNCTION dbi_link.reset_connection(
    data_source_id INTEGER
) IS $$
Resets a connection.  Think timed-out ones and other kinds of
connection death.
$$;

CREATE OR REPLACE FUNCTION dbi_link.begin_work(
    data_source_id INTEGER
)
RETURNS VOID
STRICT
LANGUAGE plperlU
AS $$
my $query = "SELECT dbi_link.cache_connection( $_[0] )";
warn $query if $_SHARED{debug};
my $rv = spi_exec_query($query);

spi_exec_query('SELECT dbi_link.dbi_link_init()');
my $rv = $_SHARED{dbh}{ $_[0] }->begin_work;
warn $rv if $_SHARED{debug};
return;
$$;

COMMENT ON FUNCTION dbi_link.begin_work(
    data_source_id INTEGER
) IS $$
Wraps the DBI function of the same name.
$$;

CREATE OR REPLACE FUNCTION dbi_link.commit(
    data_source_id INTEGER
)
RETURNS VOID
STRICT
LANGUAGE plperlU
AS $$
spi_exec_query('SELECT dbi_link.dbi_link_init()');
my $rv = $_SHARED{dbh}{ $_[0] }->commit;
warn $rv if $_SHARED{debug};
return;
$$;

COMMENT ON FUNCTION dbi_link.commit(
    data_source_id INTEGER
) IS $$
Wraps the DBI function of the same name.
$$;

CREATE OR REPLACE FUNCTION dbi_link.rollback(
    data_source_id INTEGER
)
RETURNS VOID
STRICT
LANGUAGE plperlU
AS $$
spi_exec_query('SELECT dbi_link.dbi_link_init()');
my $rv = $_SHARED{dbh}{ $_[0] }->rollback;
warn $rv if $_SHARED{debug};
return;
$$;

COMMENT ON FUNCTION dbi_link.rollback(
    data_source_id INTEGER
) IS $$
Wraps the DBI function of the same name.
$$;

CREATE OR REPLACE FUNCTION dbi_link.grant_admin_role(in_role_name TEXT)
RETURNS VOID
STRICT
LANGUAGE plperlU
AS $$
spi_exec_query('SELECT dbi_link.dbi_link_init()');

my $user = $_SHARED{quote_ident}->($_[0]);

my $do_once = {
    grant_db => "GRANT ALL ON DATABASE " .
                spi_exec_query(
                    'SELECT current_database()'
                )->{rows}[0]{current_database} .
                " TO $user",
    grant_usage => "GRANT USAGE ON SCHEMA dbi_link TO $user",
};

foreach my $once (sort keys %$do_once) {
    warn $do_once->{$once} if $_SHARED{debug};
    spi_exec_query($do_once->{$once});
}

my $sql;
$sql->{tables} = <<SQL;
SELECT
    CASE WHEN c.relkind = 'S' THEN
        'GRANT SELECT, UPDATE ON dbi_link.'
    ELSE
        'GRANT SELECT, INSERT, UPDATE, DELETE ON dbi_link.'
    END || 
    quote_ident(c.relname) ||
    ' TO $user' AS the_command
FROM
    pg_catalog.pg_class c
LEFT JOIN
    pg_catalog.pg_namespace n
    ON (n.oid = c.relnamespace)
WHERE
    c.relkind IN ('r','v','S')
AND
    n.nspname = 'dbi_link'
SQL

$sql->{functions} = <<SQL;
SELECT
    'GRANT EXECUTE ON FUNCTION dbi_link.' ||
    quote_ident(p.proname) ||
    '(' ||
    pg_catalog.oidvectortypes(p.proargtypes) ||
    ') TO $user' AS the_command
FROM
    pg_catalog.pg_proc p
JOIN
    pg_catalog.pg_namespace n
    ON (n.oid = p.pronamespace)
WHERE
    n.nspname = 'dbi_link'
SQL

foreach my $key (keys %$sql) {
    warn $sql->{$key} if $_SHARED{debug};
    my $result = spi_exec_query($sql->{$key});
    die $result->{status} unless $result->{status} eq 'SPI_OK_SELECT';
    foreach my $i (0 .. $result->{processed}-1) {
        my $the_command = $result->{rows}[$i]{the_command};
        warn $the_command if $_SHARED{debug};
        spi_exec_query($the_command);
    }
}
$$;

/*
    CREATE ROLE dbi_link_admin;
    SELECT dbi_link.grant_admin_role('dbi_link_admin');
*/

CREATE OR REPLACE FUNCTION dbi_link.grant_user_role(in_role_name TEXT)
RETURNS VOID
STRICT
LANGUAGE plperlU
AS $$
spi_exec_query('SELECT dbi_link.dbi_link_init()');

my $user = $_SHARED{quote_ident}->($_[0]);

my $do_once = {
    grant_usage => "GRANT USAGE ON SCHEMA dbi_link TO $user",
};

foreach my $once (sort keys %$do_once) {
    warn $do_once->{$once} if $_SHARED{debug};
    spi_exec_query($do_once->{$once});
}

my $sql;
$sql->{tables} = <<SQL;
SELECT
    'GRANT SELECT ON dbi_link.'
    quote_ident(c.relname) ||
    ' TO $user' AS the_command
FROM
    pg_catalog.pg_class c
JOIN
    pg_catalog.pg_namespace n
    ON (n.oid = c.relnamespace)
WHERE
    c.relkind IN ('r','v')
AND
    n.nspname = 'dbi_link'
SQL

$sql->{functions} = <<SQL;
SELECT
    'GRANT EXECUTE ON FUNCTION dbi_link.' ||
    quote_ident(p.proname) ||
    '(' ||
    pg_catalog.oidvectortypes(p.proargtypes) ||
    ') TO $user' AS the_command
FROM
    pg_catalog.pg_proc p
JOIN
    pg_catalog.pg_namespace n
    ON (n.oid = p.pronamespace)
WHERE
    n.nspname = 'dbi_link'
SQL

foreach my $key (keys %$sql) {
    warn $sql->{$key} if $_SHARED{debug};
    my $result = spi_exec_query($sql->{$key});
    die $result->{status} unless $result->{status} eq 'SPI_OK_SELECT';
    foreach my $i (0 .. $result->{processed}-1) {
        my $the_command = $result->{rows}[$i]{the_command};
        warn $the_command if $_SHARED{debug};
        spi_exec_query($the_command);
    }
}
$$;

/*
    CREATE ROLE dbi_link_user;
    SELECT dbi_link.grant_user_role('dbi_link_user');
*/

CREATE OR REPLACE FUNCTION dbi_link.grant_usage_on_schema_to_role(
    in_schema TEXT,
    in_role TEXT
)
RETURNS VOID
STRICT
LANGUAGE plperlU
AS $$

my ($schema, $role) = @_;
$schema = $_SHARED{quote_literal}->{$schema};
$role = $_SHARED{quote_literal}->{$role};

my $sql = "SELECT count(*) AS the_count FROM pg_catalog.pg_namespace WHERE npsname = $schema";
my $result = spi_exec_query($sql);
die "Error executing\n    $sql\nError: $result->{status}"
    unless $result->{status} eq 'SPI_OK_SELECT';
die "No such schema as $_[0]" unless $result->{rows}[0]{the_count};

$sql = "SELECT count(*) FROM pg_catalog.pg_roles WHERE rolname = $role";
$result = spi_exec_query($sql);
die "Error executing\n    $sql\nError: $result->{status}"
    unless $result->{status} eq 'SPI_OK_SELECT';
die "No such schema as $_[1]" unless $result->{rows}[0]{the_count};

$sql = <<SQL;
SELECT
    'GRANT ' ||
    CASE
        WHEN c.relkind = 'r' THEN 'INSERT'
        ELSE 'SELECT, INSERT, UPDATE, DELETE'
    END ||
    ' ON $schema.'
    quote_ident(c.relname) ||
    ' TO $role' AS the_command
FROM
    pg_catalog.pg_class c
JOIN
    pg_catalog.pg_namespace n
    ON (n.oid = c.relnamespace)
WHERE
    c.relkind IN ('r','v')
AND
    n.nspname = $schema
SQL

$result = spi_exec_query($sql);
die $result->{status} unless $result->{status} eq 'SPI_OK_SELECT';
foreach my $i (0 .. $result->{processed}-1) {
    my $the_command = $result->{rows}[$i]{the_command};
    warn $the_command if $_SHARED{debug};
    spi_exec_query($the_command);
}
$$;

CREATE OR REPLACE FUNCTION dbi_link.dbi_quote(
    in_data_source_id INTEGER,
    in_text TEXT
)
RETURNS TEXT
STRICT
LANGUAGE plperlU
AS $$
spi_exec_query("SELECT dbi_link.cache_connection( $_[0] )");

return $_SHARED{dbh}{ $_[0] }->quote(
    $_[1]
);
$$;

COMMENT ON FUNCTION dbi_link.dbi_quote(
    in_data_source_id INTEGER,
    in_text TEXT
)
IS $$
This uses the DBI quote mechanism for a given data_source_id to quote
a string.
$$;

CREATE OR REPLACE FUNCTION dbi_link.dbi_quote_identifier(
    in_data_source_id INTEGER,
    in_text TEXT
)
RETURNS TEXT
STRICT
LANGUAGE plperlU
AS $$
spi_exec_query("SELECT dbi_link.cache_connection( $_[0] )");

return $_SHARED{dbh}{ $_[0] }->quote_identifier(
    $_[1]
);
$$;

COMMENT ON FUNCTION dbi_link.dbi_quote_identifier(
    in_data_source_id INTEGER,
    in_text TEXT
)
IS $$
This uses the DBI quote_identifier mechanism for a given
data_source_id to quote a string.
$$;

