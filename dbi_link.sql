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

COMMENT ON FUNCTION dbi_link.is_yaml(TEXT) IS $$
Pretty self-explanatory ;)
$$;

CREATE DOMAIN yaml AS TEXT
    CHECK (
        dbi_link.is_yaml(VALUE)
    );

COMMENT ON DOMAIN dbi_link.yaml IS $$
Pretty self-explanatory ;)
$$;

CREATE OR REPLACE FUNCTION dbi_link.is_data_source(TEXT)
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
elog NOTICE, Dump($settings) if $_SHARED{debug};
elog ERROR, "In dbi_link.add_dbi_connection_environment, settings is a >@{[ref($settings)]}<, not an array reference"
    unless (ref($settings) eq 'ARRAY');
my $count = 0;
foreach my $setting (@$settings) {
    elog ERROR, "In dbi_link.add_dbi_connection_environment, setting $count is not even a hash reference"
        unless (ref($settings) eq 'HASH');
    elog ERROR, "In dbi_link.add_dbi_connection_environment, setting $count does have the proper components"
        unless (
            exists $settings->{env_name} &&
            exists $settings->{env_value} &&
            exists $settings->{env_action}
        );
    elog ERROR, "In dbi_link.add_dbi_connection_environment, setting $count does have the proper right-hand sides"
        if (
            ref($settings->{env_name}) ||
            ref($settings->{env_value}) ||
            ref($settings->{env_action})
        );
    foreach my $setting (qw(env_name env_value env_action)) {
        if (defined $settings->{$setting}) {
            $settings->{$setting} = $_SHARED{quote_literal}->(
                $settings->{$setting}
            );
        }
        else {
            $settings->{$setting} = 'NULL';
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
    $settings->{env_name},
    $settings->{env_value},
    $settings->{env_action}
)
SQL
    elog NOTICE, "In dbi_link.add_dbi_connection_environment, executing:\n$sql";
    my $rv = spi_exec_query($sql);
    if ($rv->{status} ne 'SPI_OK_INSERT') {
        elog ERROR, "In dbi_link.add_dbi_connection_environment, could not insert into dbi_link.dbi_connection_environment: $rv->{status}";
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
    elog ERROR, "Server version is $_SHARED{server_version}.  You need at least $_SHARED{min_pg_version} to run DBI-Link.";
}
my $shared = populate_hashref();

foreach my $sub (keys %$shared) {
    my $ref = ref($_SHARED{$sub});
    elog NOTICE, $ref if $_SHARED{debug};
    if ($ref eq 'CODE') {
        elog NOTICE, "$sub already set." if $_SHARED{debug};
    }
    else {
        elog NOTICE, "Setting $sub in \%_SHARED hash." if $_SHARED{debug};
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
    dbh_attributes,
    remote_schema,
    remote_catalog,
    local_schema
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
    elog NOTICE, "In get_dbh, input connection info is\n".Dump($connection_info) if $_SHARED{debug};
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
        elog ERROR, "You must provide all of data_source, user_name, auth and dbh_attributes to get a database handle.";
    }

    if (defined $connection_info->{dbi_connection_environment}) {
        elog ERROR, "In get_dbh, dbi_connection_environment must be an array reference."
            unless (ref($connection_info->{dbi_connection_environment}) eq 'ARRAY');
        foreach my $setting (@$connection_info->{dbi_connection_environment}) {
            foreach my $key (qw(env_name env_value env_action)) {
                elog ERROR, "In get_dbh, missing key $key"
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
                elog ERROR, "In get_dbh, env_action may only be one of {overwrite, prepend, append}.";
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
        elog ERROR, <<ERROR;
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

elog NOTICE, "In cache_connection, there's no shared dbh $_[0]";

my $info = $_SHARED{get_connection_info}->({
    data_source_id => $_[0]
});
elog NOTICE, Dump($info) if $_SHARED{debug};

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
elog NOTICE, $query if $_SHARED{debug};
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
    elog ERROR, 'Must issue a query!';
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

spi_exec_query("SELECT cache_connection($_[0])");

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
elog ERROR, "In shadow_trigger_function, data_source_id must be an integer"
    unless ($data_source_id =~ /^\d+$/);
my $query = "SELECT cache_connection( $data_source_id )";
elog NOTICE, "In shadow_trigger_function, calling\n    $query" if $_SHARED{debug};
elog NOTICE, "In shadow_trigger_function, the trigger payload is\n". Dump(\$_TD) if $_SHARED{debug};
my $rv = spi_exec_query($query);

my $table = $_TD->{relname};

########################################################################
#                                                                      #
# We are only INSERTing into the shadow table, so to distinguish OLD.* #
# from NEW.*, we do the following based on the prefix of the column    #
# name.                                                                #
#                                                                      #
########################################################################
my ($old, $new, $key, $col);
foreach $key (grep {/^.?old_/} keys %{ $_TD->{'new'} }) {
    ($col = $key) =~ s/old_//;
    if (defined $_TD->{new}{$key}) {
        $old->{$col} = $_SHARED{dbh}{ $data_source_id }->quote($_TD->{'new'}{$key});
    }
    else {
        $old->{$col} = 'NULL';
    }
}

foreach my $key (grep {/^.?new_/} keys %{ $_TD->{'new'} }) {
    ($col = $key) =~ s/new_//;
    if (defined $_TD->{new}{$key}) {
        $new->{$col} = $_SHARED{dbh}{ $data_source_id }->quote($_TD->{'new'}{$key});
    }
    else {
        $new->{$col} = 'NULL';
    }
}

elog NOTICE, "old is:\n". Dump(\$old)."\n\nnew is:\n". Dump(\$new);

my $iud = {
    I => \&insert,
    U => \&update,
    D => \&delete,
};

elog NOTICE, "In trigger on $table, action is $_TD->{'new'}{iud_action}" if $_SHARED{debug};
if ($iud->{ $_TD->{'new'}{iud_action} }) {
    $table =~ s/.*\.//;
    $table =~ s/_shadow$//;
    $iud->{ $_TD->{'new'}{iud_action} }->();
}
else {
    elog ERROR, "Trigger event was $_TD->{'new'}{iud_action}<, but should have been one of I, U or D!"
}

return 'SKIP';

sub insert {
    my $sql = <<SQL;
INSERT INTO $table (
    @{[join(",\n    ", sort keys %$new) ]}
)
VALUES (
    @{[join(
        ",\n    ",
        map {
            $new->{$_}
        }
        sort keys %$new
    ) ]}
)
SQL
    elog NOTICE, "SQL is\n$sql" if $_SHARED{debug};
    my $sth = $_SHARED{dbh}{ $data_source_id }->prepare($sql);
    $sth->execute();
    $sth->finish();
}

sub update {
    my $sql = <<SQL;
UPDATE $table
SET
    @{[ join(",\n    ", map { "$_ = $new->{$_}" } sort keys %$new) ]}
WHERE
    @{[
        join(
            "\nAND ",
            map {
                ($old->{$_} eq 'NULL') ?
                "$_ IS NULL"           :
                "$_ = " . $old->{$_}
            } sort keys %$old
        )
    ]}
SQL
    elog NOTICE, "SQL is\n$sql" if $_SHARED{debug};
    my $sth = $_SHARED{dbh}{ $data_source_id }->prepare($sql);
    $sth->execute();
    $sth->finish();
}

sub delete {
    my $sql = <<SQL;
DELETE FROM $table
WHERE
    @{[
        join(
            "\nAND ",
            map {
                ($old->{$_} eq 'NULL') ?
                "$_ IS NULL"           :
                "$_ = $old->{$_}"
            } sort keys %$old
        )
    ]}
SQL
    elog NOTICE, "SQL is\n$sql" if $_SHARED{debug};
    my $sth = $_SHARED{dbh}{ $data_source_id }->prepare($sql);
    $sth->execute();
    $sth->finish();
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

elog NOTICE, $sql if $_SHARED{debug};
my $driver_there = spi_exec_query($sql);
if ($driver_there->{processed} == 0) {
    elog ERROR, "Driver $driver is not available.  Can't look at database."
}

my $attr_href = Load($params->{dbh_attributes});
my $dbh = DBI->connect(
    $params->{data_source},
    $params->{user_name},
    $params->{auth},
    $attr_href,
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
    elog NOTICE, "Checking whether $driver has $method..." if $_SHARED{debug};
    if ($dbh->can($method)) {
        elog NOTICE, "$driver has $method :)" if $_SHARED{debug};
    }
    else {
        elog ERROR, (<<ERR);
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

elog NOTICE, $sql if $_SHARED{debug};
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
elog NOTICE, $sql if $_SHARED{debug};
my $result = spi_exec_query($sql);
if ($result->{processed} == 0) {
    elog ERROR, "Couldn't retrieve the dbi connection id via currval()!";
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

elog ERROR, "Must have a local_schema!" unless $local_schema =~ /\S/;
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

elog NOTICE, "In create_schema, attempting\n$sql_check_for_schema\n" if $_SHARED{debug};

my $result = spi_exec_query($sql_check_for_schema);

if ($result->{processed} != 0) {
    elog ERROR, "Schema $local_schema already exists.";
}
else {
    my $sql_create_schema = "CREATE SCHEMA $identifier_local_schema";
    my $rv = spi_exec_query($sql_create_schema);
    if ($rv->{status} eq 'SPI_OK_UTILITY') {
        elog NOTICE, "Created schema $local_schema." if $_SHARED{debug}
    }
    else {
        elog ERROR, "Could not create schema $local_schema.  Status was\n$rv->{status}";
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

elog NOTICE, $set_search if $_SHARED{debug};
my $rv = spi_exec_query($set_search);
my $types = "'TABLE','VIEW'";


spi_exec_query("SELECT cache_connection( $params->{data_source_id} )");

my $sth = $_SHARED{dbh}{ $params->{data_source_id} }->table_info(
    $params->{remote_catalog},
    $params->{remote_schema},
    '%',
    $types
);

while(my $table = $sth->fetchrow_hashref) {
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
    elog NOTICE, "Getting column info for >$table->{TABLE_NAME}<" if $_SHARED{debug};
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
    while(my $column = $sth2->fetchrow_hashref) {
        push @raw_cols, $column->{COLUMN_NAME};
        my $cn = $_SHARED{quote_ident}->(
            $column->{COLUMN_NAME}
        );
        push @cols, $cn;
        $comments{ $cn } = $column->{TYPE_NAME};
        elog NOTICE, "Adding column $cn to table $table->{TABLE_NAME}"
            if $_SHARED{debug};
        if ( $column->{TYPE_NAME} =~ /integer/i ) {
            push @types, 'INTEGER';
        }
        else {
            push @types, 'TEXT';
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
    elog NOTICE, $sql;
    my $rv = spi_exec_query($sql);
    if ($rv->{status} eq 'SPI_OK_UTILITY') {
        elog NOTICE, "Created view $base_name."
    }
    else {
        elog ERROR, "Could not create view $base_name.  $rv->{status}";
    }

    foreach my $comment (keys %comments) {
        $sql = <<SQL;
COMMENT ON COLUMN $identifier_local_schema.$base_name.$comment IS $quote
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
    elog NOTICE, "Trying to create shadow table $shadow_table\n$sql\n" if $_SHARED{debug};
    $rv = spi_exec_query($sql);
    if ($rv->{status} eq 'SPI_OK_UTILITY') {
        elog NOTICE, "Created shadow table $shadow_table." if $_SHARED{debug};
    }
    else {
        elog ERROR, "Could not create shadow table $shadow_table.  $rv->{status}";
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
    elog NOTICE, "Trying to create trigger on shadow table\n$sql\n" if $_SHARED{debug};
    $rv = spi_exec_query($sql);
    if ($rv->{status} eq 'SPI_OK_UTILITY') {
        elog NOTICE, "Created trigger on shadow table $shadow_table." if $_SHARED{debug};
    }
    else {
        elog ERROR, "Could not create trigger on shadow table $shadow_table.  $rv->{status}";
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
        elog NOTICE, "Created INSERT rule on VIEW $base_name."
    }
    else {
        elog ERROR, "Could not create INSERT rule on VIEW $base_name.  $rv->{status}";
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
        elog NOTICE, "Created UPDATE rule on VIEW $base_name."
    }
    else {
        elog ERROR, "Could not create UPDATE rule on VIEW $base_name.  $rv->{status}";
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
        elog NOTICE, "Created DELETE rule on VIEW $base_name."
    }
    else {
        elog ERROR, "Could not create DELETE rule on VIEW $base_name.  $rv->{status}";
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

elog NOTICE, $sql if $_SHARED{debug};

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

elog NOTICE, $sql if $_SHARED{debug};

spi_exec_query($sql);

return;
$$;

CREATE OR REPLACE FUNCTION dbi_link.refresh_schema(
    data_source_id INTEGER
)
RETURNS VOID
LANGUAGE plperlU
AS $$

spi_exec_query('SELECT dbi_link.dbi_link_init()');

my $data_source_id = shift;
my $connection_info = $_SHARED{get_connection_info}->({
    data_source_id => $data_source_id,
});

elog NOTICE, "connection info is\n".Dump($connection_info)
    if $_SHARED{debug};

my $literal =  $_SHARED{quote_literal}->(
    $connection_info->{local_schema}
);

my $sql_check_for_schema = <<SQL;
SELECT 1
FROM pg_namespace
WHERE nspname = $literal
SQL

elog NOTICE, "Attempting\n$sql_check_for_schema\n" if $_SHARED{debug};

my $result = spi_exec_query($sql_check_for_schema);

my $identifier = $_SHARED{quote_ident}->(
    $connection_info->{local_schema}
);

if ($result->{processed} == 1) {
    elog NOTICE, "Found schema $identifier"
        if $_SHARED{debug};
    my $search_path = join(
        ',',
        grep { !/$identifier/ }
            split (/,/, spi_exec_query(
                    'SHOW search_path'
                )->{rows}[0]{search_path}
            )
        );
    spi_exec_query("UPDATE pg_catalog.pg_settings SET setting = '$search_path' WHERE name = 'search_path'");

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

elog NOTICE, $sql if $_SHARED{debug};

spi_exec_query($sql);

return;
$$;
