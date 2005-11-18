--------------------------------------------------
--                                              --
--  This depends on functionality in DBI.sql.   --
--                                              --
--------------------------------------------------
CREATE OR REPLACE FUNCTION remote_select (
  data_source TEXT
, db_user TEXT
, db_password TEXT
, dbh_attr TEXT
, query TEXT
)
RETURNS SETOF RECORD
LANGUAGE plperlu AS $$
use strict;
use DBI;

my $DEBUG = 1;
my($data_source, $db_user, $db_password, $dbh_attr, $query) = @_;
##################################################################
#                                                                #
# Sanity checks: must have a query, and it must be SELECT query. #
# TODO: check for multiple queries.                              #
#                                                                #
##################################################################
if (length($query) == 0) {
    elog ERROR, 'Must issue a query!';
} elsif ($query !~ /^select/i ) {
    elog ERROR, 'Must issue a SELECT query!';
}

my $driver = $data_source;
$driver =~ s/^dbi:([^:]+):.*/$1/;
##################################################
#                                                #
# Is the named driver available on this machine? #
#                                                #
##################################################
my $dtsql = <<SQL;
SELECT count(*)
FROM dbi_link.available_drivers()
WHERE available_drivers = '$driver'
SQL
my $driver_there = spi_exec_query($dtsql);
if ($driver_there->{rows}[0] == 0) {
    bail(message => "Driver $driver does not appear to be available.");
}

#######################################
#                                     #
# Attempt to connect with the driver. #
#                                     #
#######################################
my $attr = eval($dbh_attr);
my $dbh = DBI->connect(
  $data_source
, $db_user
, $db_password
, $attr
);

if ($DBI::errstr) {
    bail (
        header => "Could not connect to database",
        message => "data source: $data_source
user: $db_user
password: $db_password
attributes:
$dbh_attr",
        error => $DBI::errstr
    );
} else {
    elog NOTICE, "Connected to database
user: $db_user
password: $db_password
attributes:
$dbh_attr" if $DEBUG;
}

##################
#                #
# Prepare query. #
#                #
##################
my $sth = $dbh->prepare($query);
if ($DBI::errstr) {
    bail (header => "Cannot prepare", message => $query, error => $DBI::errstr);
}

######################
#                    #
# Execute ye Querye. #
#                    #
######################
$sth->execute();
if ($DBI::errstr) {
    bail (header => "Cannot execute", message => $query, error => $DBI::errstr);
}

#########################################
#                                       #
# This next line from the DBI man page. #
#                                       #
#########################################
my $rowset = $sth->fetchall_arrayref({});
$sth->finish;
$dbh->disconnect;
return $rowset;

sub bail {
    my %parms = (
      header => undef
    , message => undef
    , error => undef
    , @_
    );
    elog ERROR, join("\n", map{$parms{$_}} grep {$parms{$_} =~ /\S/} qw(header message error));
}
   
$$;

COMMENT ON FUNCTION remote_select (
  data_source TEXT
, db_user TEXT
, db_password TEXT
, dbh_attr TEXT
, query TEXT
) IS $$
This function does SELECTs on a remote data source de novo.
$$;

CREATE OR REPLACE FUNCTION remote_select (
  data_source_id INTEGER
, query TEXT
)
RETURNS SETOF RECORD
LANGUAGE plperlu AS $$
my($data_source_id, $query) = @_;
##################################################################
#                                                                #
# Sanity checks: must have a query, and it must be SELECT query. #
# TODO: check for multiple queries.                              #
#                                                                #
##################################################################
if (length($query) == 0) {
    elog ERROR, 'Must issue a query!';
} elsif ($query !~ /^select/i ) {
    elog ERROR, 'Must issue a SELECT query!';
}

use DBI;

##################################################
#                                                #
# Is the named driver available on this machine? #
#                                                #
##################################################
my $dtsql = <<SQL;
SELECT data_source, user_name, auth, dbh_attr
FROM dbi_link.dbi_connection
WHERE ad = $data_source_id
SQL
my ($data_source, $user_name, $auth, $dbh_attr);
my $driver_there = spi_exec_query($dtsql);
my $nrows = $driver_there->{processed};
if ($nrows == 0) {
    bail(message => "No such database connection as $data_source_id!");
} elsif ($nrows != 1) {
    bail(message => "This can't happen!  data_source_id = $data_source_id is a primary key!");
} else {
    $data_source = $driver_there->{rows}[0]{data_source};
    $user_name = $driver_there->{rows}[0]{user_name};
    $auth = $driver_there->{rows}[0]{auth};
    $dbh_attr = $driver_there->{rows}[0]{dbh_attr};
}

#######################################
#                                     #
# Attempt to connect with the driver. #
#                                     #
#######################################
my $attr = eval($dbh_attr);
my $dbh = DBI->connect(
  $data_source
, $user_name
, $auth
, $attr
);

if ($DBI::errstr) {
    bail (
        header => "Could not connect to database",
        message => "$data_source
user: $user_name
password: $auth
attributes:
$dbh_attr",
        error => $DBI::errstr
    );
}

##################
#                #
# Prepare query. #
#                #
##################
my $sth = $dbh->prepare($query);
if ($DBI::errstr) {
    bail (header => "Cannot prepare", message => $query, error => $DBI::errstr);
}

######################
#                    #
# Execute ye Querye. #
#                    #
######################
$sth->execute();
if ($DBI::errstr) {
    bail (header => "Cannot execute", message => $query, error => $DBI::errstr);
}

my $rowset = $sth->fetchall_arrayref({});
$sth->finish;
$dbh->disconnect;
return $rowset;

sub bail {
    my %parms = (
      header => undef
    , message => undef
    , error => undef
    , @_
    );
    elog ERROR, join("\n", map{$parms{$_}} grep {$parms{$_} =~ /\S/} qw(header message error));
}
   
$$;
COMMENT ON FUNCTION remote_select (
  data_source_id INTEGER
, query TEXT
) IS $$
This function does SELECTs on a remote data source stored in
dbi_link.data_sources.
$$;

CREATE OR REPLACE FUNCTION shadow_trigger_func()
RETURNS TRIGGER
LANGUAGE plperlu
AS $$
#####################################################
#                                                   #
# Immediately reject anything that's not an INSERT. #
#                                                   #
#####################################################
if ($_TD->{event} ne 'INSERT') {
    return "SKIP";
}

use strict;
use DBI;

my $data_source_id = $_[0];
##################################################
#                                                #
# Is the named driver available on this machine? #
#                                                #
##################################################
my $dtsql = <<SQL;
SELECT data_source, user_name, auth, dbh_attr
FROM dbi_link.dbi_connection
WHERE ad = $data_source_id
SQL
my ($data_source, $user_name, $auth, $dbh_attr);
my $driver_there = spi_exec_query($dtsql);
my $nrows = $driver_there->{processed};
if ($nrows == 0) {
    bail(message => "No such database connection as $data_source_id!");
} elsif ($nrows != 1) {
    bail(message => "This can't happen!  data_source_id = $data_source_id is a primary key!");
} else {
    $data_source = $driver_there->{rows}[0]{data_source};
    $user_name = $driver_there->{rows}[0]{user_name};
    $auth = $driver_there->{rows}[0]{auth};
    $dbh_attr = $driver_there->{rows}[0]{dbh_attr};
}

#######################################
#                                     #
# Attempt to connect with the driver. #
#                                     #
#######################################
my $attr = undef;
if (length($dbh_attr) > 0 ) {
    $attr = eval($dbh_attr);
}

my $dbh = DBI->connect(
  $data_source
, $user_name
, $auth
, $attr
);

if ($DBI::errstr) {
    bail (
        header => "Could not connect to database",
        message => "$data_source
user: $user_name
password: $auth
attributes:
$dbh_attr",
        error => $DBI::errstr
    );
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
        } else {
            $old->{$2} = 'NULL';
        }
    } else {
        if (defined $_TD->{'new'}->{$_}) {
            $new->{$2} = $dbh->quote($_TD->{'new'}->{$key});
        } else {
            $new->{$2} = 'NULL';
        }
    }
}

my $iud = (
  I => \&insert
, U => \&update
, D => \&delete
);

if ($iud->{ $_TD->{'new'}{iud_action} }) {
    $iud->{ $_TD->{'new'}{iud_action} }->();
} else {
    elog ERROR, "Trigger event was $_TD->{'new'}{iud_action}<, but should have been one of I, U or D!"
}

return 'SKIP';

sub insert {
    my $table = $_TD->{relname};
    my $sql = <<SQL;
INSERT INTO $table (
  @{[join("\n, ", sort keys %$new) ]}
) VALUES (
  @{[join("\n, ", map { $_ = $new->{$_} } sort keys %$new) ]}
)
SQL
    my $sth = $dbh->prepare($sql);
    $sth->execute($sql);
}

sub update {
    my $table = $_TD->{relname};
    my $sql = <<SQL;
UPDATE $table
SET
  @{[ join("\n, ", map { $_ = $new->{$_} } sort keys %$new) ]}
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
    my $table = $_TD->{relname};
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

sub bail {
    my %parms = (
      header => undef
    , message => undef
    , error => undef
    , @_
    );
    elog ERROR, join("\n", map{$parms{$_}} grep {$parms{$_} =~ /\S/} qw(header message error));
}

$$;
