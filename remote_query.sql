--------------------------------------------------
--                                              --
--  This depends on functionality in DBI.sql.   --
--                                              --
--------------------------------------------------
CREATE OR REPLACE FUNCTION remote_query (
  driver TEXT
, host TEXT
, port INTEGER
, database TEXT
, db_user TEXT
, db_password TEXT
, query TEXT
, safe BOOLEAN
)
RETURNS SETOF RECORD
LANGUAGE plperlu AS $$
my($driver, $host, $port, $database, $db_user, $db_password, $query, $safe) = @_;
##################################################################
#                                                                #
# Sanity checks: must have a query, and it must be SELECT query. #
# TODO: check for multiple queries.                              #
#                                                                #
##################################################################
if (length($query) == 0) {
    elog ERROR, 'Must issue a query!';
} elsif ($query !~ /^select/i && $safe) {
    elog ERROR, 'Must issue a SELECT query!';
}

use DBI;

##################################################
#                                                #
# Is the named driver available on this machine? #
#                                                #
##################################################
$driver =~ s/'/''/g;
my $dtsql = <<SQL;
SELECT ad
FROM available_drivers() AS "ad"
WHERE ad = '$driver'
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
my $db_label = ($driver eq 'Pg')?'dbname':'database';
$host = (length $host)?$host:'localhost';
my $dbh = DBI->connect(
  "dbi:$driver:$db_label=$database;host=$host;port=$port"
, $user
, $password
  { RaiseError => 0
  , PrintError => 0
  , AutoCommit => 0
  }
);

if ($DBI::errstr) {
    bail (header => "Could not connect to database
type: $driver
host: $host
database: $database
user: $db_user
password: $db_password", error => $DBI::errstr);
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

################################################################
#                                                              #
# Must return a reference to an array of hashrefs.             #
# Should this be a one-off DBI feature like fetchall_arrayref? #
#                                                              #
################################################################
my $rowset;
while(my $row = $sth->fetchrow_hashref) {
    push @$rowset, $row;
}
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
    elog ERROR, join("\n\n", map{$parms{$_}} grep {$parms{$_} =~ /\S/} qw(header message error));
}
   
$$;

COMMENT ON FUNCTION data_link (
  driver TEXT
, host TEXT
, port INTEGER
, database TEXT
, db_user TEXT
, db_password TEXT
, query TEXT
) IS $$
Copyright (c) 2004, David Fetter
All rights reserved.
                                                                                
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
                                                                                
    * Redistributions of source code must retain the above copyright
      notice, this list of conditions and the following disclaimer.
                                                                                
    * Redistributions in binary form must reproduce the above
      copyright notice, this list of conditions and the following
      disclaimer in the documentation and/or other materials provided
      with the distribution.
                                                                                
    * Neither the name of the the PostgreSQL Project nor the names of its
      contributors may be used to endorse or promote products derived
      from this software without specific prior written permission.
                                                                                
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
$$;

