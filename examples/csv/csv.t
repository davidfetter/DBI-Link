#!/usr/bin/perl -l

use strict;
use warnings;
use JSON;

$|++;
my $DEBUG=0;
use DBI;
my $dbh = DBI->connect(
    'dbi:CSV:f_dir=.',
    {
        RaiseError => 1,
        csv_eol => "\n",
        csv_sep_char => ",",
        csv_quote_char => '"',
    },
);

my @methods = qw(table_info column_info list_tables quote quote_ident);
foreach my $method (@methods) {
     if ( $dbh->can($method) ) {
         print "Handle has method $method. w00t!"
     }
     else {
         print "Sadly, handle does not have method $method. D'oh!";
         # $dbh->disconnect;
         # exit;
     }
}

print "All table info for 'DB'\n", to_json($dbh->{csv_tables});

my @tables = grep { /csv$/i } $dbh->func('list_tables');
foreach my $table (@tables) {
    print "Found table $table";
    my $sql = "SELECT * FROM $table";
    print $sql if $DEBUG;
    my $sth = $dbh->prepare($sql);
    $sth->execute();
    print "    column names:\n", join("\n        ", @{ $sth->{NAME} });
    print "    number of fields: $sth->{NUM_OF_FIELDS}";
    print "    table info for $table:\n", to_json($dbh->{csv_tables});
    my $whole_table = $sth->fetchall_arrayref({});
#################################################################
#                                                               #
#     while(my $row = $sth->fetchrow_hashref) {                 #
#         print map { "\t$_ => $row->{$_}" } @{ $sth->{NAME} }; #
#     }                                                         #
#                                                               #
#################################################################
    $sth->finish();
    print to_json($whole_table);
}

$dbh->disconnect;
