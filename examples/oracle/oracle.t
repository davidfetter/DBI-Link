#!/usr/bin/perl -l
use strict;
use warnings;

$|++;
use YAML;
use DBI;
use DBD::Oracle qw(:ora_types);

my $dbh = DBI->connect(
    'dbi:Oracle:host=localhost;sid=xe',
    'hr',
    'foobar',
    {
        AutoCommit => 1,
        RaiseError => 1,
    }
);

my @methods = qw(table_info column_info primary_key_info);
foreach my $method (@methods) {
     if ( $dbh->can($method) ) {
         print "Handle has method $method. w00t!"
     }
     else {
         $dbh->disconnect;
         print "Sadly, handle does not have method $method. D'oh!";
         exit;
     }
}

my $sth=$dbh->table_info('%', '%', '%', 'TABLE');
while(my $table = $sth->fetchrow_hashref) {
    my $t;
    $t->{'Table Name'} = $table->{TABLE_NAME};
    $t->{'Column Info'} = $dbh->column_info(
        undef,
        $table->{TABLE_SCHEM},
        $table->{TABLE_NAME},
        '%'
    )->fetchall_arrayref({});
    $t->{'Primary Key Info'} = $dbh->primary_key_info(
        undef,
        $table->{TABLE_SCHEM},
        $table->{TABLE_NAME}
    )->fetchall_arrayref({});
    print map {"$_: ". Dump($t->{$_})} grep{ defined $t->{$_} } 'Table Name', 'Column Info', 'Primary Key Info';
    print;
}
$sth->finish;
$dbh->disconnect;
