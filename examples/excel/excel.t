#!/usr/bin/perl -l
use strict;
use warnings;

$|++;
use DBI;
my $dbh = DBI->connect(
    'dbi:Excel:file=/home/shackle/dbi-link/test/settings.xls',
    undef,
    undef,
    {
        RaiseError => 1
    }
);

my @methods = qw(table_info column_info);
foreach my $method (@methods) {
     if ($dbh->can($method)) {
         print "Handle has method $method. w00t!"
     }
     else {
         $dbh->disconnect;
         print "Sadly, handle does not have method $method. D'oh!";
         exit;
     }
}

my $sth_table=$dbh->table_info('%', '%', '%', 'TABLE');
my $sth_column;
while(my $table = $sth_table->fetchrow_hashref) {
    foreach my $header (sort {$a cmp $b} keys %$table) {
        print "$header: $table->{$header}";
    }
    $sth_column = $dbh->column_info(undef, '%', $table->{TABLE_NAME}, '%');
    while(my $column = $sth_column->fetchrow_hashref) {
        foreach my $col (sort {$a cmp $b} keys %$column) {
            print "\t$col: $column->{$col}";
        }
    }
    $sth_column->finish;
    print;
}

$sth_table->finish;
$dbh->disconnect
