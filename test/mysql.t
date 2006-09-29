#!/usr/bin/perl -l
use strict;
use warnings;

$|++;
use DBI;
my $dbh = DBI->connect(
    'dbi:mysql:database=world;host=localhost',
    'root',
    undef,
    {
        RaiseError => 1,
        FetchHashKeyName => 'NAME_lc',
    }
);

my @methods = qw(table_info column_info);
foreach my $method (@methods) {
     if ( $dbh->can($method) ) {
         print "Handle has method $method. w00t!"
     } else {
         $dbh->disconnect;
         print "Sadly, handle does not have method $method. D'oh!";
         exit;
     }
}

my $sth_table=$dbh->table_info('%', '%', '%', 'TABLE');
my $sth_column;
while(my $table = $sth_table->fetchrow_hashref) {
    print "Table $table->{TABLE_NAME}";
    $sth_column = $dbh->column_info(
        undef,
        undef,
        $table->{TABLE_NAME},
        '%',
    );
    my @cols = qw(ORDINAL_POSITION COLUMN_NAME mysql_type_name);
    print "\t", join("\t", @cols);
    while(my $column = $sth_column->fetchrow_hashref) {
        print "\t", join("\t", map {
            (defined $column->{$_}) ? $column->{$_} : 'NULL'
        } (@cols));
    }
    $sth_column->finish;
    print '';
}

$sth_table->finish;
$dbh->disconnect;
