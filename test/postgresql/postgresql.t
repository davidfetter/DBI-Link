#!/usr/bin/perl -l
use strict;
use warnings;

$|++;
use DBI;
use DBD::Pg qw(:pg_types);
my $dbh = DBI->connect(
    'dbi:Pg:dbname=test',
    'postgres',
    undef,
    {
        AutoCommit => 1
        RaiseError => 1
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

my $sth_table=$dbh->table_info('%', 'public', '%', 'TABLE');
my $sth_column;
while(my $table = $sth_table->fetchrow_hashref) {
    print "Table $table->{TABLE_NAME}";
    $sth_column = $dbh->column_info(undef, 'public', $table->{TABLE_NAME}, '%');
    my @cols = qw(ORDINAL_POSITION COLUMN_NAME pg_type);
    print "\t", join("\t", @cols);
    while(my $column = $sth_column->fetchrow_hashref) {
        print "\t", join("\t", map {
            (defined $column->{$_}) ? $column->{$_} : 'NULL'
        } (@cols));
    }
    $sth_column->finish;
    print '';
    my @pks = $dbh->primary_key(
        undef,
        'public',
        $table->{TABLE_NAME}
    );
    foreach my $pk_pos (0..$#pks) {
        print "\tPK column @{[$pk_pos + 1]}: $pks[$pk_pos]";
    }
    print '';
}

$sth_table->finish;
$dbh->disconnect;
