#!/usr/bin/perl
use strict;
use warnings;

$|++;

BEGIN {
use YAML;

my $yaml = <<YAML;
---
- env_action: overwrite
  env_name: NLS_LANG
  env_value: AMERICAN_AMERICA.AL32UTF8
- env_action: overwrite
  env_name: ORACLE_HOME
  env_value: /usr/lib/oracle/xe/app/oracle/product/10.2.0/client
- env_action: overwrite
  env_name: SQLPATH
  env_value: /usr/lib/oracle/xe/app/oracle/product/10.2.0/client/sqlplus
- env_action: prepend
  env_name: PATH
  env_value: '/usr/lib/oracle/xe/app/oracle/product/10.2.0/client/bin'
- env_action: overwrite
  env_name: LD_LIBRARY_PATH
  env_value: '/usr/lib/oracle/xe/app/oracle/product/10.2.0/client/lib'
YAML

my $env = Load($yaml);
foreach my $setting (@$env) {
    print "Got to setting $setting->{env_name}\n    ";
    if ($setting->{env_action} eq 'append') {
        print <<VALS;
    Action: append
    Name: $setting->{env_name}
    Value: $setting->{env_value}
VALS
        $ENV{$setting->{env_name}} = join(
            ':',
            grep {length($_) > 0} 
                $ENV{$setting->{env_name}}, 
                $setting->{env_value},
        );
    }
    elsif ($setting->{env_action} eq 'prepend') {
        print <<VALS;
    Action: prepend
    Name: $setting->{env_name}
    Value: $setting->{env_value}
VALS
        $ENV{$setting->{env_name}} = join(
            ':',
            grep {length($_) > 0} 
                $setting->{env_value},
                $ENV{$setting->{env_name}}, 
        );
    }
    elsif ($setting->{env_action} eq 'overwrite') {
        print <<VALS;
    Action: overwrite
    Name: $setting->{env_name}
    Value: $setting->{env_value}
VALS
        $ENV{ $setting->{env_name} } = $setting->{env_value};
    }
    else {
        die "D'oh!";
    }
}
}
print Dump(\%ENV);

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

my $sth=$dbh->column_info('%', '%', '%', '%');
my $tables = $sth->fetchall_arrayref({});
print Dump($tables);
$sth->finish;
$dbh->disconnect;
exit;

################################################################
#                                                              #
# my $sth_column;                                              #
# while(my $table = $sth_table->fetchrow_hashref) {            #
#     print "Table $table->{TABLE_NAME}:\n";                   #
#     $sth_column = $dbh->column_info(                         #
#         undef,                                               #
#         'public',                                            #
#         $table->{TABLE_NAME},                                #
#         '%'                                                  #
#     );                                                       #
#     my $column = $sth_column->fetchrow_hashref;              #
#     my @keys = sort keys %$column;                           #
#     map {print "    $_: $column->{$_}\n" } @keys;            #
#     while($column = $sth_column->fetchrow_hashref) {         #
#         map {print "    $_: $column->{$_}\n" } @keys;        #
#     }                                                        #
#     $sth_column->finish;                                     #
#     print "\n";                                              #
#     my @pks = $dbh->primary_key(                             #
#         undef,                                               #
#         'public',                                            #
#         $table->{TABLE_NAME},                                #
#     );                                                       #
#     foreach my $pk_pos (0..$#pks) {                          #
#         print "\tPK column @{[$pk_pos + 1]}: $pks[$pk_pos]"; #
#     }                                                        #
#     print '';                                                #
# }                                                            #
#                                                              #
# $sth_table->finish;                                          #
# $dbh->disconnect;                                            #
#                                                              #
################################################################
