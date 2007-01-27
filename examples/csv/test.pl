#!/usr/bin/env perl
use strict;
use warnings;

use YAML;

my $foo = {
    RaiseError => 1,
    csv_eol => "\n",
    csv_sep_char => ",",
    csv_quote_char => '"',
};

print Dump($foo);
