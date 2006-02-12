#!/usr/bin/env perl

use warnings;
use strict;
use ExtUtils::MakeMaker;

use 5.008001;

WriteMakefile(
    NAME           => 'dbi-link',
    VERSION        => '2.0.0',
    PREREQ_PM      => {
        'DBI'         => 1.37,
    },
    ABSTRACT       => 'DBI-Link is a partial implementation of SQL/MED using DBI',
    AUTHOR         => 'David Fetter <david@fetter.org>',
);
