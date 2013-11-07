#!/usr/bin/env perl

use warnings;
use strict;
use ExtUtils::MakeMaker;

require 5.14.0;

WriteMakefile(
    NAME           => 'dbi-link',
    VERSION        => '2.1.0',
    PREREQ_PM      => {
        'DBI'         => 1.52,
    },
    ABSTRACT       => 'DBI-Link is a partial implementation of SQL/MED using DBI',
    AUTHOR         => 'David Fetter <david@fetter.org>',
);
