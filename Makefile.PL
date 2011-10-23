use strict;
use warnings;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME                => 'Zabbix::Sender',
    AUTHOR              => q{"Dominik Schulz" <"lkml@ds.gauner.org">},
    VERSION_FROM        => 'lib/Zabbix/Sender.pm',
    ABSTRACT_FROM       => 'lib/Zabbix/Sender.pm',
    ($ExtUtils::MakeMaker::VERSION >= 6.3002
      ? ('LICENSE'=> 'perl')
      : ()),
    PL_FILES            => {},
    PREREQ_PM => {
        'Test::More' => 0,
        'Moose'	=> 1.09,
        'namespace::autoclean' => 0.09,
        'JSON' => 2.21,
        'IO::Socket' => 0,
        'IO::Select' => 0,
        'Net::Domain' => 0,
    },
    dist                => { COMPRESS => 'gzip -9f', SUFFIX => 'gz', },
    clean               => { FILES => 'Zabbix-Sender-*' },
);
