#!/usr/bin/perl

use strict;
use warnings;

use Getopt::Long;
use Zabbix::Sender;

my $opts = {
	'item' => undef, # Zabbix Item
	'value' => undef, # Value of Zabbix Item
	'server' => undef,
	'port' => 10055,
	'timeout' => 30,
};

GetOptions(
	'item=s'	=> \$opts->{'item'},
	'value=s'	=> \$opts->{'value'},
	'server=s' => \$opts->{'server'},
	'port=i'	=> \$opts->{'port'},
	'timeout=i'	=> \$opts->{'timeout'},
);

foreach my $key (qw(item value server)) {
	if(!defined($opts->{$key})) {
		die("Usage: $0 --item=X --value=Z --server=Y [--port=int] [--timeout=int]\n");
	}
}

my $Sender = Zabbix::Sender->new($opts);
my $status = $Sender->send($opts->{'item'},$opts->{'value'});
if($status) {
	exit 0;
} else {
	exit 1;
}
