package Zabbix::Sender;
# ABSTRACT: A pure-perl implementation of zabbix-sender.

use Moose;
use namespace::autoclean;

use Carp;
use JSON;
use IO::Socket;
use IO::Select;
use Net::Domain;

=head1 NAME

Zabbix::Sender - A pure-perl implementation of zabbix-sender.

=cut

has 'server' => (
    'is'       => 'rw',
    'isa'      => 'Str',
    'required' => 1,
);

has 'port' => (
    'is'      => 'rw',
    'isa'     => 'Int',
    'default' => 10051,
);

has 'timeout' => (
    'is'      => 'rw',
    'isa'     => 'Int',
    'default' => 30,
);

has 'hostname' => (
    'is'      => 'rw',
    'isa'     => 'Str',
    'lazy'    => 1,
    'builder' => '_init_hostname',
);

has 'interval' => (
    'is'      => 'rw',
    'isa'     => 'Int',
    'default' => 1,
);

has 'retries' => (
    'is'      => 'rw',
    'isa'     => 'Int',
    'default' => 3,
);

has 'keepalive' => (
    'is'    => 'rw',
    'isa'   => 'Bool',
    'default' => 0,
);

has '_json' => (
    'is'      => 'rw',
    'isa'     => 'JSON',
    'lazy'    => 1,
    'builder' => '_init_json',
);

has '_last_sent' => (
    'is'      => 'rw',
    'isa'     => 'Int',
    'default' => 0,
);

has '_socket' => (
    'is'    => 'rw',
    'isa'   => 'Maybe[IO::Socket]',
);

has 'response' => (
    'is'    => 'rw',
    'isa'   => 'HashRef',
    'default'   => sub { {} },
);

has 'bulk_buf' => (
    'is'    => 'rw',
    'isa'   => 'ArrayRef',
    'default'   => sub { [] },
);

=head1 SYNOPSIS

This code snippet shows how to send the value "OK" for the item "my.zabbix.item"
to the zabbix server/proxy at "my.zabbix.server.example" on port "10055".

    use Zabbix::Sender;

    my $Sender = Zabbix::Sender->new({
    	'server' => 'my.zabbix.server.example',
    	'port' => 10055,
    });
    $Sender->send('my.zabbix.item','OK');

=head1 SUBROUTINES/METHODS

=head2 _init_json

Zabbix 1.8 uses a JSON encoded payload after a custom Zabbix header.
So this initializes the JSON object.

=cut

sub _init_json {
    my $self = shift;

    my $JSON = JSON::->new->utf8();

    return $JSON;
}

=head2 _init_hostname

The hostname of the sending instance may be given in the constructor.

If not it is detected here.

=cut

sub _init_hostname {
    my $self = shift;

    return Net::Domain::hostname() . '.' . Net::Domain::hostdomain();
}

=head2 zabbix_template_1_8

ZABBIX 1.8 TEMPLATE

a4 - ZBXD
b  - 0x01
V - Length of Request in Bytes (64-bit integer), aligned left, padded with 0x00, low 32 bits
V - High 32 bits of length (always 0 in Zabbix::Sender)
a* - JSON encoded request

This may be changed to a HashRef if future version of zabbix change the header template.

=cut

has 'zabbix_template_1_8' => (
    'is'      => 'ro',
    'isa'     => 'Str',
    'default' => "a4 b V V a*",
);

=head2 _encode_request

This method encodes the item and value as a json string and creates
the required header according to the template defined above.

=cut

sub _encode_request {
    my $self  = shift;
    my $values = shift;

    my @data;
    for my $ref (@{$values}) {
        push @data, {
            'host'  => $ref->[0],
            'key'   => $ref->[1],
            'value' => $ref->[2],
            'clock' => $ref->[3],
        }
    }

    my $data = {
        'request' => 'sender data',
        'data'    => \@data,
    };

    my $output = '';
    my $json   = $self->_json()->encode($data);

    # turn on byte semantics to get the real length of the string
    use bytes;
    my $length = length($json);
    no bytes;

    ## no critic (ProhibitBitwiseOperators)
    $output = pack(
        $self->zabbix_template_1_8(),
        "ZBXD", 0x01,
        $length, 0x00,
        $json
    );
    ## use critic

    return $output;
}

=head2 _decode_answer

This method tries to decode the answer received from the server.

Returns true if response indicates success, false if response indicates
failure, undefined value if response was empty or cannot be decoded.

=cut

sub _decode_answer {
    my $self = shift;
    my $data = shift;

    my ( $ident, $answer );
    $ident = substr( $data, 0, 4 ) if length($data) > 3;
    if ($ident and $ident eq 'ZBXD') {
        # Headers are optional since Zabbix 2.0.8 and 2.1.7
        if (length($data) > 12) {
            $answer = substr( $data, 13 );
        } else {
            croak "Invalid response header received";
            return;
        }
    } else {
        $answer = $data;
    }

    if ( $answer ) {
        my $ref = $self->_json()->decode($answer);
        if ($ref) {
            $self->response($ref);
            return $ref->{'response'} eq 'success' ? 1 : '';
        } else {
            $self->response(undef);
        }
    }
    return;
}

=head2 send

Send the given item with the given value to the server.

Takes two arguments: item and value. Both should be scalars.

=cut

# DGR: Anything but send just doesn't makes sense here. And since this is a pure-OO module
# and if the implementor avoids indirect object notation you should be fine.
## no critic (ProhibitBuiltinHomonyms)
sub send {
## use critic
    my $self  = shift;
    my $item  = shift;
    my $value = shift;
    my $clock = shift || time;

    my $data = $self->_encode_request( [ [ $self->hostname(), $item, $value, $clock ] ] );
    my $status = 0;
    foreach my $i ( 1 .. $self->retries() ) {
        if ( $self->_send( $data ) ) {
            $status = 1;
            last;
        }
    }

    if ($status) {
        return 1;
    }
    else {
        return;
    }

}

sub _send {
    my $self  = shift;
    my $data  = shift;

    if ( time() - $self->_last_sent() < $self->interval() ) {
        my $sleep = $self->interval() - ( time() - $self->_last_sent() );
        $sleep ||= 0;
        sleep $sleep;
    }

    unless ($self->_socket()) {
        return
            unless $self->_connect();
    }
    $self->_socket()->send( $data );
    my $Select  = IO::Select::->new($self->_socket());
    my @Handles = $Select->can_read( $self->timeout() );

    my $status = 0;
    if ( scalar(@Handles) > 0 ) {
        my $result;
        $self->_socket()->recv( $result, 1024 );
        if ( $self->_decode_answer($result) ) {
            $status = 1;
        }
    }
    $self->_disconnect() unless $self->keepalive();
    if ($status) {
        return $status;
    }
    else {
        return;
    }
}

sub _connect {
    my $self = shift;

    my $Socket = IO::Socket::INET::->new(
        PeerAddr => $self->server(),
        PeerPort => $self->port(),
        Proto    => 'tcp',
        Timeout  => $self->timeout(),
    ) or return;

    $self->_socket($Socket);

    return 1;
}

sub _disconnect {
    my $self = shift;

    if(!$self->_socket()) {
        return;
    }

    $self->_socket()->close();
    $self->_socket(undef);

    return 1;
}

=head2 bulk_buf_add

Adds values to the stack of values to bulk_send.

It accepts arguments in forms:

$sender->bulk_buf_add($key, $value, $clock, ...);
$sender->bulk_buf_add([$key, $value, $clock], ...);
$sender->bulk_buf_add($hostname, [ [$key, $value, $clock], ...], ...);

Last form allows to add values for several hosts at once.

$clock is optional and may be undef, empty or omitted.

=cut

sub bulk_buf_add {
    my $self = shift;

    my @values;
    while (@_) {
        my $arg = shift;
        if ($arg) {
            if (ref $arg) {
                if (ref $arg eq 'ARRAY' and (@{$arg} == 2 or @{$arg} == 3)) {
                    # Array of (key, value[, clock])
                    push @values, [ $self->hostname(),
                        $arg->[0], $arg->[1], $arg->[2] || time ];
                } else {
                    croak "Invalid argument";
                    return;
                }
            } else {
                my $arg2 = shift;
                if ($arg2) {
                    if (ref $arg2) {
                        unless (ref $arg2 eq 'ARRAY') {
                            croak "Invalid argument";
                            return;
                        }
                        my $hostname = $arg;
                        for my $ref (@{$arg2}) {
                            if (ref $ref and ref $ref eq 'ARRAY'
                                    and (@{$ref} == 2 or @{$ref} == 3)) {
                                # (key, value[, clock])
                                $ref->[2] = time
                                    unless $ref->[2];
                                push @values, [ $hostname, $ref->[0],
                                    $ref->[1], $ref->[2] || time ];
                            } else {
                                croak "Invalid argument";
                                return;
                            }
                        }
                    } else {
                        # (hostname, key, value[, clock])
                        my $key = $arg;
                        my $value = $arg2;
                        my $clock = shift || time;
                        push @values, [ $self->hostname(), $key, $value, $clock ];
                    }
                } else {
                    croak "Insufficient number of arguments";
                    return;
                }
            }
        } else {
            croak "Insufficient number of arguments";
            return;
        }
    }

    push @{$self->bulk_buf()}, @values;
    return 1;
}

sub bulk_buf_clear {
    my $self = shift;

    $self->bulk_buf([]);
}

=head2 bulk_send

Same as bulk_buf_add, but also send all added values to the server at the end.

=cut

sub bulk_send {
    my $self  = shift;

    if (@_) {
        $self->bulk_buf_add(@_)
            or return;
    }

    my $data = $self->_encode_request( $self->bulk_buf() );
    my $status = 0;
    foreach my $i ( 1 .. $self->retries() ) {
        if ( $self->_send( $data ) ) {
            $status = 1;
            last;
        }
    }

    if ($status) {
        $self->bulk_buf_clear();
        return 1;
    }
    else {
        return;
    }

}

=head2 DEMOLISH

Disconnects any open sockets on destruction.

=cut

sub DEMOLISH {
    my $self = shift;

    $self->_disconnect();

    return 1;
}

no Moose;
__PACKAGE__->meta->make_immutable;

=head1 AUTHOR

"Dominik Schulz", C<< <"lkml at ds.gauner.org"> >>

=head1 BUGS

Please report any bugs or feature requests to C<bug-zabbix-sender at rt.cpan.org>, or through
the web interface at L<http://rt.cpan.org/NoAuth/ReportBug.html?Queue=Zabbix-Sender>.  I will be notified, and then you'll
automatically be notified of progress on your bug as I make changes.

=head1 SUPPORT

You can find documentation for this module with the perldoc command.

    perldoc Zabbix::Sender


You can also look for information at:

=over 4

=item * RT: CPAN's request tracker

L<http://rt.cpan.org/NoAuth/Bugs.html?Dist=Zabbix-Sender>

=item * AnnoCPAN: Annotated CPAN documentation

L<http://annocpan.org/dist/Zabbix-Sender>

=item * CPAN Ratings

L<http://cpanratings.perl.org/d/Zabbix-Sender>

=item * Search CPAN

L<http://search.cpan.org/dist/Zabbix-Sender/>

=back


=head1 ACKNOWLEDGEMENTS

This code is based on the documentation and sample code found at:

=over 4

=item http://www.zabbix.com/documentation/1.8/protocols

=back

=head1 LICENSE AND COPYRIGHT

Copyright 2011 Dominik Schulz.

This program is free software; you can redistribute it and/or modify it
under the terms of either: the GNU General Public License as published
by the Free Software Foundation; or the Artistic License.

See http://dev.perl.org/licenses/ for more information.


=cut

1;    # End of Zabbix::Sender
