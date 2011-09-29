package MongoDBx::Protocol;
{
  $MongoDBx::Protocol::VERSION = '0.02';
}

use strict;
use warnings;

# ABSTRACT: pure perl implementation of MongoDB protocol



# for '<' in pack/unpack templates (ensure that bytes order is little-endian)
require v5.8.5;

use Bit::Vector;
use BSON qw();
use Carp qw(croak);
use Params::Validate qw(:all);

my $OP_CODES = {
    reply        => 1,
    msg          => 1000,
    update       => 2001,
    insert       => 2002,
    query        => 2004,
    getmore     => 2005,
    delete       => 2006,
    kill_cursors => 2007,
};
my $OP_CODE2STR = {};
foreach my $str (keys %$OP_CODES) {
   $OP_CODE2STR->{ $OP_CODES->{$str} } = $str;
}

my $FLAGS = {
    # OP_UPDATE
    Upsert      => 0,
    MultiUpdate => 1,

    # OP_INSERT
    ContinueOnError => 0,

    # OP_QUERY
    TailableCursor  => 1,
    SlaveOk         => 2,
    OplogReplay     => 3,
    NoCursorTimeout => 4,
    AwaitData       => 5,
    Exhaust         => 6,
    Partial         => 7,

    # OP_DELETE
    SingleRemove => 0,
};

my $REPLY_FLAGS = {
    0 => 'CursorNotFound',
    1 => 'QueryFailure',
    2 => 'ShardConfigStale',
    3 => 'AwaitCapable',
};

my $HEADER_FIELD     = { header => { type => HASHREF, default => {} } };
my $FLAGS_FIELD      = { flags => { type => HASHREF, default => {} } };
my $COLLECTION_FIELD = { fullCollectionName => { type => SCALAR } };



sub new {
    my $class = shift;
    my $self = {};
    bless $self, $class;
    return $self;
}


sub update {
    my $self = shift;
    my $p = validate(@_, {
        %$HEADER_FIELD,
        %$FLAGS_FIELD,
        %$COLLECTION_FIELD,
        selector => { type => HASHREF },
        update   => { type => HASHREF },
    });

    my $msg = _int32(0) .
              _cstring($p->{fullCollectionName}) .
              _flags($p->{flags}) .
              _documents($p->{selector}) .
              _documents($p->{update});

    return $self->_with_header($p->{header}, \$msg, 'update');
}


sub insert {
    my $self = shift;
    my $p = validate(@_, {
         %$HEADER_FIELD,
         %$FLAGS_FIELD,
         %$COLLECTION_FIELD,
         documents => { type => ARRAYREF },
    });

    my $msg = _flags($p->{flags}) .
              _cstring($p->{fullCollectionName}) .
              _documents($p->{documents});

    return $self->_with_header($p->{header}, \$msg, 'insert');

}


sub query {
    my $self = shift;
    my $p = validate(@_, {
        %$HEADER_FIELD,
        %$FLAGS_FIELD,
        %$COLLECTION_FIELD,
        numberToSkip   => { type => SCALAR, regex => qr/^\d+$/o, default => 0 },
        numberToReturn => { type => SCALAR, regex => qr/^\d+$/o, default => 1 },
        query => { type => HASHREF },
        returnFieldSelector => { type => HASHREF, optional => 1 },
    });

    my $msg = _flags($p->{flags}) .
              _cstring($p->{fullCollectionName}) .
              _int32($p->{numberToSkip}) .
              _int32($p->{numberToReturn}) .
              _documents($p->{query}) .
              _documents($p->{returnFieldSelector});

    return $self->_with_header($p->{header}, \$msg, 'query');
}



sub getmore {
    my $self = shift;
    my $p = validate(@_, {
        %$HEADER_FIELD,
        %$COLLECTION_FIELD,
        numberToReturn => { type => SCALAR, regex => qr/^\d+$/o, default => 1 },
        cursorID => { type => SCALAR, regex => qr/^\d+$/o },
    });

    my $msg = _int32(0) .
              _cstring($p->{fullCollectionName}) .
              _int32($p->{numberToReturn}) .
              _int64($p->{cursorID});

    return $self->_with_header($p->{header}, \$msg, 'getmore');
}


sub delete {
    my $self = shift;
    my $p = validate(@_, {
        %$HEADER_FIELD,
        %$COLLECTION_FIELD,
        %$FLAGS_FIELD,
        selector => { type => HASHREF },
    });

    my $msg = _int32(0) .
              _cstring($p->{fullCollectionName}) .
              _flags($p->{flags}) .
              _documents($p->{selector});

    return $self->_with_header($p->{header}, \$msg, 'delete');
}



sub kill_cursors {
    my $self = shift;
    my $p = validate(@_, {
        %$HEADER_FIELD,
        cursorIDs => { type => ARRAYREF },
    });

    my $ids_msg = '';
    foreach my $id (@{$p->{cursorIDs}}) {
        $ids_msg .= _int64($id);
    }

    my $msg = _int32(0) .
              _int32(scalar @{$p->{cursorIDs}}) .
              $ids_msg;

    return $self->_with_header($p->{header}, \$msg, 'kill_cursors');
}



sub msg {
    my $self = shift;
    my $p = validate(@_, {
        %$HEADER_FIELD,
        message => { type => SCALAR },
    });

    my $msg = _cstring($p->{message});

    return $self->_with_header($p->{header}, \$msg, 'msg');
}


sub decode_reply {
    my $self = shift;
    my $reply = shift;
    croak("Too small reply") if (length($reply) < (4*4 + 4 + 8 + 4 +4));

    my $header = substr($reply, 0, 4*4, '');
    my $responseFlags = substr($reply, 0, 4, '');
    my $cursorID = substr($reply, 0, 8, '');
    my $startingFrom = substr($reply, 0, 4, '');
    my $numberReturned = substr($reply, 0, 4, '');

    my $documents = $reply;

    my $res = {
        header         => _decode_header($header),
        responseFlags  => _decode_flags($responseFlags),
        cursorID       => _decode_int64($cursorID),
        startingFrom   => _decode_int32($startingFrom),
        numberReturned => _decode_int32($numberReturned),
        documents      => _decode_documents($documents),
    };

    return $res;
}

sub _decode_header {
    my $h = shift;

    my $messageLength = substr($h, 0, 4, '');
    my $requestID     = substr($h, 0, 4, '');
    my $responseTo    = substr($h, 0, 4, '');
    my $opCode        = substr($h, 0, 4, '');

    my $op_code_int = _decode_int32($opCode);
    my $op_code_str = $OP_CODE2STR->{$op_code_int};
    croak("Unknown op_code [$op_code_int]") unless defined($op_code_str);

    my $header = {
        messageLength => _decode_int32($messageLength),
        requestID     => _decode_int32($requestID),
        responseTo    => _decode_int32($responseTo),
        opCode        => $op_code_str,
    };

    return $header;
}

sub _decode_int32 {
    my $int32 = shift;
    return unpack("l<", $int32);
}

sub _decode_int64 {
    my $int64 = shift;
    return unpack("q<", $int64);
}

sub _decode_flags {
    my $flags = shift;

    my $v = Bit::Vector->new(32);
    $v->from_Dec(_decode_int32($flags));

    my $str_flags = {};
    foreach my $reply_flag_bit (keys %$REPLY_FLAGS) {
        my $reply_flag = $REPLY_FLAGS->{$reply_flag_bit};
        my $reply_flag_value = 0;
        if ($v->bit_test($reply_flag_bit)) {
            $reply_flag_value = 1;
        }
        $str_flags->{$reply_flag} = $reply_flag_value;
    }

    return $str_flags;
}

sub _decode_documents {
    my $docs_str = shift;

    my @docs = ();
    while (length($docs_str)) {
        my $l = _decode_int32(substr($docs_str, 0, 4));
        if (length($docs_str) < $l) {
            croak "Incorrect length of bson document";
        }
        my $doc_str = substr($docs_str, 0, $l, '');
        push @docs, BSON::decode($doc_str);
    }

    return \@docs;
}

sub _cstring {
    my $string = shift;
    return $string . "\x00";
}

sub _int32 {
    my $int32 = shift;
    return pack("l<", $int32);
}

sub _int64 {
    my $int64 = shift;
    return pack("q<", $int64);
}

sub _flags {
    my $flags = shift;
    my $v = Bit::Vector->new(32);
    while (my ($flag_name, $flag_value) = each %$flags) {
        if (defined(my $flag_bit = $FLAGS->{$flag_name}) && $flag_value) {
            $v->Bit_On($flag_bit);
        }
    }

    return _int32($v->to_Dec);
}

sub _documents {
    my $d = shift;
    return '' unless ($d);

    my @docs;
    if (ref($d) eq 'HASH') {
        @docs = ($d);
    } elsif (ref($d) eq 'ARRAY') {
        @docs = @$d;
    }

    my $msg = '';
    foreach my $doc (@docs) {
        $msg .= BSON::encode($doc);
    }

    return $msg;
}

sub _with_header {
    my ($self, $header, $msg_ref, $op) = @_;

    my $msg = $$msg_ref;
    my $length = 4*4 + length($msg);

    my $h = _int32($length) . _int32($header->{requestID} || 0) .
            _int32($header->{responseTo} || 0) . _int32($OP_CODES->{$op});

    return $h . $msg;
}


1;

__END__
=pod

=head1 NAME

MongoDBx::Protocol - pure perl implementation of MongoDB protocol

=head1 VERSION

version 0.02

=head1 SYNOPSIS

    my $p = MongoDBx::Protocol->new;
    my $msg = $p->insert({
        header => { requestID => 3, responseTo => 2 },
        fullCollectionName => 'test.test',
        documents => [ { a => b }, { c => d } ],
    });
    my $sock = IO::Socket::INET->new(
        PeerAddr => 'localhost', PeerPort => 27017, Proto => 'tcp'
    );
    $sock->print($msg);

=head1 DESCRIPTION

This is a pure perl implementation of MongoDB protocol as described at L<http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol>.
Such modules as L<MongoDB> and L<AnyMongo> haven't public API for dealing with MongoDB protocol at low level. Using MongoDBx::Protocol you can encode/decode messages which you can send/recieve to/from MongoDB.

This module doesn't try to work very effectively, for this purposes you should use some XS code from L<MongoDB> module.

For encoding and decoding BSON documents this module uses L<BSON> module.

=head1 METHODS

All encoding/decoding methods takes/returns hashref. See the key names for hashes and detailed description of this keys at MongoDB site.

=over 4

=item new()

Creates new instance of MongoDBx::Protocol.

=item update($options)

Returns binary string that represents updating a documents in MongoDB.

C<$options> is a hashref that contains following keys:

=over 4

=item I<header> (optional)

Hashref with C<requestID> and C<responseTo> keys. Default value for this keys is 0.

=item I<flags> (optional)

Hashref with possible flags C<Upsert>, C<MultiUpdate>.

=item I<fullCollectionName>

Fully qualified name of colllection (for example, "foo.bar").

=item I<selector>

Hashref that will be passed to L<BSON> module for encoding to BSON document.

This hashref represents the query to select the document.

=item I<update>

Hashref with specification of the update to perform.

=back

=item insert($options)

C<$options> contains:

=over 4

=item I<header> (optional)

=item I<flags> (optional)

Possible flag is C<ContinueOnError>.

=item I<fullCollectionName>

=item I<documents>

Arrayref of documents that will be encoded to BSON.

=back

=item query($options)

C<$options> contains:

=over 4

=item I<header> (optional)

=item I<flags> (optional)

Possible flags are: C<TailableCursor>, C<SlaveOk>, C<OplogReplay>, C<NoCursorTimeout>, C<AwaitData>, C<Exhaust>, C<Partial>.

=item I<fullCollectionName>

=item I<numberToSkip> (optional)

Default is 0.

=item I<numberToReturn> (optional)

Default is 1.

=item I<query>

=item I<returnFieldSelector> (optional)

Which fields will be returned.

=back

=item getmore($options)

C<$options> contains:

=over 4

=item I<header>(optional)

=item I<fullCollectionName>

=item I<numberToReturn> (optional)

Default is 1.

=item I<cursorID>

64-bit cursorID recieved from MongoDB during OP_QUERY.

=back

=item delete($options)

C<$options> contains:

=over 4

=item I<header> (optional)

=item I<fullCollectionName>

=item I<flags> (optional)

Possible flag is C<SingleRemove>.

=item I<selector>

=back

=item kill_cursors($options)

C<$options> contains:

=over 4

=item I<header> (optional)

=item I<cursorIDs>

Arrayref of cursorIDs.

=back

=item msg($options)

C<$options> contains

=over 4

=item I<header> (optional)

=item I<message>

String with message.

MongoDB docs marked this method as deprecated for clients.

=back

=item decode_reply($reply_str)

Decodes binary response C<$reply_str> from MongoDB and returns hashref with a following structure:

=over 4

=item I<header>

=item I<responseFlags>

=item I<cursorID>

=item I<startingFrom>

=item I<numberReturned>

=item I<documents>

=back

=back

=head1 KNOWN BUGS

Works only on 64-bit Perl. I'll fix it soon.

=head1 SEE ALSO

L<http://www.mongodb.org/display/DOCS/Mongo+Wire+Protocol>

L<BSON>, L<MongoDB>, L<AnyMongo>

=head1 AUTHOR

Yury Zavarin <yury.zavarin@gmail.com>

=head1 COPYRIGHT AND LICENSE

This software is copyright (c) 2011 by Yury Zavarin.

This is free software; you can redistribute it and/or modify it under
the same terms as the Perl 5 programming language system itself.

=cut

