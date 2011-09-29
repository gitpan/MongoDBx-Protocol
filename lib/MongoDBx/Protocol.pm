package MongoDBx::Protocol;
{
  $MongoDBx::Protocol::VERSION = '0.03';
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
    getmore      => 2005,
    delete       => 2006,
    kill_cursors => 2007,
};
my $OP_CODE2STR = {};
foreach my $str (keys %$OP_CODES) {
   $OP_CODE2STR->{ $OP_CODES->{$str} } = $str;
}

my $FLAG2BIT = {
    update => {
        Upsert      => 0,
        MultiUpdate => 1,
    },

    insert => {
        ContinueOnError => 0,
    },

    query => {
        TailableCursor  => 1,
        SlaveOk         => 2,
        OplogReplay     => 3,
        NoCursorTimeout => 4,
        AwaitData       => 5,
        Exhaust         => 6,
        Partial         => 7,
    },

    delete => {
        SingleRemove => 0,
    },

    reply => {
        CursorNotFound   => 0,
        QueryFailure     => 1,
        ShardConfigStale => 2,
        AwaitCapable     => 3,
    },
};

my $BIT2FLAG = {};
foreach my $op_code_str (keys %$FLAG2BIT) {
    foreach my $flag_name (keys %{$FLAG2BIT->{$op_code_str}}) {
        my $bit_value = $FLAG2BIT->{$op_code_str}->{$flag_name};
        $BIT2FLAG->{$op_code_str}->{$bit_value} = $flag_name;
    }
}

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

    my $op_code_str = 'update';

    my $msg = _int32(0) .
              _cstring($p->{fullCollectionName}) .
              _flags($p->{flags}, $op_code_str) .
              _documents($p->{selector}) .
              _documents($p->{update});

    return $self->_with_header($p->{header}, \$msg, $op_code_str);
}


sub insert {
    my $self = shift;
    my $p = validate(@_, {
         %$HEADER_FIELD,
         %$FLAGS_FIELD,
         %$COLLECTION_FIELD,
         documents => { type => ARRAYREF },
    });

    my $op_code_str = 'insert';

    my $msg = _flags($p->{flags}, $op_code_str) .
              _cstring($p->{fullCollectionName}) .
              _documents($p->{documents});

    return $self->_with_header($p->{header}, \$msg, $op_code_str);

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

    my $op_code_str = 'query';
    my $msg = _flags($p->{flags}, $op_code_str) .
              _cstring($p->{fullCollectionName}) .
              _int32($p->{numberToSkip}) .
              _int32($p->{numberToReturn}) .
              _documents($p->{query}) .
              _documents($p->{returnFieldSelector});

    return $self->_with_header($p->{header}, \$msg, $op_code_str);
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

    my $op_code_str = 'delete';

    my $msg = _int32(0) .
              _cstring($p->{fullCollectionName}) .
              _flags($p->{flags}, $op_code_str) .
              _documents($p->{selector});

    return $self->_with_header($p->{header}, \$msg, $op_code_str);
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


sub reply {
    my $self = shift;

    my $p = validate(@_, {
        %$HEADER_FIELD,
        responseFlags => { type => HASHREF, default => {} },
        cursorID      => { type => SCALAR, regex => qr/^\d+$/o },
        startingFrom  => { type => SCALAR, regex => qr/^\d+$/o, default => 0 },
        documents     => { type => ARRAYREF },
    });

    my $op_code_str = 'reply';

    my $msg = _flags($p->{responseFlags}, $op_code_str) .
              _int64($p->{cursorID}) .
              _int32($p->{startingFrom}) .
              _int32( scalar @{$p->{documents}} ) .
              _documents($p->{documents});

    return $self->_with_header($p->{header}, \$msg, $op_code_str);
}


sub decode {
    my $self = shift;
    my ($data, $options) = @_;

    croak "empty data" unless (defined($data));
    croak "too small data" if (length($data) < 4);
    my @a = unpack("C*", substr($data, 0, 4));
    my $len = _decode_int32(substr($data, 0, 4));
    if (length($data) != $len) {
        die "can't parse data, real length of the data != length in the header";
    }

    my $header = _decode_header(substr($data, 0, 4*4, ''));

    my $op_code = $header->{opCode};
    my $decode_method = "_decode_${op_code}";
    my $res = $self->$decode_method(\$data, $options);

    $res->{header} = $header;

    return $res;
}

sub _decode_update {
    my ($self, $data_ref, $options) = @_;
    my $data = $$data_ref;

    my $zero = _decode_int32(substr($data, 0, 4, ''));
    if ($zero != 0) {
        croak("can't parse 'update' message: no zero int32");
    }

    my $coll;
    ($data, $coll) = _decode_cstring($data);

    my $flags = _decode_flags(substr($data, 0, 4, ''), 'update');
    my $docs = _decode_documents(\$data, $options);
    if (scalar(@$docs) != 2) {
        croak "update message should contains only 2 docs";
    }

    my $res = {
        fullCollectionName => $coll,
        flags              => $flags,
        selector           => $docs->[0],
        update             => $docs->[1],
    };

    return $res;
}

sub _decode_insert {
    my ($self, $data_ref, $options) = @_;
    my $data = $$data_ref;

    my $flags = _decode_flags(substr($data, 0, 4, ''), 'insert');
    my $coll;
    ($data, $coll) = _decode_cstring($data);
    my $docs = _decode_documents(\$data, $options);

    my $res = {
        flags => $flags,
        fullCollectionName => $coll,
        documents => $docs,
    };

    return $res;
}

sub _decode_query {
    my ($self, $data_ref, $options) = @_;
    my $data = $$data_ref;

    my $flags = _decode_flags(substr($data, 0, 4, ''), 'query');

    my $coll;
    ($data, $coll) = _decode_cstring($data);

    my $numberToSkip = _decode_int32(substr($data, 0, 4, ''));
    my $numberToReturn = _decode_int32(substr($data, 0, 4, ''));

    my $docs = _decode_documents(\$data, $options);
    if (scalar(@$docs) !~ /^1|2$/) {
        croak "query message should contains only 1 or 2 docs";
    }

    my $res = {
        flags              => $flags,
        fullCollectionName => $coll,
        numberToSkip       => $numberToSkip,
        numberToReturn     => $numberToReturn,
        query              => $docs->[0],
    };
    if ($docs->[1]) {
        $res->{returnFieldSelector} = $docs->[1];
    };

    return $res;
}

sub _decode_getmore {
    my ($self, $data_ref, $options) = @_;
    my $data = $$data_ref;

    my $zero = _decode_int32(substr($data, 0, 4, ''));
    if ($zero != 0) {
        croak("can't parse 'getmore' message: no zero int32");
    }

    my $coll;
    ($data, $coll) = _decode_cstring($data);

    my $res = {
        fullCollectionName => $coll,
        numberToReturn     => _decode_int32(substr($data, 0, 4, '')),
        cursorID           => _decode_int64(substr($data, 0, 8, '')),
    };

    return $res;
}

sub _decode_delete {
    my ($self, $data_ref, $options) = @_;
    my $data = $$data_ref;

    my $zero = _decode_int32(substr($data, 0, 4, ''));
    if ($zero != 0) {
        croak("can't parse 'delete' message: no zero int32");
    }

    my $coll;
    ($data, $coll) = _decode_cstring($data);

    my $flags = _decode_flags(substr($data, 0, 4, ''), 'delete');

    my $docs = _decode_documents(\$data, $options);
    if (scalar(@$docs) != 1) {
        croak "delete message should contains only 1 doc";
    }

    my $res = {
        fullCollectionName => $coll,
        flags              => $flags,
        selector           => $docs->[0],
    };

    return $res;
}

sub _decode_kill_cursors {
    my ($self, $data_ref, $options) = @_;
    my $data = $$data_ref;

    my $zero = _decode_int32(substr($data, 0, 4, ''));
    if ($zero != 0) {
        croak("can't parse 'kill_cursors' message: no zero int32");
    }

    my $n_cursors = _decode_int32(substr($data, 0, 4, ''));
    if (length($data) != $n_cursors * 8) {
        croak("real number of cursors != number of cursors in the message");
    }

    my @cursors;
    while ($data) {
        push @cursors, _decode_int64(substr($data, 0, 8, ''));
    }

    my $res = {
        numberOfCursorIDs => $n_cursors,
        cursorIDs         => \@cursors,
    };

    return $res;
}

sub _decode_msg {
    my ($self, $data_ref, $options) = @_;
    my $data = $$data_ref;

    my $msg;
    ($data, $msg) = _decode_cstring($data);

    if (length($data) > 0) {
        croak("can't parse 'msg' message: there are additional bytes at the end");
    }

    return { message => $msg };
}

sub _decode_reply {
    my ($self, $data_ref, $options) = @_;
    my $data = $$data_ref;

    my $res = {
        responseFlags  => _decode_flags(substr($data, 0, 4, ''), 'reply'),
        cursorID       => _decode_int64(substr($data, 0, 8, '')),
        startingFrom   => _decode_int32(substr($data, 0, 4, '')),
        numberReturned => _decode_int32(substr($data, 0, 4, '')),
        documents      => _decode_documents(\$data, $options),
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
    my ($flags, $op_code_str) = @_;

    my $v = Bit::Vector->new(32);
    $v->from_Dec(_decode_int32($flags));

    my $str_flags = {};
    my $all_op_code_bits = $BIT2FLAG->{$op_code_str};
    foreach my $reply_flag_bit (keys %$all_op_code_bits) {
        if ($v->bit_test($reply_flag_bit)) {
            my $reply_flag = $all_op_code_bits->{$reply_flag_bit};
            $str_flags->{$reply_flag} = 1;
        }
    }

    return $str_flags;
}

sub _decode_documents {
    my ($data_ref, $options) = @_;
    my %bson_options;
    if ($options && $options->{ixhash}) {
        %bson_options = (ixhash => 1);
    }

    my $data = $$data_ref;

    my @docs = ();
    while (length($data)) {
        my $l = _decode_int32(substr($data, 0, 4));
        if (length($data) < $l) {
            croak "Incorrect length of bson document";
        }
        my $doc_str = substr($data, 0, $l, '');
        push @docs, BSON::decode($doc_str, %bson_options);
    }

    return \@docs;
}

sub _decode_cstring {
    my $data = shift;
    my $idx = index($data, "\x00");
    if ($idx < 0) {
        croak("Can't find string terminator");
    }

    my $string = substr($data, 0, $idx);
    substr($data, 0, $idx + 1, '');

    return ($data, $string);
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
    my ($flags, $op_code_str) = @_;
    my $v = Bit::Vector->new(32);

    my $all_op_code_flags = $FLAG2BIT->{$op_code_str};
    while (my ($flag_name, $flag_value) = each %$flags) {
        if (defined(my $flag_bit = $all_op_code_flags->{$flag_name}) && $flag_value) {
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

version 0.03

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

C<$options> contains:

=over 4

=item I<header> (optional)

=item I<message>

String with message.

MongoDB docs marked this method as deprecated for clients.

=back

=item reply($options)

C<$options> contains:

=over 4

=item I<header> (optional);

=item I<responseFlags> (optional)

Possible flags are: C<CursorNotFound>, C<QueryFailure>, C<ShardConfigStale>, C<AwaitCapable>.

=item I<cursorID>

=item I<startingFrom> (optional)

Default is 0.

=item I<documents>

Arrayref of documents.

=back

=item decode($data, $options)

Opposite to encode methods. Takes binary string C<$data> and returns hashref with parsed data in the same format as it uses to encode.

C<$options> is a hashref. Now it can contain C<ixhash> flag, it says that documents should be returned as L<Tie::IxHash> for preserving keys order in the hash.

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

