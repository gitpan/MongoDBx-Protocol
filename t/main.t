#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use parent qw(Test::Class);

use MongoDBx::Protocol;

sub setup : Test(setup) {
    my $self = shift;
    $self->{p} = MongoDBx::Protocol->new;
}

sub teardown : Test(teardown) {
    my $self = shift;
    delete $self->{p};
}

sub isa_test : Test {
    my $self = shift;
    isa_ok($self->{p}, 'MongoDBx::Protocol');
}

sub update : Test(2) {
    my $self = shift;

    my $init_hash = {
        fullCollectionName => 'test.test',
        flags => { Upsert => 1 },
        selector => { a => 'b' },
        update   => { x => 'y' },
    };

    my $msg = $self->{p}->update($init_hash);

    is_deeply(
        [ unpack("C*", $msg) ],
        [ qw/62 0 0 0 0 0 0 0 0 0 0 0 209 7 0 0 0 0 0 0
             116 101 115 116 46 116 101 115 116 0 1 0 0 0
             14 0 0 0 2 97 0 2 0 0 0 98 0 0 14 0 0 0 2 120
             0 2 0 0 0 121 0 0 /],
        'update encode'
    );

    $init_hash->{header} = {
        'responseTo' => 0,
        'messageLength' => 62,
        'opCode' => 'update',
        'requestID' => 0
    };

    is_deeply($self->{p}->decode($msg), $init_hash, 'decode update');
}

sub insert : Test(2) {
    my $self = shift;

    my $init_hash = {
        header => { requestID => 2, responseTo => 1 },
        flags => { ContinueOnError => 1 },
        fullCollectionName => 'test.test',
        documents => [
            { asdf => 'fdsa' },
            { a => 'b', b => 'c' },
        ],
    };
    my $msg = $self->{p}->insert($init_hash);

    is_deeply(
        [ unpack("C*", $msg) ],
        [ qw/73 0 0 0 2 0 0 0 1 0 0 0 210 7 0 0 1 0 0 0
             116 101 115 116 46 116 101 115 116 0 20 0 0
             0 2 97 115 100 102 0 5 0 0 0 102 100 115 97
             0 0 23 0 0 0 2 97 0 2 0 0 0 98 0 2 98 0 2 0
             0 0 99 0 0/ ],
        'insert encode'
    );


    $init_hash->{header}->{opCode} = 'insert';
    $init_hash->{header}->{messageLength} = 73;

    is_deeply($self->{p}->decode($msg), $init_hash, 'decode insert');
}

sub query : Test(2) {
    my $self = shift;

    my $init_hash = {
        fullCollectionName => 'test.test',
        numberToSkip => 0,
        numberToReturn => 2,
        query => { a => 'b' },
        returnFieldSelector => { a => 1, z => 1 },
    };
    my $msg = $self->{p}->query($init_hash);

    is_deeply(
        [ unpack("C*", $msg) ],
        [ qw/71 0 0 0 0 0 0 0 0 0 0 0 212 7 0 0 0 0 0 0
             116 101 115 116 46 116 101 115 116 0 0 0 0
             0 2 0 0 0 14 0 0 0 2 97 0 2 0 0 0 98 0 0 19
             0 0 0 16 97 0 1 0 0 0 16 122 0 1 0 0 0 0/ ],
        'query encode'
    );

    $init_hash->{flags} = {};
    $init_hash->{header} = {
        responseTo    => 0,
        messageLength => 71,
        opCode        => 'query',
        requestID     => 0
    };

    is_deeply($self->{p}->decode($msg), $init_hash, 'decode query');
}

sub getmore : Test(2) {
    my $self = shift;

    my $init_hash = {
        fullCollectionName => 'test.test',
        numberToReturn     => 2,
        cursorID           => 123,
    };
    my $msg = $self->{p}->getmore($init_hash);

    is_deeply(
        [ unpack("C*", $msg) ],
        [ qw/42 0 0 0 0 0 0 0 0 0 0 0 213 7 0 0 0 0 0 0
             116 101 115 116 46 116 101 115 116 0 2 0 0 0
             123 0 0 0 0 0 0 0/],
        'getmore encode'
    );


    $init_hash->{header} = {
        'responseTo' => 0,
        'messageLength' => 42,
        'opCode' => 'getmore',
        'requestID' => 0
    };

    is_deeply($self->{p}->decode($msg), $init_hash, 'decode getmore');
}

sub delete : Test(2) {
    my $self = shift;

    my $init_hash = {
        flags => { SingleRemove => 1 },
        fullCollectionName => 'test.test',
        selector => { a => 'b' },
    };
    my $msg = $self->{p}->delete($init_hash);

    is_deeply(
        [ unpack("C*", $msg) ],
        [ qw/48 0 0 0 0 0 0 0 0 0 0 0 214 7 0 0 0 0 0 0
             116 101 115 116 46 116 101 115 116 0 1 0 0
             0 14 0 0 0 2 97 0 2 0 0 0 98 0 0/ ],
        'delete encode'
    );


    $init_hash->{header} = {
        responseTo    => 0,
        messageLength => 48,
        opCode        => 'delete',
        requestID     => 0,
    };

    is_deeply($self->{p}->decode($msg), $init_hash, 'decode delete');
}

sub kill_cursors : Test(2) {
    my $self = shift;

    my $init_hash = {
        cursorIDs => [123, -1]
    };
    my $msg = $self->{p}->kill_cursors($init_hash);

    is_deeply(
        [ unpack("C*", $msg) ],
        [ qw/40 0 0 0 0 0 0 0 0 0 0 0 215 7 0 0 0 0 0 0 2 0 0 0
             123 0 0 0 0 0 0 0 255 255 255 255 255 255 255 255/ ],
        'kill_cursors encode'
    );

    $init_hash->{numberOfCursorIDs} = 2;
    $init_hash->{header} = {
        responseTo    => 0,
        messageLength => 40,
        opCode        => 'kill_cursors',
        requestID     => 0,
    };

    is_deeply($self->{p}->decode($msg), $init_hash, 'decode kill_cursors');
}

sub msg : Test(2) {
    my $self = shift;

    my $init_hash = {
        message => "asdf"
    };
    my $msg = $self->{p}->msg($init_hash);

    is_deeply(
        [ unpack("C*", $msg) ],
        [ qw/21 0 0 0 0 0 0 0 0 0 0 0 232 3 0 0 97 115 100 102 0/ ],
        'msg encode'
    );


    $init_hash->{header} = {
        responseTo    => 0,
        messageLength => 21,
        opCode        => 'msg',
        requestID     => 0,
    };

    is_deeply($self->{p}->decode($msg), $init_hash, 'decode msg');
}

sub reply : Test(2) {
    my $self = shift;

    my $init_hash = {
        responseFlags => { AwaitCapable => 1 },
        cursorID      => 123,
        startingFrom  => 1,
        documents     => [ { a => 'b'}, { c => 'd' } ],
    };

    my $msg = $self->{p}->reply($init_hash);

    is_deeply(
        [ unpack("C*", $msg) ],
        [ qw/64 0 0 0 0 0 0 0 0 0 0 0 1 0 0 0 8 0 0 0
             123 0 0 0 0 0 0 0 1 0 0 0 2 0 0 0 14 0 0
             0 2 97 0 2 0 0 0 98 0 0 14 0 0 0 2 99 0 2
             0 0 0 100 0 0/],
         'encode reply'
    );


    $init_hash->{numberReturned} = 2;
    $init_hash->{header} = {
        responseTo    => 0,
        messageLength => 64,
        opCode        => 'reply',
        requestID     => 0,
    };

    is_deeply($self->{p}->decode($msg), $init_hash, 'decode reply');
}

__PACKAGE__->new->runtests;
