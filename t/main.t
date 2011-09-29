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

sub update : Test {
    my $self = shift;

    my $msg = $self->{p}->update({
        fullCollectionName => 'test.test',
        flags => { Upsert => 1 },
        selector => { a => 'b' },
        update   => { x => 'y' },
    });

    is_deeply(
        [ unpack("C*", $msg) ],
        [ qw/62 0 0 0 0 0 0 0 0 0 0 0 209 7 0 0 0 0 0 0
             116 101 115 116 46 116 101 115 116 0 1 0 0 0
             14 0 0 0 2 97 0 2 0 0 0 98 0 0 14 0 0 0 2 120
             0 2 0 0 0 121 0 0 /],
        'update encode'
    );
}

sub insert : Test {
    my $self = shift;

    my $msg = $self->{p}->insert({
        header => { requestID => 2, responseTo => 1 },
        flags => { ContinueOnError => 1 },
        fullCollectionName => 'test.test',
        documents => [
            { asdf => 'fdsa' },
            { a => 'b', b => 'c' },
        ],
    });

    is_deeply(
        [ unpack("C*", $msg) ],
        [ qw/73 0 0 0 2 0 0 0 1 0 0 0 210 7 0 0 1 0 0 0
             116 101 115 116 46 116 101 115 116 0 20 0 0
             0 2 97 115 100 102 0 5 0 0 0 102 100 115 97
             0 0 23 0 0 0 2 97 0 2 0 0 0 98 0 2 98 0 2 0
             0 0 99 0 0/ ],
        'insert encode'
    );
}

sub query : Test {
    my $self = shift;

    my $msg = $self->{p}->query({
        fullCollectionName => 'test.test',
        numberToSkip => 0,
        numberToReturn => 2,
        query => { a => 'b' },
        returnFieldSelector => { a => 1, z => 1 },
    });

    is_deeply(
        [ unpack("C*", $msg) ],
        [ qw/71 0 0 0 0 0 0 0 0 0 0 0 212 7 0 0 0 0 0 0
             116 101 115 116 46 116 101 115 116 0 0 0 0
             0 2 0 0 0 14 0 0 0 2 97 0 2 0 0 0 98 0 0 19
             0 0 0 16 97 0 1 0 0 0 16 122 0 1 0 0 0 0/ ],
        'query encode'
    );
}

sub getmore : Test {
    my $self = shift;

    my $msg = $self->{p}->getmore({
        fullCollectionName => 'test.test',
        numberToReturn => 2,
        cursorID => 123,
    });

    is_deeply(
        [ unpack("C*", $msg) ],
        [ qw/42 0 0 0 0 0 0 0 0 0 0 0 213 7 0 0 0 0 0 0
             116 101 115 116 46 116 101 115 116 0 2 0 0 0
             123 0 0 0 0 0 0 0/],
        'getmore encode'
    );
}

sub delete : Test {
    my $self = shift;

    my $msg = $self->{p}->delete({
        flags => { SingleRemove => 1 },
        fullCollectionName => 'test.test',
        selector => { a => 'b' },
    });

    is_deeply(
        [ unpack("C*", $msg) ],
        [ qw/48 0 0 0 0 0 0 0 0 0 0 0 214 7 0 0 0 0 0 0
             116 101 115 116 46 116 101 115 116 0 1 0 0
             0 14 0 0 0 2 97 0 2 0 0 0 98 0 0/ ],
        'delete encode'
    );
}

sub kill_cursors : Test {
    my $self = shift;

    my $msg = $self->{p}->kill_cursors({
        cursorIDs => [123, -1]
    });

    is_deeply(
        [ unpack("C*", $msg) ],
        [ qw/40 0 0 0 0 0 0 0 0 0 0 0 215 7 0 0 0 0 0 0 2 0 0 0
             123 0 0 0 0 0 0 0 255 255 255 255 255 255 255 255/ ],
        'kill_cursors encode'
    );
}

sub msg : Test {
    my $self = shift;

    my $msg = $self->{p}->msg({
        message => "asdf"
    });

    is_deeply(
        [ unpack("C*", $msg) ],
        [ qw/21 0 0 0 0 0 0 0 0 0 0 0 232 3 0 0 97 115 100 102 0/ ],
        'msg encode'
    );
}

sub decode_reply : Test {
    my $self = shift;

    my $reply = [ qw/108 0 0 0 141 55 30 200 0 0 0 0 1 0 0 0 8 0 0 0
                     122 116 227 145 18 158 43 32 0 0 0 0 2 0 0 0 31
                     0 0 0 7 95 105 100 0 78 132 146 88 238 66 63 220
                     88 4 39 189 2 97 0 2 0 0 0 98 0 0 41 0 0 0 7 95
                     105 100 0 78 132 146 93 238 66 63 220 88 4 39 190
                     2 97 0 2 0 0 0 98 0 2 122 0 3 0 0 0 122 122 0 0/ ];
    my $reply_str = pack("C*", @$reply);

    my $expected = {
        'startingFrom' => 0,
        'documents' => [
             {
                 'a' => 'b',
             },
             {
                 'a' => 'b',
                 'z' => 'zz'
             }
        ],
        'numberReturned' => 2,
        'responseFlags' => {
            'QueryFailure' => 0,
            'CursorNotFound' => 0,
            'ShardConfigStale' => 0,
            'AwaitCapable' => 1
        },
        'cursorID' => '2318120235806454906',
        'header' => {
            'responseTo' => 0,
            'messageLength' => 108,
            'opCode' => 'reply',
            'requestID' => -937543795
        }
    };

    my $res_reply = $self->{p}->decode_reply($reply_str);
    foreach my $doc (@{$res_reply->{documents}}) {
        delete $doc->{_id};
    }

    is_deeply($res_reply, $expected, 'decode reply');
}


__PACKAGE__->new->runtests;
