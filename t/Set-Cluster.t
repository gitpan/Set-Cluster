
use strict;
use Test::More tests => 12;
BEGIN { use_ok('Set::Cluster') };

my $c = Set::Cluster->new;
isa_ok( $c, "Set::Cluster");

my $items = { 
	'Oranges' => 17, 
	'Apples' => 3, 
	'Lemons' => 10, 
	'Pears' => 12, 
	Strawberries => 15,
	Melons => 10,
	Kiwis => 5,
	Bananas => 12,
	};
my @nodes = qw(A B C);

$c->setup( nodes => [@nodes], items => $items );

my $result = Set::Cluster::Result->new;
isa_ok( $result, "Set::Cluster::Result");
foreach my $n (@nodes) {
	$result->{$n} = [];
}
cmp_ok( $c->lowest($result), 'eq', "A", "Got lowest (ordered by name)");
$result->{A} = ['Lemons'];
$result->{B} = ['Apples'];
cmp_ok( $c->lowest($result), 'eq', "C", "Got lowest (by weight)");

$c->calculate(0);
cmp_ok( scalar keys %{$c->results}, '==', 1, "Got 1 scenario, for just a distribution");

$c->calculate(1);
cmp_ok( scalar keys %{$c->results}, '==', 4, "Got 4 scenarios for a single failure");

my @a = $c->items( node => "A", fail => "B" );
cmp_ok( join(',', sort @a), 'eq', "Lemons,Oranges,Strawberries", "items list okay");

@a = $c->takeover( node => "A", fail => "C" );
cmp_ok( join(",", sort @a), 'eq', "Kiwis,Pears", "takeover list okay");

$c->calculate(2);
cmp_ok( scalar keys %{$c->results}, '==', 10, "Got 10 scenarios for a dual failure");

$c->calculate(3);
cmp_ok( scalar keys %{$c->results}, '==', 10, "Triple failure not possible!");

$c->calculate(30);
cmp_ok( scalar keys %{$c->results}, '==', 10, "30 levels certinaly doesn't make sense!");
