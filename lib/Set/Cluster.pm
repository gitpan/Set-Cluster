package Set::Cluster::Result;

use 5.004;

use strict;
use warnings;
use Storable qw(dclone);
use Carp;

sub copy {
	my $self = shift;
	croak "can't copy class $self" unless (ref $self);
	my $copy = Storable::dclone($self);
	return $copy;
}

sub new {
	my $class = shift;
	my $self = {};
	return bless $self, $class;
}


package Set::Cluster;

use strict;
use warnings;

use Carp;

our $VERSION = '0.01';

sub new {
	my $class = shift;
	my $self = { results => {} };
	return bless $self, $class;
}

sub setup {
	my $self = shift;
	my %args = @_;
	$self->{nodes} = $args{nodes} || croak "No nodes specified";
	$self->{items} = $args{items} || croak "No items specified";
}

sub results { shift->{results}; }

sub calculate {
	my $self = shift;
	my $levels = shift || 0;

	$levels = scalar @{$self->{nodes}} - 1 if ($levels >= scalar @{$self->{nodes}});

	# Setup nodes and distribute first time
	my $result = Set::Cluster::Result->new;
	foreach my $n (@{$self->{nodes}}) {
		$result->{$n} = [];
	}
	$self->distribute($result, [keys %{$self->{items}}]);

	$self->process("", $result, $levels);
}

sub process {
	my ($self, $scenario, $result, $levels) = @_;
	$self->{results}->{$scenario} = $result;
	if ($levels > 0) {
		foreach my $failed_node (keys %$result) {
			my $new_state = $result->copy;
			my $scenario = join(",", split(",",$scenario), $failed_node);
			my @items = @{$new_state->{$failed_node}};
			delete $new_state->{$failed_node};
			$self->distribute($new_state, [@items]);
			$self->process($scenario, $new_state, $levels-1);
		}
	}
}

# Logic - sort items by weight. From highest, add item to 
# node with lowest total weight
sub distribute {
	my ($self, $state, $items) = @_;

	my @nodes = keys %$state;
	my @items = sort { $self->{items}->{$a} <=> $self->{items}->{$b} || $a cmp $b } @$items;

	while (my $i = pop @items) {
		my $node = $self->lowest($state);
		push @{$state->{$node}}, $i;
	}
	return $state;
}

# TODO: Should keep total with the node, to save recalculating each time
sub lowest {
	my $self = shift;
	my $state = shift;
	my %totals;
	foreach my $n (keys %$state) {
		$totals{$n} = 0;
		map { $totals{$n} += $self->{items}->{$_} } @{$state->{$n}};
	}
	my @a = sort { $totals{$a} <=> $totals{$b} || $a cmp $b } keys %totals;
	return shift @a;
}

sub items {
	my $self = shift;
	my %args = @_;
	return @{$self->results->{$args{fail}}->{$args{node}}};
}

sub takeover {
	my $self = shift;
	my %args = @_;
	my $node = $args{node};
	my @fail = split (",", $args{fail});
	pop @fail;
	my $fail = join(",", @fail);
	my $prior = $self->results->{$fail}->{$node};
	my $now = $self->results->{$args{fail}}->{$node};
	my %seen;
	map {$seen{$_}++} @$now;
	map {$seen{$_}--} @$prior;
	my @result = ();
	map { push @result, $_ if $seen{$_}==1 } keys %seen;
	return @result;
}


1;
__END__

=head1 NAME

Set::Cluster - Distribute items across nodes in a load balanced way and show takeovers in failure scenarios

=head1 SYNOPSIS

  # Hash of items, with relative weighting
  $h = { 'Oranges' => 17, 'Apples' => 3, 'Lemons' => 10, 'Pears' => 12 };

  $c = Set::Cluster->new;
  $c->setup( nodes => [qw(A B C)], items => $h );
  $c->calculate(2);		# Go 2 levels deep
  $results = $c->results;	# Returns a hash with how items are split across the nodes

  # Convenience functions to parse $c->results
  @takeover = $c->takeover( node => "A", fail => "C" );	# Items taken over due to a failure of C
  @items = $c->items( node => "A", fail => "B" );	# All items for A, when B has failed

=head1 DESCRIPTION

This is an attempt at abstracting clustering. The idea is that you can define a list of items
with relative weightings, and a list of nodes that the items should be spread across.

The plugin then calculates where items will be distributed to balance the weightings.
If you calculate more than 1 level, it will show what happens if there is a failure.
When a node fails, its items are distributed amongst the remaining nodes.

=head1 DISTRIBUTION ALGORITHM

The algorithm used is simple: sort the items by the largest weight, then add to the node
with the lowest total weight so far.

There is a limitation in that nodes that have not failed should not redistribute their
items - this is because of my main reason for creating this module (see HISTORY).

This is not the best algorithm in the world, so if you can implement a better one,
I'd love to hear.

=head1 OBJECT METHODS

=over 4

=item setup( nodes => [list of node names], items => {item1 => 10, item2 => 15, ...} )

Sets the list of nodes and all the items.

=item calculate(levels)

Works out all the possible failure scenarios. A level of 0 means just distribute,
a level of 1 works out a single point of failure scenario, etc.

=item results

Returns the hash ref holding all the results. The structure is:

  '' => Set::Cluster::Result=HASH(0x84e01c4)
      'A' => ARRAY(0x84eb370)
         0  'Oranges'
         1  'Lemons'
      'B' => ARRAY(0x84e0050)
         0  'Strawberries'
         1  'Melons'
         2  'Apples'
      'C' => ARRAY(0x84dffb4)
         0  'Pears'
         1  'Bananas'
         2  'Kiwis'
  'A' => Set::Cluster::Result=HASH(0x84eb328)
      'B' => ARRAY(0x84ebaa8)
         0  'Strawberries'
         1  'Melons'
         2  'Apples'
         3  'Oranges'
      'C' => ARRAY(0x84ebad8)
         0  'Pears'
         1  'Bananas'
         2  'Kiwis'
         3  'Lemons'
  ...

The first section shows a distribution with no failures. The second section is what happens when
node A fails. If there is more than one failure, the key will be "failure1,failure2".

=item takeover( node => $name, fail => $scenario )

Returns an unordered list of items that were added to the specified node at the failure scenario specified.

=item items( node => $name, fail => $scenario )

Returns an unordered list of items that the specified node has at the time of the failure
scenario.

=back

=head1 HISTORY

This plugin was originally designed for generating Nagios (http://nagios.org) 
configurations for clustered slave monitoring servers. The items are hosts, with the
weighting as the number of services monitored on each host.

As this module can predict what 
would happen in failure scenarios, an event handler can be setup to start monitoring 
particular hosts in a takeover situation.

If you have a different use for this module, I would be interested to hear.

=head1 VERSIONING

Only methods listed in this documentation are public.

These modules are experimental and so the interfaces may change up until Set::Cluster
hits version 1.0, but every attempt will be made to make backwards compatible.

=head1 AUTHOR

Ton Voon, E<lt>ton.voon@altinity.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2006 by Altinity Limited

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.8.4 or,
at your option, any later version of Perl 5 you may have available.


=cut
