#   See pod docs below __END__

package R2M;

use DBI;
use MongoDB;

use Data::Dumper;  # for debugging

sub new {
    my($class) = shift;  
    
    # A hash is used as the "body" of the object:
    my $this = {};

    $this->{localTZ} = DateTime::TimeZone->new( name => 'local' );

    bless $this, $class;

    return $this;
}

sub groom {
    my($this) = shift;      
    my($stype, $rawval) = @_;

#    print "stype: $stype, rawval $rawval\n";
    # bail out fast...
    if(!defined $rawval || $rawval eq "") {
	return undef;
    }

    my $newval = $rawval;

    if($stype == DBI::SQL_TYPE_DATE) {
	#   YYYY-MM-DD
	my ($year, $z, $month, $z, $day) = unpack("A4A1A2A1A2", $rawval);

	$newval = DateTime->new(
	    year      => $year,
	    month     => $month,
	    day       => $day,
	    time_zone => $this->{localTZ}
	    );
#      hour      => 1,
#      minute    => 30,
#      second    => 0,
#      time_zone => 'America/Chicago',


    } elsif($stype == DBI::SQL_CHAR
	) {

	#  Kill trailing whitespace; not important in mongoDB
	$newval =~ s/\s+$//;

    } elsif($stype == DBI::SQL_TINYINT
	    || $stype == DBI::SQL_SMALLINT
	    || $stype == DBI::SQL_INTEGER
	) {
	#
	#  &#$*&^$! 
	#  The mongoDB perl driver docs are clear:
	#  64-bit Platforms
	#    Numbers without a decimal point will be saved and returned as
	#    64-bit integers. 
	#    Note that there is no way to save a 32-bit int on a 64-bit machine.
	#
	#  Thus, it's NumberLong() everywhere.   Rats.
	#
	$newval = $rawval + 0; #?!?

    } elsif($stype == DBI::SQL_DECIMAL
	    || $stype == DBI::SQL_NUMERIC

	    || $stype == DBI::SQL_BIGINT

	    || $stype == DBI::SQL_FLOAT
	    || $stype == DBI::SQL_REAL
	    || $stype == DBI::SQL_DOUBLE
	) {
	$newval = $rawval + 0; #?!?
    }

    return $newval;
}



#  processCcollection($isTop, $coll, $info, $where, $depth)
sub processCollection {
    my($this) = shift;      

    my($isTop, $coll, $info, $where, $depth) = @_;


    my $flds = $info->{flds};
    #print "process info\n";
    #print Dumper($flds);


    my @lvals = ();
    my $links = {};

    my @ffuncs = ();


    my @accums = ();

    #
    #  Step 1
    #  Go through the set of target fields (i.e. things being set into the doc
    #  for insert to mongoDB.  We do two things here:
    #  1.  Extract the column names that will be necessary to provide raw
    #      material to the functions.  This is The Select List. 
    #
    #  2.  Convert all non-join assignments to the full internal form, e.g.
    #          target1 => "SOURCE1"
    #      becomes
    #          target1 => [ "fld", { src = ["SOURCE1"] } ]
    #
    #  Step 2
    #  For each 
    #  
    #  

    #  This is a hashref because we only need to grab one of a particular
    #  column, e.g. select A, A, A from table means we only need to get
    #  A once.
    my $selectFlds = {}; 

    for my $k (keys %{$flds}) {
	if(ref($flds->{$k}) eq "ARRAY") {

	    # [ cmd, args ]
	    my $cmd = $flds->{$k}->[0];

	    if($cmd eq "join") {
		my $args = $flds->{$k}->[1];

		# Grab the raw materials for the where clause later:
		# link->[0] is the parent col
		my $link = $args->{link}->[0];
#		print "join to $link\n";
		$selectFlds->{$link}++;

		# Capture this as a "one item" multijoin
		# from:  [ "join", args, info ]
		# to:    [ [ args, info ] ]
		# In short, drop the cmd by taking only [1] and [2] and
		# wrap the shorter array in an array...
		$links->{$k} = [ [ $flds->{$k}->[1] , $flds->{$k}->[2] ]];


	    } elsif($cmd eq "multijoin") {
		my $mj = $flds->{$k}->[1];		

		for(my $n = 0; $n < $#$mj + 1; $n++) {

		    my $jn = $mj->[$n];

		    my $args = $jn->[0];
		    
		    # Grab the raw materials for the where clause later:
		    # link->[0] is the parent col
		    my $link = $args->{link}->[0];

		    $selectFlds->{$link}++;
		    
		    # Capture this a "one item" multijoin!
#		print Dumper($flds->{$k});
		}
		#$links->{$k} = $flds->{$k};  # the FULL arrayref!
		$links->{$k} = $mj;


	    } elsif($cmd eq "fld") {
		my $args = $flds->{$k}->[1];

		if(defined $args->{val}) {
		    push(@lvals, { n => $k, v => $args->{val}});

		} else {
		    my $srcs = [ "*" ]; # default is all items

		    if(defined $args->{colsrc}) {
			$srcs = $args->{colsrc};  # .. unless supplied
		    }
		    
		    if(ref($srcs) eq "ARRAY") {
			# ?
		    } else {
			my $one = $srcs;
			$srcs = [ $one ]; # change to arrayref...
		    }

		    my @yy = map { $_ =~ /^\*$/ } @{$srcs};
		    if($#yy > -1) { # one or more '*'
			my $v = $this->{spec}->{tables}->{$info->{tblsrc}}->{coldefs};
			$srcs = [ keys %{$v} ];
		    }

		    map { $selectFlds->{$_}++; } @{$srcs};

		    # OK for args->{f} to be undef...
		    push(@ffuncs, {
			n => $k,
			srcs => $srcs,
			f => $args->{f}});
		}
	    }

	} else {
	    # print "$k is string $flds->{$k}\n";
	    $selectFlds->{$flds->{$k}}++;
	    push(@ffuncs, { n => $k, srcs => [ $flds->{$k} ] });
        }
    }


    my $revmap = {};
    my @select = keys %{$selectFlds}; # LOCKED!  Done!
    for(my $n = 0; $n < $#select + 1; $n++) {
	$revmap->{$select[$n]} = $n;
    }
    #  At this point, @select is The Select List.
    #  $revmap allows us to find the position of a field in the 
    #  list given the name.

    my @stypes = ();
    for my $k (@select) {
	my $nk = uc($k);
	my $v = $this->{spec}->{tables}->{$info->{tblsrc}}->{coldefs}->{$nk}->{"DATA_TYPE"};
#	print "$k is type $v\n";
	push(@stypes, $v);
    }

    # At this point, @stypes is position-2-position aligned with @select.
    # Each field in @select will have a corresponding type in @stypes


    my $realtbl = $this->{spec}->{tables}->{$info->{tblsrc}}->{table};
    my $sql = "select " . join(",",@select) . " from " . $realtbl;
    if(defined $where) {
	$sql .= " where $where";
    }
#    print $sql, "\n";

    my $dbname = $this->{spec}->{tables}->{$info->{tblsrc}}->{db};
    my $db = $this->{dbs}->{$dbname};
    my $dbh = $db->{dbh};
    my $dbalias = $db->{alias};

#    print "apply select to database $dbname ($dbalias)\n";

    my $sth = $dbh->prepare($sql);
    $sth->execute();

    my $row;
    my $doccnt = 0;

    my $ctx = {
	_r2m => {
	    startTime => $this->{startTime},
	    global => $this->{global},
	    table => $realtbl
	}
    };    

    while(defined($row = $sth->fetchrow_arrayref())) {
	my $doc = undef;

	$doccnt++;

	#print "fetch row from $realtbl where $where\n";

	#  select f1, f2, f3, r1, r2
	
	#  First, do the literals:
	for my $m (@lvals) {
	    $doc->{$m->{n}} = $m->{v};
	}

	# Next grab the funcs!
	for my $m (@ffuncs) {
	    my $k = $m->{n};

	    my $mdb_val = undef;

	    if(defined $m->{f}) {
		$ctx->{_r2m}->{n} = $doccnt;
		$ctx->{_r2m}->{depth} = $depth;

		if($#{$m->{srcs}} == 0) {
		    # single scalar
		    my $kk = $m->{srcs}->[0]; # first and only...
		    my $onev = $row->[$revmap->{$kk}];

		    $mdb_val = $m->{f}($ctx, $onev);
		} else {
		    my $rawvals = {};
		    map { $rawvals->{$_} = $row->[$revmap->{$_}] } @{$m->{srcs}};
		    $mdb_val = $m->{f}($ctx, $rawvals);
		}

	    } else {
		my $idx = $revmap->{$m->{srcs}->[0]};
		my $xval = $row->[$idx];
		my $stype = $stypes[$idx];
		$mdb_val = $this->groom($stype, $xval);
	    }

	    if(defined $mdb_val) {
		$doc->{$k} = $mdb_val;
	    }
	}


	# Next, the links
	for my $k (keys %{$links}) {
	    my $linkarr = $links->{$k};

#	    print "linkarr: ", Dumper($linkarr);
#	    print "linkarr len: ", $#$linkarr, "\n";

	    my @subaccums = ();

	    for(my $n = 0; $n < $#$linkarr + 1; $n++) {
		my @partials = ();

		my $link = $linkarr->[$n];

		my $targ = $link->[0]->{link}->[1];
		
		my $idx = $revmap->{$link->[0]->{link}->[0]};
		my $val = $row->[$idx];
		
		#  TBD TBD TBD
		#  This neeeds examination.
		my $subw = "$targ = \'$val\'";  # TBD!  Fix THIS!

		@partials = $this->processCollection(0, $coll, $link->[1], $subw, $depth + 1);	    
		if($#partials != -1) {
#		    print "** collected " , $#partials + 1 , " from $subw\n";
		    push(@subaccums, @partials);
#		    print "** new subaccums is " , $#subaccums + 1 , "\n";
		} else {
#		    print "** collected NONE from $subw\n";
#		    print "** accums is " , $#subaccums + 1, "\n";
		}
	    }

	    # Only set if >0
	    if($#subaccums != -1) {
		#my $lt = $link->[1]->{type};
		#if($lt eq "1:n") {
		#    $doc->{$k} = \@allAccums;
		#} elsif($lt eq "1:1") {
		#    $doc->{$k} = $allAccums[0]; # first and only doc!
		#}
#		print "setting " , $#subaccums + 1 , " items into $k\n";
		$doc->{$k} = \@subaccums;
	    }
	}

	#  It is possible that given the params for column->field xfer, NO
	#  non-null items were found and/or calcd by a sub.  In other words,
	#  a row in the RDBMS was visited, but NOTHING was pulled out.  No
	#  problem -- we simply skip this row.
	#  Note that if you REALLY need things like keys even if the rest of
	#  the info is null then make sure your input sources actually always
	#  have keys!  There is no magic here.
	#
	if(defined $doc) {
	    if($isTop == 1) {
		#print "insert into mongo\n";
		my $id = $coll->insert($doc);
		#print "inserted $id\n";
		@accums = ();
	    } else {
		push(@accums, $doc);
	    }
	} else {
#	    print "NO flds found for xfer; skip\n";
	}
    }

    #print "end of proc accums: " ,  $#accums + 1 ,  "\n";
    return @accums;
}


sub getContext {
    my($this) = shift;      
    return $this->{global};
}

sub run {
    my($this) = shift;      
    my($spec) = shift;

    # Not sure about this:
    $this->{spec} = $spec; # ref the whole spec....

    $this->{startTime} = time;  # Ha!  Seconds since epoch (NOT millis)
    $this->{global} = {}; 

    #  mongo!
    my $h  = $spec->{mongodb}->{host};
    my $p  = $spec->{mongodb}->{port};
    my $db = $spec->{mongodb}->{db};

    $this->{mc} = MongoDB::MongoClient->new(host => $h, port => $p);
    $this->{mdb} = $this->{mc}->get_database($db);

    # rdbs!
    my $rdbs = $spec->{rdbs};
    for $k (keys %{$rdbs}) {
	print $k, "\n";
	my $item = $rdbs->{$k};

	$this->{dbs}->{$k}->{dbh} = DBI->connect(
	    $item->{conn},
	    $item->{user},
	    $item->{pw},
	    $item->{args});
	$this->{dbs}->{$k}->{alias} = $item->{alias};

	print "$item->{alias} connected as $k\n";
    }

    # tables !
    my $tbls = $spec->{tables};
    for $k (keys %{$tbls}) {

	my $item = $tbls->{$k};

	if(!defined $item->{table}) {
	    $item->{table} = $k; # take name of key
	}

	my $db  = $item->{db};
	my $dbh = $this->{dbs}->{$item->{db}}->{dbh};

	my $sth = $dbh->column_info( undef, undef, $item->{table}, undef );

	# The internal metadata tends to be case wobbly.  It is unclear
	# if the name you use to create a column (like camelCase) is 
	# saved as camelcase or CAMELCASE or camelCase.
	# So....
	# Walk the map, for each key ucase it, and point that key to 
	# the content of the original.  In a sense, two keys to the same
	# data...
	$item->{coldefs} = $sth->fetchall_hashref("COLUMN_NAME");
	for my $k (keys %{$item->{coldefs}}) {
	    my $v = $item->{coldefs}->{$k};
	    my $nk = uc($k);
	    $item->{coldefs}->{$nk} = $v;
	    #delete $item->{coldefs}->{$k};
	}
	#print "$item->{table} schema captured\n";
	#print Dumper( $item->{coldefs} );
    }

    # load 'em up!
    my $colls = $spec->{collections};
    for $k (keys %{$colls}) {
	my $coll = $this->{mdb}->get_collection( $k );
	$this->processCollection(1, $coll, $colls->{$k}, undef, 0);
	# mongodb insert[$k]($doc);
    }

    #  shut 'em down...
    my $rdbs = $spec->{rdbs};
    for $k (keys %{$rdbs}) {

	my $item = $rdbs->{$k};

	my $dbh = $this->{dbs}->{$k}->{dbh};

	$dbh->disconnect();
    }

}


1;

__END__

=pod

=head1 NAME

R2M - DBD/DBI based relational to MongoDB bulk transfer framework

=head1 SYNOPSIS

 (look at r2m.n.pl for guidance)


=head1 AUTHORS and MAINTAINERS

Buzz Moschetti F<E<lt>buzz[at]mongodb.orgE<gt>> (sort of)

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2014 Buzz Moschetti

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself. 

=cut
