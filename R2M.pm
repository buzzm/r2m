#   See pod docs below __END__


package R2M;

use DBI;
use DateTime;
use DateTime::Format::DBI;

use Data::Dumper;  # for debugging

sub new {
    my($class) = shift;  
    
    # A hash is used as the "body" of the object:
    my $this = {};

    $this->{UTZ} = DateTime::TimeZone->new( name => '+0000' );

    $this->{global} = {}; 

    bless $this, $class;

    return $this;
}


sub groom {
    my($this) = shift;      
    my($stype, $rawval, $dbdateparser) = @_;

#    print "stype: $stype, rawval $rawval\n";
    # bail out fast...
    if(!defined $rawval || $rawval eq "") {
	return undef;
    }

    my $newval = $rawval;


    if($stype == DBI::SQL_TYPE_DATE
       || $stype == DBI::SQL_TIMESTAMP) {

	$newval = $dbdateparser->parse_datetime($rawval);

	# Add in the UTZ timezone!
	$newval->set_time_zone($this->{UTZ});

    } elsif($stype == DBI::SQL_TYPE_TIMESTAMP_WITH_TIMEZONE) {
	# Does not creating floating timezone issues so OK for MongoDB
	$newval = $dbdateparser->parse_datetime($rawval);

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

	#  Very very weird interaction if LATER ON you take a scalar ref for
	#  BLOB data and rawval has had the +0 trick applied to it.
	#  This works:
	#    	$newval = $rawval;
	#    	$newval += 0;
	#  This does NOT work (it leads to the "type (ref) unhandled" error:
	#    	$newval = $rawval + 0;
	#  
	$newval = $rawval;
	$newval += 0;

    } elsif($stype == DBI::SQL_DECIMAL
	    || $stype == DBI::SQL_NUMERIC

	    || $stype == DBI::SQL_BIGINT

	    || $stype == DBI::SQL_FLOAT
	    || $stype == DBI::SQL_REAL
	    || $stype == DBI::SQL_DOUBLE
	) {
	$newval = $rawval;
	$newval += 0.0; # see above...


    } elsif($stype == DBI::SQL_LONGVARBINARY
	    || $stype == DBI::SQL_VARBINARY
	    || $stype == DBI::SQL_BINARY
	) {

	$newval = \$rawval;
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
    my $dbh          = $db->{dbh};
    my $dbdateparser = $db->{dbdateparser};
    my $dbalias      = $db->{alias};

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
		$mdb_val = $this->groom($stype, $xval, $dbdateparser);
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


    #  emitter!
    $this->{emitter} = $spec->{emitter};


    # rdbs!
    my $rdbs = $spec->{rdbs};
    for $k (keys %{$rdbs}) {
	my $item = $rdbs->{$k};

	my $dbh = DBI->connect(
	    $item->{conn},
	    $item->{user},
	    $item->{pw},
	    $item->{args});

	$this->{dbs}->{$k}->{dbh} = $dbh;

	if(defined $item->{dateparser}) {
	    $this->{dbs}->{$k}->{dbdateparser} = $item->{dateparser};
	} else {
	    $this->{dbs}->{$k}->{dbdateparser} = DateTime::Format::DBI->new($dbh);
	}

	$this->{dbs}->{$k}->{alias} = $item->{alias};

#	print "$item->{alias} connected as $k\n";
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

	my $coll = $this->{emitter}->getColl($k);

	$this->processCollection(1, $coll, $colls->{$k}, undef, 0);
    }

    #  Shut 'em down...
    #  emitter
    $this->{emitter}->close();

    my $rdbs = $spec->{rdbs};
    for $k (keys %{$rdbs}) {
	my $item = $rdbs->{$k};
	my $dbh = $this->{dbs}->{$k}->{dbh};
	$dbh->disconnect();
    }

}






package R2M::MongoDB;
use Carp;

# new R2M::MongoDB({ options });
sub new {
    require MongoDB;

    my($class) = shift;  
    
    # A hash is used as the "body" of the object:
    my $this = {};
    bless $this, $class;

    my $args = shift;

    if(!defined $args->{db}) {
	croak "db => targetDB must be defined in R2M::MongoDB->new options";
    }

    if(defined $args->{client}) {
	$this->{mc} = $args->{client};
    } else {
	# Defaults:
	my $a2 = {
	    host => "localhost",
	    port => 27017
	};
	for $fld (qw/host port username password/) {
	    if(defined $args->{$fld}) {
		$a2->{$fld} = $args->{$fld};
	    }
	}
	$this->{mc} = MongoDB::MongoClient->new($a2);
    }

    $this->{mdb} = $this->{mc}->get_database($args->{db});

    return $this;
}

sub close {
    my($this) = shift;      

    #  Per the MongoDB::MongoClient docs:
    #    There is no way to explicitly disconnect from the database. 
    #    However, the connection will automatically be closed and cleaned up
    #    when no references to the MongoDB::MongoClient object exist, which
    #    occurs when $client goes out of scope
    #    (or earlier if you undefine it with undef

    # So... undef it!  
    undef $this->{mc};
}

# getColl(collName)
sub getColl {
    my($this) = shift;      
    my($collName) = @_;

    # This will return an object that already has the method
    # insert(hashref)
    return $this->{mdb}->get_collection( $collName );    
}





package R2M::JSON::Collection;
use MIME::Base64 qw( encode_base64 );

sub new {
    require IO::Handle;

    my($class) = shift;  
    
    # A hash is used as the "body" of the object:
    my $this = {};
    bless $this, $class;

    my($basedir, $collname) = @_;

    my $fname = $basedir . "/". $collname . ".json";

    $this->{fname} = $fname;

    open my $fh, ">", $fname or die "$fname: $!";

    $this->{fh} = $fh;
    $this->{cnt} = 0;

    return $this;
}


sub insert {
    my($this) = shift;      
    my($doc) = @_;

    #$this->{fh}->print("foo!");
    walkMap($this->{fh}, $doc, 0);

    $this->{fh}->print("\n");

    $this->{cnt}++;
}



sub emit {
    my($fh, $ov, $fld, $depth) = @_;

    my $tx = ref($ov);

    if(defined $fld) {
	$fh->print("\"$fld\":");
    }

    if($tx eq "") {
	if($ov =~ /^[+-]?\d+(\.\d+)?$/) {
	    $fh->print("$ov");
	} else {
	    $ov =~ s/\"/\\\"/g;
	    $fh->print("\"$ov\"");
	}
    } elsif($tx eq "HASH") {
	walkMap($fh,$ov,$depth+1);
    } elsif($tx eq "ARRAY") {
	walkList($fh,$ov,$depth+1);

    } elsif($tx eq "DateTime") {
	#$fh->print("{\"\$date\":\"${ov}.000Z\" }");

	my $tzs = $ov->time_zone()->name();
	if($tzs eq "UTC") {
	    $tzs = "Z";
	}

	my $zz = sprintf("%04D-%02d-%02dT%02d:%02d:%02d.%03d%s",
			 $ov->year(),
			 $ov->month(),
			 $ov->day(),
			 $ov->hour(),
			 $ov->minute(),
			 $ov->second(),
			 $ov->millisecond(),
			 $tzs
	    );

	$fh->print("{\"\$date\":\"${zz}\" }");

    } elsif($tx eq "SCALAR") {
	# Deref scalar and tell encode NOT to use \n (2nd arg is EOL char):
	my $b64val = encode_base64($$ov, '');
	$fh->print("{\"\$binary\":\"${b64val}\",\"\$type\":\"00\"}");
    }
}

sub walkMap {
    my($fh, $m, $depth) = @_;

    my $ov = undef;

    $fh->print("{");
    for my $k (keys %{$m}) {
	if(defined $ov) {
	    $fh->print(",");
	}
	$ov = $m->{$k};
	emit($fh, $ov, $k, $depth);
    }
    $fh->print("}");
}

sub walkList {
    my($fh, $list, $depth) = @_;
    my $len = @$list;

    $fh->print("[");
    for(my $jj = 0; $jj < $len; $jj++) {
	if($jj > 0) {
	    $fh->print(",");
	}
	my $ov = $list->[$jj];
	emit($fh, $ov, undef, $depth); # undef means no fldname!
    }    
    $fh->print("]");
}




package R2M::JSON;

# new R2M::JSON({basedir => "/tmp"})
sub new {
    my($class) = shift;  
    
    # A hash is used as the "body" of the object:
    my $this = {};
    bless $this, $class;

    my $args = shift;

    $this->{basedir} = $args->{basedir};

    return $this;
}


# getColl(collName)
sub getColl {
    my($this) = shift;      
    my($collName) = @_;
    
    my $collObj = new R2M::JSON::Collection($this->{basedir}, $collName);

    #  Hang onto the fh for closing later....
    $this->{files}->{$collName} = {
	fh => $collObj->{fh},
	fn => $collObj->{fname}
    };

    return $collObj;
}

sub close {
    my($this) = shift;      

    for $k (keys %{$this->{files}}) {
	my $info = $this->{files}->{$k};

	print "closing $info->{fn} ...\n";

	my $fh = $info->{fh};

	$fh->close();
    }
}






1;

__END__

=pod

=head1 NAME

R2M - DBD/DBI based relational to MongoDB bulk transfer framework

=head1 SYNOPSIS

 (look at r2m.n.pl for docs/guidance)

Important:  The DBI framework fetches dates and datetimes as strings
in the default output format of the source database, not
real DateTime (or other) objects.  R2M depends on the
DateTime::Format::DBI module to figure out the right DateTime::Format::XXX
string-to-DateTime parser based on the relational databases to which you are
connecting.
If you are pointing at Postgres, for example. you'll need
to have DateTime::Format::Pg installed because that is what 
DateTime::Format::DBI will choose.  For Oracle, DateTime::Format::Oracle.
This shouldn't be an issue because since R2M relies on the DBI ecosystem
from the get-go, you likely will already have these very convenient and
useful parsers installed.  See r2m.1.pl for custom dateparser setup.

More on dates:  If using simple source-to-target spec, the types
DBI::SQL_TYPE_DATE and DBI::SQL_TIMESTAMP do not carry timezone info.
They will be assigned GMT (+0000) timezone.  DBI::SQL_TYPE_TIMESTAMP_WITH_TIMEZONE,
however, has a timezone and will be set up as such in MongoDB.
If you are using a custom transfer subroutine (see r2m.2.pl) then remember
that the subroutine is passed the raw character string value; it becomes
your responsibility to do whatever you need to convert it into a DateTime
object that will be picked up as a real date by the MongoDB perl driver.
Of course, if you really want to move a character string like "2014-03-03" into
MongoDB as a string and not a real date, that's your choice.  But it's not 
very useful that way.


=head1 AUTHORS and MAINTAINERS

Buzz Moschetti F<E<lt>buzz[at]mongodb.orgE<gt>> (sort of)

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2014 Buzz Moschetti

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself. 

=cut
