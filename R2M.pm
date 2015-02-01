#   See pod docs below __END__


#
#  Sigh.  After wrestling with DateTime::Format::Oracle and NLS_ env vars
#  and not picking up microseconds properly, I am punting and just dropping
#  in my own Oracle datetime parser.
#  It assumes the setup code somewhere has called "alter session" to change
#  the session nls_ formats to what is expected below.
#
package R2M::Oracle::DateParser;
use DateTime;

sub new {
    my($class) = shift;  
    my $this = {};
    # It is good NOT to create this over and over again; expensive
    $this->{UTZ} = DateTime::TimeZone->new( name => '+0000' );
    bless $this, $class;
    return $this;
}

sub parse_datetime {
    my($this) = shift;      
    my($inval) = @_;

    my $newval = undef;

    # nls_date_format => "YYYY-MM-DD",
    # nls_timestamp_format => "YYYY-MM-DD HH24:MI:SS.FF6",
    # nls_timestamp_tz_format => "YYYY-MM-DD HH24:MI:SS.FF6 TZHTZM"

    my $len = length($inval);

    # Defaults:    
    my($Xyy, $Xmon, $Xdd, $Xhh, $Xmm, $Xss, $Xns, $Xtz) =
      (  0,     0 ,   0 ,   0,    0 ,   0 ,   0,  $this->{UTZ});

#    print "inval: [$inval] ($len)\n";

    if($len == 10) { # 2015-01-01
	my $ms = 0;
	my($yy, $z, $mon, $z, $dd) = unpack("A4A1A2A1A2", $inval);
	($Xyy, $Xmon, $Xdd) = ($yy, $mon, $dd);

    } elsif($len == 26) { # 2015-01-01 18:15:39.619662
	my($yy, $z, $mon, $z, $dd, $z, $hh, $z, $mm, $z, $ss, $z, $ms) = unpack("A4A1A2A1A2A1A2A1A2A1A2A1A6", $inval);

	my $ns = $ms * 1000; # nanos, not micros!

	($Xyy, $Xmon, $Xdd, $Xhh, $Xmm, $Xss, $Xns) =
	    ($yy, $mon, $dd, $hh, $mm, $ss, $ns);

    } elsif($len == 32) { # 2015-01-01 18:15:39.619662 -0500
	my($yy, $z, $mon, $z, $dd, $z, $hh, $z, $mm, $z, $ss, $z, $ms, $z, $rawtz) = unpack("A4A1A2A1A2A1A2A1A2A1A2A1A6A1A5", $inval);

	my $ns = $ms * 1000; # nanos, not micros!

	($Xyy, $Xmon, $Xdd, $Xhh, $Xmm, $Xss, $Xns, $Xtz) =
	 ($yy, $mon,  $dd,  $hh,  $mm,  $ss,  $ns,  $rawtz);
    }
    
    $newval = DateTime->new(
	    year      => $Xyy,
	    month     => $Xmon,
	    day       => $Xdd,
	    hour      => $Xhh,
	    minute    => $Xmm,
	    second    => $Xss,
	    nanosecond=> $Xns,
	    time_zone => $Xtz
	    );

    return $newval;
}




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

    # Bail out fast on empty data.  We will not store "".
    if(!defined $rawval || $rawval eq "") {
	return undef;
    }

    my $newval = $rawval;
    
    # Most of the time, the type is DBI::SQL_VARCHAR which does not need
    # to be groomed, so check for that first and potentially bail out:
    if($stype == DBI::SQL_VARCHAR) {
	return $newval;
    }

    # OK, slog through the others.

    if($stype == DBI::SQL_TYPE_DATE
       || $stype == DBI::SQL_DATE
       || $stype == DBI::SQL_TIMESTAMP
       || $stype == DBI::SQL_TYPE_TIMESTAMP) {

	$newval = $dbdateparser->parse_datetime($rawval);
	
	# Add in the UTZ timezone!
	$newval->set_time_zone($this->{UTZ});

    } elsif($stype == DBI::SQL_TYPE_TIMESTAMP_WITH_TIMEZONE) {

	# Does not creating floating timezone issues so OK for MongoDB
	$newval = $dbdateparser->parse_datetime($rawval);

    } elsif($stype == DBI::SQL_CHAR
	) {

	#  Kill trailing whitespace; not important in mongoDB.
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

	# See the MongoDB perl driver docs for more detail on how
	# a scalar ref makes the driver store type blob instead of string.
	$newval = \$rawval;
    }

    return $newval;
}



#  processCollection($isTop, $coll, $info, $where, $depth, $linktype, $limit)
#  $limit = 0 means let it run to max

sub processCollection {
    my($this) = shift;      

    my($isTop, $coll, $info, $where, $depth, $linktype, $limit) = @_;


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
	my $v = $this->{spec}->{tables}->{$info->{tblsrc}}->{coldefs}->{$nk}->{"R2M_NORM_DATA_TYPE"};
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


    #  MAIN SELECT FETCH
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


	#
        # Next, the links.
	# If no links, then this whole recursive subsection is BYPASSED
	#
	for my $k (keys %{$links}) {
	    my $linkarr = $links->{$k};

#	    print "linkarr: ", Dumper($linkarr);
#	    print "linkarr len: ", $#$linkarr, "\n";

	    my @subaccums = ();

	    # Take the zeroeth join type.  When we go recursive,
	    # the linkarr will be different, i.e. $linkarr->[0]->[0]->{type}
	    # is a different thing...
	    my $innerlinktype = $linkarr->[0]->[0]->{type};

	    for(my $n = 0; $n < $#$linkarr + 1; $n++) {
		my @partials = ();

		my $link = $linkarr->[$n];

		my $targ = $link->[0]->{link}->[1];
		
		my $idx = $revmap->{$link->[0]->{link}->[0]};
		my $val = $row->[$idx];
		
		#  Only something to do if the parent link actually
		#  has a value:
		if(defined $val) {

		    #  TBD TBD TBD
		    #  This needs examination.
		    #  Apparently, most dbengines are good are taking
		    #  ANY inbound string and converting it to the right
		    #  type; thus, it is not necessary to examine the
		    #  target type and quote or not quote or otherwise futz with
		    #  it.  
		    #
		    #  I tried this with strings (obviously), dates, and ints
		    #  and it works against postgres and Oracle so...
		    #
		    my $subw = "$targ = \'$val\'";  # TBD!  Enhance THIS!

		    my $innerlimit = 0;
		    if($innerlinktype eq "1:1") {
			$innerlimit = 1;
		    }
		 
		    # Go recursive!
		    @partials = $this->processCollection(0, $coll, $link->[1], $subw, $depth + 1, $innerlinktype, $innerlimit);	    
		    
		    if($#partials != -1) {
#		    print "** collected " , $#partials + 1 , " from $subw\n";
			push(@subaccums, @partials);
#		    print "** new subaccums is " , $#subaccums + 1 , "\n";
		    } else {
#		    print "** collected NONE from $subw\n";
#		    print "** accums is " , $#subaccums + 1, "\n";
		    }
		}
	    }

	    # Only set if >0
	    if($#subaccums != -1) {

		# This 1:1,n logic appears here and not outside the links
		# loop because only links have this issue.  A "normal" pass
		# over a table is SUPPOSED to produce 1:n.
		if(!defined($innerlinktype) || $innerlinktype eq "1:n") {
		    $doc->{$k} = \@subaccums;
		} elsif($innerlinktype eq "1:1") {
		    $doc->{$k} = $subaccums[0];
		}
#		print "setting " , $#subaccums + 1 , " items into $k\n";

# maybe undelete
#		$doc->{$k} = \@subaccums;
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
		my $id = $coll->insert($doc);
		#print "inserted $id\n";
		@accums = ();
	    } else {
		push(@accums, $doc);
	    }

	    # At this point we have done "something of value."
	    # The reason we check for limit here and not in the normal place
	    # which would be the end of the loop is because certain 1:1 
	    # or $limit
	    # processing conditions may not "accept" the first joined doc.
	    # In this case, the logic must be allowed to proceed to the next
	    # candidate.  In other words, 1:1 means take the first good subdoc
	    # you find if you can.
	    if($limit > 0 && $doccnt == $limit) {
		break;
	    }

	} else {
#	    print "NO flds found for xfer; skip\n";
	}

    } # END OF MAIN SELECT FETCH

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

	my $dbh = undef;

	if(defined $item->{dbh}) {
	    $dbh = $item->{dbh};
	} else {
	    $dbh = DBI->connect(
		$item->{conn},
		$item->{user},
		$item->{pw},
		$item->{args});
	}

	$this->{dbs}->{$k}->{dbh} = $dbh;

	# Oooo!
	$this->{dbs}->{$k}->{dbtype} = "UNKNOWN";
	my @dbtypes = eval { DBI::_dbtype_names($dbh,0) };
	if($#dbtypes == 0) {  # should only be one....?
	    $this->{dbs}->{$k}->{dbtype} = uc($dbtypes[0]); # uc() just in case...
	}


	if(defined $item->{dateparser}) {
	    $this->{dbs}->{$k}->{dbdateparser} = $item->{dateparser};
	} else {
	    # 
	    #  AAAAAAAAUUUUUUUUUUGH
	    #  The Date handling setup is soooo brittle with Oracle that
	    #  it is best if we hardcode our own environment and force dates
	    #  and times to be in a very specific, controlled env...
	    #
	    if($this->{dbs}->{$k}->{dbtype} eq "ORACLE") {
		#  Fortunately, this setup on Oracle has NO compile/runtime
		#  dependency (thank goodness), i.e. if you're using postgres
		#  have no clientside Oracle anything, 
		# 
		#  Note: We keep dashes and stuff in the format just to make
		#  sure that 2015-01-01 remains a string and not 20150101 which
		#  could be an int and likely make things weird later...
		#
		my $fmtFuncs = {
		    nls_date_format => "YYYY-MM-DD",
		    nls_timestamp_format => "YYYY-MM-DD HH24:MI:SS.FF6",
		    nls_timestamp_tz_format => "YYYY-MM-DD HH24:MI:SS.FF6 TZHTZM"
		};
		for my $k (keys %{$fmtFuncs}) {
		    my $sql =  "alter session set $k = '"
			. $fmtFuncs->{$k}
		    . "'";
		    
		    $dbh->do($sql);
		}

		$this->{dbs}->{$k}->{dbdateparser} = new R2M::Oracle::DateParser();

	    } else { # Not Oracle; go for DBI...
		$this->{dbs}->{$k}->{dbdateparser} = DateTime::Format::DBI->new($dbh);
	    }
	}

	$this->{dbs}->{$k}->{alias} = $item->{alias};

	doSpecialDBhandling($dbh);
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

	
	# Internal Metadata
	# Welcome to the land of common interfaces but with varying
	# (implementation dependent) hashrefs that come back AND
	# differing values in those hashrefs.   Sigh...
	#
	# The internal metadata tends to be case wobbly.  It is unclear
	# if the name you use to create a column (like camelCase) is 
	# saved as camelcase or CAMELCASE or camelCase.
	# This applies to columns and tables.
	# SQL is more case-forgiving.
	# 
	# To start, use column_info() on that table name supplied in
	# the spec.  It is very possible that the case wobblies
	# will impact us.   For example, postgres is strict case sensitive
	# but Oracle uppercases the table names in metadata.
	# So...
	# If at first you don't get the table, try again with uppercase...
	#

	my $sth = $dbh->column_info( undef, undef, $item->{table}, undef );
	my $defs = $sth->fetchall_hashref("COLUMN_NAME");

	my $ndefs = scalar keys %{$defs};
	if($ndefs == 0) {
	    # Try uppercase...
	    my $uctbl = uc($item->{table});
#	    print "retrying with table $uctbl\n";
	    $sth = $dbh->column_info( undef, undef, $uctbl, undef );
	    $defs = $sth->fetchall_hashref("COLUMN_NAME");
	}

	# One way or another, we now have coldefs...
	$item->{coldefs} = $defs;

	# Fixup those defs...
	for my $k (keys %{$item->{coldefs}}) {
	    my $v = $item->{coldefs}->{$k};

	    # While we're here:
	    # DBD::Postgres does not populate SQL_DATA_TYPE; it only 
	    # populates DATA_TYPE.  DBD::Oracle populates DATA_TYPE and
	    # SQL_DATA_TYPE consistently, but if SQL_DATA_TYPE is undef
	    # (like for timestamp WITH timezone) then DATA_TYPE will carry
	    # the code -- except when THAT is also null, fall back to 
	    # TYPE_NAME and brute force it.
	    # This is so weird that it's best to create our own field called
	    # R2M_NORM_DATA_TYPE which is still in the domain of the DBI::
	    # codes but represents the best thing...
	    #
	    my $stype = $v->{SQL_DATA_TYPE};
	    if(!defined $stype) {
		$stype = $v->{DATA_TYPE};
	    }
	    if(!defined $stype) {
		if($v->{TYPE_NAME} eq "BINARY_DOUBLE") {
		    $stype = DBI::SQL_DOUBLE;
		}
	    }
	    if(!defined $stype) {
		# Fall back to string and hope...
		$stype = DBI::SQL_VARCHAR;
	    }

	    $v->{R2M_NORM_DATA_TYPE} = $stype;

	    # And now the second hack.  
	    # Ucase the key and point that key to the content of the original.
	    # It's easier to do this than worry about what calls into the
	    # engine catalog are case-sensitive
	    my $nk = uc($k);
	    $item->{coldefs}->{$nk} = $v;

#	    print Dumper($v);
	}
#	print "$item->{table} schema captured\n";
#	print Dumper( $item->{coldefs} );
    }

    # load 'em up!
    my $colls = $spec->{collections};
    for $k (keys %{$colls}) {

	my $coll = $this->{emitter}->getColl($k);

	# Kickstart with 1:n and limit=0 which means everything
	$this->processCollection(1, $coll, $colls->{$k}, undef, 0, "1:n", 0);
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


#
#  Initially, this was done for Oracle.  
#  Probably should NOT have this buried in R2M but Oracle is SOOOOO
#  prevalent that it's just better to set it up here.   
#  
sub doSpecialDBhandling {
    my($dbh) = @_;

    #  Yikes...
    my @dbtypes = eval { DBI::_dbtype_names($dbh,0) };

    if($#dbtypes == 0) {  # should only be one....?

	my $dbt = $dbtypes[0];

	if(uc($dbt) eq "ORACLE") {
	    #  Fortunately, this setup on Oracle has NO compile/runtime
	    #  dependency (thank goodness). 
	    # 
	    #  Note: We keep dashes and stuff in the format just to make
	    #  sure that 2015-01-01 remains a string and not 20150101 which
	    #  could be an int and likely make things weird later...
	    #
	    my $fmtFuncs = {
		nls_date_format => "YYYY-MM-DD",
		nls_timestamp_format => "YYYY-MM-DD HH24:MI:SS.FF6",
		nls_timestamp_tz_format => "YYYY-MM-DD HH24:MI:SS.FF6 TZHTZM"
	    };
	    for my $k (keys %{$fmtFuncs}) {
		my $sql =  "alter session set $k = '"
		    . $fmtFuncs->{$k}
		. "'";

		#print "sql: $sql\n";

		$dbh->do($sql);
	    }
	}	    
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

	my $tzo = $ov->time_zone();

	my $tzs = $tzo->name(); # go for a default

	#
	#  Mongo DB does not store floating timezones; it gets 
	#  pegged to Z, so we must make BOTH the UTC and any kind
	#  of floating time a Z!
	#
	if($tzo->is_floating || $tzo->is_utc) {
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
