use R2M;
use Data::Dumper;


#
#  See docs below about this handy function
#
sub myDateMaker {
    my($tz,$val) = @_;

    my $newval = undef;

    my $year = undef;
    my $month = undef;
    my $day = 1; # possibly will be overridden...
    
    if($val ne "") { # watch for blanks / nulls!
	if(length($val) == 8) { # YYYYMMDD
	    ($year, $month, $day) = unpack("A4A2A2", $val);
	} elsif(length($val) == 6) { # YYYYMM
	    ($year, $month) = unpack("A4A2", $val);
	}
	
	if(defined $year) {
	    # did something! set DateTime into newval
	    $newval = DateTime->new(
		year      => $year,
		month     => $month,
		day       => $day,
		time_zone => $tz,
		);
	}
    }
    return $newval;
}


my $qq = {
    # Swap emitter and XXemitter for different outputs:
    emitter => new R2M::JSON({ basedir => "/tmp" }),
    #XXemitter => new R2M::MongoDB({ db=>"r2m", host =>"localhost", port => 27017}),

    rdbs => {
	D1 => {
	    conn => "DBI:Pg:dbname=mydb;host=localhost",
	    alias => "a nice PG DB",
	    user => "postgres",
	    pw => "postgres",
	    args => { AutoCommit => 0 }
	}
    },

    tables => {
	contact => {
	    db => "D1"
	}
    },


    collections => {
      contacts => {
	tblsrc => "contact",   

	flds => {
	    # Very often, you have to deal with dates coming out of the
	    # source RDBMS that are ... not dates.  Sometimes well-formatted
	    # strings, sometimes not.   But usually, you'll want to at least
	    # TRY to put them into MongoDB as real dates.
	    #
	    # As an example here, the source column a text field that
	    # contain YYYYMMDD or YYYYMM strings.   YYYYMM is considered to be
	    # day 1 on the given year and month.  So here's a little code
	    # to help us through this and create a proper DateTime object
	    # for MongoDB.  Besides the "main" source field "SDATE", the 
	    # function can draw upon any other data either in the row or 
	    # in the context.   For example (but not coded here), if "REGION"
	    # as a peer field was
	    # present, then REGION = "US" might switch interpretation of
	    # the string to be YYYYDDMM vs. REGION = "EMEA" mean YYYYMMDD.
	    #
	    # Note that we'll make use of context to create
	    # a UTC (+0000) TimeZone only ONCE before the run and then use
	    # it over and over again.   There's nothing special about the 
	    # field name "myTZ" in this case; it's just a name.  Whatever
	    # you set into context before the run is available at 
	    # $ctx->{_r2m}->{global} during the run -- and after, too!
	    #
            # Obviously, you can set this up as a separate function and just
	    # call it from f() and that's a good idea for the sake of processing
	    # consistency.   As an example, goodDate2 calls myDateMaker
	    # which is defined above.  This is particularly so if you wish
	    # employ fancy conditional parsing and date construction logic.
	    # 
	    # 
	    #
	    # As an extra little demo, we'll store the 
	    # UNMODIFIED string value as a peer in case we want to check later.
	    # But because those dates look like numbers, perl will
	    # automagically save them as numbers, not strings.  Boo on that.
	    #
	    origDate => 'SDATE',

	    goodDate => [ "fld", {
		colsrc => "SDATE",
		f => sub {
		    my($ctx,$val) = @_;

		    my $newval = undef;

		    my $year = undef;
		    my $month = undef;
		    my $day = 1; # possibly will be overridden...

		    if($val ne "") { # watch for blanks / nulls!
			if(length($val) == 8) { # YYYYMMDD
			    ($year, $month, $day) = unpack("A4A2A2", $val);
			} elsif(length($val) == 6) { # YYYYMM
			    ($year, $month) = unpack("A4A2", $val);
			}

			if(defined $year) {
			    # did something! set DateTime into newval
			    $newval = DateTime->new(
				year      => $year,
				month     => $month,
				day       => $day,
				time_zone => $ctx->{_r2m}->{global}->{myTZ}
				);
			}
		    }
		    return $newval;
		}
	    }],

	    goodDate2 => [ "fld", {
		colsrc => "SDATE",
		f => sub {
		    my($ctx,$val) = @_;
		    return myDateMaker($ctx->{_r2m}->{global}->{myTZ}, $val);
		}
	    }]
	}
      }
    }
};

my $r2m = new R2M();

my $ctx = $r2m->getContext();
$ctx->{myTZ} = DateTime::TimeZone->new( name => '+0000' );

$r2m->run($qq);
