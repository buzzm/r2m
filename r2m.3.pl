use R2M;

my $qq = {
    # Swap emitter and XXemitter for different outputs:
    emitter => new R2M::JSON({ basedir => "/tmp" }),
    #emitter => new R2M::MongoDB({ db=>"r2m", host =>"localhost", port => 27017}),

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
	    # The fld function can take more than one source column
	    # and is not restricted to returning a scalar.  ANY valid
	    # perl containing objects suitable for mongoDB can be
	    # supplied.  In the >1 source field mode, the paramter to
	    # the processing function is a hashref of values instead of
	    # a single scalar, where the keys of the hashref match the 
	    # input "colsrc".
	    #
            # Here, we construct a field "name" that is a nested structure
	    # of two fields from the row.  
	    # Be careful to ensure you're not setting nulls and things.
	    # When you supply the func, it is entirely up to you to control
	    # what is going in!  To NOT store the item, return undef.
	    #
            # Powerful!
	    name => [ "fld", {
#		colsrc => ["*"],
#		colsrc => ["FNAME", "LNAME"],
		f => sub {
		    my($ctx,$vals) = @_;

		    my $nameStruct = {};

		    my $fn = $vals->{"FNAME"}; 
		    $fn =~ s/\s+$//;
		    if($fn ne "") {
			$nameStruct->{first} = $fn;
		    }

		    my $ln = $vals->{"LNAME"}; 
		    $ln =~ s/\s+$//;
		    if($ln ne "") {
			$nameStruct->{last} = $ln;
		    }

		    # Was anything set?
		    return scalar keys %{$nameStruct} > 0 ? $nameStruct : undef;
		}
		}]
	}
      }
    }
};

my $ctx = new R2M();
$ctx->run($qq);
