use R2M;

my $qq = {
    mongodb => {
	host => "localhost",
	port => 27017,
	db => "r2m"
    },

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
	    # of two fields from the row.  Powerful!
	    name => [ "fld", {
#		colsrc => ["*"],
#		colsrc => ["FNAME", "LNAME"],
		f => sub {
		    my($ctx,$vals) = @_;
		    my $fn = $vals->{"FNAME"}; $fn =~ s/\s+$//;
		    my $ln = $vals->{"LNAME"}; $ln =~ s/\s+$//;

		    return { first => $fn, last => $ln };
		}
		}]
	}
      }
    }
};

my $ctx = new R2M();
$ctx->run($qq);
