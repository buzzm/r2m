use R2M;
use Data::Dumper;

my $qq = {
    # Swap emitter and XXemitter for different outputs:
    emitter => new R2M::JSON({ basedir => "/tmp" }),
    XXemitter => new R2M::MongoDB({ db=>"r2m", host =>"localhost", port => 27017}),

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
	    # If the value of the map is not a string, it must be an arrayref.
	    # The arrayref is of the form [ "functionName", args, ... ]
	    #
            # The easiest function is "fld", which requires at a minimum the
	    # name of the source field.  The following example is equivalent to
	    # firstname => "FNAME":
	    firstname => [ "fld", {
		colsrc => "FNAME"
			   }],

	    # A great capability of R2M is that a fld function can have a 
	    # processing function.  When such a function is provided, no
	    # default processing (i.e. rtrim) is performed on the field.  
	    # Instead, you get to manipulate the value any way you wish 
	    # and return the desired object to be set into mongoDB.  
	    # Beware of setting undefs and empty strings
	    #
            # The processing function takes two args: a hashref context 
	    # (more on this later) and a value, one of:
	    # 1.  A scalar value if "colsrc" is a single column name
	    # 2.  A hashref containing the values of the columns named in
	    #     "colsrc" if "colsrc" is supplied
	    # 3.  A hashref containing the values of ALL the columns in
	    #     the parent table if "colsrc" is NOT supplied (think select *)
	    # 4.  A hashref containing the values of ALL the columns in
	    #     the parent table if "colsrc" is the scalar string "*"
	    #
	    # In general, asking for specific columns instead of "*" means
            # pulling less material from the DB.
	    #
	    # Note that
	    # it is OK to "ask" for the source column more than once across
	    # target field, but the target field name must be unique!
	    #
            # In this example, we force all FNAMEs after rtrimming ourselves.
	    # Any perl function and expression can be used here to do 
	    # whatever you need: boundary checking, etc.  This is the single
	    # value example so $val is the raw value of column FNAME.
	    #
	    upperfirstname => [ "fld", {
		colsrc => "FNAME",
		f => sub {
		    my($ctx,$val) = @_;
		    my $newval = undef;
		    if($val ne "") {
			$val =~ s/\s+$//;
			$newval = uc($val);
		    }
		    return $newval;
		}
	    }],
	    
	    lastname => "LNAME",
	    hdate => "HIREDATE",
	    amt1 => "AMT1",
	    amt2 => "AMT2"
	}
      }
    }
};

my $ctx = new R2M();
$ctx->run($qq);
