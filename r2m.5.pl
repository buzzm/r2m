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
	CTC => {
	    db => "D1",
	    table => "contact"
	},
	phones => {
	    db => "D1"
	},
	devices => {
	    db => "D1"
	}
    },

    collections => {
      contacts => {
	tblsrc => "CTC",
	flds => {
	    fname => "FNAME",
	    
	    #  "multijoin" allows you to place 2 or more sets of data
	    #  from sources joins into a single array field in mongoDB.
	    #  This is very useful to create polymorphic collections of
	    #  data.  You can also use it to ask the SAME table for info
	    #  but with applying different logic on the resultant rows
	    #  coming back.
	    #  The example below takes table phones, joins it to
	    #  contact on "did", and loads fields in phones into a new
	    #  structure in collection "things".  It then joins against did
	    #  to table "devices", and loads those records into things as
	    #  well.
	    #
            #  Super powerful!
	    #
	    things => [ "multijoin", [
			    # first thing to put into "things"
			    [ {
				type => "1:n",
				link => ["did", "did"]  # [ parentCol, childCol ]
			      },
			      { tblsrc => "phones",
				flds => {
				    origin => [ "fld", { val => "phones" } ],
				    data => [ "fld", {
					colsrc => ["TYPE", "RINGS", "NUMBER"],
					f => sub {
					    my($ctx, $vals) = @_;
					    return {
						type => $vals->{"TYPE"},
						rings => $vals->{"RINGS"},
						number => $vals->{"NUMBER"}
					    };
					}
					      }]
				}
			      }
			    ]
			    
			    # second thing to put into "things"
			    ,[
				{ type => "1:n",
				  link => ["did", "did"]  # [ parentCol, childCol ]
				},
				{ tblsrc => "devices",
				  flds => {
				      origin => [ "fld", { val => "devices" } ],
				      data => [ "fld", {
					  colsrc => ["TYPE", "IP"],
					  f => sub {
					      my($ctx, $vals) = @_;
					      return {
						  type => $vals->{"TYPE"},
						  ip => $vals->{"IP"}
					      };
					  }
						}]
				  }
				}
			    ]
			]
		    ]
	}
      }

      # future C2 here....
    }
};

my $ctx = new R2M();

$ctx->run($qq);
