use R2M;
use Data::Dumper;

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
	CTC => {
	    db => "D1",
	    table => "contact"
	},
	phones => { db => "D1" },
	txhist => { db => "D1" },
	contacttype => { db => "D1" }
    },

    collections => {
      contacts => {
	tblsrc => "CTC",
	flds => {
	    fname => "FNAME",
	    lname => "LNAME",
	    hd => "hiredate",
	    


	    ctype => [ "join", {
		type => "1:1",
		link => ["contacttype", "ctype" ]
		       },
		       { tblsrc => "contacttype",
			 flds => {
			     desc => "description"
			 }
		       }
		]

	    , phones => [ "join", {
			  # parent, child
			  link => ["did", "did"]  
                      },
		      { tblsrc => "phones",
			flds => {
			    rings => "RINGS",
			    type => "TYPE",
			    number => [ "fld", {
				colsrc => "NUMBER",
				f => sub {
				    my($ctx,$val) = @_;
				    $val =~ s/^1-/+1 /;
				    return $val;
				}
					}],

			    #
			    #  Example of second level of recursive
			    #  cascade.
			    #
			    txs => [ "join", {
				link => ["activated", "tdate" ]
				     },
				     { tblsrc => "txhist",
				       flds => {
					   comment => "comment",

					   # uppercase the same source col
					   # into another field for fun:
					   uc => [ "fld", {
					       colsrc => "comment",
					       f => sub {
						   my($ctx,$val) = @_;
						  # print Dumper($ctx);
						   return uc($val); 
					       }
						   }]
				       }
				     }
				     ]

			    }
		      }
		]
	}
      }

      # future C2 here....
   }
};

my $ctx = new R2M();

$ctx->run($qq);
