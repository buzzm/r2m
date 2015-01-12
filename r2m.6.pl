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
	},
	devices => {
	    db => "D1"
	}
    },


    collections => {

	#  Just showing we can load more than one collection
      contacts => {
	tblsrc => "contact",   
	flds => {
	    firstname => "FNAME",
	    lastName => "LNAME",
	    hdate => "HIREDATE",
	    amt1 => "AMT1",
	    amt2 => "AMT2"
	}
      },

	#  Here's collection 2.
      devices => {
	  tblsrc => "devices",
	  flds => {
	      type => "TYPE",
	      ip => "IP"
	  }
      }
    }
};


#  Still thinking about exactly how to set this up...
my $ctx = new R2M();
$ctx->run($qq); 
