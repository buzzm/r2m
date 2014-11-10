use R2M;

my $qq = {
    
    #  Basic mongoDB connection stuff:
    mongodb => {
	host => "localhost",
	port => 27017,
	db => "r2m"
    },

    rdbs => {
	#  Each DB connection gets a handle name; D1 is just fine.
	#  Create as many as need.
	#  Note: R2M does NOT join directly across DBs so have no fear;
	#  put as many as need here:
	D1 => {
	    #  Connect using DBI params!
	    conn => "DBI:Pg:dbname=mydb;host=localhost",

	    #  For debugging purposes; not used by DBI
	    alias => "a nice PG DB",
	    user => "postgres",
	    pw => "postgres",
	    args => { AutoCommit => 0 }
	}
    },

    tables => {
	# Each table is named and linked to the source DB where it 
	# can be found.  "contact" is the table name and it can be 
	# in database D1 described above:
	contact => {
	    db => "D1"
	}
    },


    collections => {
	#  The heart of the matter.
	#  One or more named target collections will be populated from source
	#  tables.  The model is "pull into mongoDB from RDBMS" rather than
	#  "push into mongoDB from RDBMS" because mongoDB has a richer target
	#  type system (nested structs, arrays, etc.)
      contacts => {
	  # This is the name as it is declared in the tables section above
	tblsrc => "contact",   

	# In the simplest use case, Each key in {flds} is a target field in the
	# mongoDB collection, and the value names the column in the table named
	# above in "src".
	# Target fields for mongoDB are (of course) CASE SENSITIVE.
	# Case sensitivity for source fields for RDBMS is ... ambiguous.  Because
	# so is SQL and individual RDBMS engine handling...
	# Basic types are handled correctly, e.g. dates end up as mongoDB dates.
	#
        # ALSO:  fixed length fields (e.g. char(64)) are rtrimmed!
	# 
	flds => {
	    firstname => "FNAME",
	    lastName => "LNAME",
	    hdate => "HIREDATE",
	    amt1 => "AMT1",
	    amt2 => "AMT2"
	}
      }
    }
};


#  Still thinking about exactly how to set this up...
my $ctx = new R2M();
$ctx->run($qq); 
