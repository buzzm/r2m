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
	CTC => {
	    db => "D1",
	    table => "contact"
	},
	phones => {
	    db => "D1"
	}
    },


    collections => {
      contacts => {
	tblsrc => "CTC",
	flds => {
	    lname => [ "fld", {
		colsrc => "LNAME",
		f => sub {
		    my($ctx,$val) = @_;
		    #
		    #  $ctx is a holding pen for data.
		    #  You can put anything you want into it and take it out
		    #  later.  The scope/lifetype of ctx is for the duration
		    #  of the iteration through one table.  Nested iterations
		    #  will have their own scope.
		    #  R2M prepopulates several things for you, safely tucked
		    #  away under a key named _r2m:
		    #
                    #  $ctx->{_r2m}->{startTime}
		    #      Starting time (Seconds since epoch) when run() exec'd
		    #
		    #  $ctx->{_r2m}->{n}
		    #      Ordinal position of doc in iteration starting with 1
		    #
		    #  $ctx->{_r2m}->{table}
		    #      Name of table being iterated
		    #
                    #  $ctx->{_r2m}->{depth}
		    #      Level into recursion starting with 0
		    #
		    #  $ctx->{_r2m}->{global}
		    #      A hashref that is maintained throughout the ENTIRE
		    #      run across tables, joins, etc.  It is a place where
		    #      you can place anything that you wish to live for 
		    #      the duration of the run.  After run() is complete,
		    #      you can fetch the context with getContext().
		    #
		    #  All other fields in $ctx will go out of scope upon
		    #  completion of the iteration through the table
		    #  
		    #

		    # Just bump a counter in global to show that it can be done:
		    $ctx->{_r2m}->{global}->{counter}++;

		    # Bump a counter "locally" and watch it reset...
		    $ctx->{myCounter}++;

		    print "ctx: ", Dumper($ctx);

		    return $val;
		}
		       }],
	    
	    phones => [ "join", {
		          type => "1:n",
			  link => ["did", "did"]  
                      },
		      { tblsrc => "phones",
			flds => {
			    number => [ "fld", {
				colsrc => "NUMBER",
				f => sub {
				    my($ctx,$val) = @_;

				    # This global and the parent global are
				    # the same global!
				    $ctx->{_r2m}->{global}->{counter}++;

				    # ...but this one will reset frequently!
				    $ctx->{myCounter}++;

				    print "ctx: ", Dumper($ctx);

				    return $val;
				}
					}]
			}
		      }]
	}
      }

      # future C2 here....
    }
};

my $ctx = new R2M();

$ctx->run($qq);

print Dumper($ctx->getContext());
