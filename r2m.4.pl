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
	#  Table may be aliased.   If the table field is present,
	#  then that is used for the RDBMS lookup.  The src references in
	#  collections will still use the key, e.g. CTC in this example:
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
	    fname => "FNAME",
	    lname => "LNAME",
	    blob => "BLOB",
	    
	    #  "join" is a powerful function that lets you embed documents
	    #  from other tables.
	    phones => [ "join", {
		          type => "1:n",

                          # item 0 is the column name in the parent
                          # item 1 is the column name in the child
			  # For each row in the parent, the value of item 0
			  # will be extracted and the child table queried
			  # where item 1 = value.
			  link => ["did", "did"]  
                      },
			#  The second arg to join is the same construcion
			#  as a regular spec with src and fields!  It is
			#  processed recursively; thus, any field here 
			#  can itself have a join!
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
