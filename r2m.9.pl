use R2M;


#
# This example show both CSV processing and our own custom dateparser.
#
#

package MyDateParser;
use DateTime;

sub new {
    my($class) = shift;  
    my $this = {};
    # It is good NOT to create this over and over again; expensive
    $this->{UTZ} = DateTime::TimeZone->new( name => '+0000' );
    bless $this, $class;
    return $this;
}

sub parse_datetime {
    my($this) = shift;      
    my($inval) = @_;   # YYYYMMDD

    my $newval = undef;

    my $len = length($inval);

    # Defaults:    
    my($Xyy, $Xmon, $Xdd, $Xhh, $Xmm, $Xss, $Xns, $Xtz) =
      (  0,     0 ,   0 ,   0,    0 ,   0 ,   0,  $this->{UTZ});

#    print "inval: [$inval] ($len)\n";

    if($len == 8) { # 20150101
	my $ms = 0;
	my($yy, $mon, $dd) = unpack("A4A2A2", $inval);
	($Xyy, $Xmon, $Xdd) = ($yy, $mon, $dd);

    }
    
    $newval = DateTime->new(
	    year      => $Xyy,
	    month     => $Xmon,
	    day       => $Xdd,
	    hour      => $Xhh,
	    minute    => $Xmm,
	    second    => $Xss,
	    nanosecond=> $Xns,
	    time_zone => $Xtz
	    );

    return $newval;
}


my $qq = {

    emitter => new R2M::JSON({ basedir =>"/tmp" }),

    rdbs => {
	D1 => {
	    conn => "DBI:CSV:",
	    # no user or password...
	    args => {
		f_ext      => ".csv/r",
		f_dir           => ".",
		csv_sep_char    => ",",
		csv_quote_char  => undef,
		csv_escape_char => undef,
		csv_tables => { "contact" => { col_names => [qw/fname lname hdate/ ] }}
	    },

	    alias => "CSV",

	    #  You don't have to craft your own dateparser if you use 
	    #  the following standard Oracle datestring input formats:
	    #    2015-01-01
            #    2015-01-01 18:15:39.619662
            #    2015-01-01 18:15:39.619662 -0500
	    #  We are assuming YYYYMMDD here so we need our own parser.
	    dateparser => new MyDateParser()
	}
    },

    tables => {
	contact => {
	    db => "D1",

	    # Many DBD imps do not fully support the DBI::column_info() method.
	    # It is therefore necessary for us to explicitly map column names
	    # to types.   We do this via the columnSpecs field.
	    #
            # Of note, if you declare a field to be a SQL_DATE (or SQL_TIMESTAMP)
	    # then R2M will call the dateparser associated with this input source.
	    # If you have multiple formats for dates, e.g. YYYYMMDDm YY-MM-DD all
	    # mixed together, your parser will have to sniff and test to pick the 
	    # right one to convert.
	    # It is HIGHLY recommended that you do NOT bring int and string 
	    # representations of dates into MongoDB as int and string.  Convert them
	    # to date!
	    #
	    columnSpecs => {
		"fname" => DBI::SQL_VARCHAR,
		"lname" => DBI::SQL_VARCHAR,
		"hdate" => DBI::SQL_DATE
	    }
	}
    },

    collections => {
      contacts => {
	tblsrc => "contact",   

	flds => {
	    firstname => "FNAME"
	    , lastName => "LNAME"
	    , hDate => "hdate"
	}
      }
    }
};


#  Still thinking about exactly how to set this up...
my $ctx = new R2M();
$ctx->run($qq); 
