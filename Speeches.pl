####################################################################
## speakers.pl A CLI utility for analyzing turn-taking            ##
## Creates a MySQL database with tables for speakers and speeches ##
## from corpora of dramatic texts in TEI5 compliant XML 		  ##
## By Barry Shelton												  ##
#################################################################### 

#!/usr/bin/perl
use strict;
use diagnostics;
use feature 'say';
use utf8;

##########################################
### Evaluate module installation #########
### and automatically resolve    #########
###          YMMV                #########  
########################################## 

my @packages= ("XML::LibXML", 
			   "XML::LibXML::XPathContext",
			   "DBI", 
			   "Statistics::Descriptive",
			   "Parallel::ForkManager");

foreach my $package (@packages) {
	eval {"use $package; 1"
	} or do {
		say "I need to install CPAN modules. I can try to resolve this automatically. If this fails, you need to install $package from CPAN on your own";
		say "Do you want me to try to get $package from CPAN? Y/N:";
		my $yn = <STDIN>;
		chomp $yn;
		$yn = lc $yn;
		if ($yn eq "y"){
		my @args = ("perl", "-MCPAN", "-e", "install $package");
		system @args;
		} else {die "Suit yourself. You need to install $package. Thus, I die."};
}}

#Import Modules
use XML::LibXML;
use XML::LibXML::XPathContext;
use Data::Dumper;
use DBI;
use Statistics::Descriptive;
use Parallel::ForkManager;
no warnings "experimental::autoderef";
use List::MoreUtils qw{ any };
use List::Util 'sum';
no warnings 'utf8';

##############################
## MySQL Database Creation ###
##############################

say "Please enter your MySQL user name:"; #Query user for MySQL connection credentials and DB name 
my $username = <STDIN>;
say "Please enter your MySQL password:";
my $password = <STDIN>;
say "Please enter the name of the db you want to create:";
my $db = <STDIN>;
chomp ($username, $password, $db);
my $dsn = "DBI:mysql:Driver={SQL Server}";
my @source_files = glob ("*.xml");
say @source_files;
my %attr = (PrintError=>0, RaiseError=>1);
my $dbh = DBI->connect($dsn,$username,$password, \%attr);
$dbh->{mysql_enable_utf8} = 1;
my @ddl = ( 								#an array of SQL queries to execute below
	"CREATE DATABASE IF NOT EXISTS $db;",
	
	"USE $db;",

	"CREATE TABLE IF NOT EXISTS speakers 
		 (author varchar(255),
		 title mediumtext,
		 date mediumint(255), 
		 name varchar(255),
		 spc mediumint(255),
		 wc mediumint(255),
		 mean mediumint(255),
		 sd mediumint(255)) ENGINE=InnoDB;",
	 
	"CREATE TABLE IF NOT EXISTS speeches 
		(name varchar (255),
		wc mediumint(255),
		spid mediumint(255),
		author varchar(255),
		title varchar(255),
		content mediumtext) ENGINE=InnoDB;"    
);
#Execute SQL queries
for my $sql(@ddl){
  $dbh->do($sql);
}  
say "All tables created successfully!";
say "Sorting...Hang on, this could take a while....";

########################
## Sub-routines to  #### 
## do the real work ####
########################

my @characs = getChars();
my @allspeeches = getSpeeches();

########################
#### Push Data     #####
#### Into MYSQL DB #####
########################


foreach my $charac(@characs){
	#Placeholders for SQL query
	my $sql = "INSERT INTO speakers(author, title, date, name, spc, wc, mean, sd) VALUES(?,?,?,?,?,?,?,?)";
	#Prepare SQL statement
	my $stmt = $dbh->prepare($sql);
	#De-reference each hash and execute SQL query 
	$stmt->execute($charac->{author}, $charac->{title}, $charac->{date}, $charac->{name}, $charac->{spc}, $charac->{wc}, $charac->{mean}, $charac->{sd});
		$stmt->finish();

}

foreach my $speech (@allspeeches){
		my $sql = "INSERT INTO speeches(name, wc, spid, author, title, content) VALUES(?,?,?,?,?,?)";
		my $stmt = $dbh->prepare($sql);
		#De-reference each hash and execute SQL query
		$stmt->execute($speech->{name}, $speech->{wc}, $speech->{spid}, $speech->{author}, $speech->{title}, $speech->{content});
			$stmt->finish();
		
}

$dbh->disconnect(); # Disconnect from DB handle and report successful completion
say "Finished with everything !!!!!!!!!!!!!!";

sub getSpeeches{	
	say "Scanning files for speech data...";	
	foreach my $filename (@source_files) {
		my $dom = XML::LibXML->load_xml(location => $filename);#Parse XML DOM
		my $xpc = XML::LibXML::XPathContext->new($dom);
		my $spid = 0; 
		$xpc->registerNs('tei',  'http://www.tei-c.org/ns/1.0');	
			my (@turns) = ($xpc->findnodes('//tei:body//tei:sp')); #Get all speeches in play
				foreach my $turn(@turns){ #For every speech in the play
				my $sp = $turn->getAttribute('who');#Get XML Node Content as metadata
				my $title = $xpc->findnodes('//tei:fileDesc/tei:titleStmt//tei:title[1]');
				my $author = $xpc->findnodes('//tei:fileDesc/tei:titleStmt//tei:author[1]');
				$title = $title->to_literal();
				$author = $author->to_literal();
				my $xmlid = $turn->getAttribute('xml:id');
				if (!$sp){$sp = "NULL"}; #Dummy string in case no character is named
				$sp =~ s/[^\w\n]//g; #substitute some garbage
				$sp = substr($sp,0,25); #Substring character name if too long
				$spid++; #auto-increment arbitrary id
				my ($content) = $turn->textContent;	# Get each speech	
				my @content = $content =~ /\w+/g;   # Break each speech into array of words 
				my $wc = scalar @content; #count words with Perl magic. Scalar @ returns array length!
				$content = join (' ', @content); #Join array of words into a single string for storage
				my %thisspeech = ("spid"=> $spid, "wc"=> $wc, "name"=> $sp, "author"=>$author, "title"=>$title, "content"=>$content); #Hash to store data foreach speech
				push(@allspeeches,\%thisspeech); #Push this hash onto array of hash references. 
				}
	}	

return @allspeeches;	
}



sub getChars{	
	say "Scanning files for speaker data...";	
	foreach my $filename (@source_files) {
		my $dom = XML::LibXML->load_xml(location => $filename);
		my $xpc = XML::LibXML::XPathContext->new($dom); #Parse XML DOM
		$xpc->registerNs('tei',  'http://www.tei-c.org/ns/1.0'); #Register namespace	
		my $author = $xpc->findnodes('//tei:fileDesc/tei:titleStmt//tei:author[1]'); #Gett XML Node Content as Metadata
		my $date = $xpc->findnodes('//tei:fileDesc/tei:titleStmt//tei:date[1]');
		my $title = $xpc->findnodes('//tei:fileDesc/tei:titleStmt//tei:title[1]');
		my %speaker; #A hash for each speaker in the play
		$author = $author->to_literal();
		$date = $date->to_literal();
		$title = $title->to_literal();
		my (@turns) = ($xpc->findnodes('//tei:body//tei:sp')); #Get array of turns
			foreach my $turn(@turns){
				my $sp = $turn->getAttribute('who');        
				if (!$sp){$sp = "NULL"};
				my ($content) = $turn->textContent;		
				my @content = $content =~ /\w+/g;
				my $wc = scalar @content; #Count words per speech
				push(@{$speaker{$sp}}, $wc); #Result: A count foreach speech by each speaker in this play--a hash of arrays (speakers=>[speeches]) 				
			}

		my @characters = keys %speaker; #get keys of hash (speaker names) generated above
			foreach my $character(@characters){
				my $stat = Statistics::Descriptive::Full->new();
				my @vals = values $speaker{$character}; #get values of hash (array of wordcounts) generated above
				my $sum = sum @vals; #count total words per speaker
				my $spc = scalar @vals; #count speeches per speaker
				$character =~ s/[^\w\n]//g; #cleanup character name as above
				$character = substr($character,0,25);
				my $mean = $sum / $spc; #average wordcount per speaker
				$stat->add_data(@vals);
				my $sd = $stat->standard_deviation(); #standard deviation of wordcount per speaker
				my %charac = ("author"=> $author, "title"=> $title, "date"=> $date, "name"=> $character, "spc"=>$spc, "wc"=>$sum, "mean"=>$mean, "sd"=>$sd, "speeches"=>[@vals]);
				push(@characs,\%charac); #push this hash onto array of hash references
			}
	}	
return @characs;	
}
