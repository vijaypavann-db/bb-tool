use strict;
use Common::MiscRoutines;
use DWSLanguage; 
use Data::Dumper;
#use CodeGeneration::PySpark; 

my $mr = new Common::MiscRoutines();
my $LAN = new DWSLanguage(); 


sub convert_decode 
{
	my $expr = shift; 
	$mr->log_msg("STARTING DECODE CONVERSION $expr");

	$expr =~ /DECODE\s*\((.*)\)$/i;
	my $param = "($1)";

	my @args = $mr->get_direct_function_args($param);

	#Check if even or odd because first argument not required

	$mr->log_msg("DECODE ARGS " . Dumper(@args));
	my $idx = 0; 

	my $ret = '';
	while ($idx < $#args - 1)
	{
		if(!$idx)
		{
			$ret .= "when($args[0] \= ($args[$idx+1]),$args[$idx+2])";
		}
		else 
		{
			$ret .= ".when($args[0] \= ($args[$idx+1]),$args[$idx+2])";
		}
		$idx = $idx + 2;
	}

	#if number of elements is even then add optional default arg
	my $odd_check = scalar(@args) % 2 ;
	if(!$odd_check)
	{
		$ret .= ".otherwise($args[$#args])";

	}

	$mr->log_msg("FINAL CONVERSION DECODE ARGS:$#args & $odd_check:\n $ret");
	return $ret;
}

