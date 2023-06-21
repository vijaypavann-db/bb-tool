use strict;
use Data::Dumper;
use Common::MiscRoutines;
use DWSLanguage;

my $MR = new Common::MiscRoutines;
my $LAN = new DWSLanguage();
my %CFG = (); #entries to be initialized
my $CONVERTER = undef;
my $INDENT = 0; #keep track of indents
my $INDENT_ENTIRE_SCRIPT = 0;
my $EXCEPTION_BLOCK = 'EXCEPTION_BLOCK';
my %SCRIPT_PARAMS_AND_VARS = ();
my %CURSORS = ();
my $STATEMENT_COUNT = 0;
my $FILE_OUT;
my $GOTO_PRESENT = 0;
my %GOTO_ID = (); #record GOTO statement IDs

my $pyq = '"""'; #python multiline quote


sub init_hooks #register this function in the config file
{
	my $param = shift;
	%CFG = %{$param->{CONFIG}};
	$CONVERTER = $param->{CONVERTER};
	print "INIT_HOOKS Called. config:\n" . Dumper(\%CFG);
}

sub convert_program_declaration
{
	my $ar = shift;
	my $str = join("\n", @$ar);
	my $inner = '';
	if ($str =~ /\((.*)\)/gis)
	{
		$inner = $1;
	}
	else
	{
		return ''; ##Script takes no arguments';
	}
	#my $mr = new Common::MiscRoutines();

	my @args = split(/\,/, $inner);
	print "ARGS: " . Dumper(\@args);
	my @final_args = ();
	my $i = 1;
	foreach my $x (@args)
	{
		$x =~ s/\n/ /g;
		$x = $MR->trim($x);
		my @tmp = split(/ /, $x);
		my $tmp = $tmp[0];
		my $type = $tmp[1];
		$SCRIPT_PARAMS_AND_VARS{$tmp} = $type;
		push(@final_args, "$tmp = sys.argv[$i]");
		$i++;
	}
	return "#SCRIPT ARGUMENTS:\n" . join("\n",@final_args) . "\n";
}

sub convert_comment
{
	my $ar = shift;
	if (ref($ar) eq 'ARRAY')
	{
		map {$_ = '#' . $_} @$ar;
		return plug_indent(join("\n", @$ar));
	}
	else
	{
		$ar =~ s/\n/\n#/gis;
		return plug_indent($ar);
	}
}

sub convert_var_declare
{
	my $ar = shift;
	my @final = ();
	#my %subst = ('\|\|' => '+');
	foreach my $x (@$ar)
	{
		$x = $MR->trim($x);
		if ($x eq '')
		{
			push(@final,"");
			next;
		}
		next if $x =~ /^\s*declare\s*$/i;
		my $tmp = (split(/ /, $x))[0];
		my $value = 'None';

		if ($x =~ /\:\=(.*)/)
		{
			$value = adjust_expr($MR->trim($1));
		}
		my $final = "$tmp = $value";
		$SCRIPT_PARAMS_AND_VARS{$tmp} = 1;
		#map {$final =~ s/$_/$subst{$_}/g} keys %subst;
		push(@final, $final);
	}
	return join("\n", @final);
}

sub convert_assignment
{
	my $ar = shift;
	$MR->log_msg("convert_assignment: " . Dumper(\%SCRIPT_PARAMS_AND_VARS));
	my $sql = join("\n", @$ar);
	my @ar = split(/\:\=/, $sql);
	if ($#ar == 1) #2nd element is the value or expression
	{
		$ar[1] = adjust_expr($ar[1]);
		$sql = $ar[0] . ' = ' . $MR->trim($ar[1]);
	}
	#$sql =~ s/\:\=/=/gis;
	$sql = adjust_expr($sql);
	return plug_indent($MR->trim($sql));
}

sub convert_dml
{
	my $ar = shift;
	my $sql = join("\n", @$ar);
	#$sql = adjust_expr($sql);
	my $quote = "'";
	$quote = '"""' if $sql =~ /\n/; #change quotes to triple quotes if the statement contains newlines
	my $sql = $MR->trim($CONVERTER->convert_sql_fragment($sql));
	my $ret = plug_indent("$CFG{rowcount_varname} = $CFG{dbh_varname}.exec($quote$sql$quote)",1);
	if ($CFG{pre_sql_line})
	{
		my $tmp = $CFG{pre_sql_line};
		$STATEMENT_COUNT++;
		$tmp =~ s/\%STATEMENT_COUNT\%/$STATEMENT_COUNT/gis;
		$ret = plug_indent($tmp) . "\n$ret";
	}
	#$ret = plug_indent($CFG{pre_sql_line}) . "\n$ret" if $CFG{pre_sql_line};
	$ret .= "\n" . plug_indent($CFG{rowcount_message}) if $CFG{rowcount_message};
	return $ret;
}

sub use_directive
{
	my $ar = shift;
	my $sql = join("\n", @$ar);
	#$sql = adjust_expr($sql);
	my $quote = "'";
	$quote = '"""' if $sql =~ /\n/; #change quotes to triple quotes if the statement contains newlines
	my $sql = $MR->trim($CONVERTER->convert_sql_fragment($sql));
	my $ret = plug_indent("$CFG{dbh_varname}.exec($quote$sql$quote)");
	if ($CFG{pre_sql_line})
	{
		my $tmp = $CFG{pre_sql_line};
		$STATEMENT_COUNT++;
		$tmp =~ s/\%STATEMENT_COUNT\%/$STATEMENT_COUNT/gis;
		$ret = plug_indent($tmp) . "\n$ret";
	}
	#$ret = plug_indent($CFG{pre_sql_line}) . "\n$ret" if $CFG{pre_sql_line};
	return $ret;
}

sub handle_label
{
	my $ar = shift;
	my $sql = join("\n", @$ar);
	my $label = 'UNKNOWN_LABEL';
	$sql =~ s/\r\n/ /g;
	$sql =~ s/\n/ /g;
	$STATEMENT_COUNT++;
	if ($sql =~ /\.LABEL\s+(\w+)/)
	{
		$label = $1;
		$GOTO_ID{$label} = $STATEMENT_COUNT
	}
	
	$sql = "statement_id = $STATEMENT_COUNT #original line: $sql\n";
	my $ret = plug_indent("#Label assignment\n$sql");
	return $ret;
}

sub convert_ddl
{
	my $ar = shift;
	my $sql = join("\n", @$ar);
	#$sql = adjust_expr($sql);
	my $quote = "'";
	$quote = '"""' if $sql =~ /\n/; #change quotes to triple quotes if the statement contains newlines
	my $sql = $MR->trim($CONVERTER->convert_sql_fragment($sql));
	my $ret = plug_indent("$CFG{dbh_varname}.exec($quote$sql$quote)");
	if ($CFG{pre_sql_line})
	{
		my $tmp = $CFG{pre_sql_line};
		$STATEMENT_COUNT++;
		$tmp =~ s/\%STATEMENT_COUNT\%/$STATEMENT_COUNT/gis;
		$ret = plug_indent($tmp) . "\n$ret";
	}
	#$ret = plug_indent($CFG{pre_sql_line}) . "\n$ret" if $CFG{pre_sql_line};
	return $ret;
}


sub handle_dynamic_sql
{
	my $ar = shift;
	my $sql = join("\n", @$ar);
	$sql = adjust_expr($sql);
	my $quote = "'";
	$quote = '"""' if $sql =~ /\n/; #change quotes to triple quotes if the statement contains newlines
	my $sql = $MR->trim($CONVERTER->convert_sql_fragment($sql));
	my $ret = plug_indent("$CFG{rowcount_varname} = $CFG{dbh_varname}.exec($sql)",1);
	$ret = plug_indent($CFG{pre_sql_line}) . "\n$ret" if $CFG{pre_sql_line};
	return $ret;	
}


sub convert_return
{
	my $ar = shift;
	my $sql = join("\n", @$ar);
	$sql =~ s/\n/ /gs;
	$MR->log_msg("convert_return: $sql");

	if ($sql =~ /RETURN(.*)/i)
	{
		my $ret_code = $1 || 0;
		$ret_code =~ s/\;//;
		$ret_code = $MR->trim($ret_code);
		return plug_indent("exit($ret_code)");
	}
	return plug_indent("exit(0)");
}

sub raise_notice
{
	my $ar = shift;
	my $sql = join("\n", @$ar);
	$sql =~ s/\n/ /gs;
	my @tok = $LAN->split_expression_tokens($sql);
	#print "TOKENS: " . Dumper(\@tok);
	shift @tok; #get rid of RAISE
	shift @tok; #get rid of NOTICE
	my $msg = shift @tok;
	while ($msg =~ /\[\%\]/)
	{
		my $comma = shift @tok;
		my $param = shift @tok;
		$msg =~ s/\[\%\]/\{$param\}/;
	}
	#$sql =~ s/raise\s+notice/print(/gis;
	#$sql .= ')';
	$msg = adjust_expr($msg);
	$msg = "print (f$msg)";
	return plug_indent($MR->trim($msg));
}

sub execute_into
{
	my $ar = shift;
	my $sql = join("\n", @$ar);
	$sql =~ s/\n/ /gs;
	my @tok = $LAN->split_expression_tokens($sql);
	my ($exec_kw, $stmt, $into, $var) = @tok;
	$stmt = adjust_expr($stmt);
	return "$var = $CFG{dbh_varname}.select_value($stmt)"
	#return $sql;
}

sub handle_commit
{
	my $ar = shift;
	my $sql = join("\n", @$ar);
	return "$CFG{dbh_varname}.commit()\n";
}

#simple version for now, assuming 1 variable is being assigned a value
sub read_dml_into_var
{
	my $ar = shift;
	my $sql = join("\n", @$ar);
	$sql =~ s/\n/ /gs;
	#my $var_name = '';
	if ($sql =~ /(.*)into\s+(\w+)(.*)/)
	{
		my ($before, $var_name, $after) = map {$MR->trim($_)} ($1, $2, $3);
		my $tmp = adjust_expr("$before $after");

		$sql = "$var_name = $CFG{dbh_varname}.select_value('$tmp')";
	}
	return $MR->trim($sql);
}

sub read_dml_into_file
{
	my $ar = shift;
	my $sql = join("\n", @$ar);
	#$sql =~ s/\n/ /gs;
	my $quote = "'";
	$quote = '"""' if $sql =~ /\n/;
	my $sql = $MR->trim($CONVERTER->convert_sql_fragment($sql));
	my $ret = plug_indent("$CFG{rowcount_varname} = $CFG{dbh_varname}.execIntoFile('$FILE_OUT', $quote$sql$quote)",1);
	if ($CFG{pre_sql_line})
	{
		my $tmp = $CFG{pre_sql_line};
		$STATEMENT_COUNT++;
		$tmp =~ s/\%STATEMENT_COUNT\%/$STATEMENT_COUNT/gis;
		$ret = plug_indent($tmp) . "\n$ret";
	}
	#$ret = plug_indent($CFG{pre_sql_line}) . "\n$ret" if $CFG{pre_sql_line};
	$ret .= "\n" . plug_indent($CFG{rowcount_message}) if $CFG{rowcount_message};
	return $ret;
}

sub adjust_expr
{
	my $expr = shift;
	my $quote = '"';
	my $multiline_flag = 0;
	if ($expr =~ /\n/)
	{
		$quote = '"""';
		$multiline_flag = 1;
	}
	$MR->log_msg("adjust_expr: multiline: $multiline_flag, SQL: $expr");
	#my $
	#$expr =~ s/\|\|/\ + /g; # || to +
	#$expr =~ s/\'\'\'/\\\'\\\'/g; #consecutive single quptes escape
	#handle escape quotes
	my $len = length($expr);
	my @chars = map {substr( $expr, $_, 1)} 0 .. $len -1;
	my $inside_str = 0;
	my$i = 0;
	while ($i < $len)
	{
		#handle quote escape
		my $orig_char = $chars[$i];
		#print "CHAR: $orig_char, inside: $inside_str\n";
		if ($orig_char eq "'" && !$inside_str)
		{
			$inside_str = 1;
			$chars[$i] = $quote;
		}
		elsif ($orig_char eq "'" && substr($expr,$i,2) eq "''" && $inside_str)
		{
			#$inside_str = !$inside_str;
			if ($inside_str)
			{
				#$chars[$i] = "\\'";
				#$chars[$i+1] = "\\'";
				$chars[$i+1] = ""; #no need for the escape single quote
				$i+2;
			}
		}
		elsif ($orig_char eq "'" && $inside_str)
		{
			$inside_str = 0;
			$chars[$i] = $quote;
		}

		#handle concat
		if ($chars[$i] eq "|" && substr($expr,$i,2) eq "||" && !$inside_str)
		{
				$chars[$i] = " + ";
				$chars[$i+1] = "";
				$i+2;
		}

		$i++;
	}
	$expr = join("", @chars);
	$expr =~ s/\;$//; #get rid of closing semicolon
	$expr = $MR->trim($expr);
	return $expr;
}


sub convert_else_if
{
	my $ar = shift;
	my $sql = join("\n", @$ar);
	$MR->log_msg("Starting convert_else_if: $sql");
	if ($sql =~ /^\s*ELSE.*IF(.*)THEN(.*)/i)
	{
		my $cond = $1;
		my $end = $2;
		$cond =~ s/\=/\=\=/g;
		$INDENT--;
		my $ret = plug_indent("elif $cond:");
		$INDENT++;
		if ($end ne '')
		{
			$ret .= "\n" . plug_indent("end");
		}
		
		return $ret;
	}
}

sub convert_else
{
	my $ar = shift;
	my $sql = join("\n", @$ar);
	$MR->log_msg("Starting convert_else_if: $sql");
	if ($sql =~ /^\s*ELSE(.*)/i)
	{
		$INDENT--;
		my $ret = plug_indent("else:");
		$INDENT++;
		return $ret;
	}
}


sub convert_end_if
{
	$INDENT--;
	return plug_indent("#END IF")
}

sub convert_start_if
{
	my $ar = shift;
	my $sql = join("\n", @$ar);
	if ($sql =~ /^\s*IF(.*)THEN/i)
	{
		my $cond = $1;
		$cond =~ s/\=/\=\=/g;
		my $ret = plug_indent("if $cond:");
		$INDENT++;
		return $ret;
	}
}

sub plug_indent
{
	my $str = shift;
	my $additional_indent_flag = shift;
	my $indent = "\t" x $INDENT ;
	$indent .= $CFG{rowcount_assignment_indent} if $additional_indent_flag;
	$indent = "$CFG{code_indent}$indent" if $CFG{code_indent};
	$str =~ s/\n/\n$indent/gs;
	$str = $indent . $str;
	return $str;
}

sub convert_conditions
{
	my $str = shift;
	return $str unless $str =~ /\=/; #check for equal signs
	my @tok = $LAN->split_expression_tokens($str);

}

sub handle_cursor_declaration
{
	my $ar = shift;
	my $sql = join("\n", @$ar);
	$MR->log_msg("handle_cursor_declaration: $sql");
	if ($sql =~ /CURSOR\s+(\w+)\s*(\(.*\)?)\bIS\b(.*)/gis)
	{
		my ($nm, $params, $select) = ($1,$2,$3);
		my @lines = (
			"#Cursor $nm",
			"${nm}_SELECT = ${pyq}$select${pyq}\n\n"
		);
		return join("\n", @lines);
	}
	elsif ($sql =~ /CURSOR\s+(\w+)\s*\bIS\b(.*)/gis)
	{
		my ($nm, $select) = ($1,$2);
		my @lines = (
			"#Cursor $nm",
			"${nm}_SELECT = ${pyq}$select${pyq}"
		);
		return join("\n", @lines);
	}
	return "#CURSOR DECLARED, PARSE IMPLEMENTATION NEEEDED\n";

	#return "CURSOR DECL";
}


sub handle_exceptions
{
	my $ar = shift;
	my $sql = join("\n", @$ar);
	$MR->log_msg("handle_exceptions: $sql");
	$sql =~ s/EXCEPTION\s+WHEN\s+OTHERS\s+THEN//gis;
	$sql = raise_notice([$sql]);

	$INDENT_ENTIRE_SCRIPT = 1;
	return "$EXCEPTION_BLOCK:$sql";
}

sub handle_program_conclusion
{
	my $ar = shift;
	my $sql = join("\n", @$ar);
	return "#PROGRAM ENDED\n";
}

sub finalize_content
{
	my $ar = shift;
	my $options = shift;
	my $indent_start = $options->{db_connect_init_call_index};
	$MR->log_msg("finalize_content: indent_start: $indent_start");
	my $do_indent = 1;
	if ($INDENT_ENTIRE_SCRIPT)
	{
		my $curr_idx = 0;
		foreach (@$ar)
		{
			if ($_ =~ /^$EXCEPTION_BLOCK/)
			{
				$_ =~ s/$EXCEPTION_BLOCK\://;
				$_ =~ s/\n/\n\t/gs;
				$_ = "\t$_";
				$_ = "except snowflake.connector.errors.ProgrammingError as e:\n" . $_;
				$do_indent = 0;
			}
			elsif ($do_indent)
			{
				if ($indent_start >= 0 && $curr_idx == $indent_start)
				{
					$_ = "try:\n$_";
				}
				if ($indent_start >= 0 && $curr_idx >= $indent_start)
				{
					$_ =~ s/\n/\n\t/g;
					$_ = "\t" . $_ if $curr_idx > $indent_start;
				}
			}
			$curr_idx++;
		}
	}
}

sub os_command
{
	my $ar = shift;
	$MR->log_msg("os_command: " . Dumper($ar));
	my $sql = join("\n", @$ar);
	$sql =~ s/\n/ /gs;
	$sql =~ s/\.os\s//i;
	$sql =~ s/\.run file\s//i;
	my $quote = "'";
	$quote = '"""' if $sql =~ /\n/ or $sql =~ /"/ or $sql =~ /'/;
	#return plug_indent("os.system($quote$sql$quote)");
	return plug_indent("dbh.system($quote$sql$quote)");
	#return $sql;
}

sub export_command
{
	my $ar = shift;
	$MR->log_msg("os_command: " . Dumper($ar));
	my $stmt = join("\n", @$ar);
	$stmt =~ s/\n/ /gs;
	if ($stmt =~ /\.EXPORT\s+FILE=(.*)/)
	{
		$stmt = $1;
	}
	#my @tok = $LAN->split_expression_tokens($sql);
	#my ($exec_kw, $stmt, $into, $var) = @tok;
	#$stmt = adjust_expr($stmt);
	$FILE_OUT = $stmt;
	return "#Using file $FILE_OUT for capturing output"
	#return $sql;
}


#check for GOTO statements
sub prescan_code
{
	my $filename = shift;
	print "******** prescan_code $filename *********\n";
	my $ret = {};
	if (! open(IN, $filename) )
	{
		print "Cannot open file $filename - $!\n";
		return $ret;
	}
	while(my $ln = <IN>)
	{

	}

	close(IN);
	return {};
}


# args: CONFIG => $self->{CONFIG}, PARSER => $self, CONTENT => \@all_lines, LANGUAGE => $lan, MR => $tmp_MR }
sub post_conversion_adjustment
{
	my $h = shift;
	my $cfg = $h->{CONFIG};
	my $parser = $h->{PARSER};
	my $cont = $h->{CONTENT};
	print "GOTO STATEMENTS: " . Dumper(\%GOTO_ID) . "\n";
	foreach my $ln (@$cont)
	{
		if ($ln =~ /GOTO_REPLACE_(\w+)/)
		{
			my $label = $1;
			$ln =~ s/GOTO_REPLACE_(\w+)/statement_id = $GOTO_ID{$label}/;
		}
	}

	return $cont;
}

sub test
{
	my $str = "select 'test1 '' test2' || '''another one'";
	print adjust_expr($str);
}

