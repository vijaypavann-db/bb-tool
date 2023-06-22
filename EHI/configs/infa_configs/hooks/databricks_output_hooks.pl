use strict;
use Data::Dumper;
use Common::MiscRoutines;
use DWSLanguage;

no strict 'refs';

my $MR = new Common::MiscRoutines(MESSAGE_PREFIX => 'DBRKS_HOOKS');
my $LAN = new DWSLanguage();
my %CFG = (); #entries to be initialized
my $CFG_POINTER = undef;
my $CONVERTER = undef;
my $INDENT = 0; #keep track of indents
my $INDENT_ENTIRE_SCRIPT = 0;
my $FILENAME = '';
my %PRESCAN = ();
my $STOP_OUTPUT = 0;

my %USE_VARIABLE_QUOTE = ();

# notebook markdown specs
my $nb_COMMAND_sql     = '-- COMMAND ----------';
my $nb_COMMAND_python  = '# COMMAND ----------';

my %conv_cat = (); # Conversion catalog

my $sql_case_num = 0;   # Counter for hiding SQL CASE...END

my $global_indent_count = 0;

# For hiding anything (e.g. hiding comments so that we don't perform conversions on them) 
my $hide_num = 0;
my %hide_hash = ();

my $CATALOG = {};
sub databricks_prescan_wrapper
{
	my $filename = shift;
	$FILENAME = $filename; #save in a global var
	$MR->log_msg("******** databricks_prescan_wrapper $filename *********. CFG: $CFG_POINTER");

	my $ret = {PRESCAN_INFO => \%PRESCAN};

	return $ret;
}

sub init_databricks_hooks #register this function in the config file
{
	my $param = shift;
	%CFG = %{$param->{CONFIG}};
	$CFG_POINTER = $param->{CONFIG}; #give the ability to modify config incrementally
	$CONVERTER = $param->{CONVERTER};
	%USE_VARIABLE_QUOTE = ();

	foreach my $k (keys %$param)
	{
		$MR->log_msg("Init hooks params: $k: $param->{$k}");
	}
	$MR->log_msg("INIT_HOOKS Called. config:\n" . Dumper(\%CFG));

	#Reinitilize vars for when -d option is used:
	$INDENT = 0; #keep track of indents
	%PRESCAN = ();

	if($CFG_POINTER->{catalog_file_path})
	{
        fill_catalog_file($CFG_POINTER->{catalog_file_path});
    }
    
	$ENV{CONFIG} = $param->{CONFIG};
	$ENV{CONFIG}->{FILENAME} = $FILENAME;
}

sub preprocess_for_databricks
{
	my $cont = shift; 
	#my @ret = $MR->deep_copy($cont);
	$MR->log_msg("preprocess_for_databricks");
	# if (defined $CFG_POINTER->{load_files}) #this is done so the global vars are being visible by load_files scripts
	# {
	# 	foreach my $f (@{$CFG_POINTER->{load_files}})
	# 	{
	# 		if (! -s $f)
	# 		{
	# 			$MR->log_error("load_files: File $f does not exist or have 0 size!");
	# 			next;
	# 		}
	#
	# 		# delete $ENV{PRESCAN};
	# 		# delete $ENV{CONFIG};
	#
	# 		my $fc = $MR->read_file_content($f);
	# 		eval($fc);
	# 		my $eval_ret = $@;
	# 		$MR->log_error("load_files: Loading of File $f returned : $eval_ret") if $eval_ret;
	# 	}
	# }
	my $modpack = eval('use DWSModulePacking; new DWSModulePacking(IGNORE_DB => 1);');
	$MR->log_msg("reload_modules Eval returned: $@") if $@;
	$modpack->init();
	if (defined $CFG_POINTER->{load_files}) #this is done so the global vars are being visible by load_files scripts
	{
		foreach my $f (@{$CFG_POINTER->{load_files}})
		{
			if (! -s $f)
			{
				$MR->log_error("load_files: File $f does not exist or have 0 size!");
				next;
			}
			my $fc = $f=~/dwsmod/?$modpack->decode_file($f):$MR->read_file_content($f);
			eval($fc);
			my $eval_ret = $@;
			$MR->log_error("load_files: Loading of File $f returned : $eval_ret") if $eval_ret;
		}
	}


	if (defined $CFG_POINTER->{source_prescan_routine})
	{
		my $prescan_hook = $CFG_POINTER->{source_prescan_routine};
		$MR->log_msg("Executing source specific hook $prescan_hook. MR: $MR");
		%PRESCAN = eval($prescan_hook . '($cont)');
		my $ret = $@;
		if ($ret)
		{
			$MR->log_error("************ EVAL ERROR prescan_hook: $ret ************");
			exit -1;
		}
	}
	if (defined $CFG_POINTER->{source_prescan_widgets})
	{
		my $prescan_hook = $CFG_POINTER->{source_prescan_widgets};
		$MR->log_msg("Executing source specific hook $prescan_hook. MR: $MR");
		%PRESCAN = eval($prescan_hook . '($cont)');
		my $ret = $@;
		if ($ret)
		{
			$MR->log_error("************ EVAL ERROR prescan_hook: $ret ************");
			exit -1;
		}
	}

	# Read the conversion catalog file
	my $conv_catalog = "$ENV{TEMP}/sqlconv_conversion_catalog.txt";
	$conv_catalog = $ENV{CONFIG}->{conversion_catalog_file} if ($ENV{CONFIG}->{conversion_catalog_file});
	my @conversion_catalog = $MR->read_file_content_as_array($conv_catalog);
	foreach my $conv_cat_line (@conversion_catalog)
	{
		# The ":::" separates key from value. It's up to the process that uses the information to extract
		# whatever is needed
		if ($conv_cat_line =~ m{(.*?):::(.*)}) {
			my ($conv_cat_key, $conv_cat_val) = ($1, $2);
			$conv_cat{$conv_cat_key} = $conv_cat_val;
		}
	}

	case_to_if($cont);

	return @$cont;
}
sub case_to_if
{
	my $cont = shift;
	my $cont_string = join("\n", @$cont);

	# Disguise Stored Procedure CASE
	$cont_string =~ s{ ( ; | THEN )                                           # Anchor to ";" or "THEN"
		               ( ((\s*\-\-<<<c_o_m_m_e_n_t:\s+[0-9]+\s*)+)? | \s* )   # Then possible comments or space
		               CASE\b                                                 # Then CASE
	                 }
	                 {$1$2<:SP_C_A_S_E:>}xsig;
	# Disguise SP END CASE
	$cont_string =~ s{\bEND\s+CASE\b}{<:SP_E_N_D_C_A_S_E:>}sig;

	# Hide Regular SQL CASE...END
	$cont_string =~ s{\bCASE\b.*?\bEND\b}{hide_sql_case($&)}esig;

	# Change "WHEN ... =" to "WHEN ... =="
	$cont_string =~ s{(\bWHEN\b.*?\bTHEN\b)}{handle_when_equal($1)}esig;

	# For Searched CASE, convert each first WHEN to an IF (SP_SEARCHED_I_F for now)
	$cont_string =~ s{<:SP_C_A_S_E:>
					  ( ((\s*\-\-<<<c_o_m_m_e_n_t:\s+[0-9]+\s*)+)? | \s* )
					  WHEN\b}
					 {SP_SEARCHED_C_A_S_E$1SP_SEARCHED_I_F}sxgi;

	# Hide the other Searched WHENs (determination of Searched WHEN, as opposed to Simple WHEN, is done in sub)
	$cont_string =~ s{\bWHEN\s+.*?\s+THEN\b}{hide_sp_searched_when($&)}esig;

	# Now convert remaining (i.e. Simple CASE) first WHENs to IF
	$cont_string =~ s{(<:SP_C_A_S_E:>\s*(.*?)\s*)WHEN\b}{$1IF $2 == }sig;

	my @case_subject = ();
	my @cont_array = split(/\n/, $cont_string);

	# Go line-by-line
	foreach my $cont_line (@cont_array)
	{
		$cont_line =~ s{\s+$}{};

		# Save CASE subjects. NOTE: Sometimes we save nothing (""), but we always have to save so
		# that when we hit an END CASE (SP_E_N_D_C_A_S_E) and "pop", we match the "push"
		if ($cont_line =~ m{(<:SP_C_A_S_E:>|SP_SEARCHED_C_A_S_E)\s*(.*)})
		{
			push(@case_subject, $2);
		}

		# Avoid "WHEN [NOT] MATCHED" (happens in "MERGE INTO..." statement)
		next if ($cont_line =~ m{\bWHEN\s+(NOT\s+)?\bMATCHED\b}i);

		# Change Simple WHENs (Searched WHENs are hidden) to ELSEIFs
		# $cont_line =~ s{\bWHEN\b(.*?)\bTHEN\b}{ ELSEIF $case_subject[$#case_subject] == $1 THEN}sig;

		# If we hit and END CASE, then we need to pop the CASE subject off the stack
		if ($cont_line =~ m{<:SP_E_N_D_C_A_S_E:>\s*(.*)})
		{
			pop(@case_subject);
		}
	}

	$cont_string = join("\n", @cont_array);
	$cont_string =~ s{(<:SP_C_A_S_E:>\s*(.*?)\s*)IF\b}{IF}sig;
	$cont_string =~ s{<:SP_E_N_D_C_A_S_E:>(\s*;)?}{END IF;}sig;
	$cont_string =~ s{SP_SEARCHED_I_F}{IF}sig;
	# $cont_string =~ s{SP_SEARCHED_W_H_E_N}{ELSEIF}sig;
	$cont_string =~ s{SP_SEARCHED_C_A_S_E}{}sig;
	$cont_string =~ s{<:SQL_C_A_S_E:([0-9]+)}{$ENV{PRESCAN}->{SQL_CASE}->{$1}}esig;

	# handle uncaught cases
	$cont_string =~ s{\<\:SP_C_A_S_E\:\>}{CASE}sig;
	$cont_string =~ s{SP_SEARCHED_W_H_E_N}{WHEN}sig;

	@$cont = split(/\n/, $cont_string);
}

sub hide_sp_searched_when 
# Hide a "WHEN" in a Searched CASE in a Stored Procedure  
{
	my $when = shift;
	my $check_when = $when;
	$check_when =~ s{'.*?'}{}gs;
	if ($check_when =~ m{=|<|>})
	{
		$when =~ s{\bWHEN\b}{SP_SEARCHED_W_H_E_N}i;
	}
	return $when;
}

sub hide_sql_case
# Hide a regular SQL CASE
{
	my $case = shift;
	$sql_case_num++;
	$ENV{PRESCAN}->{SQL_CASE}->{$sql_case_num} = $case;
	return "\n<:SQL_C_A_S_E:" . $sql_case_num . "\n";
}

sub handle_when_equal
# Convert "=" to "==" in a Stored Procedure CASE "WHEN" clause
{
	my $when = shift;

	# Avoid "WHEN [NOT] MATCHED" (happens in "MERGE INTO..." statement)
	return $when if ($when =~ m{\bWHEN\s+(NOT\s+)?\bMATCHED\b}i);

	$when = hide($when, "'.*?'", '<:literals:>');
	$when =~ s{=}{==};
	$when = unhide($when, '<:literals:>');
	return $when;
}

sub databricks_default_handler
{
	my $ar = shift;
	return '' if $STOP_OUTPUT;
	my $sql = join("\n", @$ar);
	$MR->log_msg("databricks_default_handler: $sql");
	$sql = convert_dml($ar);

	$sql = adjust_statement($sql);

	return $sql;
}

sub adjust_statement
{
	my $sql = shift;

	# Add "ALTER TABLE <table name> ADD CONSTRAINT..." for various things:

	my $constraints = '';
	
	# CHECKs for BETWEENs
	foreach my $table_name (keys %{ $ENV{PRESCAN}->{BETWEEN} }) 
	{
		if ($sql =~ m{\sTABLE\s+$table_name})
		{
			foreach my $col_name (keys %{ $ENV{PRESCAN}->{BETWEEN}->{$table_name} })
			{
				$constraints .= "\nALTER TABLE $table_name ADD CONSTRAINT ${col_name}_RANGE CHECK " 
				             .  "(" . $col_name . " " . $ENV{PRESCAN}->{BETWEEN}->{$table_name}->{$col_name} . ");";
			}
		}
		$constraints .= "\n" if ($constraints);
	}

	# CHECKS for upper case
	foreach my $table_name (keys %{ $ENV{PRESCAN}->{UPPERCASE} }) 
	{
		if ($sql =~ m{\sTABLE\s+$table_name})
		{
			foreach my $col_name (keys %{ $ENV{PRESCAN}->{UPPERCASE}->{$table_name} })
			{
				$constraints .= "\nALTER TABLE $table_name ADD CONSTRAINT ${col_name}_uppercase CHECK " 
				             .  "(" . $col_name . " == upper(" . $col_name . "));";
			}
		}
		$constraints .= "\n" if ($constraints);
	}

	# Other CHECKs on columns
	foreach my $table_name (keys %{ $ENV{PRESCAN}->{COL_CHECKS} }) 
	{
		if ($sql =~ m{\sTABLE\s+$table_name})
		{
			foreach my $col_name (keys %{ $ENV{PRESCAN}->{COL_CHECKS}->{$table_name} })
			{
				$constraints .= "\nALTER TABLE $table_name ADD CONSTRAINT ${col_name}_checks CHECK (\n"
							 .  join(" AND\n",  @{ $ENV{PRESCAN}->{COL_CHECKS}->{$table_name}->{$col_name} }) . "\n);";
			}
		}
	}

	# Primary keys
	foreach my $table_name (keys %{ $ENV{PRESCAN}->{PRIMARY_KEYS} }) 
	{
		if ($sql =~ m{\sTABLE\s+$table_name})
		{
			foreach my $pk_name (keys %{ $ENV{PRESCAN}->{PRIMARY_KEYS}->{$table_name} })
			{
				$constraints .= "\nALTER TABLE $table_name ADD CONSTRAINT ${pk_name}_pk PRIMARY KEY \( $ENV{PRESCAN}->{PRIMARY_KEYS}->{$table_name}->{$pk_name}\);";
			}
		}
	}

	# Foreign keys
	foreach my $table_name (keys %{ $ENV{PRESCAN}->{FOREIGN_KEYS} }) 
	{
		if ($sql =~ m{\sTABLE\s+$table_name})
		{
			foreach my $fk_name (keys %{ $ENV{PRESCAN}->{FOREIGN_KEYS}->{$table_name} })
			{
				$constraints .= "\nALTER TABLE $table_name ADD CONSTRAINT ${fk_name} FOREIGN KEY \( $ENV{PRESCAN}->{FOREIGN_KEYS}->{$table_name}->{$fk_name} \);";
			}
		}
	}

	$sql .= $constraints;
	
	return $sql;
}

sub convert_dml
{
	my $ar = shift;
	my $sql = '';
	if (ref($ar) eq 'ARRAY') 
	{
		$sql = join("\n", @$ar);
	}
	else
	{
		$sql = $ar;
	}
	$MR->log_msg("convert_dml:\n$sql");
	my $ret = $CONVERTER->convert_sql_fragment($MR->trim($sql));
	$ret = $MR->trim($ret);
	return $ret . "\n";
}

#creating dynamic structure to store variables that need to be turned into widgets
sub databricks_sql_widget
{
	my $main = $ENV{CONFIG}->{VAR_DECL};
	my @final = ();
	foreach my $var ( keys %{$ENV{PRESCAN}->{VARIABLES}})
	{
		my $widget = $main; 
		$MR->log_msg("VAR widget: $widget // $var" . Dumper($var));
		$widget =~ s{%NAME%}{$ENV{PRESCAN}->{VARIABLES}->{$var}->{NAME}}ig;
		$widget =~ s{%DEFAULT%}{$ENV{PRESCAN}->{VARIABLES}->{$var}->{DEFAULT_VALUE}}gis;
		$widget =~ s/hiveconf\://gis;
		$MR->log_msg("VAR final widget: $widget");

		push(@final, $widget);
	}
	my $output = join("\n", @final);

	$ENV{WIDGET} = $output;

	return $output;
}

sub databricks_proc_arg_defs
# NOTE: We read the template file here, and create what will be the start of a Notebook.
# THEN we will update the template further in databricks_variable_declarations.
# So we MUST ALWAYS do this subroutine (in order to get the template file), but we
# may or may not have args to populate the template with here.
{
	my $ar = shift;   # This probably only contains "__PROC_DEF_PLACEHOLDER__;"
	$MR->log_msg("databricks_proc_arg_defs");
	#For simple SQL widgets directly in configuration file
	if(!$ENV{CONFIG}->{invoked_notebook_template_file})
	{
		$MR->log_msg("Starting SQL WIDGET");
		my $widgets = databricks_sql_widget();
		return "<:nowrap:>$widgets";
	}

	# Get the template from the file. Everything beyond __END___ is ignored
	my $nb_main = $MR->read_file_content($ENV{CONFIG}->{invoked_notebook_template_file});
	$nb_main =~ s{__END__.*}{}si;

	# Extract (with a substitution) each section into its own template
	my %templates = ();
	foreach my $nb_section ('ARG_DEF', 'ARG_GET', 'VAR_DECL', 'CONTINUE_HANDLER', 'ERROR_HANDLER', 'EXCEPT_BLOCK', 'RESULT_BLOCK') {
		$nb_main =~ s{(<\?$nb_section:> )  (.*?)  ( </$nb_section>       ) } {$1\n$3}xsig  && (        $templates{$nb_section} = $2);
		                                                                                             # $templ{section}{$nb_section}
	}

	# Result and except blocks have inner, reoccurring lines, so we need to extract those from their templates
	my %repeats = ();
	$templates{RESULT_BLOCK} =~ s{ ( <\?SET_RESULT_JSON:> )  (.*?)  ( </SET_RESULT_JSON> ) } {$1\n$3}xsig && ($repeats{SET_RESULT_VAR} = $2);
	$templates{EXCEPT_BLOCK} =~ s{ ( <\?ERROR_HANDLER_CALL:> )  (.*?)  ( </ERROR_HANDLER_CALL> ) } {$1\n$3}xsig && ($repeats{ERROR_HANDLER_CALL} = $2);

	# Remove trailing spaces from these templates
	$templates{ARG_DEF}  =~ s{\s+$}{};
	$templates{ARG_GET}  =~ s{\s+$}{};
	$templates{VAR_DECL} =~ s{\s+$}{};

	# For each Notebook arg, do a "definition" and a "get", if it is an "input" type,
	# and a "set result", if it is (possibly also) an "output" type 
	my %accum = ();  # For accumulating results of repeating thing
	my %final = ();  # For final things, ready to be put back into the main code
	foreach my $arg_def (@{ $ENV{PRESCAN}->{PROC_ARGS} })
	{
		$ENV{PRESCAN}->{ALL_VAR_NAMES}->{uc($MR->trim($arg_def->{NAME}))} = 1;

		if ($arg_def->{ARG_TYPE} eq 'IN' || $arg_def->{ARG_TYPE} eq 'INOUT')
		{
			# Arg definition
			my $new_arg_def = $templates{ARG_DEF};  # $templates{ARG_DEF};
			$new_arg_def =~ s{%ARG_NAME%}{$arg_def->{NAME}}ig;
			$new_arg_def =~ s{%ARG_VALUE%}{}ig;                 # This is always empty?
			$final{ARG_DEF} .= $new_arg_def;

			# Arg "get"
			my $new_arg_get = $templates{ARG_GET};
			$new_arg_get =~ s{%ARG_NAME%}{$arg_def->{NAME}}ig;
			my $data_type = lc($arg_def->{DATA_TYPE});
			$new_arg_get =~ s{%ARG_DATA_TYPE%}{$data_type}ig;
			$final{ARG_GET} .= $new_arg_get;
		}

		# Arg "set result" (so that it can be returned)
		if ($arg_def->{ARG_TYPE} eq 'OUT' || $arg_def->{ARG_TYPE} eq 'INOUT')
		{
			my $new_set_result_var = $repeats{SET_RESULT_VAR};
			$new_set_result_var =~ s{%VAR_NAME%}{$arg_def->{NAME}}ig;
			$accum{SET_RESULT_VAR} .= $new_set_result_var;
		}
	}

	if ($accum{SET_RESULT_VAR})
	{
		$accum{SET_RESULT_VAR} =~ s{,\s*$}{};  # Remove trailing comma
		$templates{RESULT_BLOCK} =~ s{ <\?SET_RESULT_JSON:>   .*? </SET_RESULT_JSON>  }{$accum{SET_RESULT_VAR}}xsi;
		$final{RESULT_BLOCK} = $templates{RESULT_BLOCK};
	}
	else
	{
		$final{RESULT_BLOCK} = '';
	}

	# Gather each variable into a "final" list
	foreach my $var_def (@{$ENV{PRESCAN}->{VARIABLES}})
	{
		$ENV{PRESCAN}->{ALL_VAR_NAMES}->{uc($MR->trim($var_def->{NAME}))} = 1;

		my $new_var_decl = $templates{VAR_DECL};
		$new_var_decl =~ s{%VAR_NAME%}{$var_def->{NAME}}ig;

		my $data_type   = $var_def->{DATA_TYPE};
		my $default_val = $var_def->{DEFAULT_VALUE};

		# Remove surrounding quotes or double-quotes
		$default_val =~ s{^ (['"]) (.*) \1 }{$2}xs;

		# Set Databricks default value according to data type and whether or not a default value is present:
		#                                                |  Default Value 
		#     Data Type                                  |    Present /           | Databricks Default value
		#                      (DEC=decimal)             |  Not Present?          |
		#     -----------------------------------------   ------------------------  ------------------------------------------------------
		if    ($data_type =~ m{^DEC}                   &&    $default_val )      {  $default_val = 'decimal.Decimal(' . $default_val . ')'}
		elsif ($data_type =~ m{^DEC}                   &&  ! $default_val )      {  $default_val = 'decimal.Decimal(0)'}
		elsif (   $MR->is_datatype_number($data_type)  &&    $default_val )      {}   # No action ($default_val stays as-is)
		elsif (   $MR->is_datatype_number($data_type)  &&  ! $default_val )      {  $default_val = '0'   }
		elsif ( ! $MR->is_datatype_number($data_type)  &&    $default_val )      {  $default_val = '"' . $default_val . '"'   } 
		elsif ( ! $MR->is_datatype_number($data_type)  &&  ! $default_val )      {  $default_val = '""'   } 

		# If default value contains line feed(s), then we need to triple-double-quote it
		$default_val = '""' . $default_val . '""' if ($default_val =~ m{\n});

		$new_var_decl =~ s{%VAR_DEFAULT_VALUE%}{$default_val}ig;
		$final{VAR_DECL} .= $new_var_decl;
	}

	# Continue handler
	foreach my $continue_handler (@{$ENV{PRESCAN}->{DECLARE_CONTINUE_HANDLER}})
	{
		my $new_continue_handler = $templates{CONTINUE_HANDLER};
		$continue_handler =~ s{^}{# }mg;  # Comment out the entire thing
		$new_continue_handler =~ s{%DECLARE_CONTINUE%}{$continue_handler};
		$final{CONTINUE_HANDLER} .= $new_continue_handler;
	}

	# Error handler and "except:"
	my $error_handler_num = 0;
	foreach my $exit_handler (@{$ENV{PRESCAN}->{DECLARE_EXIT_HANDLER}})
	{
		my $new_error_handler = $templates{ERROR_HANDLER};
		my $new_error_handler_call = $repeats{ERROR_HANDLER_CALL};
		$error_handler_num++;
		$exit_handler->{CONDITION} =~ s{\n}{ }g; # Want this to fit on a single comment line
		$new_error_handler =~ s{%ERROR_HANDLER_NUM%}{$error_handler_num};
		$new_error_handler_call =~ s{%ERROR_HANDLER_NUM%}{$error_handler_num};
		$new_error_handler =~ s{%ERROR_CONDITION%}{$exit_handler->{CONDITION}};

		# $new_error_handler =~ s{%ERROR_ACTION%}{$exit_handler->{ACTION}};
		my $error_handler_stmts = '';

		# Insert a ";" after a BEGIN so that it goes through the below split separately
		$exit_handler->{ACTION} =~ s{^.*?\bBEGIN\s}{$&;}si;

		# We might have a mixture of SQL statements and SET statements, so they need to
		# be split out and the SQL statements need to be wrapped in "spark.sql..."
		foreach my $stmt (split (/;/, $exit_handler->{ACTION}))
		{
			if ($stmt =~ m{^.*?\bBEGIN\s}si)       # Comment out BEGIN
			{
				my $begin = $&;
				$begin =~ s{^}{# }mg;
				$error_handler_stmts .= "$begin\n";
				next;
			}
			if ($stmt =~ m{^\s*SET\s}si) 
			{
				$stmt =~ s{^\s+}{};

				# Convert: SET X = '...\n...' to: X = """...\n...""", otherwise just remove the "SET"
				$stmt =~ s{\bSET\s+(\w+\s*\=\s*)((['"])(.*?\n.*?)\s*\3)}{$1"""$4"""}si;
				$stmt =~ s{\bSET\s+(\w+\s*\=)}{$1}si;

				$error_handler_stmts .= "  $stmt\n";
				next;
			}
			if ($stmt =~ m{^\s*END\b}si)          # Comment out END
			{
				my $end = $&;
				$end =~ s{^}{# }mg;
				$error_handler_stmts .= "$end\n";
				next;
			}
			# Otherwise (SQL)
			$error_handler_stmts .= '  spark.sql("""' . "\n"
			                     .  "      $stmt\n"
			                     .  '  """.format(' . "\n"
			                     .  '        # SQLSTATE=SQLSTATE' . "\n"
			                     .  '  )).show(truncate = False)' . "\n";
		}
		$error_handler_stmts =~ s{\n\n}{\n}g;
		$new_error_handler =~ s{%ERROR_ACTION%}{$error_handler_stmts};

		$final{ERROR_HANDLER} .= $new_error_handler;
		$accum{ERROR_HANDLER_CALL} .= $new_error_handler_call;
	}

	if ($error_handler_num)
	{
		$nb_main =~ s{%TRY%}{# COMMAND ----------\ntry:\n--<indent++>};
		$templates{EXCEPT_BLOCK} =~ s{<\?ERROR_HANDLER_CALL:> .*? <?ERROR_HANDLER_CALL>}{$accum{ERROR_HANDLER_CALL}}xsi;
		$final{EXCEPT_BLOCK} = $templates{EXCEPT_BLOCK};
	}
	else
	{
		$nb_main =~ s{%TRY%}{};
		$final{EXCEPT_BLOCK} = '';
	}

	# Put the various final sections back into the main template 
	foreach my $nb_section ('ARG_DEF', 'ARG_GET', 'VAR_DECL', 'CONTINUE_HANDLER', 'ERROR_HANDLER', 'EXCEPT_BLOCK', 'RESULT_BLOCK') {
		$nb_main =~ s{ <\?$nb_section:>   .*?  </$nb_section>  } {$final{$nb_section}}xsi;
	}

	# For the bits below %BODY%, we need to put them aside so that the rest of the Notebook can be inserted
	# (elsewhere), so we put these bits into an "and bit"
	$nb_main =~ s{%BODY% (.*) } {}xsi && push (@{ $ENV{PRESCAN}->{NOTEBOOK_END} }, $1);

	return "<:nowrap:>$nb_main";
}

sub databricks_select_into
{
	my $cont = shift;

	# Prevents false matches	
	return $cont if ( ! $ENV{PRESCAN}->{SELECT_INTO});

	my $select_into = shift($ENV{PRESCAN}->{SELECT_INTO});

	#--- Get the INTO cols (Needed for "col1, col2, = spark...")
	my $into_cols = '';
	my $into = $select_into->{INTO};
	$into =~ s{^\s*INTO\s+}{};
	while ($into =~ m{(\w+)}g) {
		my $into_col = $1;
		$into_cols .= "$into_col, ";
		$ENV{PRESCAN}->{ALL_VAR_NAMES}->{uc($MR->trim($into_col))} = 1;
	}

	#--- For the WHERE, conevrt :var to {var} (needed for "WHERE...")
	my $from_where = $select_into->{WHERE};
	$from_where =~ s{:(\w+)}{\{$1\}}g;

	#--- Also need WHERE cols with just plain names for "format(column=var)..."
	my ($format_cols) = $select_into->{WHERE} =~ m{^\s*WHERE\s+(.*)}si;
	$format_cols =~ s{:}{}g;
		
	my $output = $into_cols . '= spark.sql("""' . "\n" .
                $select_into->{SELECT} . "\n" .
                $select_into->{FROM} . "\n" . 
                $from_where  . "\n" .
                '""".format(' . "\n" .
                $format_cols . "\n" .
             ')).first().asDict().values()' . "\n";

    return "<:nowrap:>$output";
}

sub databricks_create_macro
{
	my $cont = shift;

	# Prevents false matches	
	return $cont if ( ! $ENV{PRESCAN}->{MACRO});

	my $macro = shift($ENV{PRESCAN}->{MACRO});

	$macro->{SQL} =~ s{:(\w+)}{\{$1\}}g;

	my @format;
	foreach my $arg (@{ $macro->{ARGS} })
	{
		push(@format, "$arg=$arg");
	}

	my $template = $ENV{CONFIG}->{macro_template};
	$template =~ s{%MACRO_NAME%}{$macro->{MACRO_NAME}};
	$template =~ s{%MACRO_PARMS%}{join(',', @{$macro->{ARGS}})}ex;
	$template =~ s{%MACRO_SQL%}{$macro->{SQL}};
	$template =~ s{%MACRO_FORMAT%}{join(',', @format)}ex;
	return "<:nowrap:>$template";
}

sub databricks_notebook_run
{
	my $cont = shift;
	return $cont unless ($cont =~ m{\bCALL\s+([\w.]+)(.*)}si);
	my ($call_name, $call_args) = ($1, $2);

	# Remove everything except actual arg vals from $call_args
	$call_args =~ s{;\s*$}{};
	$call_args =~ s{^\s*\(}{};
	$call_args =~ s{\)\s*$}{};
	my @call_args = split(/,/, $call_args);
	foreach (@call_args)
	{
		s{^\s+}{};
		s{\s+$}{};
	}

	# Get parm info from conversion catalog data. The catalog entries that we need look like this:
	#    stored_procedure_args,<proc_name>,<arg_num>:::<arg_name>,<arg_io_type>,<arg_data_type>
	# E.g.:
	#    stored_procedure_args,NameLookup,0:::Name,IN,CHAR
	#    stored_procedure_args,NameLookup,1:::FullName,OUT,CHAR
	#    stored_procedure_args,CalcTax,0:::Amt,IN,INTEGER
	#    stored_procedure_args,CalcTax,1:::Tax,OUT,DECIMAL

	# my %parms = ();
	my @resultj_parms = ();     # For gathering IN and INOUT arg types for the "_resultJ" in template
	my @result_parms = ();      # For gathering OUT and INOUT arg types for the "_result" in template
	my @result_out_parms = ();  # For gathering CALL stmt args to use in the "_result" in template
	my $found_call = 0;

	# Find the keys that we need (e.g. "stored_procedure_args,CalcTax,<arg_num>")
	foreach my $key (sort(keys(%conv_cat)))
	{
		if ($key =~ m{^stored_procedure_args,$call_name,(.*)})
		{
			$found_call = 1;
			my $parm_num = $1;

			if ($parm_num eq 'x')    # Means that there are no parms!
			{
				$found_call = 'noparms';
				last;
			}

			# Get the parm name, io type (IN / OUT / INOUT), and data type for the key
			my ($parm_name, $parm_io_type, $parm_data_type) = $conv_cat{$key} =~ m{(.*?),(.*?),(.*)};

			# For IN and INOUT types, save the name and value (value is what is in the CALL statement)
			my $parm = ();
			if ($parm_io_type eq 'IN' or $parm_io_type eq 'INOUT')
			{
				$parm->{NAME}  = $parm_name;
				$parm->{VALUE} = $call_args[$parm_num];
				push (@resultj_parms, $parm);
			}

			# Save same for OUT and INOUT
			if ($parm_io_type eq 'OUT' or $parm_io_type eq 'INOUT')
			{
				push(@result_parms, "\"$parm_name\"");
				push(@result_out_parms, $call_args[$parm_num]);
			}
		}
	}

	# Check to see if we got anything for the CALL name (Could be a call to something OTHER than a stored procedure)
	return $cont if ( ! $found_call);

	# If there are no parms, use a specific template for that
	if ($found_call eq 'noparms')
	{
		my $templ = $ENV{CONFIG}->{notebook_run_noparms_template};
		$templ =~ s{%NOTEBOOK_NAME%}{$call_name}g;

		return "<:nowrap:>$templ";
	}

	# Final _resultJ info
	my @final_resultj_parms;
	foreach my $parm (@resultj_parms)
	{
		push(@final_resultj_parms, "\"$parm->{NAME}\" : str\($parm->{VALUE}\)");
	}

	# Use the saved info to populate the Notebook template
	my $templ = $ENV{CONFIG}->{notebook_run_template};
	$templ =~ s{%NOTEBOOK_NAME%}{$call_name}g;
	$templ =~ s{%IN_PARMS_RESULTJ%}{join(",\n", @final_resultj_parms) . "\n"}eg;
	$templ =~ s{%IN_PARMS_RESULT%}{join(", ", @result_parms)}eg;
	$templ =~ s{%OUT_PARMS%}{join(", ", @result_out_parms) . ','}eg;

	return "<:nowrap:>$templ";
}

sub databricks_set_variable
# Handle a "SET" statement
{
	my $cont = shift;

	my $check_cont = $cont;
	$check_cont =~ s{^\s*\-\-.*}{}mg;                              # Remove comment lines
	return $cont if ( $check_cont =~ m{^\s*(UPDATE|MERGE)\s}si);   # Ignore if the "SET" is in an UPDATE or MERGE
	return $cont if ( $check_cont =~ m{"""\s*(UPDATE|MERGE)\s}si);    # Could be in already-generated """...""" code

	# "from": "^(\s*(THEN\s+|ELSE\s+)?\s*)SET\s+(\w+\s*\=)", "to": "<:nowrap:>$1$3"
	$cont =~ s{\bSET\s+(\w+\s*\=)}{<:nowrap:>$1}is;

	$ENV{PRESCAN}->{ALL_VAR_NAMES}->{uc($MR->trim($1))} = 1;

	return $cont;
}

sub is_all_comments_or_blanks
{
	my $cont = shift;
	# print STDERR "Check for comment / blank: $cont\n";
	$cont =~ s{^\s*\-\-.*}{}mg;
	$cont =~ s{/\*.*?\*/}{}sg;

	if ($cont =~ m{\S})
	{
		# print STDERR "   NOT comment / blank\n";
		return 0;
	}
	else
	{
		# print STDERR "   IS comment / blank\n";
		return 1;
	}
}
sub convert_comment 
{
	my $cont = shift;

	######################################################################
	# We can't convert comments (e.g /* style to -- style) because we 
	# will lose track of masked comments !!!!!!!!!!
	######################################################################

	# $cont =~ s{/\*.*?\*/}{convert_comment2($&)}esg;

	return $cont;
}
sub convert_comment2
{
	my $comment = shift;
	$comment =~ s{^/\*}{};
	$comment =~ s{\*/$}{};
	$comment =~ s{^}{--}mg;
	return $comment;
}
sub convert_comment_oldxxxxxxxxxxxxxxxxx 
{
	my $cont = shift;
	$cont =~ s{/\*}{}mg;
	$cont =~ s{\*/}{}mg;
	$cont =~ s{^}{\-\-}mg;
	return $cont;
}

sub databricks_sample_percent 
{
	my $cont = shift;

	$MR->log_msg("databricks_sample_percent");

	# Report an error in the log AND in oputput if we don't have what we need
	if ( ! $ENV{PRESCAN}->{SAMPLE_PERCENT}) 
	{
		$MR->log_err("*** ERROR: Nothing found for handling \"SAMPLE <percent>\"");
		return "--ERROR: This is not being handled properly:\n$cont\n-- END ERROR\n";
		# return "$ENV{PRESCAN}->{COMMENT_CHAR}ERROR: This is not being handled properly:\n$cont\n$ENV{PRESCAN}->{COMMENT_CHAR} END ERROR\n";
	}

	# Do a *shift* to get *corresponding push* that we did in the source hook routine
	my $sample_percent_info = shift($ENV{PRESCAN}->{SAMPLE_PERCENT});

	# my $output_code = '';

	# Convert something like "SAMPLE 0.5, 0.25 ..." to "TABLESAMPLE (50 PERCENT)
	# LIMITATIONS:
	#    - We can only handle the first arg after "SAMPLE", so would be 0.5 in the above

	$sample_percent_info =~ m{(0)?\.([0-9]+)};
	my $num = "0.$2";
	$num = ( 0 + $num ) * 100;
	return "TABLESAMPLE ($num PERCENT)\n";;
}

sub convert_exponent 
# Convert "<expression1> ** <expression2>" to "POW(<expression1>, <expression2>)"
# Note: <expression> can be either a "word", i.e. \w+, or a balanced set of parens
{
	my $cont = shift;
	my $paren_count = 0;

	# Tag the parens
	$cont =~ s{(\(|\))}{parens($1, $paren_count)}eg;

	# Use tagged parens to find and convert to POW format
	$cont =~ s{ ( <BB_LEFT_PAREN:(\d+)BB_LEFT_PAREN>(.*?)<BB_RIGHT_PAREN:(\2)BB_RIGHT_PAREN>  | \w+ ) \s*\*\*\s*  ( <BB_LEFT_PAREN:(\d+)BB_LEFT_PAREN>(.*?)<BBRIGHT_PAREN:(\6)BB_RIGHT_PAREN>  | \w+ )  }{POW($1, $5)}gx;

	# Clean-up (remove tagged parens)
	$cont =~ s{(<BB_LEFT_PAREN:\d+BB_LEFT_PAREN>)}{\(}g;
	$cont =~ s{(<BB_RIGHT_PAREN:\d+BB_RIGHT_PAREN>)}{\)}g;

	return $cont;
}
sub parens 
# Put tags arouind parens
{

	my $paren = shift;
	my $paren_count = shift;
	my $return = '';
	if ($paren eq '(') 
	{
		$paren_count++;
		$return = '<BB_LEFT_PAREN:' .  $paren_count . "BB_LEFT_PAREN>";
	} 
	else 
	{
		$return = '<BB_RIGHT_PAREN:' .  $paren_count . "BB_RIGHT_PAREN>";
		$paren_count--;
	}
	return $return;
}

sub databricks_testthing1 
# This is a placeholder subroutine for handling "...PLACEHOLDER__"
{
	my $cont = shift;          #  Do we need this? Probably says "__TEST_THING1_PLACEHOLDER__"!!!
	$MR->log_msg("databricks_testthing1");

	# Report an error in the log AND in oputput if we don't have what we need
	if ( ! $ENV{PRESCAN}->{TESTTHING1}) 
	{
		$MR->log_err("*** ERROR: Nothing found for handling \"testthing1\"");
		return "--ERROR: This is not being handled properly:\n$cont\n-- END ERROR\n";
	}

	# Do a *shift* to get *corresponding push* that we did in the source hook routine
	my $testthing1_info = shift($ENV{PRESCAN}->{TESTTHING1});

	my $output_code = '';

	foreach my $x (keys %{ $testthing1_info }) 
	{
		$output_code .= "This is what we did with $x: $testthing1_info->{$x}\n";
	}
	return $output_code;
}

sub databricks_copy_into
{
	my $cont = shift;
	$MR->log_msg("databricks_copy_into");
	my $instance_num = 1 - $#{$ENV{PRESCAN}->{IMPORT}};
	my $import_info = shift($ENV{PRESCAN}->{IMPORT});

	# my $copy_into_template_code = $MR->read_file_content($ENV{CONFIG}->{data_import_template_file});
	my $copy_into_template_code = '';
	if ($ENV{PRESCAN}->{BTEQ_MODE})
	{
		$copy_into_template_code = $MR->read_file_content($ENV{CONFIG}->{copy_into_xsql_template_file});
	}
	else
	{
		$copy_into_template_code = $MR->read_file_content($ENV{CONFIG}->{copy_into_sparksql_template_file});
	}

	$copy_into_template_code =~ s{__END__.*}{}si;  # Remove everything from __END__ onward
	if ( ! $copy_into_template_code) 
	{
		$MR->log_err("\n****************************\n"
		           . "*** ERROR: Cannot convert this source:\n$import_info->{ORIGINAL_SOURCE}"
		           . "*** REASON: Missing attribute \"data_import_template\" in config. Should look something like this: "
			. 'COPY INTO %TABLE_NAME% \nFROM (\n SELECT \n%COLUMN_NAMES%\n FROM %FILE_NAME%\n)\nFILEFORMAT = CSV\nDELIMITER = \"%DELIMITER%\"\n;'
		    . "\n****************************");
	}

	# We need: Table name, File name, "USING" clause, "INSERT" clause, Delimiter
	my $bad_copy_into = 0;
	foreach my $attribute ('TABLE_NAME', 'FILE_NAME')   # Took out 'INSERT' and 'DELIMITER': won't be present for "IMPORT DATA". Also 'USING'
	{
		if ( ! $import_info->{$attribute}) 
		{
			# report as error
			$MR->log_err("Attribute \"$attribute\" not found during generation of COPY INTO");
			$bad_copy_into = 1;
		}
	}
	if ($bad_copy_into) 
	{
		$copy_into_template_code = "--FIXME Found unsupported source configuration during generation of COPY INTO.\n"
		                  . "/* Original source:\n"
		                  . $import_info->{ORIGINAL_SOURCE}
		                  . "*/\n";
		return $copy_into_template_code;
	} 
	# Foreach "attribute" (e.g. "TABLE_NAME", "FILE_NAME", etc) in $ENV{{PRESCAN}->{IMPORT}->{<attribute>}
	foreach my $attribute (keys(%{ $import_info })) 
	{
		# Convert the matching %<attribute>% values in the template, e.g. %TABLE_NAME% changes to value of $ENV{{PRESCAN}->{IMPORT}->{TABLE_NAME}
		$copy_into_template_code =~ s{%$attribute%}{$import_info->{$attribute}}g;
	}

	my @select_cols = get_infile_select_cols($import_info->{INFILE_COLS}, $import_info->{MAP_COL_NAMES}, 
								$import_info->{DELIMITER}, $import_info->{VALUES_BY_COL_NAME});
	$import_info->{DELIMITER} ? $copy_into_template_code =~ s{%FILE_FORMAT%}{CSV} 
							  : $copy_into_template_code =~ s{%FILE_FORMAT%}{PARQUET};

	# Convert 
	my $select_cols_str = convert_dml(join(",\n", @select_cols));

	# Remove tagging info for conditional tags, e.g.: <?sometag:>...</sometag>
	foreach my $optional_tag ('DELIMITER', 'BAD_RECS_PATH')
	{
		if ($import_info->{$optional_tag})   # If the tag was used then delete the conditional tags
		{
			$copy_into_template_code =~ s{<\?$optional_tag:>}{}g;
			$copy_into_template_code =~ s{</$optional_tag>}{}g;
		} 
		else                                 # The tag was not used, so delete everything between the conditional tags
		{
			$copy_into_template_code =~ s{<\?$optional_tag:>.*?</$optional_tag>}{}sg;
		}
	}

	$copy_into_template_code =~ s{%COLUMN_NAMES%}{$select_cols_str};

	# Remove code that is conditional on a contained %...% tag not being set. e.g. if we have this:
	#    OPTIONS: <?MYTAG:> X=1 Y=2 SOMEVALUE=%MYTAG%, %OTHER_TAG%</MYTAG> END
	# And %MYTAG% gets left as %MYTAG% (i.e. not converted to anything), then what's left will be:
	#    OPTIONS:  END
	$copy_into_template_code =~ s{<\?(\w+):>.*?%\1%.*?</\1>}{}g;

	# Clean up left-over comma (comma before right paren)
	$copy_into_template_code =~ s{,(\s*\))}{$1};

	$copy_into_template_code =~ s{%NUM%}{$instance_num}g;

	# Report any unused %...% tags 
	if ($copy_into_template_code =~ m{\%\w+\%}) 
	{
		$MR->log_err("Warning: Unused \"\%...\%\" tag(s) in data_import_template in config resulted in this output:\n  $copy_into_template_code");

	} 
	# return $copy_into_template_code;
	return "<:nowrap:>$copy_into_template_code";
}

sub databricks_merge_into
# For doing MERGE INTO (although we could end up doing COPY INTO)
{
	my $cont = shift;
	$MR->log_msg("databricks_copy_into");
	my $instance_num = 1 - $#{$ENV{PRESCAN}->{MLOAD}};
	my $merge_info = shift($ENV{PRESCAN}->{MLOAD});
	my $merge_into_template_code = '';
	if ($ENV{PRESCAN}->{BTEQ_MODE})
	{
		$merge_into_template_code = $MR->read_file_content($ENV{CONFIG}->{merge_into_xsql_template_file});
	}
	else
	{
		$merge_into_template_code = $MR->read_file_content($ENV{CONFIG}->{merge_into_sparksql_template_file});
	}
	$merge_into_template_code =~ s{__END__.*}{}si;  # Remove everything from __END__ onward

	# "Global" things (i.e. we don't need to drill down to find them, although they might end up not coming out)
	$merge_into_template_code =~ s{%NUM%}{$instance_num}g;
	$merge_into_template_code =~ s{%FILE_NAME%}{$merge_info->{IMPORT}->{INFILE_NAME}}g;
	# $merge_into_template_code =~ s{%DELIMITER%}{"$merge_info->{IMPORT}->{DELIMITER}"}g;
	$merge_info->{IMPORT}->{DELIMITER} ? $merge_into_template_code =~ s{%FORMAT%}{CSV} 
	                                   : $merge_into_template_code =~ s{%FORMAT%}{PARQUET};

	# Don't think we need this
	my $merge_into_template_code_orig = $merge_into_template_code;  # Save template for re-use

	# Get all sections into separate merge-type-specific code blocks, deleting them from the main template.
	# Then as we check for each "APPLY" operation and change its template code, we will put the changed template code back 
	# into the main template, appending to the main template as we go
	my ($view_for_merge_template, $merge_upsert_template, $merge_copy_into_template, $merge_update_template, $merge_delete_template) = ('','','','','');
	# $merge_into_template_code =~ s{\n\s*<\?VIEW_FOR_MERGE:> (.*) </VIEW_FOR_MERGE>} {}xsig && ($view_for_merge_template  = $1);
	$merge_into_template_code =~ s{\n\s*<\?MERGE_UPSERT:>   (.*) </MERGE_UPSERT>  } {}xsig && ($merge_upsert_template    = $1);
	$merge_into_template_code =~ s{\n\s*<\?COPY_INTO:>      (.*) </COPY_INTO>     } {}xsig && ($merge_copy_into_template = $1);
	$merge_into_template_code =~ s{\n\s*<\?MERGE_UPDATE:>   (.*) </MERGE_UPDATE>  } {}xsig && ($merge_update_template    = $1);
	$merge_into_template_code =~ s{\n\s*<\?MERGE_DELETE:>   (.*) </MERGE_DELETE>  } {}xsig && ($merge_delete_template    = $1);

	# For each "APPLY" we will do a MERGE or COPY INTO
	my $need_view = 0;
	foreach my $apply ( @{ $merge_info->{IMPORT}->{APPLY} })
	{
		my $apply_op   = $apply->{DML_LABEL};
		my $apply_cond = $apply->{COND};

		# ** UPSERT ** - An INSERT AND an UPDATE become an UPSERT (WHEN MATCHED THEN UPDATE...WHEN NOT MATCHED THEN INSERT...)
		if ($merge_info->{DML_LABEL}->{$apply_op}->{INSERT} && $merge_info->{DML_LABEL}->{$apply_op}->{UPDATE})
		{
			my $new_merge_upsert_template = $merge_upsert_template;  # Create a new copy of the upsert template

			#---------- "SELECT" 
			my @select_cols = get_infile_select_cols($merge_info->{LAYOUT}->{INFILE_COLS}, $merge_info->{DML_LABEL}->{$apply_op}->{MAP_COL_NAMES},
						$merge_info->{IMPORT}->{DELIMITER}, $merge_info->{DML_LABEL}->{$apply_op}->{INSERT}->{VALUES_BY_COL_NAME});

			my $select_cols_str = convert_dml(join(",\n", @select_cols));

			$new_merge_upsert_template =~ s{%TITLE%}{$apply_op};
			$new_merge_upsert_template =~ s{%TABLE_NAME%}{$merge_info->{DML_LABEL}->{$apply_op}->{TABLE_NAME}};
			$new_merge_upsert_template =~ s{%COLUMN_NAMES%}{$select_cols_str};

			#---------- "ON"
			# Get the MERGE "ON" clause from the UPDATE's WHERE clause
			my ($merge_on)     = $merge_info->{DML_LABEL}->{$apply_op}->{UPDATE}->{SQL} =~ m{\sWHERE\s+ (.*?) ( ; | $ )}sxi;

			# Prefix the WHERE column names with "t." (target) or "s." (source)
			$merge_on =~ s{(\w+\s*=)}{t.$1}g;
			$merge_on =~ s{:}{s.}g;

			$new_merge_upsert_template =~ s{%ON%}{$merge_on};

			#---------- "WHEN MATCHED THEN UPDATE"
			# Get the "WHEN MATCHED THEN UPDATE SET..." from the UPDATE's SET clause
			my ($merge_update) = $merge_info->{DML_LABEL}->{$apply_op}->{UPDATE}->{SQL} =~ m{\sSET\s+   (.*?) ( ; | WHERE | $ )}sxi;

			# Prefix the UPDATE source column names with "s." (source)
			$merge_update =~ s{:}{s.}g;

			$new_merge_upsert_template =~ s{%UPDATE_SET%}{$merge_update};

			#---------- "WHEN NOT MATCHED THEN
			#            INSERT"
			# Get the "WHEN NOT MATCHED THEN INSERT..." from the INSERT's column names
			my ($merge_insert) = $merge_info->{DML_LABEL}->{$apply_op}->{INSERT}->{SQL} =~ m{\bINSERT\s+INTO\s+[\w.]+\s*\( (.*?) \)}sxi;
			$new_merge_upsert_template =~ s{%INSERT_COLUMN_NAMES%}{$merge_insert};

			#---------- "VALUES"
			# Get the "WHEN NOT MATCHED THEN INSERT... VALUES..." (the VALUES... part) from the INSERT's VALUES clause
			my ($merge_values) = $merge_info->{DML_LABEL}->{$apply_op}->{INSERT}->{SQL} =~ m{\bVALUES\s*\( (.*?) \)}sxi;

			# Prefix the VALUES column names with "s." (source)
			$merge_values =~ s{:}{s.}g;

			$new_merge_upsert_template =~ s{%INSERT_VALUES%}{$merge_values};

			# Put the modified code back into the main template. We also need the VIEW template code
			$merge_into_template_code .= $new_merge_upsert_template;
			$need_view = 1;
		}

		# An INSERT on its own becomes a COPY INTO
		elsif ($merge_info->{DML_LABEL}->{$apply_op}->{INSERT})
		{
			my $new_merge_copy_into_template = $merge_copy_into_template;

			#---------- "SELECT" 
			my @select_cols = get_infile_select_cols($merge_info->{LAYOUT}->{INFILE_COLS}, $merge_info->{DML_LABEL}->{$apply_op}->{MAP_COL_NAMES},
						$merge_info->{IMPORT}->{DELIMITER}, $merge_info->{DML_LABEL}->{$apply_op}->{INSERT}->{VALUES_BY_COL_NAME});

			# Convert 
			my $select_cols_str = convert_dml(join(",\n", @select_cols));

			$new_merge_copy_into_template =~ s{%TITLE%}{$apply_op};
			$new_merge_copy_into_template =~ s{%TABLE_NAME%}{$merge_info->{DML_LABEL}->{$apply_op}->{TABLE_NAME}};
			$new_merge_copy_into_template =~ s{%COLUMN_NAMES%}{$select_cols_str};

			$merge_info->{IMPORT}->{DELIMITER} ? $new_merge_copy_into_template =~ s{%FILE_FORMAT%}{CSV} 
			                                   : $new_merge_copy_into_template =~ s{%FILE_FORMAT%}{PARQUET};

			# Put the modified code back into the main template
			$merge_into_template_code .= $new_merge_copy_into_template;
		}

		# An UPDATE on its own becomes a MERGE...WHEN MATCHED THEN UPDATE
		elsif ($merge_info->{DML_LABEL}->{$apply_op}->{UPDATE})
		{
			my $new_merge_update_template = $merge_update_template;  # Create a new copy of the update template

			#---------- "SELECT" 
			my @select_cols = get_infile_select_cols($merge_info->{LAYOUT}->{INFILE_COLS}, $merge_info->{DML_LABEL}->{$apply_op}->{MAP_COL_NAMES},
						$merge_info->{IMPORT}->{DELIMITER}, $merge_info->{DML_LABEL}->{$apply_op}->{INSERT}->{VALUES_BY_COL_NAME});

			my $select_cols_str = convert_dml(join(",\n", @select_cols));

			$new_merge_update_template =~ s{%TITLE%}{$apply_op};
			$new_merge_update_template =~ s{%TABLE_NAME%}{$merge_info->{DML_LABEL}->{$apply_op}->{TABLE_NAME}};
			$new_merge_update_template =~ s{%COLUMN_NAMES%}{$select_cols_str};

			# The APPLY operation points to the WHERE condition
			my $select_where = $apply_cond;

			$new_merge_update_template =~ s{%WHERE%}{$select_where};

			#---------- "ON"
			# Get the MERGE "ON" clause from the UPDATE's WHERE clause
			my ($merge_on)     = $merge_info->{DML_LABEL}->{$apply_op}->{UPDATE}->{SQL} =~ m{\sWHERE\s+ (.*?) ( ; | $ )}sxi;

			# Prefix the WHERE column names with "t." (target) or "s." (source)
			$merge_on =~ s{(\w+\s*=)}{t.$1}g;
			$merge_on =~ s{:}{s.}g;

			$new_merge_update_template =~ s{%ON%}{$merge_on};

			#---------- "WHEN MATCHED THEN UPDATE"
			# Get the "WHEN MATCHED THEN UPDATE SET..." from the UPDATE's SET clause
			my ($merge_update) = $merge_info->{DML_LABEL}->{$apply_op}->{UPDATE}->{SQL} =~ m{\sSET\s+   (.*?) ( ; | WHERE | $ )}sxi;

			# Prefix the UPDATE source column names with "s." (source)
			$merge_update =~ s{:}{s.}g;

			$new_merge_update_template =~ s{%UPDATE_SET%}{$merge_update};

			# Put the modified code back into the main template. We also need the VIEW template code
			$merge_into_template_code .= $new_merge_update_template;
			$need_view = 1;
		}

		# A DELETE becomes a MERGE...WHEN MATCHED THEN DELETE
		elsif ($merge_info->{DML_LABEL}->{$apply_op}->{DELETE})
		{
			my $new_merge_delete_template = $merge_delete_template;  # Create a new copy of the delete template

			#---------- "SELECT" 
			my @select_cols = get_infile_select_cols($merge_info->{LAYOUT}->{INFILE_COLS}, $merge_info->{DML_LABEL}->{$apply_op}->{MAP_COL_NAMES},
						$merge_info->{IMPORT}->{DELIMITER}, $merge_info->{DML_LABEL}->{$apply_op}->{INSERT}->{VALUES_BY_COL_NAME});

			my $select_cols_str = convert_dml(join(",\n", @select_cols));

			$new_merge_delete_template =~ s{%TITLE%}{$apply_op};
			$new_merge_delete_template =~ s{%TABLE_NAME%}{$merge_info->{DML_LABEL}->{$apply_op}->{TABLE_NAME}};
			$new_merge_delete_template =~ s{%COLUMN_NAMES%}{$select_cols_str};

			# The APPLY operation points to the WHERE condition
			my $select_where = $apply_cond;
			$new_merge_delete_template =~ s{%WHERE%}{$select_where};

			#---------- "ON"
			# Get the MERGE "ON" clause from the DELETE's's WHERE clause
			my ($merge_on)     = $merge_info->{DML_LABEL}->{$apply_op}->{DELETE}->{SQL} =~ m{\sWHERE\s+ (.*?) ( ; | $ )}sxi;

			# Prefix the WHERE column names with "t." (target) or "s." (source)
			$merge_on =~ s{(\w+\s*=)}{t.$1}g;
			$merge_on =~ s{:}{s.}g;

			$new_merge_delete_template =~ s{%ON%}{$merge_on};

			# Put the modified code back into the main template. We also need the VIEW template code
			$merge_into_template_code .= $new_merge_delete_template;
			$need_view = 1;
		}
	}

	# If we need the CREATE VIEW... (which we will if we created any MERGEs)
	if ($need_view)
	{
		if ($merge_info->{IMPORT}->{DELIMITER})     # Need the options related to CSV type processing
		{
			$merge_into_template_code =~ s{%DELIMITER%}{"$merge_info->{IMPORT}->{DELIMITER}"};

			# Don't leave blank lines
			$merge_into_template_code =~ s{^\s*<\?DELIMITER:>\s*\n}{}mig;  
			$merge_into_template_code =~ s{^\s*</DELIMITER>\s*\n}{}mig;

			# ...Othwerwise
			$merge_into_template_code =~ s{<\?DELIMITER:>}{}ig;
			$merge_into_template_code =~ s{</DELIMITER>}{}ig;
		}
		else                                        # Don't need the options related to CSV type processing
		{
			$merge_into_template_code =~ s{^\s*<\?DELIMITER:>.*?</DELIMITER>\s*\n}{}sig;   # Don't leave blank lines
			$merge_into_template_code =~ s{<\?DELIMITER:>.*?</DELIMITER>}{}sig;            # ...Otherwise
		}
		$merge_into_template_code =~ s{<\?VIEW_FOR_MERGE:>}{}ig;
		$merge_into_template_code =~ s{</VIEW_FOR_MERGE>}{}ig;

	}
	else  # Don't need the VIEW
	{
		$merge_into_template_code =~ s{\n\s*<\?VIEW_FOR_MERGE:> .* </VIEW_FOR_MERGE>}{}xsig;
	}

	return "<:nowrap:>" . $merge_into_template_code;
}

sub get_infile_select_cols
# Create a SELECT columns list by traversing the column defs that are passed (these are cols in def of an input file),
# figuring out (using the passed col names mapping and values) how to construct each returned column def.
# We also need to create column defs differently for delimited files vs not (Parquet)
{
	my $infile_column_defs = shift;      # From an input file layout
	my $map_col_names = shift;           # Map cols in INSERT to cols in VALUES / Input file
	my $delimiter = shift;               # Indicates whether input file is CSV type or not  (Parquet)
	my $values_by_col_name = shift;      # Hash of VALUES-col-name => VALUES-col-def (as opposed to VALUES array)
	
	my @select_cols = ();  # We will create and return this

	my $csv_col_num = 0;
	# Process columns in order of how they appear in the original input file layout
	foreach my $col_num (sort {$a <=> $b} (keys(%{ $infile_column_defs })))
	{ 
		my $infile_col_name = $infile_column_defs->{$col_num}->{COL_NAME};
		my $insert_col_name = $map_col_names->{$infile_col_name};
		if ($delimiter) 
		{
			# push(@select_cols, "$values_by_col_name->{$infile_col_name} as " . $insert_col_name);
			# The column order for delimited data is just _c0, _c1, etc
			# push(@select_cols, "_c$csv_col_num as " . $insert_col_name);

			# ... but we still need the info from $values_by_col_name, because it might contain something like 
			# "FORMAT (...)"
			my $select_col = $values_by_col_name->{$infile_col_name};
			$select_col =~ s{\b$infile_col_name\b}{_c$csv_col_num};
			push(@select_cols, "$select_col as $insert_col_name");

			$csv_col_num++;
		}
		else
		{
			my $select_col = $infile_column_defs->{$col_num}->{COL_NAME};
			my %casts_convert = (INTEGER => 'INTEGER', DATE => 'DATE', BYTE => 'BINARY', VARBYTE => 'BINARY', BLOB => 'BINARY', 
				                 CLOB  => 'STRING', BYTEINT => 'BYTE', 'LONG\s+VARCHAR' => 'STRING');
			foreach my $cast_type (keys %casts_convert)
			{
				if ($infile_column_defs->{$col_num}->{DATA_TYPE} =~ m{^$cast_type$}i)
				{
					$select_col .= "::$casts_convert{$cast_type} as " . $infile_column_defs->{$col_num}->{COL_NAME};
				}
			}
			push(@select_cols, $select_col);
		}
	}
	return @select_cols;
}

sub databricks_insert_overwrite
{
	my $cont = shift;
	$MR->log_msg("databricks_insert_overwrite");
	my $instance_num = 1 - $#{$ENV{PRESCAN}->{EXPORT}};
	my $export_info = shift($ENV{PRESCAN}->{EXPORT});

	my $insert_overwrite_template_code = $MR->read_file_content($ENV{CONFIG}->{insert_overwrite_template_file});
	$insert_overwrite_template_code =~ s{__END__.*}{}si;  # Remove everything from __END__ onward

	# Convert 
	my $select_statement = convert_dml($export_info->{SELECT_STATEMENT});

	# Foreach "attribute" (e.g. "TABLE_NAME", "FILE_NAME", etc) in $ENV{{PRESCAN}->{EXPORT}->{<attribute>}
	foreach my $attribute (keys(%{ $export_info })) 
	{
		# Convert the matching %<attribute>% values in the template, e.g. %TABLE_NAME% changes to value of $ENV{{PRESCAN}->{EXPORT}->{TABLE_NAME}
		$insert_overwrite_template_code =~ s{%$attribute%}{$export_info->{$attribute}}g;
	}

	$insert_overwrite_template_code =~ s{%NUM%}{$instance_num}g;

	return "<:nowrap:>$insert_overwrite_template_code";
}

#Subroutine for SQL dks to push widget at top of output before calling databricks_finalize_code to add notebook divisor
sub databricks_finalize_sql
{
	my $ar = shift;
	my $options = shift;

	$MR->log_msg("STARTING DATABRICKS FINALIZE SQL: " . Dumper($ar) . "OPtions: " . Dumper($options) . "\nWIDGETS: " . Dumper($ENV{WIDGET}));

	# $ar = $ENV{WIDGET} . $ar;
	# Insert "CREATE WIDGET..." code as first element in $ar ARRAY 
	unshift($ar, $ENV{WIDGET});
	databricks_finalize_code($ar,$options);
	return $ar; 
}

sub databricks_finalize_code
{
	my $ar = shift;
	my $options = shift;

	return unless  $CFG_POINTER->{use_notebook_md};
	return unless $CFG_POINTER->{consolidate_nb_statements};

	# If the output code has any "Notebook Initialize" blocks, create a Notebook cell
	# for them at the top of the code and move them there. This ensures that all
	# widgets (dbutils.widgets.text(...) are set in one logical place
	if (grep(/#\s*BEGIN\s+Notebook\s+Init/i, @{$ar}))                     # If any "Notebook Init"s...
	{
		unshift(@{$ar}, '<:nowrap:>');                                    # ...insert a new first cell
		foreach my $frag (@{$ar})
		{
			while ($frag =~ s{\#\s*BEGIN\s+Notebook\s+Init.*?\n           # ...remove "Notebook init"
				               (.*?)
				              \#\s*END\s+Notebook\s+init.*?\n}{}xsig)     # block, and...
			{
				$ar->[0] .= $1;                                           # ... add the bits between
			}                                                             # to the first cell
		}
	}

	my @new_stmt_array = ();
	# Put multi-line(frag) comments into one fragment
	my @comment_block = ();
	my $comment_block = '';
	foreach my $fr(@$ar) {
		if (is_all_comments_or_blanks($fr)) {
			push(@comment_block, $fr)
		} else {
			if (@comment_block) {
				my $comment_block = join(' ', @comment_block);
				push (@new_stmt_array, $comment_block);
				@comment_block = ();
				push (@new_stmt_array, $fr);
			} else {
				push (@new_stmt_array, $fr);
			}
		}
	}
	if (@comment_block) {
		my $comment_block = join(' ', @comment_block);
		push (@new_stmt_array, $comment_block);
	}

	while (scalar(@$ar) >= 1) {shift(@$ar);} #blank out the array.  Can't assign a new array, bc it is passed by ref

	foreach my $fr (@new_stmt_array)
	{
		my $final_fr = databricks_wrap_statement($fr);
		push(@$ar, $final_fr);
	}

	# Change array: 
	#    n:   --FIXME...
	#    n+1: (\-\-|#)\s*COMMAND ---------
	# to    
	#    n+1: --FIXME...
	#         $1 COMMAND ----------
	my $count = 0;
	foreach my $fr (@$ar)
	{
		# If this element is FIXME and next element is -- COMMAND
		if ($fr =~ m{^(\s*\-\-\s*)FIXME(.*)}m && $ar->[$count + 1] =~ m{^(\s*\-\-|\#)\s*COMMAND\s*\-+})
		{
			# Remove FIXME from this element
			$fr =~ s{^(\s*\-\-\s*)FIXME(.*)}{}m;
			my $fixme = "$1<:fixme:>$2";

			# ...and add it to beginning of next element
			$ar->[$count + 1] =~ s{^((\s*\-\-|\#)\s*COMMAND\s*\-+)}{$1\n$fixme\n};
		}
		$count++;
	}

	foreach (@$ar) {
		s{<:fixme:>}{FIXME}g;
	}


	if ($ENV{CONFIG}->{global_substitutions}) 
	{
		foreach my $fr (@$ar) 
		{
			foreach my $gsub (@{ $ENV{CONFIG}->{global_substitutions}  })
			{
				if ($gsub->{extension_call})
				{
					if ($fr =~ m{$gsub->{from}})
					{
						my $subroutine_name = substr($gsub->{extension_call}, 2);
						if (exists &{$subroutine_name})
						{
							my $replacement = &$subroutine_name($gsub->{from});
							$fr =~ s{$gsub->{from}}{$replacement};
						}
						else
						{
							$MR->log_error("\n**************** ERROR ***************\n"
							             . "extension_call \"$gsub->{extension_call}\" does not exist\n"
							             . Dumper($gsub) );
						}
					}
				}
				else
				{
					my $eval_gsub = "my \$gsub_count = 0; while (\$fr =~ s{$gsub->{from}}{$gsub->{to}}sgi) {die \"Global substitution stuck in loop!!\" if \$gsub_count++ > 1000}";
					eval ($eval_gsub);
					my $ret = $@;
					if ($ret)
					{
						$MR->log_error("************ EVAL ERROR in global substitution: $ret ************");
						$MR->log_error("*** Failing eval code: $eval_gsub");
						$MR->log_error("*** Input to substitution (\$fr): $fr\n");
						exit -1;
					}
				}
			}
		}
	}

	# Convert "--<indent..." to actual indent spaces
	my $indent_count = 0;
	my $indent_spaces = '';
	foreach my $fr (@$ar) 
	{
		my $new_fr = '';
		foreach my $line (split(/(\n)/, $fr))
		{
			next if ($line =~ m{(s_p_i_f|s_p_e_n_d_i_f)});   # Remove these tags

			# We can't have separate Notebook Cells ("# COMMAND...") in indented code
			if ($indent_count > 0)
			{
				next if ($line =~ m{^\s*(\#|\-\-)\s*COMMAND\b});
			}
		   if ($line =~ m{\-\-<indent\+\+>})           # Indent more
		   {
		      $indent_count++;
		      next;
		      # $line =~ s{<indent\+\+>}{};
		   } 
		   elsif ($line =~ m{\-\-<indent\-\->})        # Indent less
		   {
		      $indent_count--;
		      next;
		   } 
		   elsif ($line =~ m{\-\-<indent=0>})            # No indent (back to first column)
		   {
		      $indent_count = 0;
		      # $line =~ s{<indent=0>}{};
		      next;
		   }
		   $indent_spaces = '  ' x $indent_count;      # Each indent is two spaces
		   $line = $indent_spaces . $line;
		   $new_fr .= $line;
		}
		$fr = $new_fr;
	}

	# Do changes in reverse order, ***with NO "g" modifier***, deleting, to avoid dups
	foreach my $fr (reverse(@$ar)) 
	{
		# Added this loop to do in reverse (necessary?) order of key number in comments hash
		my $numkeys = scalar(keys(%{$ENV{PRESCAN}->{COMMENTS}}));
		foreach my $keynum (reverse(1..$numkeys)) {

			if ($fr =~ m{\-\-<<<c_o_m_m_e_n_t:})
			{
				# Convert masked comments marked as "<:sql_comment:>" back to orig "--" 
				$fr =~ s{\-\-<<<c_o_m_m_e_n_t:\s+($keynum)<:sql_comment:>}{$ENV{PRESCAN}->{COMMENTS}->{$1}}
				                                                 && delete($ENV{PRESCAN}->{COMMENTS}->{$1});

				# Convert remaining masked comments to "--" or "#" style, depending on SQL wrapper
				if ($ENV{PRESCAN}->{use_sql_statement_wrapper})
				{
					$fr =~ s{\-\-<<<c_o_m_m_e_n_t:\s+($keynum)(?=[^0-9]|$ )}{\# <:pycomment:> $ENV{PRESCAN}->{COMMENTS}->{$1}}x
					                                                   && delete($ENV{PRESCAN}->{COMMENTS}->{$1});
					$fr =~ s{\# <:pycomment:> }{\# }g;
				}
				else
				{
					$fr =~ s{\-\-<<<c_o_m_m_e_n_t:\s+($keynum)(?=[^0-9]|$ )}{$ENV{PRESCAN}->{COMMENTS}->{$1}}x
					                                 && delete ($ENV{PRESCAN}->{COMMENTS}->{$1});
				}
			} 
		}
	}
	# Restore C single-line comments (these can'y be done in reverse order)
	foreach my $fr (@$ar) 
	{
		if ($fr =~ m{/\*<<<c_o_m_m_e_n_t:})
		{
			$fr =~ s{/\*<<<c_o_m_m_e_n_t:\s+([0-9]+)\*/}{$ENV{PRESCAN}->{COMMENTS_C_SINGLE_LINE}->{$1}}g
			                                   && delete($ENV{PRESCAN}->{COMMENTS_C_SINGLE_LINE}->{$1});
		}
	}

	foreach my $fr (@$ar) 
	{
		# Remove any extraneous <:sql_comment:> tags (Happens if we add comments like --FIXME, which are not masked)
		$fr =~ s{<:sql_comment:>}{}g;

		# Remove other extraneous comments (these are erroneous dups)
		$fr =~ s{^.*?<<<c_o_m_m_e_n_t:.*?(\n|$ )}{}xmg;
                                         #^^^^^^ Needs to be (\n|$) at end. Else we end up w/ some blanmk lines
	}

	# try: ... except: ... must all be in one Notebook cell, i.e., no "# COMMAND ----------" dividers allowed
	my $got_a_try = 0;
	foreach my $fr (@$ar) 
	{
		if ($fr =~ m{\# COMMAND ----------\n\s*try:})
		{
			$got_a_try = 1;   # We will start removing "# COMMAND ----------" dividers from now on
			next;
		}
		if ($got_a_try)
		{
			$fr =~ s{^\s*\# COMMAND ----------\s*\n}{}mg;
		}
	}

	# Append any end bits
	push(@$ar, @{ $ENV{PRESCAN}->{NOTEBOOK_END} }) if ($ENV{PRESCAN}->{NOTEBOOK_END});

	# Convert all variable names to upper case
	#--------------------- Not doing this now ---------------------
	# foreach my $var_name (keys %{ $ENV{PRESCAN}->{ALL_VAR_NAMES} })
	# {
	# 	foreach my $fr (@$ar) 
	# 	{
	# 		$fr =~ s{\b$var_name\b}{adjust_var_name($var_name, $&, $`, $')}eig;
	# 	}
	# }

	if ($ENV{PRESCAN}->{use_sql_statement_wrapper})
	{
		$ar->[0] = "$ENV{CONFIG}->{python_header}\n" . $ar->[0];
		$CFG_POINTER->{target_file_extension} = 'py';
	}
	else
	{
		$CFG_POINTER->{target_file_extension} = 'sql';
	}

	# Adjust comment symbol
	foreach my $fr (@$ar) 
	{
		if ($CFG_POINTER->{target_file_extension} eq 'py')
		{
			$fr =~ s{/\*(.*?)\*/}{#$1}mg;
			$fr =~ s{\-\-(.*?)}{#$1}mg;
			$fr =~ s{\# COMMAND \#\#\#\#\#}{# COMMAND ----------}mg;
		}
		else
		{
			# $fr =~ s{<\# comments>}{ < -- comments >}    # (Not sure if we need this)
		}
	}

	my $whole_cont = join("\nzxasqwzxasqw\n", @$ar);  # Join on a unique marker

	$whole_cont =~ s{<<<:INSERT_OVERWRITE_TABLE:>>>}{INSERT OVERWRITE TABLE}g;

	# SQL comments inside Spark """ strings need to be (possibly converted BACK to) "--" format
	$whole_cont =~ s{ ( (xSqlStmt.query|xSqlStmt.execute|spark.sql)\s*\(\s*""" |  export_sqlstr_[0-9]+\s*=\s*""" )
    	             .*?\n\s*"""
    	            } {convert_comments_to_sql($&)}xsegi;

	while (scalar(@$ar) >= 1) {shift(@$ar);} #blank out the array.  Can't assign a new array, bc it is passed by ref

	foreach my $frag (split(/\nzxasqwzxasqw\n/, $whole_cont))
	{
		$frag =~ s{^zxasqwzxasqw.*?\n}{}mg;
		push(@{$ar}, $frag);
	}

	if ($ENV{PRESCAN}->{BTEQ_MODE})
	{
		# Insert static cell required for BTEQ
		unshift(@{$ar}, $ENV{CONFIG}->{bteq_run_xsqlstmt});
	}
}
sub convert_if_to_single_cell
{
	my $cont = shift;
	$cont =~ s{^\s*\#s_p_i_f.*?\n}{}mg;
	$cont =~ s{^\s*\# COMMAND\b.*?\n}{}mg;
	$cont =~ s{^\s*\#s_p_e_n_d_i_f.*?(\n)?}{}mg;   # Note: last one might not have line feed
	return $cont;
}
sub convert_comments_to_sql 
{
	my $cont = shift;
	# Change # comment to SQL
	$cont =~ s{\#(.*)}{--$1}g;
	return $cont;
}
sub convert_sql_comments_to_python 
{
	my $cont = shift;
	# Change "--" SQL comment to "#" python comment
	$cont =~ s{^\s*\-\-}{#}g;
	$cont =~ s{\n\s*\-\-}{\n#}g;
	return $cont;
}
sub adjust_var_name
# Convert a variable name to upper case if it occurs in the right place
{
	my $var_name = shift;
	my $match = shift;
	my $bef = shift;
	my $aft = shift;

	# Get the upper case version of var name
	my $uc_var_name = uc($match);

	return $var_name if ($uc_var_name eq $match);  # Return var name if already upper case

	# Convert if surrounded by {}
	return $uc_var_name if ($bef =~ m{\{\s*$} && $aft =~ m{^\s*\}});

	# Convert if not inside single or double quotes
	$bef =~ s{.*$match}{}si;
	$aft =~ s{$match .*}{}si;
	my $context = $bef . $match . $aft;
	$context =~ s{(['"]).*?\1}{}sg;
	return $uc_var_name if ($context =~ m{\b$var_name\b}i);

	return $match;   # Default to no conversion
}

sub databricks_wrap_statement
{
	my $sql = shift;


	#---- Comment out BEGIN / END that we don't need
	# Don't do "BEGIN\s*(" / "END\s*("
	$sql =~ s{BEGIN\s*\(}{B_E_G_I_N_P_A_R_E_N}sig; 
	$sql =~ s{END\s*\(}{E_N_D_P_A_R_E_N}sig; 

	# Don't do "END REPEAT"
	$sql =~ s{END\s*REPEAT\b}{E_N_D_R_E_P_E_A_T}sig; 

	# Comment out BEGIN 
	$sql =~ s{^\s*((\w+):)?\s*BEGIN\b}{--$1 BEGIN}sig;
	$sql =~ s{\n\s*((\w+):)?\s*BEGIN\b}{\n--$1 BEGIN}sig;

	# Don't do "END CASE"
	$sql =~ s{\bEND\s+CASE\b}{E_N_D_C_A_S_E}sig;

	# Don't do SQL "CASE...END"
	if ($sql =~ m{\bCASE\s.*?\bEND\b}si)
	{
	}
	else
	{
		# Comment out "END"
		$sql =~ s{^\s*END}{--END}sig;
		$sql =~ s{\n\s*END}{\n--END}sig;
	}
	# Restore
	$sql =~ s{E_N_D_C_A_S_E}{END CASE}sig;
	$sql =~ s{B_E_G_I_N_P_A_R_E_N}{BEGIN (}sig;
	$sql =~ s{E_N_D_P_A_R_E_N}{END (}sig;
	$sql =~ s{E_N_D_R_E_P_E_A_T}{END REPEAT}sig;

	if ($ENV{PRESCAN}->{use_sql_statement_wrapper})
	{
		if ($sql =~ s{<:nowrap:>}{}g)    # Don't wrap if explicit "<:nowrap:>" present (and remove the "<:nowrap:>")
		{
			$sql =~ s{;\s*$}{};          # And remove potential semi-colon at end
		}
		elsif (is_all_comments_or_blanks($sql))
		{
		}
		else
		{
			my $sql_wrapper_template = '';
			if ($sql =~ m{^\s*SELECT\s}i)
			{
				# $sql_wrapper_template = $ENV{CONFIG}->{xsql_dql_wrapper}; 

				# BTEQ_MODE means we need to check the result of each SQL statement (to support conditional logic)
				$ENV{PRESCAN}->{BTEQ_MODE} ? $sql_wrapper_template = $ENV{CONFIG}->{xsql_dql_wrapper} 
				                           : $sql_wrapper_template = $ENV{CONFIG}->{sparksql_wrapper};
			} 
			else
			{
				# $sql_wrapper_template = $ENV{CONFIG}->{xsql_dml_wrapper};

				# BTEQ_MODE means we need to check the result of each SQL statement (to support conditional logic)
				$ENV{PRESCAN}->{BTEQ_MODE} ? $sql_wrapper_template = $ENV{CONFIG}->{xsql_dml_wrapper} 
				                           : $sql_wrapper_template = $ENV{CONFIG}->{sparksql_wrapper};
			}
			$sql_wrapper_template =~ s{%SQL%}{$sql};
			$sql = $sql_wrapper_template;

			# Original SQL comments inside a SQL wrapper need to stay as "--"
			$sql =~ s{^(\s*\-\-.*)}{$1<:sql_comment:>}mg;
		}
	}

	# If we have a GOTO <label> 
	#    save the label in $ENV{CONFIG}->{GOTO_LABEL} or somewhere globally
	#    DO NOT add the $nb_command
	# If we have a label and it matches the saved GOTO label (always should, because of not supporting oiverlapping GOTO / LABEL
	#    delete $ENV{CONFIG}->{GOTO_LABEL}
	#    DO NOT add the $nb_command
	if ($sql =~ m{^\-\-<indent\+\+>\s*GOTO\s*([^\s;]+)}mi)
	{
		$ENV{CONFIG}->{GOTO_LABEL} = $1;
		# return $sql;
	}
	elsif ($sql =~ m{^\-\-<indent=0>\s*([^\s;]+)}mi)
	{
		my $label = $1;
		if ($ENV{CONFIG}->{GOTO_LABEL} eq $label)
		{
			delete($ENV{CONFIG}->{GOTO_LABEL});
			return $sql;
		}
	}
	elsif ($ENV{CONFIG}->{GOTO_LABEL})
	{
		return $sql;
	}

	return $sql if (is_all_comments_or_blanks($sql));

	# $sql = "\n\n$nb_COMMAND\n$sql" if $CFG_POINTER->{use_notebook_md};
	if ($CFG_POINTER->{use_notebook_md})
	{
		if ($ENV{PRESCAN}->{use_sql_statement_wrapper})
		{
			$sql = "\n\n$nb_COMMAND_python\n$sql";
		}
		else
		{
			$sql = "\n\n$nb_COMMAND_sql\n$sql";
		}
	}

	return $sql;
}

sub adjust_multiline_comment
{
	my $comment = shift;
	while ($comment =~ /(\/\*(.*?)\*\/)/gis)
	{
		my ($prematch, $match, $postmatch) = (${^PREMATCH}, ${^MATCH}, ${^POSTMATCH});
		$MR->log_msg("adjust_multiline_comment\nPRE: $prematch\nMATCH: $match\nPOST: $postmatch");
		$match =~ s/\/\*/--/gis;
		$match =~ s/\*\//--/gis;
		$match =~ s/\n/\n--/gis;
		$MR->log_msg("adjust_multiline_comment: MATCH AFTER: $match");
		$comment = $prematch . $match . $postmatch;
		#$comment = 'MULT LINE';
	}
	return $comment;
}

sub hide 
# In the provided text, convert each instance of anything that matches a provided pattern to a unique string,
# saving the unique string in a hash (for "unhiding" later), returning the modified text
{
	my $text = shift;
	my $pattern = shift;
	my $marker = shift;
	my $unique = $marker . $hide_num++ . $marker;
	while ($text =~ s{($pattern)}{$unique} ) 
	{
		$hide_hash{$unique} = $1;
		$unique = $marker . $hide_num++ . $marker;
	}
	return $text;
}

sub unhide 
# Convert "hidden" things back to their original state
{
	my $text = shift;
	my $marker = shift;
	$text =~ s{$marker(\d+)$marker}{$hide_hash{"$marker$1$marker"}}gs;
	return $text;
}

sub create_table_as_text_format
# Called from config
{
	my $cont = shift;
	my $create_table_as_text_attribs = shift($ENV{PRESCAN}->{CREATE_TABLE_AS_TEXT_ATTRIBS});
	my $table_attribs_output = '';

	$cont =~ s{__CREATE_TABLE_AS_TEXT_ATTRIBS__}{};

	# If set to "1", do not convert text table formats to "DELTA" in "CREATE TABLE"
	# "retain_text_table_formats": "1",
	if ($ENV{CONFIG}->{retain_text_table_formats})
	{
		if ($cont =~ m{\bEXTERNAL\s+TABLE\b}i)
		{
			$cont = "--<:fixme:> databricks.migrations.task update table location\n$cont";
		}
	}
	else
	{
		$table_attribs_output = "using delta\n";
	}

	return $cont . $table_attribs_output . ";\n";
}

sub load_data_into_table_sql
{
	my $file_import = shift($ENV{PRESCAN}->{FILE_IMPORT});

	my $table_name  = $file_import->{TABLE_NAME}; 
	my $inpath      = $file_import->{INPATH}; 
	my $delim       = $file_import->{DELIMITER}; 
	my $skip_header = $file_import->{SKIP_HEADER}; 
	my $output = "";
	my $header_spec = $skip_header?"\,\n'header' = 'true'":"";

	my $output = "COPY INTO $table_name
  FROM $inpath
  FILEFORMAT = \'CSV\'
  FORMAT_OPTIONS (
    'delimiter'='$delim',
     'inferSchema'='true'$header_spec
  )";

  $output = convert_dml($output);  # Make sure code goes through SQL conversion

  return $output;
}

sub create_external_table
{
	my $cont = shift;

	# Prevents false matches
	return $cont if ( ! $ENV{PRESCAN}->{CREATE_EXTERNAL_TABLE});

	my $create_external_table_info = $ENV{PRESCAN}->{CREATE_EXTERNAL_TABLE};

	my %locations = %{$create_external_table_info->{locations}};
	my @locations_stored = @{$create_external_table_info->{loc}};
	my @locations_stored_2 = @{$create_external_table_info->{loc_2}};
	my @locations_stored_3 = @{$create_external_table_info->{loc_3}};
	my $text = shift($create_external_table_info->{ORIGINAL_BLOCK});

	foreach my $location (keys %locations)
	{
		if ($text =~ /location\s*\'\s*$location\/(\w+)\s*\'/i)
		{
			my $replacement = "\${LOCATION_PATH}/";
			$text =~ s/(location|LOCATION)\s*\'\s*$location\/(\w+)\s*\'/$1 '$replacement$2'/gis;
		}
	}

	foreach my $location (@locations_stored_3)
	{
		my $replacement2 = $location->{STORAGE};
		$replacement2 =~ s/from\s*\@(\w+)\@+\.(\w+)/from \$$1.$2/;
		my $regexMatch = $replacement2;
		$regexMatch =~ s/\$/\\\$/;  #make regex friendly
		$text =~ s/\@(\w+)\@+\.(\w+)/\$$1.$2/g;

		if ($text =~ /as\s+select\s*\*\s*$regexMatch/i)
		{
			my $replacement = "\${LOCATION_PATH}/";
			$text =~ s/stored\s+as\s+parquet\s+TBLPROPERTIES\s*\([\"\w\.\=]+\)\s+as\s+select\s*\*\s*$regexMatch(\s|\n|;)/USING DELTA\nLOCATION '$replacement$location->{NAME}' as select * $replacement2$1/is;
		}
	}

	foreach my $location (@locations_stored)
	{
		if ($text =~ /STORED\s*AS\s*$location->{STORAGE}/i)
		{
			my $replacement = "\${LOCATION_PATH}/";
			$text =~ s/(stored|STORED)\s*(as|AS)\s*$location->{STORAGE}/USING DELTA\nLOCATION '$replacement$location->{NAME}';/gis;
		}
	}

	foreach my $location (@locations_stored_2)
	{
		if ($text =~ /\bCREATE\s+EXTERNAL\s+TABLE\s+\`?[\w\@]+\`?\.\`?$location\`?\s*AS\s*SELECT\s*/i)
		{
			my $replacement = "\${LOCATION_PATH}/";
			$text =~ s/\b(create|CREATE)\s+(external|EXTERNAL)\s+(table|TABLE)\s+([\w\@\`]+)\.(\`?)$location(\`?)\s*(as|AS)\s*(select|SELECT)\s*(.*)/$1 $2 $3 $4.$5$location$6 LOCATION '$replacement$location' $7 $8 $9;/gis;
			my $quotes = "$5$6";
			$text =~ s/WHERE\s*(\w+)\s*IN\s*\(\$\{(\w+)\}\)/WHERE $1 IN (\'\${$2}\')/ if $quotes;

		}
	}

	return $text;
}

sub insert_overwrite_except_table
{
	my $cont = shift;

	return $cont if ( ! $ENV{PRESCAN}->{INSERT_OVERWRITE_TABLE}); # Prevents false matches

	my $insert_overwrite_table_info = shift($ENV{PRESCAN}->{INSERT_OVERWRITE_TABLE});

	# get alias/column names from inner select
	my @columns = ( $insert_overwrite_table_info->{ORIGINAL_SOURCE} =~ /AS\s*(\w+)\s*\n/gi );
	push(@columns, ( $insert_overwrite_table_info->{ORIGINAL_SOURCE} =~ /AS\s*(\w+)\s*,/gi ));
	push(@columns, ( $insert_overwrite_table_info->{ORIGINAL_SOURCE} =~ /\s*\,\s*(\w+)\n/gi ));
	push(@columns, ( $insert_overwrite_table_info->{ORIGINAL_SOURCE} =~ /\n\s*(\w+)\s*,/gi ));
	push(@columns, ( $insert_overwrite_table_info->{ORIGINAL_SOURCE} =~ /\n\s*\w+\.(\w+)\s*,/gi ));

	#get columns from select statement to ignore (except these columns)
	my @ignore_columns = ( $insert_overwrite_table_info->{SELECT_COLS}[0] =~ /(\w+)\s*\|/gi );
	push(@ignore_columns, ( $insert_overwrite_table_info->{SELECT_COLS}[0] =~ /\|\s*(\w+)/gi ));
	push(@ignore_columns, ( $insert_overwrite_table_info->{SELECT_COLS}[0] =~ /\((\w+)\)\?/gi ));

	#make sure column names are unique, filter out @ignore_columns columns
	my %seen = ();
	foreach my $ignore_col (@ignore_columns)
	{
		$seen{$ignore_col}++;
	}
    my @unique_columns = grep { ! $seen{ $_ }++ } @columns;

	#replace except column select with column name based select
	my $cols = join(",\n", @unique_columns);
	my $insert_overwrite_table_except_pattern = $insert_overwrite_table_info->{ORIGINAL_SOURCE};
	$insert_overwrite_table_except_pattern =~ s/\s*$insert_overwrite_table_info->{SELECT_COLS}[0]\s*/\n$cols\n/;
	return $insert_overwrite_table_except_pattern;
}

sub insert_overwrite_partition_table
{
	my $cont = shift;

	# Prevents false matches
	return $cont if ( ! $ENV{PRESCAN}->{INSERT_OVERWRITE_TABLE});

	my $insert_overwrite_table_info = shift($ENV{PRESCAN}->{INSERT_OVERWRITE_TABLE});

	my $select = $insert_overwrite_table_info->{WHOLE_INSERT};
	$select =~ s/^INSERT\s*OVERWRITE\s*TABLE[\s\'\`\w\.0-9]+PARTITION\s*\([\s\'\`\w\.0-9\,\=\$]+\)\s*SELECT/SELECT/gi;
	$select =~ s/\'\$\{(\w+)\}\'/{$1}/gi;

	my @partition_cols;
	my @partition_cols_formatted;
	foreach my $partition_cols (@{$insert_overwrite_table_info->{PARTITION_COLS}})
	{
		my $col_name = $partition_cols->{name};
		$col_name =~ s/(\w+)\=\$\w+/$1/ if $partition_cols->{type} eq 'static';
		push(@partition_cols, $col_name);
		push(@partition_cols_formatted, "$col_name ='\"+var[\"$col_name\"]+\"'");
	}
	my $partition_cols = join(",", @partition_cols);
	my $partition_cols_formatted = join("\"+\" AND \"+\"", @partition_cols_formatted);

	my $from_table = $insert_overwrite_table_info->{FROM_TABLE};
	$from_table = $1 if $from_table =~ /(\w+)$/;

	my $insert_overwrite_table_partition_pattern = $ENV{CONFIG}->{commands}->{INSERT_OVERWRITE_PARTITION};
	$insert_overwrite_table_partition_pattern =~ s/\%PARTITION_COLS\%/$partition_cols/g;
	$insert_overwrite_table_partition_pattern =~ s/\%PARTITION_FILTER\%/$partition_cols_formatted/g;
	$insert_overwrite_table_partition_pattern =~ s/\%SQL\%/$select/g;
	$insert_overwrite_table_partition_pattern =~ s/\%FROM_TABLE\%/$from_table/g;
	$insert_overwrite_table_partition_pattern =~ s/\%TABLE_NAME\%/$insert_overwrite_table_info->{TABLE_NAME}/g;

	return $insert_overwrite_table_partition_pattern;
}

sub insert_into_partition_table
{
	my $cont = shift;

	if ($cont =~ /\bINSERT\s+OVERWRITE\b/gis)
	{
		$cont =~ s/\bINSERT\s+OVERWRITE\b/INSERT INTO/gis;

		if ($cont =~ /\bINTO\b\s*(.*?)\s*\bPARTITION\s*\(\s*(\w+)/gis)
		{
			my $table_tame = $1;
			my $first_column_from_partition = $2;
			my $delete_template = $ENV{CONFIG}->{commands}->{DELETE};
			$delete_template =~ s/\%TABLE_NAME\%/$table_tame/gis;
			$delete_template =~ s/\%COLUMN\%/$first_column_from_partition/gis;
			return $delete_template.$cont;
		}
		else
		{
			return $cont;
		}
	}
	else
	{
		return $cont;
	}	
}

sub insert_into_table
{
	my $cont = shift;

	return $cont if !$CFG_POINTER->{column_catalog_file};

	my $column_catalog_file = $CFG_POINTER->{column_catalog_file};
	my @column_catalog_file = $MR->read_file_content_as_array($column_catalog_file);

	my $column_name_position = 3;  #default value
	my $data_type_position = 7;  #default value
	my @timestamp_column_name_position;
	my $line_num = 0;
	foreach my $line (@column_catalog_file)
	{
		my @el = split(",", $line);
		if ($line_num == 0)
		{
			my %index;
			my $i = 0;
			$index{$_} = $i++ for (@el);
			$column_name_position = $index{"COLUMN_NAME"};
			$data_type_position = $index{"DATA_TYPE"};
		}
		elsif ($el[$data_type_position] eq "TIMESTAMP_LTZ")
		{
			push(@timestamp_column_name_position, $el[$column_name_position]);
		}
		$line_num++;
	}

	foreach my $timestamp_col (@timestamp_column_name_position)
	{
		$cont =~ s/(\'\s*\')(\s*as\s*$timestamp_col\s*,)/cast($1 as timestamp)$2/gi;
		$cont =~ s/(\"\s*\")(\s*as\s*$timestamp_col\s*,)/cast($1 as timestamp)$2/gi;
	}

	return $cont;
}


sub insert_overwrite_table
{
	my $cont = shift;

	# $MR->log_error(Dumper($ENV{PRESCAN}));

	# Prevents false matches	
	return $cont if ( ! $ENV{PRESCAN}->{INSERT_OVERWRITE_TABLE});

	my $insert_overwrite_table_info = shift($ENV{PRESCAN}->{INSERT_OVERWRITE_TABLE});

	my $partition_cols = $insert_overwrite_table_info->{PARTITION_COLS};

	# This will be set to 'multi' or 'single'
	my $partition_col_value_type = '';

	# Get separate static and dynamic partition copls into separate arrays
	my @static_partition_cols = ();
	my @dynamic_partition_cols = ();
	foreach my $partition_col (@{$partition_cols})
	{
		if ($partition_col->{type} eq 'static')
		{
			push(@static_partition_cols, $partition_col->{name});
		}
		else
		{
			push(@dynamic_partition_cols, $partition_col->{name});
			$partition_col_value_type = $partition_col->{value_type};
		}
	}
	my $output = '';
	my $sql = '';

	# If there are no dynamic partition cols, e.g. "PARTITION (x = 1, y = 2)", then no conversion is required
	if ( ! @dynamic_partition_cols)
	{
		$sql = $insert_overwrite_table_info->{WHOLE_INSERT};
		$sql =~ s{\binsert\s+overwrite\s+table\s}{<<<:INSERT_OVERWRITE_TABLE:>>> }i;
		$sql = convert_dml($sql);
		$output = $insert_overwrite_table_info->{BEFORE_INSERT}
				. $sql;
		return $output;
	}

	# If we have both dynamic and static partition cols then report and do not convert
	if (@dynamic_partition_cols and @static_partition_cols)
	{
		$sql = $insert_overwrite_table_info->{WHOLE_INSERT};
		$sql =~ s{\binsert\s+overwrite\s+table\s}{<<<:INSERT_OVERWRITE_TABLE:>>>}i;
		$sql = convert_dml($sql);
		$output = $insert_overwrite_table_info->{BEFORE_INSERT}
				. $sql;
		return "--<:fixme:> databricks.migrations.unsupported.feature partition overwrite with both static and dynamic partition columns\n"
				. $output;
	}

	# If we are here then we have only dynamic partition columns, so we need to convert to static

	# For multi-value partition cols, i.e. where the partition col is variable, i.e. variable values from the SELECT,
	# we need to generate regular SQL code (as opposed to spark.sql code)
	if ($ENV{CONFIG}->{force_delete_insert_for_single_partition_columns} or $partition_col_value_type eq 'multi'
	or $insert_overwrite_table_info->{SELECT_COLS}[$#{$insert_overwrite_table_info->{SELECT_COLS}}]  # Check LAST select col for CURRENT_DATE 
																  =~ m{\([^()]*\bcurrent_date\b}si)  # being used in a function, i.e. in parens
	{

		$sql = $insert_overwrite_table_info->{BEFORE_INSERT};
		$output =  $sql . "\n";

		# Get the SELECT cols that correspond to the partition cols, e.g. for "PARTITION(a, b)" get the last TWO SELECT cols,
		# or for "PARTITION(colx)" get the last SELECT col
		my @partition_static_cols = @{$insert_overwrite_table_info->{SELECT_COLS}}    # Take elements from this array
								 [ $#{$insert_overwrite_table_info->{SELECT_COLS}}    # starting at number-of-partition-cols
								   - $#dynamic_partition_cols                         #                        from the end 
								 ..$#{$insert_overwrite_table_info->{SELECT_COLS}} ]; # through to the end

		# Delete the partition. If the original SELECT is "*", then we need to do "DELETE...WHERE...IN...<partition_col>",
		# otherwise we will do "DELETE...WHERE...IN...<select_col>"
		my $in_select = '';
		if ($partition_static_cols[0] =~ m{^\s*\*\s*$})  #this does not activate when it should, maybe make a config override?
		{
			$in_select = "select $dynamic_partition_cols[0]";   # Use the col name from the PARTITION(...)
		}
		else
		{
			$in_select = "select $partition_static_cols[0]";    # Use the col name from the original SELECT
		}

		$sql = "DELETE FROM $insert_overwrite_table_info->{TABLE_NAME}\nWHERE "
			 . "$dynamic_partition_cols[0] in (\n"
			 # . "select $partition_static_cols[0]\n"
			 . "$in_select\n";

		my $insert_from_statement = $ENV{PRESCAN}->{INSERT_FROM_CLAUSES}->{$insert_overwrite_table_info->{FROM_TABLE}};

		if ($insert_from_statement && $ENV{CONFIG}->{use_prescan_insert_from_clauses})
		{
			my $insert = "\n(";
			$insert = $insert_overwrite_table_info->{FROM_TABLE} if $insert_overwrite_table_info->{FROM_TABLE};
			$sql .= "FROM $insert\n$insert_from_statement);\n";
		}
		else
		{
			$sql .= "FROM $insert_overwrite_table_info->{FROM_TABLE});\n";
		}

		if ($ENV{CONFIG}->{use_distinct_column_in_delete_from_clauses})
		{
			$sql =~ s/distinct \*/distinct $dynamic_partition_cols[0]/gis;
		}

		$sql = convert_dml($sql);  # Make sure code goes through SQL conversion
		$output .= $sql;

		my $from = '';
		if ($insert_overwrite_table_info->{FROM_SUBQUERY})
		{
			$from = 'FROM (';
		}
		else
		{
			$from = "FROM $insert_overwrite_table_info->{FROM_TABLE}";
		}

		$output .= "\n-- COMMAND ----------\n";

		$sql = "INSERT INTO TABLE $insert_overwrite_table_info->{TABLE_NAME} PARTITION ($dynamic_partition_cols[0])\n"
			 . "SELECT\n"
			 . join(",\n", @{$insert_overwrite_table_info->{SELECT_COLS}})
			 # . "\nFROM $insert_overwrite_table_info->{FROM_TABLE}\n"
			 . "\n$from\n"
			 . "$insert_overwrite_table_info->{FROM_CLAUSE};\n";
		$sql = convert_dml($sql);  # Make sure code goes through SQL conversion
		$output .= $sql;
	}

	# For single-value partition cols, i.e. where the value in the SELECT is constant, we need to generate spark.sql code
	else
	{
		my $before_insert = $insert_overwrite_table_info->{BEFORE_INSERT};
		# $before_insert = convert_sql_comments_to_python($before_insert);
		$output = $before_insert . "\n";

		my $spark_sql = '';
		my @partition_static_cols = @{$insert_overwrite_table_info->{SELECT_COLS}}    # Take elements from this array
								 [ $#{$insert_overwrite_table_info->{SELECT_COLS}}    # starting at number-of-partition-cols
								   - $#dynamic_partition_cols                         #                        from the end 
								 ..$#{$insert_overwrite_table_info->{SELECT_COLS}} ]; # through to the end
		$output = "%python\nspark.sql(\"\"\"\n$output";
		$output .= "     <<<:INSERT_OVERWRITE_TABLE:>>> $insert_overwrite_table_info->{TABLE_NAME} PARTITION (\n";

		my $col_num = 0;
		my @spark_sql_format = ();
		foreach (@dynamic_partition_cols)
		{
			# Create an entry like <col_name>_val = p.col<col_num> in an array to use in the spark.sql .format
			$col_num++;
			push(@spark_sql_format, $_ . '_val = spark.sql("values ' . $partition_static_cols[$col_num -1] . '").collect()[0].col' . $col_num );

			# Change the array entry from: <col_name> to: <col_name> = '{<col_name>_val}'
			$_ .= " = '{" . $_ . "_val}'";
		}

		# The select cols that correspond to the partition cols need to be removed (from the select cols)
		splice(@{$insert_overwrite_table_info->{SELECT_COLS}}, scalar(@dynamic_partition_cols) * -1);

		$output .= join(",\n        ", @dynamic_partition_cols)
				. "\n     )\n";
		$spark_sql = "     SELECT\n"
				   . join(",\n        ", @{$insert_overwrite_table_info->{SELECT_COLS}})
				   . "\n     FROM $insert_overwrite_table_info->{FROM_TABLE}\n"
				   . "\n     $insert_overwrite_table_info->{FROM_CLAUSE};\n";
		$spark_sql = convert_dml($spark_sql);  # Make sure code goes through SQL conversion

		$output .= $spark_sql		
				. "   \"\"\").format(\n";

		$output .= join(",", @spark_sql_format)
				. "\n).display();\n";

	}
	return $output;
##############################################################################################################
=comment
# IMPORTANT!!!
# This logic temporarily commented out until we can figure out how to convert dynamic partitions properly
##############################################################################################################

	# If we are here then we have only dynamic partition columns, so we need to convert to static
	my $spark_sql = '';
	my $before_insert = $insert_overwrite_table_info->{BEFORE_INSERT};
	$before_insert = convert_sql_comments_to_python($before_insert);
	my $output = "%python\n"
			   . $before_insert . "\n"
			   . "partition_value_array = spark.sql(\"\"\"values (";

	# Get the SELECT cols that correspond to the partition cols, e.g. for "PARTITION(a, b)" get the last TWO SELECT cols,
	# or for "PARTITION(colx)" get the last SELECT col
	my @partition_static_cols = @{$insert_overwrite_table_info->{SELECT_COLS}}    # Take elements from this array
							 [ $#{$insert_overwrite_table_info->{SELECT_COLS}}    # starting at number-of-partition-cols
							   - $#dynamic_partition_cols                         #                        from the end 
							 ..$#{$insert_overwrite_table_info->{SELECT_COLS}} ]; # through to the end

	# Remove column aliases (not valid, and hopefully irrelevant, in output)
	foreach (@partition_static_cols)
	{
		s{\bAS\s\w+}{ }ig;
	}

	$spark_sql = join(",\n", @partition_static_cols);
	$spark_sql = convert_dml($spark_sql);  # Make sure code goes through SQL conversion

	$output .= $spark_sql
			. ")\n\"\"\").collect()\n"
			. "for p in partition_value_array:\n";

	my $col_num = 0;
	my @spark_sql_format = ();  # This will hold the spark.sql .format... clause values
	foreach (@dynamic_partition_cols)
	{
		# Create an entry like <col_name>_val = p.col<col_num> in an array to use in the spark.sql .format
		$col_num++;
		push(@spark_sql_format, $_ . '_val = p.col' . $col_num );

		# Change the array entry from: <col_name> to: <col_name> = '{<col_name>_val}'
		$_ .= " = '{" . $_ . "_val}'";
	}

	# Delete the current partition
	# We will do this when we can figure out how (don't know syntax for multi-column partition, e.g. PARTITION (col1, col2)
	# spark.sql("""ALTER TABLE potato DROP IF EXISTS PARTITION (hour='2020-06-07T01')""")
	$output .= "   # Need to drop / delete the partition here\n";

	$output .= "   spark.sql(\"\"\"\n";

	# DELETE FROM ${hivevar:pHiveRawDb}.event_log_temp WHERE idh_ingestion_month = '{idh_ingestion_month_val}'.
	$output .= "      DELETE FROM $insert_overwrite_table_info->{TABLE_NAME}\n      WHERE ";
	$output .= join("\n      AND ", @dynamic_partition_cols) . ";\n";

	# The "<<<:INSERT_OVERWRITE_TABLE:>>>" below is a hack. What happens is that we are converting "INSERT OVERWRITE TABLE",
	# but the "INSERT OVERWRITE TABLE" part stays the same, so when we use convert_dml subroutine to convert the SQL that is
	# inside the "INSERT OVERWRITE TABLE", it ends up executing databricks_insert_overwrite_table (this subroutine) again,
	# resulting in garbage. So to prevent this subroutine being executed again, we change the reason that it gets executed
	# (match on "INSERT OVERWRITE TABLE") to something unique ("<<<:INSERT_OVERWRITE_TABLE:>>>"), and then we will change
	# that back to "INSERT OVERWRITE TABLE" later
	$spark_sql = "     <<<:INSERT_OVERWRITE_TABLE:>>> $insert_overwrite_table_info->{TABLE_NAME} PARTITION (\n";

	# my $col_num = 0;
	# my @spark_sql_format = ();  # This will hold the spark.sql .format... clause values
	# foreach (@dynamic_partition_cols)
	# {
	# 	# Create an entry like <col_name>_val = p.col<col_num> in an array to use in the spark.sql .format
	# 	$col_num++;
	# 	push(@spark_sql_format, $_ . '_val = p.col' . $col_num );

	# 	# Change the array entry from: <col_name> to: <col_name> = '{<col_name>_val}'
	# 	$_ .= " = '{" . $_ . "_val}'";
	# }
	$spark_sql .= join(",\n        ", @dynamic_partition_cols)
			. "\n     )\n"
			. "     SELECT\n"
			. join(",\n        ", @{$insert_overwrite_table_info->{SELECT_COLS}})
			. "\n     FROM $insert_overwrite_table_info->{FROM_TABLE}\n";

	$spark_sql = convert_dml($spark_sql);  # Make sure code goes through SQL conversion
	
	

	# $output = convert_dml($output);  # Make sure code goes through SQL conversion
	return $output;
=cut
############################################################################################################
}

sub insert_overwrite_sql 
{
	my $text = shift; 
	$MR->log_msg("STARTING INSERT OVERWRITE  $text");

	$text =~ /insert\s*overwrite\s*table\s*(.*?)\s*partition\s*\((.*?)\)\s*select\s*(.*?)\s+from\s+(.*)/gis;
	my $tblname =  $1;
	my $partitioned_column =  $2; 
	my $all_cols =  $3;
	#my $last_column =  $MR->trim($4);
	my $rest =  $4 ;

	my @arg = $MR->get_direct_function_args($all_cols);

	my $last_column = $arg[$#arg];
	$all_cols = join("\n,", @arg[0..$#arg-1]);
	$MR->log_msg("INSERT OVERWRITE SQL ARGS:  " . Dumper(@arg));
	if($last_column =~ /current_date/gis)
	{
		my $additional_widget = $ENV{CONFIG}->{VAR_DECL};
		 $additional_widget =~ s/%NAME%/current_date_val/gis;
		 #$additional_widget =~ s/ \"A\"/current_date_widget/gis;

		$ENV{WIDGET} .= "\n$additional_widget";
		$last_column = "\$current_date_val";

	}

	$MR->log_msg("ELEMENTS table : $tblname \n partitioned_column: $partitioned_column \nall columns:$all_cols\n last column :$last_column \n rest : $rest");

	#Perform static insertion 
	if($last_column =~ /getArgument/gis || $last_column =~ /\$/gis )
	{
		$last_column =~ s/(.*?)\s*as\s*.*/$1/gis;
		$MR->log_msg("last column vv : $last_column");
		$last_column =~ s/\'\$\{hivevar\:(.*?)\}\'/\$$1/gis;
		$last_column =~ s/\'\$\{(.*?)\}\'/\$$1/gis;
		$last_column =~ s/\w+\((.*?)\)/$1/gis;
	
	my $output = "INSERT OVERWRITE TABLE $tblname partition($partitioned_column = $last_column)
	select $all_cols
	from $rest ";
	return $output if ($output =~ /\;/gis);
	return $output . ";"; 
	}

	return "--NON STATIC INSERTION \n$text";

}


sub replace_params_as_spark
{
	my $sql = shift; 

	$MR->log_msg("STARTING REPLACE PARAM FOR SPARK"); 
	if($sql =~ /{(\w+)}/)
	{
		my $table_name = $1;
		$sql =~ /(spark.sql\(.*)\"\"\"(.*)/gis;
		$sql = $1 . "\"\"\".format($table_name)$2";

	}
	return $sql; 
}

#this sub inserts unused columns into INSERT(..) SELECT clauses, because Databricks does not automatically handle NULL column inserts
sub insert_with_select
{
	my $ar = shift;
	if (!$CFG_POINTER->{use_catalog})
	{
        return;
    }
    
	my $str = join("\n", @$ar);
	$MR->log_msg("Entering insert_with_select: $str");
	my $missed_insert_columns = '';
	my $missed_select_columns = '';
	if ($str =~ /\s+(\w+\.?\w*\.?\w*)\s*\((.*)\)\s*SELECT\b/gis)
	{
        my $table_name = $1;
        my @insert_columns = split(/\,/, $2);
		my %hashed_insert_columns = map {uc($MR->trim($_)) => 1} @insert_columns;
		if (!$CATALOG->{$table_name})
		{
            return $str;
        }
        
		foreach my $col (keys $CATALOG->{$table_name})
		{
			$col = $MR->trim($col);
			if (!$hashed_insert_columns{$col})
			{
				$missed_insert_columns .= ", $col\n";
				$missed_select_columns .= ", null as $col\n";
            }
		}
		$str =~ s/(.*)\((.*)\)(\s*SELECT\b.*?)FROM/$1($2$missed_insert_columns)$3$missed_select_columns FROM/gis;
    }
    $MR->log_msg("Output insert_with_select: $str");
	return $str;
}

# change update with sub-select to merge
sub databricks_update
{
	my $ar = shift;
    
	my $str = join("\n", @$ar);
	$MR->log_msg("Entering databricks_update: $str");

	if ($str =~ /\bUPDATE\b\s*(.*?)\s*\b(SET.*?)\s*\bFROM\s*(\(.*?\)\s+\w+\s+)\bWHERE\b\s*(.*)/gis)
	{
		#$update =~ /\bUPDATE\b\s*(.*?)\s*\b(SET.*?)\s*\bFROM\s*(\(.*?\)\s+\w+\s+)\bWHERE\b\s*(.*)/gis;
		my $table = $1;
		my $set = $2;
		my $from_query = $3;   
		my $where = $4;   
		my $ret_str = "MERG INTO $table \n USING $from_query\n ON $where\nwhen matched then update\n$set;";
		return $ret_str
	}

	$str =~ /\bUPDATE\b\s*(.*?)\s*\b(SET.*?)\s*\bWHERE\b\s*(.*)\;/gis;
	my $table = $1;
	my $set = $2;
	my $where = $3;
	
	my @where_cond_array = ();
	my $sub_select_count = 0;
	my $index = 0;
	#	this loop fill @where_cond_array one by one condition
	while ($where ne '')
	{
		$where =~ /(\w+)/gis;
		if (lc($1) eq 'and')
		{
			$where =~ s/^(\w+)//;
		}
		
		my $left_part = '';
		my $right_part = '';
		
		$where = "$where";
		$where =~ /(\w+\s*(\>\=|\>\=|\=|\>|\<|\<\>|\!\=|IN|NOT IN))\s*(.*)/gis;
		$left_part = $1;
		$right_part = $MR->trim($2);
		
		# condition to find out do we have sub-select or simple condition
		if ($right_part =~ /^\(/)
		{
			my $start_prent_count = 0;
			my $end_prent_count = 0;
			my $formed_str = '';
			my $character_count = 0;
			foreach my $char (split('', $right_part))
			{
				$character_count += 1;
				if($char eq '(')
				{
					$start_prent_count += 1;
				}
				elsif($char eq ')')
				{
					$end_prent_count += 1;
				}
				if ($start_prent_count == $end_prent_count)
				{
					last;
				}
				$formed_str .= $char;
			}
			$index += 1;
			my $tbl = "table_$index";
			$formed_str =~ s/(.*?\bSELECT\s*.*?)(\bFROM.*)/$1 $tbl $2/gis;
			$where = substr($right_part,$character_count);
			$right_part = $formed_str;
			$sub_select_count += 1;
			$left_part =~ /(\w+)/;
			my $col = $1;
			push(@where_cond_array,{EXPRESSION=>$left_part.$right_part,SUB_SELECT=>1, SUB_TABLE=>$tbl, COLUMN=>$col, SUB_SELECT_NUMBER => $sub_select_count});
		}
		else
		{
			$right_part =~ /^(\w+)(.*)/gis;
			$right_part = $1;
			$where = $2;
			push(@where_cond_array,{EXPRESSION=>$left_part.$right_part,SUB_SELECT=>0});
		}
	}
	
	my $ret_str = "MERG INTO $table update_table";
	my $cond_string = '';
	
	if ($sub_select_count == 0)
	{
		$ret_str = $str;
	}
	else
	{
		foreach my $cond (@where_cond_array)
		{
			if ($cond_string ne '')
			{
				$cond_string .= ' and ';
			}
	
			if ($cond->{SUB_SELECT})
			{
				if($sub_select_count == 1)
				{
					$ret_str .= "\n USING ".$cond->{EXPRESSION}." sub_table ON \n";
					$cond_string .= 'update_table.'.$cond->{COLUMN}.' = sub_table.'. $cond->{SUB_TABLE}."\n";				
				}
				else
				{
					# if we have more then one sub selects then we should use cross join otherwise just merge
					if ($cond->{SUB_SELECT_NUMBER} == 1)
					{
						$ret_str .= "\n USING ".$cond->{EXPRESSION}."\n";
					}
					else
					{
						$ret_str .= "CROSS JOIN ".$cond->{EXPRESSION}." query_".$cond->{SUB_SELECT_NUMBER}. "\nsub_table ON \n";
					}				
				}
				$cond_string .= 'update_table.'.$cond->{COLUMN}.' = sub_table.'. $cond->{SUB_TABLE}."\n";
			}
			else
			{
				$cond_string .= $cond->{EXPRESSION};
			}
		}
	}
	
	$ret_str .= "$cond_string \nwhen matched then update\n $set;";
	return $ret_str;
}

#sub databricks_update_where
#{
#	my $ar = shift;
#    
#	my $str = join("\n", @$ar);
#	$MR->log_msg("Entering databricks_update: $str");
#	$str =~ /\bfrom\b\s*(.*)\;/gis;
#	my $from_to_where = $1;
#	
#	my $formed_str = '';
#	my $start_prent_count = 0;
#	my $end_prent_count = 0;
#	my $index = 0;
#
#	my $ret_str = '';
#	my @from_blocks = ();
#	
#	foreach my $char (split('', $from_to_where))
#	{
#		$index += 1;
#		
#		if($char eq '(')
#		{
#			$start_prent_count += 1;
#		}
#		elsif($char eq ')')
#		{
#			$end_prent_count += 1;
#		}
#	
#		$formed_str .= $char;
#		if ($start_prent_count == $end_prent_count)
#		{
#			#push(@from_blocks,)
#			if (lc($char) eq 'w')
#			{
#				if(lc(substr($from_to_where,$index-1,5)) eq 'where')
#				{
#					$ret_str = substr($from_to_where,$index-1);
#					last;
#				}
#			}
#		}
#	}
#
#	return $ret_str;
#}

sub fill_catalog_file
{
	my $path = shift;
	my @cont = $MR->read_file_content_as_array($path);
	foreach my $ln (@cont) #iterate through lines
	{
		my @table_cont = split(/\|/, $ln);
		$CATALOG->{uc($MR->trim($table_cont[0]))}->{uc($MR->trim($table_cont[1]))} = 1;
	}
	$MR->debug_msg("Catalog File: " . Dumper($CATALOG));
}