use strict;
# use warnings;

my %PRESCAN = ();
my %PRESCAN_INFO = ();

my $PRESCAN_TPT = 0;
my $FILENAME = undef;


delete $ENV{PRESCAN};
delete $ENV{CONFIG};

# my $PRESCAN_PROC_DEF = ();
my $proc_arg_count = 0;
my $declare_var_count = 0;

my $comment_num = 0;

# For writing info to the conversion catalog file
my @conversion_catalog = ();       # Current conversion catalog
my @conversion_catalog_add = ();   # Things that we will add to the conversion catalog
my @new_conversion_catalog = ();   # The content that we will write to the conversion catalog file

#should be called by 'load_files' directive in configs
sub teradata_prescan
{
	my $td_source_ref = shift;
	$MR->log_msg("Begin teradata_prescan");

	$PRESCAN_TPT = 0;

	# Convert all comment lines (first non-whitespace value on line is "--") to unique ids
	# NOTE: This MODIFIES the source
	$ENV{PRESCAN}->{BTEQ_MODE} = '0';
	$ENV{PRESCAN}->{use_sql_statement_wrapper} = '0';
	$ENV{PRESCAN}->{PROC_NAME} = '';
	foreach my $line (@$td_source_ref) 
	{
		if ($line =~ m{^\s*\*/})
		{
		}
		else
		{
			$line =~ s{^(\s*)\*}{$1--};   # Convert BTEQ comment (begin with asterisk) to "--" comment
		}

		if ($line =~ m{^\s*\.(LABEL|LOGON|LOGOFF|EXIT|QUIT|EXPORT|IMPORT|REPEAT|RUN|IF|GOTO|REMARK)\b}i)
		{
			$ENV{PRESCAN}->{use_sql_statement_wrapper} = 1;
		}
		if ($line =~ m{^\s*\.(IF|GOTO)\b}i)
		{
			$ENV{PRESCAN}->{BTEQ_MODE} = 1;
		}

		# Convert: <...code...> /*...comment...*/ to: --...comment...\n<...code...>
		# $line =~ s{(.*?)/\*(.*)\*/.*}{--$2\n$1};
		if ($line =~ s{(/\*.*\*/)}{mask_single_line_c_comment($1)}me)
		{
			$line =~ s{/\*<<<c_o_m_m_e_n_t}{\n$&};
		}

		$line =~ s{^(\s*--.*)}{mask_comment($1)}me;

		if ($line =~ /^\s*DEFINE\s+JOB/gis)
		{
			$ENV{CONFIG_TPT}->process_file($CFG_POINTER->{FILENAME});
			$ENV{PRESCAN}->{TPT_INFO} = $ENV{CONFIG_TPT}->{TPT_INFO};
		}
	}

	my $td_source_lines = join("\n", @$td_source_ref);

	check_labels($td_source_lines);

	##########################################################################################################
	# When we need things that span multiple statements (more than one ";") we need to use $td_source_lines
	##########################################################################################################

	# NOTE:
	# Use s{...}{mask_semi_colons} logic to create a fragment that will remain intact when going through 
	# the split on >>> code_fragment_breakers <<<

	# For FastLoad and Mload, remove all ";" between "BEGIN LOAD/LOADING" and "END LOAD/LOADING"

	# For FastLoad we could have BEGIN LOADING...DEFINE or DEFINE...BEGIN LOADING, and END LOADING is optional
	# (in which case we have to grab until EOF
	$td_source_lines =~ s{(\bDEFINE\b.*?\bBEGIN\s+LOADING\b.*?\bEND\s+LOADING\b)}{mask_semi_colons($1)}sieg ||
	$td_source_lines =~ s{(\bBEGIN\s+LOADING\b.*?\bDEFINE\b.*?\bEND\s+LOADING\b)}{mask_semi_colons($1)}sieg ||
	$td_source_lines =~ s{(\bDEFINE\b.*?\bBEGIN\s+LOADING\b.*)}{mask_semi_colons($1)}sieg ||
	$td_source_lines =~ s{(\bBEGIN\s+LOADING\b.*?\bDEFINE\b.*)}{mask_semi_colons($1)}sieg;

	# Same for fastexport (BEGIN EXPORT ... END EXPORT)
	$td_source_lines =~ s{(\bBEGIN\s+EXPORT\b.*?\bEND\s+EXPORT\b)}{mask_semi_colons($1)}sieg;

	# MLoad
	$td_source_lines =~ s{(\bBEGIN\s+(IMPORT\s+)?MLOAD\b.*?\bEND\s+MLOAD\b)}{mask_semi_colons($1)}sie;
	# Keep a "DECLARE ...<no_semi-colons>... BEGIN ...<possible_semi-colons> END;" as ONE fragment 
	# Note: We include the ";" after "END" because "END" could match where we don't want it to,
	# so then we have to put it back again in substitution target
	$td_source_lines =~ s{(DECLARE\s+[^;]+(\s|:)BEGIN\s.*?\sEND\s*(\w+)?\s*);}{mask_semi_colons($1) . ';'}sieg;

	$td_source_lines =~ s{\b(CREATE|REPLACE)\s+MACRO\s.*?\sAS\s*(\(((?:(?>[^()]+)|(?2))*)\))}{mask_semi_colons($&)}sieg;

																	sub mask_semi_colons {
																		my $input = shift;
																		$input =~ s{;}{<:semi-colon:>}g;
																		return $input;
																	}

	# Remove potential ";" at end of BTEQ .EXPORT statement
	# $td_source_lines =~ s{((^|\n)\s*\.EXPORT\s+(?:reportwide\s+|DATA\s+)?FILE\s*=\s*\S+\s*);}{$1};
	$td_source_lines =~ s{((^|\n)\s*\.EXPORT\s+(?:reportwide\s+|DATA\s+)?FILE\s*=\s*(\S+|['\"].*?['\"])\s*);}{$1};

	# Get fragment split chars and split into fragments
	my $split_chars = join('|', @{$CFG_POINTER->{code_fragment_breakers}->{line_end}});
	my(@td_source_fragments) = split(/(?:$split_chars)/, $td_source_lines);

    # $ENV{GLOBALS}->{proc_arg_count} = 0;
    $proc_arg_count = 0;
    # $ENV{GLOBALS}->{declare_var_count} = 0;
    $declare_var_count = 0;
    $ENV{CONFIG} = $CFG_POINTER;     # Don't think we actually need this

	# $PRESCAN_PROC_DEF = 0;
	foreach my $source_frag (@td_source_fragments) 
	{
		# Put semi-colons back
		$source_frag =~ s{<:semi-colon:>}{;}g;
		# Remove comment lines and comments at end of lines
		$source_frag =~ s{^\s*\-\-.*\n}{}mg;
		$source_frag =~ s{\s+\-\-.*\n}{}mg;

		if ($source_frag =~ m{\bBEGIN\s+(IMPORT\s+)?MLOAD\b.*?\bEND\s+MLOAD\b}si) {       # MLoad
			prescan_mload($source_frag);
		}
		elsif ($source_frag =~ m{\bBEGIN\s+LOADING\b.*?\bDEFINE\b}si
		||  $source_frag =~ m{\bDEFINE\b.*?\bBEGIN\s+LOADING\b}si)
		{
			prescan_fastload($source_frag);
		}
		elsif ($source_frag =~ m{\bBEGIN\s+EXPORT\s}si)                  # Fastload
		{
			prescan_fastexport($source_frag);
		}
		elsif ($source_frag =~ m{(\.EXPORT\s+.*)}si)                           # BTEQ EXPORT
		{	
			prescan_bteq_export($1);
		}
		elsif ($source_frag =~ m{(\.IMPORT\s+.*)}si) {                           # BTEQ IMPORT
			prescan_bteq_import($1);
		}
		elsif ($source_frag =~ m{\sPROCEDURE\s.*?\((.*)\).*?\bBEGIN}si)
		{
			# prescan_procedure_stmt($1);
			prescan_procedure_stmt($source_frag);
		}
		elsif ($source_frag =~ m{\b(CREATE|REPLACE)\s+MACRO\s}si)
		{
			prescan_macro($source_frag);
		}
		
		# This used to be part of the above elseif, but the first DECLARE... can be stuck on the end of a CREATE PROCEDURE,
		# which means that the CREATE PROCEDURE gets picked up INSTEAD of the first DECLARE (because it's "elseif...") 
		if ($source_frag =~ m{(\bdeclare\s+.*)}si)
		{
			prescan_declare($1);
		}

		# if ($source_frag =~ m{^\s*SELECT.*?\sINTO.*?\sFROM\s}sxi) 
		if ($source_frag =~ m{\bSELECT.*?\sINTO.*?\sFROM\s}sxi) 
		{
			prescan_select_into($source_frag);
		}

		if ($source_frag =~ m{\b(SAMPLE\s+((0)?\.[0-9]+)(\s*,\s*(0)?\.[0-9]+)*)}si)
		{
			push (@{ $ENV{PRESCAN}->{SAMPLE_PERCENT} },  $1);
		}
		
		# CALL
		if ($source_frag =~ m{^\s*CALL\s+\w}si)
		{
			$ENV{PRESCAN}->{use_sql_statement_wrapper} = '1'; 
		}

		# Match on whatever we need (usually same as match where we replace match with __...PLACEHOLDER__)
		# NOTE: Do not include any potential terminating ";", because it has already been removed.
		# Pass everything ($_) to the prescan subroutine
		if ($source_frag =~ m{testthing1}) {
			prescan_testthing1($_);
		}

		if ($source_frag =~ m{\sTABLE\s+([\w.]+)})
		{
			my $table_name = $1;
			delete $ENV{PRESCAN}->{BETWEEN}->{$table_name};
			while ($source_frag =~ m{([,(].*?)(\w+)([^,]+\s)(BETWEEN\s+\S+\s+AND\s+[^,]+)}igs) 
			{
				my ($col_name, $between) = ($2, $4);
				$ENV{PRESCAN}->{BETWEEN}->{$table_name}->{$col_name} = $between;
			}
			delete $ENV{PRESCAN}->{UPPERCASE}->{$table_name};
			while ($source_frag =~ m{([,(].*?)(\w+)([^,]+\s)(UPPERCASE)}igs) 
			{
				my ($col_name, $uppercase) = ($2, $4);
				$ENV{PRESCAN}->{UPPERCASE}->{$table_name}->{$col_name} = $uppercase;
			}
			delete $ENV{PRESCAN}->{COL_CHECKS}->{$table_name};
			while ($source_frag =~ m{\b(CHECK\s*\(.*?)(?=\bCHECK\s*\(|$)}sig) {
				handle_column_check($table_name, $1);
			}
			delete $ENV{PRESCAN}->{PRIMARY_KEYS}->{$table_name};
			prescan_constraint_primary_key($table_name, $source_frag);
			delete $ENV{PRESCAN}->{FOREIGN_KEYS}->{$table_name};
			prescan_constraint_foreign_key($table_name, $source_frag);
		}
	}

	#------------------------------ Update the conversion_catalog file ------------------------------
	if(!$ENV{CONFIG}->{skip_catalog_file_maintenance})
	{
		my $conv_catalog = "$ENV{TEMP}/sqlconv_conversion_catalog.txt";
		$conv_catalog = $ENV{CONFIG}->{conversion_catalog_file} if ($ENV{CONFIG}->{conversion_catalog_file});

		# Read the entire catalog
		my @current_conversion_catalog = $MR->read_file_content_as_array($conv_catalog) if $conv_catalog;

		# Add new entries to the end
		push(@current_conversion_catalog, @conversion_catalog_add);

		my @new_conversion_catalog = ();
		
		# Loop over entries in reverse order (new entries first)
		foreach my $catalog_entry (reverse(@current_conversion_catalog))
		{
			# Ignore older identical entries, saving newer ones to the new catalog
			next if (grep(/^$catalog_entry$/,@new_conversion_catalog));
			push(@new_conversion_catalog, $catalog_entry);
		}
		# Write the new converter catalog info to the file
		$MR->write_file($conv_catalog, join("\n", @new_conversion_catalog) . "\n") if (@new_conversion_catalog);
	}
}

sub check_labels
# Report an error if we have overlapping GOTOs / LABELS, e.g. GOTO L1 ... GOTO L2 ... LABEL L1
{
	my $source = shift;
	my $label = '';
	my $prev = '';
	$MR->log_msg("Checking for unsupported overlapping GOTOs / LABELs");
	# while ($source =~ m{^\s*\.(goto|label)\s+(\w+)}mig) 
	while ($source =~ m{^\s*\.(if.*?\.goto|label)\s+(\w+)}mig)       # Find ".IF ... GOTO <label>" or ".<label>" 
	{
		$label = $2;
		$MR->log_msg("   $1 $2");
		if ( ! $prev ) 
		{
			$prev = $label;
			next;
		}
		if ($label ne $prev) 
		{
			$MR->log_err("Unsupported overlapping GOTOs / LABELs in file $CFG_POINTER->{FILENAME}");
			last;
		}
		$prev = '';
		$label = '';
	}
	$MR->log_msg("Done checking for unsupported overlapping GOTOs / LABELs");
}
sub prescan_testthing1 {
	my $testthing1_code = shift;

	my $prescan_testthing1 = ();   # Local hash ref for saving whatever you need from this instance of testthing1
	
	# Do various matches to get whatever you need from the source file for this instance of "testthing1"
	while ($testthing1_code =~ m{(\w+)=(\w+)}g) {

		# Collect whatever you need into a local hash ref
		$prescan_testthing1->{$1} = $2;
		# ...etc...
	}

	# Finally, add whatever you saved in local hash ref to the main PRESCAN, under a specific "label" (in this case, TESTTHING1)
	# NOTE: always do a *push* into $ENV{PRESCAN}->{TESTTHING1}. This is because when we find __TESTTHING1_PLACEHOLDER__ later,
	# we can do a *shift* of  $ENV{PRESCAN}->{TESTTHING1} to get the things that we saved from the corresponding *push*
	push(@{ $ENV{PRESCAN}->{TESTTHING1} }, $prescan_testthing1);
}

sub prescan_fastload {
	my $fastload_source = shift;

	my $prescan_fastload = ();
	$ENV{PRESCAN}->{use_sql_statement_wrapper} = '1';
	$prescan_fastload->{ORIGINAL_SOURCE} = $fastload_source;
	# VARTEXT
	if ($fastload_source =~ m{\bVARTEXT\s+['"](.*?)['"]}si) {
		$prescan_fastload->{DELIMITER} = $1;
	}

	# ERRORFILES (take on the first file name)
	if ($fastload_source =~ m{\bERRORFILES\s+(\w+)}si)
	{
		$prescan_fastload->{BAD_RECS_PATH} = $1;
	}

	# DEFINE (like "USING..." in BTEQ IMPORT, but not enough to be able to re-use it!
	if ($fastload_source =~ m{\bDEFINE\s+(.*?);}si)
	{
		my $define = $1;

		# Get FILE = ... Report Error if not found
		# Note: Below is a substitution, because we should then be left with just the actual field defs, separated by commas
		if ($define =~ s{\bFILE\s*=\s*(\S+)}{})
		{
			my $file_name = $1;
			$file_name =~ s{;}{};
			$prescan_fastload->{FILE_NAME} = $file_name;
		}
		else
		{
			$MR->log_error("No \"FILE =...\" found in FastLoad\nFastLoad content:\n$fastload_source")
		}

		# Split on comma to get field defs, e.g. FIELD1 (INT), FIELD2 (VARCHAR(10)), ...
		my $col_order = 0;
		foreach my $define_field (split(',', $define))
		{
			$col_order++;
			if ($define_field =~ m{^\s*(\w+)\s*\((.*)\)}s)    # E.g. "EMP_NAME (VARCHAR(30))"
			{
				my ($field_name, $field_def, $length) = ($1,$2, '');   
				my $data_type = '';         
				if ($field_def =~ m{(\w+)\s*\(\s*(0-9]+)\s*\)}) {   # E.g. VARCHAR(30)
					($data_type, $length) = ($1, $2);
				}
				else
				{
					$data_type = $field_def;
				}
				$prescan_fastload->{INFILE_COLS}->{$col_order}->{COL_NAME} = uc($field_name);
				$prescan_fastload->{INFILE_COLS}->{$col_order}->{DATA_TYPE} = $data_type;
				$prescan_fastload->{INFILE_COLS}->{$col_order}->{LENGTH} = $length;
			}
			else 
			{
				# ERROR??? 
			}
		}
	}

	# INSERT
	my $col_names = '';
	if ($fastload_source =~ m{\bINSERT\s+INTO\s+([\w.]+)\s*\((.*?)\)}si)     # "INSERT INTO <table> (<col names>)"
	{   # "INSERT INTO <table> (<col names>)"
		($prescan_fastload->{TABLE_NAME}, $col_names) = ($1, $2);
		my $col_order = 0;
		foreach my $col_name (split(/,/, $col_names))
		{
			$col_order++;
			$prescan_fastload->{INSERT}->{$col_order}->{COL_NAME} = uc($MR->trim($col_name));
		}
	}
	elsif ($fastload_source =~ m{\bINSERT\s+(INTO\s+)?([\w.]+)\.\*}si)                 # "INSERT INTO <table>.* Means use ALL fields from the DEFINE
	{
		$prescan_fastload->{TABLE_NAME} = $2;
		$prescan_fastload->{INSERT} = $prescan_fastload->{INFILE_COLS};
	}

	if ($fastload_source =~ m{\bVALUES\s*\((.*)\)}si) 
	{
		my $prescan_block = parse_values_clause($1);
		$prescan_fastload->{VALUES} = $prescan_block->{VALUES};
		$prescan_fastload->{VALUES_BY_COL_NAME} = $prescan_block->{VALUES_BY_COL_NAME};
	}

	# Create a lookup hash to map INSERT col names to VALUES col names
	my $col_num = 0;  
	my $map_col_names = ();
	foreach my $insert_col_num (keys %{$prescan_fastload->{INSERT} })
	{
		$map_col_names->{$prescan_fastload->{INSERT}->{$insert_col_num}->{COL_NAME}} = $prescan_fastload->{VALUES}->{$insert_col_num}->{COL_NAME};
		$map_col_names->{$prescan_fastload->{VALUES}->{$insert_col_num}->{COL_NAME}} = $prescan_fastload->{INSERT}->{$insert_col_num}->{COL_NAME};
	}
	$prescan_fastload->{MAP_COL_NAMES} = $map_col_names;

	#######################################
	# # # NOTE: We are using {IMPORT} here:
	#######################################
	push(@{ $ENV{PRESCAN}->{IMPORT} }, $prescan_fastload);
}

sub prescan_mload
{
	my $mload_source = shift;

	my $prescan_mload = ();

	# Save the "before" in case we need to search through it or report it
	$prescan_mload->{ORIGINAL_SOURCE} = $mload_source;

	# TABLES section (Don't think we end up using this)
	if ($mload_source =~ m{\.BEGIN\s+ (IMPORT\s+)? MLOAD\s+TABLES\s+  (.*?)  (\.LAYOUT\s|\.DML\s|\.IMPORT\s)}xsi)
	{
		my $table_names = $2;
		while ($table_names =~ m{([\w.]+)}g)
		{
			push(@{ $prescan_mload->{TABLES} }, $1);
		}
	}

	# .LAYOUT section
	if ($mload_source =~ m{\.LAYOUT\s+(\w+)(.*?)(\.DML\s|\.IMPORT\s|$ )}xsi)
	{
		my ($layout_name, $section_code) = ($1, $2);
		$prescan_mload->{LAYOUT}->{NAME} = $layout_name;
		my $field_order = 0;
		while ($section_code =~ m{\.FIELD\s+  (\w+)  (\s*\*|\s+\w+)  \s+ \(? (\w+)  \s*  (\([0-9,]+\))? .*?;}sigx)
		{
			my ($field_name, $field_pos, $field_type, $field_len) = ($1, $2, $3, $4);
			$field_len =~ s{[()]}{}g;     # Remove parens 
			$field_order++;

			# Do we need field position / length????
			if ($field_pos =~ m{\*})   # Derive "next" position from prev field (cumulative)
			{
			}
			else                       # Actual pos. Note: trim spaces
			{
			}

			$prescan_mload->{LAYOUT}->{INFILE_COLS}->{$field_order}->{COL_NAME} = uc($field_name);
			$prescan_mload->{LAYOUT}->{INFILE_COLS}->{$field_order}->{DATA_TYPE} = uc($field_type);
			$prescan_mload->{LAYOUT}->{INFILE_COLS}->{$field_order}->{LENGTH} = $field_len;
		}
	}

	# .IMPORT section
	if ($mload_source =~ m{\.IMPORT\s+(.*?)(\.LAYOUT\s|\.DML\s|$ )}xsi)
	{
		my ($section_code) = ($1);
		my ($infile_name, $infile_delim, $layout_name) = ('', '', '');
		($infile_name) = $section_code =~ m{\bINFILE\s+(['"].*?['"]|\S+)}i;
		$infile_name =~ s{['"]}{}g;
		($infile_delim) = $section_code =~ m{\bVARTEXT\s+['"](.*?)['"]}si;
		($layout_name)  = $section_code =~ m{\bLAYOUT\s+(\w+)}si;
		$layout_name = uc($layout_name);
		$prescan_mload->{IMPORT}->{INFILE_NAME}  = $infile_name;
		$prescan_mload->{IMPORT}->{DELIMITER}  = $infile_delim;
		$prescan_mload->{IMPORT}->{LAYOUT_NAME} = $layout_name;

		# APPLY sub-section
		while ($section_code =~ m{\bAPPLY\s+(\w+)  (.*?)  (?=(\bAPPLY\s|;)) }xsig)
		{
			my ($apply_dml, $apply_cond) = (uc($1), $2);
			my $apply = ();
			$apply->{DML_LABEL} = $apply_dml;
			$apply->{COND} = $apply_cond;
			push(@{ $prescan_mload->{IMPORT}->{APPLY}}, $apply);
		}
	}

	# DML sections
	while ($mload_source =~ m{\.DML\s+LABEL\s+(\w+)(.*?)(?=(\.LAYOUT\s|\.DML\s|\.IMPORT\s|$ ))}xsig)
	{
		my ($dml_label, $dml_sql) = (uc($1), $2);

		$dml_sql =~ s{^\s*DO\s+INSERT\s+FOR.*?;}{};     # OK to ignore?????????????
		
		while ($dml_sql =~ m{\b((UPDATE|INSERT|DELETE)\s.*?;)}sig)
		{
			my ($function, $sql) = (uc($2), $1);              # E.g. $2='UPDATE', $1='UPDATE T1 SET COL1 = ..."

			if ($function eq 'DELETE')
			{
				if ($sql =~ m{^\s*DELETE\s+FROM\s}i)  # Make sure we have "DELETE FROM tblname", not just "DELETE tblname"
				{
				}
				else
				{
					$sql =~ s{^\s*DELETE\s}{DELETE FROM }i;
				}
			}
			elsif ($function eq 'INSERT')
			{
				if ($sql =~ m{^\s*INSERT\s+INTO\s}i)  # Make sure we have "INSERT INTO tblname", not just "INSERT tblname
				{
				}
				else
				{
					$sql =~ s{^\s*INSERT\s}{INSERT INTO }i;
				}

				my $col_names = '';

				# For INSERT ... VALUES... we need to get col names in order
				if ($sql =~ m{\bINSERT\s+INTO\s+([\w.]+)\s*\((.*?)\).*?\bVALUES\s*\((.*)\)}si) {   # "INSERT INTO <table> (<col names>) VALUES (...)"
					# ($prescan_import->{TABLE_NAME}, $col_names) = ($1, $2);
					my ($table_name, $col_names, $values) = ($1, $2, $3); 
					my $col_order = 0;
					foreach my $col_name (split(/,/, $col_names))
					{
						$col_order++;
						# $prescan_import->{INSERT}->{$col_order}->{COL_NAME} = uc($MR->trim($col_name));
						$prescan_mload->{DML_LABEL}->{$dml_label}->{INSERT}->{COLS}->{$col_order}->{COL_NAME} = uc($MR->trim($col_name));
					}

					# Parse VALUES 
					my $insert_values = parse_values_clause($values);
					$prescan_mload->{DML_LABEL}->{$dml_label}->{INSERT}->{VALUES} = $insert_values->{VALUES};
					$prescan_mload->{DML_LABEL}->{$dml_label}->{INSERT}->{VALUES_BY_COL_NAME} = $insert_values->{VALUES_BY_COL_NAME};

					# Create a lookup hash to map INSERT col names to VALUES col names
					my $col_num = 0;  
					my $map_col_names = ();
					foreach my $insert_col_num (keys %{$prescan_mload->{DML_LABEL}->{$dml_label}->{INSERT}->{COLS} })
					{
						$map_col_names->{$prescan_mload->{DML_LABEL}->{$dml_label}->{INSERT}->{COLS}->{$insert_col_num}->{COL_NAME}} = 
						                 $prescan_mload->{DML_LABEL}->{$dml_label}->{INSERT}->{VALUES}->{$insert_col_num}->{COL_NAME};
						$map_col_names->{$prescan_mload->{DML_LABEL}->{$dml_label}->{INSERT}->{VALUES}->{$insert_col_num}->{COL_NAME}} = 
						                 $prescan_mload->{DML_LABEL}->{$dml_label}->{INSERT}->{COLS}->{$insert_col_num}->{COL_NAME};
					}
					$prescan_mload->{DML_LABEL}->{$dml_label}->{MAP_COL_NAMES} = $map_col_names;
				}
				elsif ($sql =~ m{\bINSERT\s+(INTO\s+)?(\S+)\.\*}i)               # If the SQL "INTO" tablename has the ".*"
				{
					my $table_name = $2;
					$sql = "INSERT INTO $table_name (\n";                     # Start creating our replacement expanded INSERT
					my @insert_cols = ();
					my @values_cols = ();

					# Fields are numbered for order to match the input file
					foreach my $field_num (sort {$a <=> $b} (keys %{ $prescan_mload->{LAYOUT}->{INFILE_COLS} } ))
					{
						# Collect the field (column) names that we will use in our expanded INSERT
						push(@insert_cols, $prescan_mload->{LAYOUT}->{INFILE_COLS}->{$field_num}->{COL_NAME});
						push(@values_cols, ':' . $prescan_mload->{LAYOUT}->{INFILE_COLS}->{$field_num}->{COL_NAME});
					}

					# Add the rest of the INSERT: col1, col2...) VALUES (col1, col2...)
					$sql .= join(",\n", @insert_cols) . "\n)\nVALUES (\n" . join(",\n", @values_cols) . "\n)\n";
				}
			}
			$prescan_mload->{DML_LABEL}->{$dml_label}->{$function}->{SQL} = $sql;    # E.g. ...{UPDATE} = 'UPDATE T1 SET...'
			my ($table_name) = $sql =~ m{^\s*(?:UPDATE|INSERT\s+INTO|DELETE\s+FROM)\s+([\w.]+)}i;
			$prescan_mload->{DML_LABEL}->{$dml_label}->{TABLE_NAME} = $table_name;
		}
	}
	push(@{ $ENV{PRESCAN}->{MLOAD} }, $prescan_mload);   # We could probably just use $ENV{PRESCAN}->{IMPORT} to keep it generic???
}

sub prescan_bteq_import {
	my $import_statement = shift;
	my $prescan_import = ();

	# Save the "before" in case we need to search through it or report it
	$prescan_import->{ORIGINAL_SOURCE} = $import_statement;

	my $col_names = '';
	if ($import_statement =~ m{\bINSERT\s+INTO\s+([\w.]+)\s*\((.*?)\)}si) {   # "INSERT INTO <table> (<col names>)"
		($prescan_import->{TABLE_NAME}, $col_names) = ($1, $2);
		my $col_order = 0;
		foreach my $col_name (split(/,/, $col_names))
		{
			$col_order++;
			$prescan_import->{INSERT}->{$col_order}->{COL_NAME} = uc($MR->trim($col_name));
		}
	}
	elsif ($import_statement =~ m{\bINSERT\s+INTO\s+([\w.]+)}si) {            # "INSERT INTO <table>" (no column names)
		$prescan_import->{TABLE_NAME} = $1;
	}

	# if ($import_statement =~ m{\bUSING\s*\((.*?)\)}si) {
	if ($import_statement =~ m{\bUSING\s*(\(.*)}si) {
		my $using_plus_rest = $1;
		my $using_col_defs = '';
		if ($using_plus_rest =~ m{
					(
						\(                      # Opening paren 
						(?: [^()]* | (?0) )*    # Match a series of non-parens or another "self"
						\)                      # Closing paren
					)
				}xsig) 
		{
			$using_col_defs = $1;
			$using_col_defs =~ s{^\s*\(}{};
			$using_col_defs =~ s{\)\s*$}{};
		} else
		{
			$MR->log_error("Cannot find \"USING (...)\" clause in IMPORT: $import_statement");
		}
		my $col_order = 0;
		foreach my $col_def (split(/,/, $using_col_defs))
		{
			$col_order++;
			$col_def =~ m{(\w+)\s+(.*)};
			my ($col_name, $data_type) = ($1, $2);    # NOTE: Data type might have "(...)", e.g. VARCHAR(100)
			my $length = '';
			if ($data_type =~ m{(\w+)\s*\(\s*(.*?)\s*\)})
			{
				($data_type, $length) = ($1, $2);
			}
			$prescan_import->{INFILE_COLS}->{$col_order}->{COL_NAME} = uc($col_name);
			$prescan_import->{INFILE_COLS}->{$col_order}->{DATA_TYPE} = $data_type;
			$prescan_import->{INFILE_COLS}->{$col_order}->{LENGTH} = $length;
		}
	}

	if ($import_statement =~ m{\bVALUES\s*\((.*)\)}si) 
	{
		my $prescan_block = parse_values_clause($1);
		$prescan_import->{VALUES} = $prescan_block->{VALUES};
		$prescan_import->{VALUES_BY_COL_NAME} = $prescan_block->{VALUES_BY_COL_NAME};
	}

	# Create a lookup hash to map INSERT col names to VALUES col names
	my $col_num = 0;  
	my $map_col_names = ();
	foreach my $insert_col_num (keys %{$prescan_import->{INSERT} })
	{
		$map_col_names->{$prescan_import->{INSERT}->{$insert_col_num}->{COL_NAME}} = $prescan_import->{VALUES}->{$insert_col_num}->{COL_NAME};
		$map_col_names->{$prescan_import->{VALUES}->{$insert_col_num}->{COL_NAME}} = $prescan_import->{INSERT}->{$insert_col_num}->{COL_NAME};
	}
	$prescan_import->{MAP_COL_NAMES} = $map_col_names;

	if ($import_statement =~ m{\bVARTEXT\s+['"](.*?)['"]}si) {
		$prescan_import->{DELIMITER} = $1;
	}
	elsif ($import_statement =~ m{\bDATA\s})
	{
		# No action; we will check for DELIMITER later
	}

	# "FILE = ..." is for FastLoad; "IMPORT INFILE ..." is for MLoad
	if ($import_statement =~ m{\b(FILE\s*=\s*|IMPORT\s+INFILE\s+)(\S+)}si) {
		$prescan_import->{FILE_NAME} = $2;
	}

	push(@{ $ENV{PRESCAN}->{IMPORT} }, $prescan_import);
}

sub prescan_bteq_export 
{
	my $export_statement = shift;
	my $prescan_export = ();

	# Save the "before" in case we need to search through it or report it
	$prescan_export->{ORIGINAL_SOURCE} = $export_statement;
	if ($export_statement =~ m{\sFILE\s*=\s*(\S+|['\"].*?['\"])}i)
	{
		$prescan_export->{FILE_NAME} = $1;
		$prescan_export->{FILE_NAME} =~ s{^['"](.*?)['"]}{$1};    # Remove potential surrounding quotes
	}

	# Grab all column names from the SELECT
	if ($export_statement =~ m{SELECT\s+(.*?)\s*(;|$)}is)
	{
		$prescan_export->{SELECT_STATEMENT} = "$1;";
	}

	push(@{ $ENV{PRESCAN}->{EXPORT} }, $prescan_export);
}

sub prescan_fastexport 
# NOTE: This looks like it is identical to prescan_bteq_export, except we have "OUTFILE <filename>"
# instead of "FILE = <filename>" 
{
	my $fastexport_statement = shift;
	my $prescan_fastexport = ();

	# Save the "before" in case we need to search through it or report it
	$prescan_fastexport->{ORIGINAL_SOURCE} = $fastexport_statement;

	if ($fastexport_statement =~ m{\sOUTFILE\s*(\S+)}i)
	{
		$prescan_fastexport->{FILE_NAME} = $1;
	}

	# Grab all column names from the SELECT
	# if ($fastexport_statement =~ m{SELECT\s+(.*?)\s+FROM\s+([\w\.]+)}is)
	if ($fastexport_statement =~ m{SELECT\s+(.*?)\s*(;|$)}is)
	{
		$prescan_fastexport->{SELECT_STATEMENT} = "$1;";
	}

	##### NOTE: We are using {EXPORT} below
	push(@{ $ENV{PRESCAN}->{EXPORT} }, $prescan_fastexport);
}

sub parse_values_clause
{
	my $values = shift;     # The entire "VALUES(...)"
	my $col_num = 0;

	# Remove commas inside parens, so that we can split on comma to get each column def
	$values =~ s{
			(
				\(                      # Opening paren (or whatever char you like)
				(?: [^()]* | (?0) )*    # Match a series of non-parens or another "self"
				\)                      # Closing char
			)
	}{mask_commas($1)}sexig;
	
	my $return = ();
	foreach my $col_def (split(/,/, $values))
	{
		$col_num++;
		my $col_name = '';

		# 
		if ($col_def =~ m{:(\w+)})
		{
			$col_name = uc($1);
		}
		elsif ($col_def =~ m{(\w+)})
		{
			$col_name = uc($1);
		}
		$col_def =~ s{\n}{ }g;
		$col_def =~ s{:$col_name\b}{$col_name}ig;   # Remove the ":", if present

		$return->{VALUES}->{$col_num}->{COL_NAME} = $col_name;
		$return->{VALUES}->{$col_num}->{VALUE_DEF} = $col_def;
		$return->{VALUES}->{$col_num}->{VALUE_DEF} =~ s{<:comma:>}{,}g;

		# Save also by colname => coldef
		$return->{VALUES_BY_COL_NAME}->{$col_name} = $col_def;
		$return->{VALUES_BY_COL_NAME}->{$col_name} =~ s{<:comma:>}{,}g;
	}
	return $return;
}

sub mask_commas 
# Return arg with all commas converted to "<:comma:>"
{
	my $text = shift;
	$text =~ s{,}{<:comma:>}g;
	return $text;
}

sub mask_comment 
# Save supplied value (should be a comment line) in a hash with a unique id, returning "--<<<c_o_m_m_e_n_t: <unique_id>"
{
	my $comment = shift;
	$comment_num++;
	$ENV{PRESCAN}->{COMMENTS}->{$comment_num} = $comment;
	return "--" . '<<<c_o_m_m_e_n_t: ' . $comment_num;   # --<<<c_o_m_m_e_n_t: 1
}

sub mask_single_line_c_comment
{
	my $comment = shift;
	$comment_num++;
	$ENV{PRESCAN}->{COMMENTS_C_SINGLE_LINE}->{$comment_num} = $comment;
	return "/*" . '<<<c_o_m_m_e_n_t: ' . $comment_num . '*/';
}

sub handle_column_check 
{
	my $table_name = shift;
	my $text = shift;
	if ($text =~ m{
				(
					\(                      # Opening paren 
					(?: [^()]* | (?0) )*    # Match a series of non-parens or another "self"
					\)                      # Closing paren
				)
			}xsig) 
	{
		my $col_check = $1;
		my ($col_name) = $col_check =~ m{\(\s*(\w+)};
		push (@{ $ENV{PRESCAN}->{COL_CHECKS}->{$table_name}->{$col_name} }, $col_check) 
					unless grep(/^\Q$col_check\E$/,@{ $ENV{PRESCAN}->{COL_CHECKS}->{$table_name}->{$col_name} });
	}
}

sub prescan_constraint_primary_key
{
	my ($table_name, $cont) = (@_);

	# Get cases like "col name ... PRIMARY KEY"
	foreach my $comma_sep_block(split(',', $cont))
	{
		next if ($comma_sep_block =~ m{\bPRIMARY\s+KEY\s*\(}i);    # Ignore for now
		my $col_name = '';
		if ($comma_sep_block =~ m{\bPRIMARY\s+KEY\b}i)
		{
			if ($comma_sep_block =~ m{\(\s*(\w+)})    # If col name is first col in table def, i.e. first word after "("
			{
				$col_name = $1;
			} elsif ($comma_sep_block =~ m{(\w+)})    # Otherwise, col name is first word
			{
				$col_name = $1;
			}
		}
		if ($col_name)
		{
			# Note: There is no constraint name, so we use the column name instead
			$ENV{PRESCAN}->{PRIMARY_KEYS}->{$table_name}->{$col_name} = $col_name;
		}
	}

	# Cases like "<constraint_name> PRIMARY KEY (...) 
	while ($cont =~ m{\b(\w+)\s+PRIMARY\s+KEY\s*\((.*?)\)}sig)
	{
		# Note: $1 is the constraint name, e.g. "primary1" in the code: CONSTRAINT primary1 (col01, col02)
		push (@{ $ENV{PRESCAN}->{PRIMARY_KEYS}->{$table_name}->{$1} }, $1);
		$ENV{PRESCAN}->{PRIMARY_KEYS}->{$table_name}->{$1} = $2;
	}
}

sub prescan_constraint_foreign_key 
# Should always be: FOREIGN KEY (...) REFERENCES <ref> (...)
{
	my ($table_name, $cont) = (@_);

	my $fkid = 0;
	while ($cont =~ m{\bFOREIGN\s+KEY\s*(\(.*?\))\s*REFERENCES\s+(\w+)\s*(\(.*?\))}sig)
	{
		$fkid++;
		my $fk_name = "fk_$fkid";
		$ENV{PRESCAN}->{FOREIGN_KEYS}->{$table_name}->{$fk_name} = "FOREIGN KEY $1 REFERENCES $2 $3";
	}
}

sub prescan_procedure_stmt 
{
	$MR->log_msg("Begin prescan_procedure_stmt");
	# Extract things from the PROCEDURE statement
	my $procedure_stmt = shift;
	# This used to be just the bit in parens in "PROCEDURE <procname> (...this bit...) ...BEGIN", but
	# now it is the whole thing 

	# $procedure_stmt =~ m{\sPROCEDURE\s.*?\((.*)\).*?\bBEGIN}si;
	$procedure_stmt =~ m{\sPROCEDURE\s+([\w.]+)\s*\((.*)\).*?\bBEGIN}si;
	my $procedure_name = $1;
	my $procedure_args = $2;
	$ENV{PRESCAN}->{PROC_NAME} = $procedure_name;

	$ENV{PRESCAN}->{use_sql_statement_wrapper} = '1';

	my $prescan_proc_args = ();

	# Save the "before" in case we need to search through it or report it
	$prescan_proc_args->{ORIGINAL_SOURCE} = $procedure_args;

	# mask out commas in things like "DEC(5,2)"
	$procedure_args =~ s{([0-9]+\s*),(\s*[0-9]+)}{$1<<<comma>>>$2}g;

	my @arg_defs = split(',', $procedure_args);
	my $arg_num = 0;
	foreach (@arg_defs) {
		$MR->log_msg("teradata proc params: $_");
		s{<<<comma>>>}{,}g;
		my ($arg_name, $arg_type, $data_type, $rest) = ('', '','');

		# Get arg name and data type 
		if (m{^\s*(IN|OUT|INOUT)\s+(\w+)\s+(\w+)(.*)}i) {         # IN/OUT/INOUT <argname> <datatype>
			($arg_type, $arg_name, $data_type, $rest) = ($1, $2, $3, $4);
		} elsif (m{^\s*(\w+)\s+(IN|OUT|INOUT)\s+(\w+)(.*)}i) {    # <argname> IN/OUT/INOUT <datatype>
			($arg_name, $arg_type, $data_type, $rest) = ($1, $2, $3, $4);
		} elsif (m{^\s*(\w+)\s+(\w+)(.*)}) {                      # <argname> <datatype> (defaults to an "IN" arg type)
			($arg_name, $arg_type, $data_type, $rest) = ($1, 'IN', $2, $3);
		} else {
			$MR->log_err("Canny find arg name in $_\n");
		}
		# my $args->{NAME}   = uc($MR->trim($arg_name));
		my $args->{NAME}   = $MR->trim($arg_name);     # No longer converting to upper case
		$args->{DATA_TYPE} = uc($MR->trim($data_type));
		$args->{FULL_DATA_TYPE} = uc($MR->trim($data_type)) . $MR->trim($rest); #need precision for some of the target systems
		$args->{ARG_TYPE}  = uc($MR->trim($arg_type));
		push (@{$ENV{PRESCAN}->{PROC_ARGS}}, $args);

		# Add to catalog. The ":::" is a separator between a key and a value
		push (@conversion_catalog_add, "stored_procedure_args,$procedure_name,$arg_num" . ':::' . "$arg_name,$arg_type,$data_type");
		$arg_num++;
	}

	# If there are no args, put an entry in the catalog with 'x' as the number of args, so that we 
	# still know that this stored procedure exists
	if ($arg_num == 0)
	{
		push (@conversion_catalog_add, "stored_procedure_args,$procedure_name,x" . ':::');
	}
}

sub prescan_declare
# Extract things from DECLARE statements.
#  .----------------------------------------------------------.
#  | NOTE: This subroutine prescans __ONE__ DECLARE statement |  
#  '----------------------------------------------------------'
{
	my $declare = shift;

	if ($declare =~ m{^\s*DECLARE\s+CONTINUE\s}si)
	{
		push(@{ $ENV{PRESCAN}->{DECLARE_CONTINUE_HANDLER} }, $declare);
		return;
	}

	if ($declare =~ m{^\s*DECLARE\s+\w+\s+CONDITION\s}si)
	{
		push(@{ $ENV{PRESCAN}->{DECLARE_CONDITION} }, $declare);
		return;
	}

	if ($declare =~ m{^\s*DECLARE\s+\w+\s+CURSOR\s}si)
	{
		push(@{ $ENV{PRESCAN}->{DECLARE_CURSOR} }, $declare);
		return;
	}

	# For a "DECLARE EXIT HANDLER FOR <conditions> <action>"
	if ($declare =~ m{^\s*(DECLARE\s+EXIT\s+HANDLER\s+FOR\s+)
								( 
									(
										(SQLSTATE\s+['"]\s*.*?\s*['"]|\w+)  # Condition(s)
										(\s*,\s*)?
									)+
								)
								(.*)  # Action
					 }xsi)
	{
		push(@{ $ENV{PRESCAN}->{DECLARE_EXIT_HANDLER} }, {CONDITION => "$1$2", ACTION => $6});
		return;
	}

	# DECLARE variables
	my ($data_type, $default_value) = ('', '');
	$declare =~ s{;\s*$}{};         # Remove semi-colon
	$declare =~ s{\s*$}{};          # Remove end spaces
	
	# DEFAULT
	my $default_value = '';
	$declare =~ s{\bDEFAULT\s+(.*)}{}si and $default_value = $1;
	#$default_value =~ s{^\s*['"](.*)['"]\s*$}{$1};    # Trim leading / trailing spaces / quotes
	$default_value =~ s{^\s*"(.*)"\s*$}{$1};    # Trim leading / trailing spaces / double quotes
	$default_value = $MR->trim($default_value);

	# Length (the bit in parens)
	my $length = '';
	my $full_data_type = (split(/\s+/, $declare))[2]; #3rd token in the statement
	$declare =~ s{\((.*)\)}{}si and $length = $1;
	$length =~ s{\s+}{}g;

	# Anything left in $delcare should be variable name(s) followed by the data type
	my ($var_names, $data_type) = $declare =~ m{^\s*DECLARE\s+(.*)\s+(\w+)}si;

	$var_names =~ s{\s+}{}g;
	foreach my $var_name (split(',', $var_names))
	{
		# my $vars->{NAME} = uc($var_name);
		my $vars->{NAME} = $var_name;          # No longer converting to upper case
		$vars->{DATA_TYPE} = uc($data_type);
		$vars->{LENGTH} = $length; 
		$vars->{DEFAULT_VALUE} = $default_value; 
		$vars->{FULL_DATA_TYPE} = $full_data_type;
		push(@{ $ENV{PRESCAN}->{VARIABLES} }, $vars);
	}
}

sub prescan_select_into  
{
	my $source = shift;

	my $prescan_select_into = ();

	# Save the "before" in case we need to search through it or report it
	$prescan_select_into->{ORIGINAL_SOURCE} = $source;

	# if ($source =~ m{^\s*(SELECT.*?)\s+(INTO.*?)\s+(FROM.*?)}sxi) {
	if ($source =~ m{\b(SELECT.*?)\s+(INTO.*?)\s+(FROM.*?)}sxi) {
		$source =~ s{;\s*$}{};

		# We know that we have SELECT, INTO, and FROM
		my ($select, $into, $from) = $source =~ m{\b(SELECT.*?)\s+(INTO.*?)\s+(FROM.*)}sxi;
		
		# Separate FROM and WHERE, if WHERE is present
		my $where = '';
		if ($from =~ m{^(.*)\s+(WHERE\s+.*)}sxi) {
			($from, $where) = ($1, $2);
		}

		$prescan_select_into->{SELECT} = $select;
		$prescan_select_into->{INTO} = $into;
		$prescan_select_into->{FROM} = $from;
		$prescan_select_into->{WHERE} = $where;

		push(@{ $ENV{PRESCAN}->{SELECT_INTO} }, $prescan_select_into);
	}
}

sub prescan_macro
{
	my $source = shift;

	my $prescan_macro = ();

	# Save the "before" in case we need to search through it or report it
	$prescan_macro->{ORIGINAL_SOURCE} = $source;

	# remove '.*'
	# remove (n,n)
	# then we can split inside first (...) on comma to get col defs

	if ($source =~ m{\b(CREATE|REPLACE)\s+MACRO\s+([\w.]+)(.*?)\bAS\s+\((.*)\)}si)
	{

		$ENV{PRESCAN}->{use_sql_statement_wrapper} = '1';

		$prescan_macro->{MACRO_NAME} = $2;
		my $arg_defs = $3;
		$prescan_macro->{SQL} = $4;

		# Arguments will be inside first set of parens, if present
		if ($arg_defs =~ m{^\s*\((.*)\)\s*$}s)
		{
			$arg_defs = $1;
			$arg_defs =~ s{'.*?'}{}sg;     # Not interested in contents of '...'
			$arg_defs =~ s{\(.*?\)}{}sg;   # or (...)
			foreach my $arg_def (split(/,/, $arg_defs))
			{
				if ($arg_def =~ m{(\w+)})
				{
					# push(@arg_names, $1;);
					push(@{ $prescan_macro->{ARGS} }, $1);
				}
			}
		}

	}
	push(@{ $ENV{PRESCAN}->{MACRO} }, $prescan_macro);
}
