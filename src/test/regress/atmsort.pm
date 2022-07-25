#
# This is the workhorse of atmsort.pl, extracted into a module so that it
# can be called more efficiently from other perl programs.
#
# Public interface:
#
#   atmsort_init(args in a hash)
#
# followed by:
#
#   run_fhs(input file handle, output file handle)
# or
#   run(input filename, output filename)
#
package atmsort;

#use Data::Dumper; # only used by commented-out debug statements.
use strict;
use warnings;
use File::Temp qw/ tempfile /;

# Load explain.pm from the same directory where atmsort.pm is.
use FindBin;
use lib "$FindBin::Bin";
use explain;

# WIP:
# points to fit:
# - MUST not reorder if order counts?
# - if 'order by' used in function or prepare statement
# - can reorder lead to fake pass?
# - strings as sql statement? EXECUTE format('REFRESH MATERIALIZED VIEW %I.%I', mviews.mv_schema, mviews.mv_name);
# - query in loops/conditionals - https://www.postgresql.org/docs/current/plpgsql-control-structures.html#PLPGSQL-RECORDS-ITERATING
# - string constants - https://www.postgresql.org/docs/14/sql-syntax-lexical.html

# optional set of prefixes to identify sql statements, query output,
# and sorted lines (for testing purposes)
#my $apref = 'a: ';
#my $bpref = 'b: ';
#my $cpref = 'c: ';
#my $dpref = 'S: ';
my $apref = '';
my $bpref = '';
my $cpref = '';
my $dpref = '';

my $glob_ignore_plans;
my @glob_init;

my $glob_orderwarn;
my $glob_verbose;
my $glob_debug;
my $glob_fqo;

my $atmsort_outfh;

sub has_exec_tok
{
        my $line = shift;
        if ($line =~ m/;|\\(?:g\S*|crosstabview)/)
        {
                return 1;
        }
        return 0;
}

# check command type, returns ref of hash {grp, str}, where grp:
#  1 for sql command
#  -1 for meta command
#  0 for unknown type
sub get_command_type
{
        my $line = shift;
        my %rv = (grp=>0, str=>'');
        if ( $line =~ m/^\s*(
                alter |
                abort | begin | end | checkpoint | (?:release\s+)?savepoint | commit |
                close | rollback | declare | fetch | move | start\s+transaction |
                lock |
                grant | revoke | reassign |
                security\s+label |
                vacuum |
                reindex |
                refresh\s+materialized\s+view |
                set | reset | show |
                do |
                prepare | execute |
                call |
                with |
                create | drop | copy | truncate |
                select | insert | update | delete |
                values |
                (?:un)?listen | notify |
                analyze | explain
                ) # NOTE: some sql statements e.g. 'select\if true "a" \endif \\ from table1' are not handled
                (?:$|\s|\(|'|"|--|\\)
                /ix )
        {
                $rv{grp} = 1;
                $rv{str} = $1;
                return \%rv;
        }
        if ( $line =~ m/^\s*\\(
                set | unset | prompt
                | if | elif | else | endif # conditional
                | i | ir | o | echo | qecho | copy # io
                | g | gdesc | gexec | gset | gx
                | crosstabview
                | copyright | errverbose
                | q | watch
                | ( d[a-zA-Z]* | l | s[fv] )\+? | z # TODO: d commands 
                | timing | setenv | cd | !
                | a | x | t | f | C | pset | H | T
                | c | connect | conninfo | encoding | password
                | lo_(?: export | import | list | unlink ) |
                | h | \?
                | e | ef | ev | p | r | s | w 
                )
                (?:$|\s) # add $ just for the tailing line of a file without ending newline
                /x )
        {
                $rv{grp} = -1;
                $rv{str} = $1;
                return \%rv;
        }
        return \%rv;
}

sub atmsort_init
{
    my %args = (
        # defaults
        IGNORE_PLANS    => 0,
        INIT_FILES      => [],
        ORDER_WARN      => 0,
        VERBOSE         => 0,
        DEBUG           => 0,
        ENABLE_NESTED_IGNORE => 0,

        # override the defaults from argument list
        @_
    );

    $glob_ignore_plans        = 0;
    @glob_init                = ();

    $glob_orderwarn           = 0;
    $glob_verbose             = 0;
    $glob_debug               = 0;
    $glob_fqo                 = {count => 0};

    my $ignore_plans;
    my @init_file;
    my $verbose;
    my $orderwarn;
    my $enble_nested_ignore;

    $glob_ignore_plans        = $args{IGNORE_PLANS};

    @glob_init = @{$args{INIT_FILES}};

    $glob_orderwarn           = $args{ORDER_WARN};
    $glob_verbose             = $args{VERBOSE};
    $glob_debug               = $args{DEBUG};

    init_match_subs();
    init_matchignores();

    _process_init_files();
}

sub _process_init_files
{
    # allow multiple init files
    if (@glob_init)
    {
        my $devnullfh;
        my $init_file_fh;

        open $devnullfh, "> /dev/null" or die "can't open /dev/null: $!";

        for my $init_file (@glob_init)
        {
            die "no such file: $init_file"
            unless (-e $init_file);

            # Perform initialization from this init_file by passing it
            # to bigloop. Open the file, and pass that as the input file
            # handle, and redirect output to /dev/null.
            open $init_file_fh, "< $init_file" or die "could not open $init_file: $!";

            atmsort_bigloop($init_file_fh, $devnullfh);

            close $init_file_fh;
        }

        close $devnullfh;
    }
}

my $glob_match_then_sub_fnlist;

sub _build_match_subs
{
    my ($here_matchsubs, $whomatch) = @_;

    my $stat = [1];

    # filter out the comments and blank lines
    $here_matchsubs =~ s/^\s*(?:#.*)?(?:[\r\n]|\x0D\x0A)//gm;

    # split up the document into separate lines
    my @foo = split(/\n/, $here_matchsubs);

    my $ii = 0;

    my $matchsubs_arr = [];
    my $msa;

    # build an array of arrays of match/subs pairs
    while ($ii < scalar(@foo))
    {
        my $lin = $foo[$ii];

        if (defined($msa))
        {
            push @{$msa}, $lin;

            push @{$matchsubs_arr}, $msa;

            undef $msa;
        }
        else
        {
            $msa = [$lin];
        }
        $ii++;
        next;
    } # end while

#    print Data::Dumper->Dump($matchsubs_arr);

    my $bigdef;

    my $fn1;

    # build a lambda function for each expression, and load it into an
    # array
    my $mscount = 1;

    for my $defi (@{$matchsubs_arr})
    {
        unless (2 == scalar(@{$defi}))
        {
            my $err1 = "bad definition: " . Data::Dumper->Dump([$defi]);
            $stat->[0] = 1;
            $stat->[1] = $err1;
            return $stat;
        }

        $bigdef = '$fn1 = sub { my $ini = shift; '. "\n";
        $bigdef .= 'if ($ini =~ ' . $defi->[0];
        $bigdef .= ') { ' . "\n";
#        $bigdef .= 'print "match\n";' . "\n";
        $bigdef .= '$ini =~ ' . $defi->[1];
        $bigdef .= '; }' . "\n";
        $bigdef .= 'return $ini; }' . "\n";

#        print $bigdef;

        if (eval $bigdef)
        {
            my $cmt = $whomatch . " matchsubs \#" . $mscount;
            $mscount++;

            # store the function pointer and the text of the function
            # definition
            push @{$glob_match_then_sub_fnlist},
            [$fn1, $bigdef, $cmt, $defi->[0], $defi->[1]];

            if ($glob_verbose && defined $atmsort_outfh)
            {
                print $atmsort_outfh "GP_IGNORE: Defined $cmt\t$defi->[0]\t$defi->[1]\n"
            }
        }
        else
        {
            my $err1 = "bad eval: $bigdef";
            $stat->[0] = 1;
            $stat->[1] = $err1;
            return $stat;
        }

    }

#    print Data::Dumper->Dump($glob_match_then_sub_fnlist);

    return $stat;

} # end _build_match_subs

# list of all the match/substitution expressions
sub init_match_subs
{
    my $here_matchsubs;


# construct a "HERE" document of match expressions followed by
# substitution expressions.  Embedded comments and blank lines are ok
# (they get filtered out).

    $here_matchsubs = << 'EOF_matchsubs';

# some cleanup of greenplum-specific messages
m/\s+\(seg.*pid.*\)/
s/\s+\(seg.*pid.*\)//

m/^(?:ERROR|WARNING|CONTEXT|NOTICE):.*connection.*failed.*(?:http|gpfdist)/
s/connection.*failed.*(http|gpfdist).*/connection failed dummy_protocol\:\/\/DUMMY_LOCATION/

# the EOF ends the HERE document
EOF_matchsubs

    $glob_match_then_sub_fnlist = [];

    my $stat = _build_match_subs($here_matchsubs, "DEFAULT");

    if (scalar(@{$stat}) > 1)
    {
        die $stat->[1];
    }

}

sub match_then_subs
{
    my $ini = shift;

    for my $ff (@{$glob_match_then_sub_fnlist})
    {
        # get the function and execute it
        my $fn1 = $ff->[0];
        if (!$glob_verbose)
        {
            $ini = &$fn1($ini);
        }
        else
        {
            my $subs = &$fn1($ini);
            unless ($subs eq $ini)
            {
                print $atmsort_outfh "GP_IGNORE: was: $ini";
                print $atmsort_outfh "GP_IGNORE: matched $ff->[-3]\t$ff->[-2]\t$ff->[-1]\n"
            }

            $ini = &$fn1($ini);
        }

    }
    return $ini;
}

my $glob_match_then_ignore_fnlist;

sub _build_match_ignores
{
    my ($here_matchignores, $whomatch) = @_;

    my $stat = [1];

    # filter out the comments and blank lines
    $here_matchignores =~ s/^\s*(?:#.*)?(?:[\r\n]|\x0D\x0A)//gm;

    # split up the document into separate lines
    my @matchignores_arr = split(/\n/, $here_matchignores);

    my $bigdef;

    my $fn1;

    # build a lambda function for each expression, and load it into an
    # array
    my $mscount = 1;

    for my $defi (@matchignores_arr)
    {
        $bigdef = '$fn1 = sub { my $ini = shift; '. "\n";
        $bigdef .= 'return ($ini =~ ' . $defi;
        $bigdef .= ') ; } ' . "\n";
#        print $bigdef;

        if (eval $bigdef)
        {
            my $cmt = $whomatch . " matchignores \#" . $mscount;
            $mscount++;

            # store the function pointer and the text of the function
            # definition
            push @{$glob_match_then_ignore_fnlist},
                    [$fn1, $bigdef, $cmt, $defi, "(ignore)"];
            if ($glob_verbose && defined $atmsort_outfh)
            {
                print $atmsort_outfh "GP_IGNORE: Defined $cmt\t$defi\n"
            }

        }
        else
        {
            my $err1 = "bad eval: $bigdef";
            $stat->[0] = 1;
            $stat->[1] = $err1;
            return $stat;
        }

    }

#    print Data::Dumper->Dump($glob_match_then_ignore_fnlist);

    return $stat;

} # end _build_match_ignores

# list of all the match/ignore expressions
sub init_matchignores
{
    my $here_matchignores;

# construct a "HERE" document of match expressions to ignore in input.
# Embedded comments and blank lines are ok (they get filtered out).

    $here_matchignores = << 'EOF_matchignores';

m/^NOTICE:  dropping a column that is part of the distribution policy/

m/^NOTICE:  table has parent\, setting distribution columns to match parent table/

m/^WARNING:  referential integrity \(.*\) constraints are not supported in Greenplum Database/

        # ignore notices for DROP sqlobject IF EXISTS "objectname"
        # eg NOTICE:  table "foo" does not exist, skipping
        #
        # the NOTICE is different from the ERROR case, which does not
        # end with "skipping"
m/^NOTICE:  \w+ \".*\" does not exist\, skipping\s*$/

# the EOF ends the HERE document
EOF_matchignores

    $glob_match_then_ignore_fnlist = [];

    my $stat = _build_match_ignores($here_matchignores, "DEFAULT");

    if (scalar(@{$stat}) > 1)
    {
        die $stat->[1];
    }

}

# if the input matches, return 1 (ignore), else return 0 (keep)
sub match_then_ignore
{
    my $ini = shift;

    for my $ff (@{$glob_match_then_ignore_fnlist})
    {
        # get the function and execute it
        my $fn1 = $ff->[0];

        if (&$fn1($ini))
        {
            if ($glob_verbose)
            {
                print $atmsort_outfh "GP_IGNORE: matched $ff->[-3]\t$ff->[-2]\t$ff->[-1]\n"
            }
            return 1; # matched
        }
    }
    return 0; # no match
}

# convert a postgresql psql formatted table into an array of hashes
sub tablelizer
{
    my ($ini, $got_line1) = @_;

    # first, split into separate lines, the find all the column headings

    my @lines = split(/\n/, $ini);

    return undef
        unless (scalar(@lines));

    # if the first line is supplied, then it has the column headers,
    # so don't try to find them (or the ---+---- separator) in
    # "lines"
    my $line1 = $got_line1;
    $line1 = shift @lines
        unless (defined($got_line1));

    # look for <space>|<space>
    my @colheads = split(/\s+\|\s+/, $line1);

    # fixup first, last column head (remove leading,trailing spaces)

    $colheads[0] =~ s/^(\s+|\s+$)//;
    $colheads[-1] =~ s/^(\s+|\s+$)//;

    return undef
        unless (scalar(@lines));

    shift @lines # skip dashed separator (unless it was skipped already)
        unless (defined($got_line1));

    my @rows;

    for my $lin (@lines)
    {
        my @cols = split(/\|/, $lin, scalar(@colheads));
        last
            unless (scalar(@cols) == scalar(@colheads));

        my $rowh = {};

        for my $colhdcnt (0..(scalar(@colheads)-1))
        {
            my $rawcol = shift @cols;

            $rawcol =~ s/^(\s+|\s+$)//;

            my $colhd = $colheads[$colhdcnt];
            $rowh->{($colhdcnt+1)} = $rawcol;
        }
        push @rows, $rowh;
    }

    return \@rows;
}
# reformat the EXPLAIN output according to the directive hash
sub format_explain
{
    my ($outarr, $directive) = @_;
    my $prefix = "";
    my $xopt = "perl"; # normal case

    $directive = {} unless (defined($directive));

    # Ignore plan content if its between start_ignore and end_ignore blocks
    # or if -ignore_plans is specified.
    $prefix = "GP_IGNORE:"
         if (exists($directive->{ignore})) || ($glob_ignore_plans);

    my @tmp_lines;
    my $sort = 0;

    if (scalar(@{$outarr}))
    {
        @tmp_lines = (
            "QUERY PLAN\n",
            ("-" x 71) . "\n",
            @{$outarr},
            "(111 rows)\n"
            );
    }

    if (exists($directive->{explain})
        && ($directive->{explain} =~ m/operator/i))
    {
        $xopt = 'operator';
        $sort = 1;
    }

    my $xplan = '';

    open(my $xplan_fh, ">", \$xplan)
        or die "Can't open in-memory file handle to variable: $!";

    explain::explain_init(OPERATION => $xopt,
                  PRUNE => 'heavily',
                  INPUT_LINES => \@tmp_lines,
                  OUTPUT_FH => $xplan_fh);

    explain::run();

    close $xplan_fh;

    my @lines = split /\n/, $xplan;

    if ($sort && scalar(@lines) > 0)
    {
        @lines = sort @lines;
    }

    # Apply prefix to each line, if requested.
    if (defined($prefix) && length($prefix))
    {
        foreach my $line (@lines)
        {
            $line = $prefix . $line;
        }
    }

    # Put back newlines and print
    foreach my $line (@lines)
    {
        $line .= "\n";
        print $atmsort_outfh $line;
    }

    return  \@lines;
}

# reformat the query output according to the directive hash
sub format_query_output
{
    my ($fqostate, $has_order, $outarr, $directive) = @_;
    my $prefix = "";

    my @a = keys $directive;
    print "directive: ", join(',', @a), "\n";
    my @b = values $directive;
    print "directive: ", join(',', @b), "\n";

    $directive = {} unless (defined($directive));

    $fqostate->{count} += 1;

    if ($glob_verbose)
    {
        print $atmsort_outfh "GP_IGNORE: start fqo $fqostate->{count}\n";
    }

    # EXPLAIN
    #
    # EXPLAIN (COSTS OFF) output is *not* processed. The output with COSTS OFF
    # shouldn't contain anything that varies across runs, and shouldn't need
    # sanitizing.
    #
    # However when -ignore_plans is specified we also need to process
    # EXPLAIN (COSTS OFF) to ignore the segments information.
    if (exists($directive->{explain})
        && ($glob_ignore_plans
            || $directive->{explain} ne 'costs_off')
        && (!exists($directive->{explain_processing})
            || ($directive->{explain_processing} =~ m/on/)))
    {
       format_explain($outarr, $directive);
       if ($glob_verbose)
       {
           print $atmsort_outfh "GP_IGNORE: end fqo $fqostate->{count}\n";
       }
       return;
    }

    $prefix = "GP_IGNORE:"
        if (exists($directive->{ignore}));

    if (exists($directive->{sortlines}))
    {
        my $firstline = $directive->{firstline};
        my $ordercols = $directive->{order};
        my $mvdlist   = $directive->{mvd};

        # lines already have newline terminator, so just rejoin them.
        my $lines = join ("", @{$outarr});

        my $ah1 = tablelizer($lines, $firstline);

        unless (defined($ah1) && scalar(@{$ah1}))
        {
#            print "No tablelizer hash for $lines, $firstline\n";
#            print STDERR "No tablelizer hash for $lines, $firstline\n";

            if ($glob_verbose)
            {
                print $atmsort_outfh "GP_IGNORE: end fqo $fqostate->{count}\n";
            }

            return;
        }

        my @allcols = sort (keys(%{$ah1->[0]}));

        my @presortcols;
        if (defined($ordercols) && length($ordercols))
        {
#        $ordercols =~ s/^.*order\s*//;
            $ordercols =~ s/\n//gm;
            $ordercols =~ s/\s//gm;

            @presortcols = split(/\s*\,\s*/, $ordercols);
        }

        my @mvdcols;
        my @mvd_deps;
        my @mvd_nodeps;
        my @mvdspec;
        if (defined($mvdlist) && length($mvdlist))
        {
            $mvdlist  =~ s/\n//gm;
            $mvdlist  =~ s/\s//gm;

            # find all the mvd specifications (separated by semicolons)
            my @allspecs = split(/\;/, $mvdlist);

#            print "allspecs:", Data::Dumper->Dump(\@allspecs);

            for my $item (@allspecs)
            {
                my $realspec;
                # split the specification list, separating the
                # specification columns on the left hand side (LHS)
                # from the "dependent" columns on the right hand side (RHS)
                my @colset = split(/\-\>/, $item, 2);
                unless (scalar(@colset) == 2)
                {
                    print $atmsort_outfh "invalid colset for $item\n";
                    print STDERR "invalid colset for $item\n";
                    next;
                }
                # specification columns (LHS)
                my @scols = split(/\,/, $colset[0]);
                unless (scalar(@scols))
                {
                    print $atmsort_outfh "invalid dependency specification: $colset[0]\n";
                    print STDERR
                        "invalid dependency specification: $colset[0]\n";
                    next;
                }
                # dependent columns (RHS)
                my @dcols = split(/\,/, $colset[1]);
                unless (scalar(@dcols))
                {
                    print $atmsort_outfh "invalid specified dependency: $colset[1]\n";
                    print STDERR "invalid specified dependency: $colset[1]\n";
                    next;
                }
                $realspec = {};
                my $scol2 = [];
                my $dcol2 = [];
                my $sdcol = [];
                $realspec->{spec} = $item;
                push @{$scol2}, @scols;
                push @{$dcol2}, @dcols;
                push @{$sdcol}, @scols, @dcols;
                $realspec->{scol} = $scol2;
                $realspec->{dcol} = $dcol2;
                $realspec->{allcol} = $sdcol;

                push @mvdcols, @scols, @dcols;
                # find all the dependent columns
                push @mvd_deps, @dcols;
                push @mvdspec, $realspec;
            }

            # find all the mvd cols which are *not* dependent.  Need
            # to handle the case of self-dependency, eg "mvd 1->1", so
            # must build set of all columns, then strip out the
            # "dependent" cols.  So this is the set of all LHS columns
            # which are never on the RHS.
            my %get_nodeps;

            for my $col (@mvdcols)
            {
                $get_nodeps{$col} = 1;
            }

            # remove dependent cols
            for my $col (@mvd_deps)
            {
                if (exists($get_nodeps{$col}))
                {
                    delete $get_nodeps{$col};
                }
            }
            # now sorted and unique, with no dependents
            @mvd_nodeps = sort (keys(%get_nodeps));
#            print "mvdspec:", Data::Dumper->Dump(\@mvdspec);
#            print "mvd no deps:", Data::Dumper->Dump(\@mvd_nodeps);
        }

        my %unsorth = map { $_ => 1 } @allcols;

        # clear sorted column list if just "order 0"
        if ((1 == scalar(@presortcols))
            && ($presortcols[0] eq "0"))
        {
            @presortcols = ();
        }


        for my $col (@presortcols)
        {
            if (exists($unsorth{$col}))
            {
                delete $unsorth{$col};
            }
        }
        for my $col (@mvdcols)
        {
            if (exists($unsorth{$col}))
            {
                delete $unsorth{$col};
            }
        }
        my @unsortcols = sort(keys(%unsorth));

#        print Data::Dumper->Dump([$ah1]);

        if (scalar(@presortcols))
        {
            my $hd1 = "sorted columns " . join(", ", @presortcols);

            print $hd1, "\n", "-"x(length($hd1)), "\n";

            for my $h_row (@{$ah1})
            {
                my @collist;

                @collist = ();

#            print "hrow:",Data::Dumper->Dump([$h_row]), "\n";

                for my $col (@presortcols)
                {
#                print "col: ($col)\n";
                    if (exists($h_row->{$col}))
                    {
                        push @collist, $h_row->{$col};
                    }
                    else
                    {
                        my $maxcol = scalar(@allcols);
                        my $errstr =
                            "specified ORDER column out of range: $col vs $maxcol\n";
                        print $atmsort_outfh $errstr;
                        print STDERR $errstr;
                        last;
                    }
                }
                print $atmsort_outfh $prefix, join(' | ', @collist), "\n";
            }
        }

        if (scalar(@mvdspec))
        {
            my @outi;

            my $hd1 = "multivalue dependency specifications";

            print $hd1, "\n", "-"x(length($hd1)), "\n";

            for my $mspec (@mvdspec)
            {
                $hd1 = $mspec->{spec};
                print $hd1, "\n", "-"x(length($hd1)), "\n";

                for my $h_row (@{$ah1})
                {
                    my @collist;

                    @collist = ();

#            print "hrow:",Data::Dumper->Dump([$h_row]), "\n";

                    for my $col (@{$mspec->{allcol}})
                    {
#                print "col: ($col)\n";
                        if (exists($h_row->{$col}))
                        {
                            push @collist, $h_row->{$col};
                        }
                        else
                        {
                            my $maxcol = scalar(@allcols);
                            my $errstr =
                            "specified MVD column out of range: $col vs $maxcol\n";
                            print $errstr;
                            print STDERR $errstr;
                            last;
                        }

                    }
                    push @outi, join(' | ', @collist);
                }
                my @ggg= sort @outi;

                for my $line (@ggg)
                {
                    print $atmsort_outfh $prefix, $line, "\n";
                }
                @outi = ();
            }
        }
        my $hd2 = "unsorted columns " . join(", ", @unsortcols);

        # the "unsorted" comparison must include all columns which are
        # not sorted or part of an mvd specification, plus the sorted
        # columns, plus the non-dependent mvd columns which aren't
        # already in the list
        if ((scalar(@presortcols))
            || scalar(@mvd_nodeps))
        {
            if (scalar(@presortcols))
            {
                if (scalar(@mvd_deps))
                {
                    my %get_presort;

                    for my $col (@presortcols)
                    {
                        $get_presort{$col} = 1;
                    }
                    # remove "dependent" (RHS) columns
                    for my $col (@mvd_deps)
                    {
                        if (exists($get_presort{$col}))
                        {
                            delete $get_presort{$col};
                        }
                    }
                    # now sorted and unique, minus all mvd dependent cols
                    @presortcols = sort (keys(%get_presort));

                }

                if (scalar(@presortcols))
                {
                    $hd2 .= " ( " . join(", ", @presortcols) . ")";
                    # have to compare all columns as unsorted
                    push @unsortcols, @presortcols;
                }
            }
            if (scalar(@mvd_nodeps))
            {
                my %get_nodeps;

                for my $col (@mvd_nodeps)
                {
                    $get_nodeps{$col} = 1;
                }
                # remove "nodeps" which are already in the output list
                for my $col (@unsortcols)
                {
                    if (exists($get_nodeps{$col}))
                    {
                        delete $get_nodeps{$col};
                    }
                }
                # now sorted and unique, minus all unsorted/sorted cols
                @mvd_nodeps = sort (keys(%get_nodeps));
                if (scalar(@mvd_nodeps))
                {
                    $hd2 .= " (( " . join(", ", @mvd_nodeps) . "))";
                    # have to compare all columns as unsorted
                    push @unsortcols, @mvd_nodeps;
                }

            }

        }

        print $hd2, "\n", "-"x(length($hd2)), "\n";

        my @finalunsort;

        if (scalar(@unsortcols))
        {
            for my $h_row (@{$ah1})
            {
                my @collist;

                @collist = ();

                for my $col (@unsortcols)
                {
                    if (exists($h_row->{$col}))
                    {
                        push @collist, $h_row->{$col};
                    }
                    else
                    {
                        my $maxcol = scalar(@allcols);
                        my $errstr =
                            "specified UNSORT column out of range: $col vs $maxcol\n";
                        print $errstr;
                        print STDERR $errstr;
                        last;
                    }

                }
                push @finalunsort, join(' | ', @collist);
            }
            my @ggg= sort @finalunsort;

            for my $line (@ggg)
            {
                print $atmsort_outfh $prefix, $line, "\n";
            }
        }

        if ($glob_verbose)
        {
            print "GP_IGNORE: end fqo $fqostate->{count}\n";
        }

        return;
    } # end order


    if ($has_order)
    {
        my @ggg= @{$outarr};

        if ($glob_orderwarn)
        {
            # If no ordering cols specified (no directive), and SELECT has
            # ORDER BY, see if number of order by cols matches all cols in
            # selected lists. Treat the order by cols as a comma separated
            # list and count them. Works ok for simple ORDER BY clauses
            if (defined($directive->{sql_statement}))
            {
                my @ocols = ($directive->{sql_statement} =~ m/select.*order\s+by\s+(.*)\;/ism);

                if (scalar(@ocols))
                {
                    my $fl2 = $directive->{firstline};
                    # lines already have newline terminator, so just rejoin them.
                    my $line2 = join ("", @{$outarr});

                    my $ah2 = tablelizer($line2, $fl2);
                    if (defined($ah2) && scalar(@{$ah2}))
                    {
                        my $allcol_count = scalar(keys(%{$ah2->[0]}));

                        # In order to count the number of ORDER BY columns we
                        # can transliterate over comma and increment by one to
                        # account for the last column not having a trailing
                        # comma. This is faster than splitting over the comma
                        # since we don't need to allocate the returned array.
                        my $ocol_count = ($ocols[0] =~ tr/,//) + 1;

                        if ($ocol_count < $allcol_count)
                        {
                            print "GP_IGNORE: ORDER_WARNING: OUTPUT ",
                            $allcol_count, " columns, but ORDER BY on ",
                            $ocol_count, " \n";
                        }
                    }
                }
            }
        } # end if $glob_orderwarn

        for my $line (@ggg)
        {
            print $atmsort_outfh $dpref, $prefix, $line;
        }
    }
    else
    {
        my @ggg= sort @{$outarr};

        for my $line (@ggg)
        {
            print $atmsort_outfh $bpref, $prefix, $line;
        }
    }

    if ($glob_verbose)
    {
        print "GP_IGNORE: end fqo $fqostate->{count}\n";
    }
}

# remove the following data which may distrub regex matching:
#  1) quoted string constant ($tag$abc$tag$ or 'abc' or E'ab\\cd' or B'0101...' or X'1FF' )
#  2) c-style comment (/**/)
#  3) quoted identifier
# NOTE: should we check some settings (e.g. standard_conforming_strings)?
sub slim_raw_text
{
	my $txt = shift;
	my $ctx_ref = shift;

	my $stage = ($ctx_ref->{stage} or '');
	my $lv = ($ctx_ref->{c_level} or 0);
	my $tag = ($ctx_ref->{s_tag} or '');
	my $esc_mode = ($ctx_ref->{esc_mode} or -1);
	my $forced_esc_mode = 0;
	my $fallback_esc_mode = -1;

	if ($glob_debug)
	{
		print 'GP_IGNORE: #slim start: ', $txt;
	}
	if ($txt =~ /^\s*\-\-/)
	{
		if (!$stage)
		{
			# the comment line/output header need no preprocessing  
			return $txt;
		}
		else
		{
		}
	}

	if ($glob_debug)
	{
		print "GP_IGNORE: new line initial stage:", $stage, "\n";
	}

	my $loc;
	my $tok;
	my $tok_len;
	my $outstr = '';
	my $last_keep = (($stage)? -1:0);
	my $esc_pos = -1;
	# TODO:
	# 1) The implementation do not support multi-bytes characters well.
	# We should avoid matching those key sequences as a part of one
	# multi-bytes character. Lucky we do not have such case in current test,
	# so the implementation just works now.
	# 2) The default escape status is unknown (not awared of settings
	# e.g. standard_conforming_strings). It's hard to decide if a backslash
	# in quoted string is literal or not. We should leave the wrong
	# string/identifer to pg parser. It can be very helpful to add a new
	# gpdiff command to confirm the info
	# 3) test on syntax may break the check, please use start_ignore to skip
	# the line with wrong syntax
	# 4) string constants with Unicode escapes is 'unsafe' when
	# standard_conforming_strings is off, but identifier is ok
	# 5) output of sql statement should be skipped
	# 6) Check output sorting for multi-line string (each but the last
	# endswith plus sign)? Is it OK in the header/data row?
	# 7) Should we enable multi-line single/double quote?
	while ($txt =~ m!((?:\\+)|(?:\$(?:[[:alpha:]_][[:alnum:]_]*)?\$)|'|"|/\*|\*/)!ig)
	{
		$loc = pos($txt);
		$tok = $1;
		$tok_len = length($tok);
		my $cmd = substr($tok, 0, 1);
		# prepare the escape sequence
		if ($cmd eq "\\")
		{
			if (($tok_len % 2) == 1)
			{
				$esc_pos = $loc;
			}
			if (!$stage)
			{
				# meta-commands of psql
			}
			next;
		}
		if ($stage eq "'") # for quoted string constants
		{
			if ($tok eq $stage)
			{
				if ( ($esc_mode < 0) || ($esc_pos != ($loc - 1)))
				{
					# end of 'string'
					$stage = '';
					$last_keep = $loc - 1; # keep the quote char
					$esc_mode = -1;
				}
			}
		}
		elsif ($stage eq '"') # for quoted identifier
		{
			if ($tok eq $stage)
			{
				if ( ($esc_mode < 0) || ($esc_pos != ($loc - 1)))
				{
					# end of "string"
					$stage = '';
					$last_keep = $loc - 1; # keep the quote char
					$esc_mode = -1;
				}
			}
		}
		elsif ($stage eq 'c') # for c-style comment
		{
			if ($tok eq '/*')
			{
				$lv++;
			}
			elsif ($tok eq '*/')
			{
				$lv--;
			}
			if ($lv < 0)
			{
				die "non-matching end of c-style comment detected";
			}
			elsif ($lv == 0)
			{
				$stage = '';
				$last_keep = $loc;
			}
		}
		elsif ($stage eq 's') # for dollar-quoted string constants
		{
			if ($tok eq $tag)
			{
				$stage = '';
				$tag = "";
				$last_keep = $loc;
			}
		}
		else
		{
			my $keep_tok = '';
			if ($stage)
			{
				die "unknown stage for slim_raw_text: " . $stage;
			}

			if ($tok eq '/*')
			{
				$stage = 'c';
				$lv = 1;
                                $keep_tok = ' ';
			}
			elsif ($tok eq '*/')
			{
				die "non-matching c-sytle comment end part detected";
			}
			elsif (($tok eq '"') or ($tok eq "'"))
			{
				# TODO: maybe we should consider settings somewhere?
				$esc_mode = $fallback_esc_mode;

				if ($txt =~ m/\b(U&|E|B|X).\G/gi)
				{
					# escape the E'xxx' sequence, the others ignored
					if ('E' eq uc($1))
					{
						$esc_mode = 1;
					}
				}
				else
				{
					pos($txt) = $loc;
				}
				# override the esc mode if forced by caller
				if ($forced_esc_mode)
				{
					$esc_mode = $forced_esc_mode;
					$forced_esc_mode = 0;
				}
				$stage = $tok;
				$keep_tok = $tok; # keep the quote char
			}
			elsif ($cmd eq '$')
			{
				$stage = 's';
				$tag = $tok;
			}
			else
			{
				die "unknown tok detected: " . $tok;
			}

			# append character sequences outside the comment/quoted string
			$tok_len =  $loc - $tok_len - $last_keep;
			if (($last_keep >= 0) && ($tok_len > 0))
			{
				$outstr .= substr($txt, $last_keep, $tok_len);
                                if ($keep_tok)
                                {
                                        $outstr .= $keep_tok;
                                }
				$last_keep = -1;
			}
		}
	}
	$ctx_ref->{stage} = $stage;
	$ctx_ref->{s_tag} = $tag;
	$ctx_ref->{c_level} = $lv;
	$ctx_ref->{esc_mode} = $esc_mode;
	if ($last_keep >= 0)
	{
		$outstr .= substr($txt, $last_keep);
	}
	else
	{
		$outstr .= "\n"; # add newline delimiter
	}
	return $outstr;
}

# The caller should've opened ATMSORT_INFILE and ATMSORT_OUTFILE file handles.
sub atmsort_bigloop
{
    my $infh = shift;
    $atmsort_outfh = shift;

    my $sql_statement = "";
    my @outarr;

    my $lastmsg = -1;
    my $getrows = 0;
    my $getstatement = 0;
    my $has_order = 0;
    my $copy_to_stdout_result = 0;
    my $describe_mode = 0;
    my $directive = {};
    my $big_ignore = 0;
    my %define_match_expression;
    my $concat_on = 0;
    my $concat_statement = "";
    my $reprocess_mode = 0;

    print $atmsort_outfh "GP_IGNORE: formatted by atmsort.pm\n";

    my $line_no = 0;
    my %slim_raw_text_ctx;

  L_bigwhile:
    while (my $ini_raw = <$infh>) # big while
    {
            $line_no++;
            if ($glob_debug)
            {
	        print $atmsort_outfh "GP_IGNORE: #readline: $line_no:", $ini_raw;
            }
            my $ini_s = $ini_raw;
	    if ((%define_match_expression) || ($big_ignore > 0) )
	    {
	    }
	    else
	    {
		    $ini_s = slim_raw_text($ini_raw, \%slim_raw_text_ctx);
	    }
      reprocess_row:
        $reprocess_mode = 0;
        my $ini = $ini_raw;

        # look for match/substitution or match/ignore expressions
        if (%define_match_expression)
        {
            # checkpoint: in match expression, non-comment line can only contains white-space chars.
            if ($ini =~ m/^\s*\-\-\s*(start|end)\_match(subs|ignore)\s*$/)
            {
                my $m_type = $2;
                my $cmd = substr($1, 0, 1);
                
                # checkpoint: nested start_(subs|ignore) is not supported
                if (($cmd ne 'e') || ($define_match_expression{"type"} ne $m_type))
                {
                    die "Non-matching operation " . $1 . "_match" . $m_type . ", " .
                        "expected end_match" . $define_match_expression{"type"};
                }
            }
            else
            {
                    #if ($ini =~ m/^\s*\-\-\s*/)
                    #{
                    #	die "detected non-standard embeded match" . $define_match_expression{"type"} . " line: " . $ini;
                    #}

                $define_match_expression{"expr"} .= $ini;
                goto L_push_outarr;
            }

            my @foo = split(/\n/, $define_match_expression{"expr"}, 2);

            unless (2 == scalar(@foo))
            {
                $ini .= "GP_IGNORE: bad match definition\n";
                undef %define_match_expression;
                goto L_push_outarr;
            }

            my $stat;

            my $doc1 = $foo[1];

            # strip off leading comment characters
            $doc1 =~ s/^\s*\-\-//gm;

            if ($define_match_expression{"type"} eq 'subs')
            {
                $stat = _build_match_subs($doc1, "USER");
            }
            else
            {
                $stat = _build_match_ignores($doc1, "USER");
            }

            if (scalar(@{$stat}) > 1)
            {
                my $outi = $stat->[1];

                # print a message showing the error
                $outi =~ s/^(.*)/GP_IGNORE: ($1)/gm;
                $ini .= $outi;
            }
            else
            {
                $ini .=  "GP_IGNORE: defined new match expression\n";
            }

            undef %define_match_expression;
            goto L_push_outarr;
        } # end defined match expression

        if ($big_ignore > 0)
        {
            if ($ini_s =~ m/^\s*\-\-\s*(start|end)\_ignore\s*$/)
            {
                my $cmd = substr($1, 0, 1);
                # checkpoint: avoid nested start_ignore
                if ($cmd ne 'e')
                {
                    die "nested start_ignore detected, please double check the context";
                }
                else
                {
                    $big_ignore--; # 1->0
                }
            }
            print $atmsort_outfh "GP_IGNORE:", $ini;
            next;
        }

        # if MATCH then SUBSTITUTE
        # see HERE document for definitions
        $ini = match_then_subs($ini);

        # if MATCH then IGNORE
        # see HERE document for definitions
        if ( match_then_ignore($ini))
        {
            next; # ignore matching lines
        }

        if ($getrows) # getting rows from SELECT output
        {
            # The end of "result set" for a COPY TO STDOUT is a bit tricky
            # to find. There is no explicit marker for it. We look for a
            # line that looks like a SQL comment or a new query, or an ERROR.
            # This is not bullet-proof, but works for the current tests.
            # TODO: the match expression is loose now. add start_of_line?
            # and maybe delete/set/reset/show/explain/analyze?
            if ($copy_to_stdout_result &&
                ($ini =~ m/(?:\-\-|ERROR|copy|create|drop|select|insert|update)/i))
            {
                my @ggg = sort @outarr;
                for my $line (@ggg)
                {
                    print $atmsort_outfh $bpref, $line;
                }

                @outarr = ();
                $getrows = 0;
                $has_order = 0;
                $copy_to_stdout_result = 0;

                # Process the row again, in case it begins another
                # COPY TO STDOUT statement, or another query.
                goto reprocess_row;
            }

            my $end_of_table = 0;

            if ($describe_mode)
            {
                # \d tables don't always end with a row count, and there may be
                # more than one of them per command. So we allow any of the
                # following to end the table:
                # - a blank line
                # - a row that doesn't have the same number of column separators
                #   as the header line
                # - a row count (checked below)
                if ($ini =~ m/^$/)
                {
                    $end_of_table = 1;
                }
                elsif (exists($directive->{firstline}))
                {
                    # Count the number of column separators in the table header
                    # and our current line.
                    my $headerSeparators = ($directive->{firstline} =~ tr/\|//);
                    my $lineSeparators = ($ini =~ tr/\|//);

                    if ($headerSeparators != $lineSeparators)
                    {
                        $end_of_table = 1;
                    }
                }

                # Don't reset describe_mode at the end of the table; there may
                # be more tables still to go.
            }

            # regex example: (5 rows)
            if ($ini =~ m/^\s*\(\d+\s+row(?:s)*\)\s*$/)
            {
                # Always ignore the rowcount for explain plan out as the
                # skeleton plans might be the same even if the row counts
                # differ because of session level GUCs.
                if (exists($directive->{explain}))
                {
                    $ini = 'GP_IGNORE:' . $ini;
                }

                $end_of_table = 1;
            }

            if ($end_of_table)
            {
                format_query_output($glob_fqo,
                                    $has_order, \@outarr, $directive);

                $directive = {};
                @outarr = ();
                $getrows = 0;
                $has_order = 0;

                print $atmsort_outfh $cpref, $ini;
                next;
            }
        }
        else # finding SQL statement or start of SELECT output
        {
            # To avoid hunting for gpdiff commands which are contained inside
            # comments first establish if the line contains a comment with any
            # trailing characters at all.
            
            # To simplify the implentation and avoid mistyping, always treat
            # the WHOLE line as one gpdiff command
            
            if ($ini =~ m/^\s*\-\-\s*(start|end)\_match(subs|ignore)\s*$/)
            {
                my $m_type = $2;
                my $cmd = substr($1, 0, 1);
                # checkpoint: end_match before start_match is invalid
                if ($cmd ne 's')
                {
                    die "Non-matching operation end_match" . $m_type . ", " .
                        "lacking of previous start_match" . $m_type;
                }

                $define_match_expression{"type"} = $m_type;
                $define_match_expression{"expr"} = $ini;
                goto L_push_outarr;
            }
            if (($ini =~ m/^\s*\-\-\s*(start|end)\_ignore\s*$/))
            {
                my $cmd = substr($1, 0, 1);
                # checkpoint: end_ignore before start_ignore is invalid
                if ($cmd ne 's')
                {
                    die "non-matching end_ignore detected";
                }
                $big_ignore += 1; # 0->1

                for my $line (@outarr)
                {
                    print $atmsort_outfh $apref, $line;
                }
                @outarr = ();

                print $atmsort_outfh 'GP_IGNORE:', $ini;
                next;
            }

            my $mux_ini = "";

            my $cmd_ref = get_command_type($ini_s);
            # TODO: maybe we should handle the case -- select 1 as "select 2;"
            # Lucky we do not has such case now
            if ($concat_on > 0)
            {
                  
		# TODO: remove comments before appended to concat_statement?
		if ($ini_s =~ m/^\s*\-\-/)
		{
                        #die "wholeline comment in multi-line sql statement is not supported";
		    print $atmsort_outfh 'GP_IGNORE:', "WARN: wholeline comment in multi-line sql statement is expected";
                }
		# abort concat stage if the current line is '\xxx'
		if ($ini_s =~ m/^\s*\\[^g]/)
		{
			print $atmsort_outfh 'GP_IGNORE: concat aborted: ', $ini;
			$concat_on = 0;
                        $mux_ini = $concat_statement;
                        # NOTE: current ini should be reprocessed
                        $reprocess_mode = 1;
			goto L_end_of_concat;
		}
		else
		{
			$concat_on += 1;
		}
                $concat_statement .= $ini_s;
                if ($glob_debug)
                {
                    print $atmsort_outfh 'GP_IGNORE: concat_statement add: ', $ini;
                }

                # stop concat if ';' or '\g..' found
		# TODO: 'explain ... \gdesc' is not explain statement, we should check the order
		# of \gdesc
                if ($ini_s =~ m/;|\\g\S*/g)
                {
                    # check if multiple statements in one line
                    if ($ini_s =~ m/;\G\s*\S/g)
                    {
                        # NOTE: ';' are used to seperate multiple mvd specs
                        if ($ini_s !~ m/\-\-\s*mvd\s.*\G/g)
                        {
				# NOTE: one exception not handled: select 1\; select 2\; select 3;
                            die "invalid to has multiple statements in one line";
                        }
			elsif ($glob_verbose)
			{
				print $atmsort_outfh 'GP_IGNORE: find mvd with multiple specs', $ini_s;
			}
                    }
                    $concat_on = 0;
                    $mux_ini = $concat_statement;

                    if ($glob_debug)
                    {
                        print $atmsort_outfh "GP_IGNORE: concat end\n";
                    }
                    goto L_end_of_concat;
                }
                next;
            }
            else
            {
                # branch: when concat not actived, try to trigger it if the line starts with alphabet char
                #if (($ini_s =~ m/^\s*[a-z]/i))
                if ($cmd_ref->{grp} > 0)
                {
                    # TODO: some complex cases are note supported now:
                    # e.g. if multi statements per line: \copy (select ...) to stdout \; select ...
                    # and \p which can returns part of a sql statement or the whole statement
                    if (has_exec_tok($ini_s))
                    {
                        $mux_ini = $ini_s;
                    }
                    else
                    {
                        $concat_on = 1;
                        $concat_statement = $ini_s;
                        if ($glob_debug)
                        {
                            print $atmsort_outfh 'GP_IGNORE: concat start: cmd:', $cmd_ref->{grp}, ', ', $cmd_ref->{str}, "\n";
                        }
                        next;
                    }
                }
                else
                {
                    $mux_ini = $ini_s;
                }
            }
	  L_end_of_concat:

          #print $atmsort_outfh 'GP_IGNORE: #summary: ', $mux_ini;
            $concat_statement = '';

            # EXPLAIN (COSTS OFF) ...
            if ($mux_ini =~ m/^\s*explain\s*(\(.*costs\s+off.*\))?/i)
            {
                if (defined($1))
                {
                    $directive->{explain} = "costs_off";
                }
                else
                {
                    $directive->{explain} = "normal";
                }
                if ($glob_debug)
                {
                    print $atmsort_outfh 'GP_IGNORE: detected explain mode', $directive->{explain}, "\n";
                }
            }
            # Note: \d is for the psql "describe"
            elsif ($mux_ini =~ m/(?:insert|update|delete|select|^\s*\\d|copy|execute)/i)
            {
                $copy_to_stdout_result = 0;
                $has_order = 0;
                $sql_statement = "";

                # Should we apply more heuristics to try to find the end of \d
                # output?
                $describe_mode = ($mux_ini =~ m/^\s*\\d/);
            }

            # Catching multiple commands and capturing the parens matches
            # makes it possible to check just the first character since
            # each command has a unique first character. This allows us to
            # use fewer regular expression matches in this hot section.
            if ($mux_ini =~ m/\-\-\s*((force_explain)\s*(operator)?\s*$|(ignore)\s*$|(order)\s+(\d+|none).*$|(mvd)\s+\d+.*$|(explain_processing_(on|off))\s+.*$)/)
            {
                my $full_command = $1;
                my $cmd = substr($full_command, 0, 1);
                if ($cmd eq 'i')
                {
                    $directive->{ignore} = 'ignore';
                }
                elsif ($cmd eq 'o')
                {
                    my $olist = $mux_ini;
                    $olist =~ s/^.*\-\-\s*order//;
                    if ($olist =~ /none/)
                    {
                        $directive->{order_none} = 1;
                    }
                    else
                    {
                        $directive->{order} = $olist;
                    }
                }
                elsif ($cmd eq 'f')
                {
                    if (defined($3))
                    {
                        $directive->{explain} = 'operator';
                    }
                    else
                    {
                        $directive->{explain} = 'normal';
                    }
                }
                elsif ($cmd eq 'e')
                {
                    $full_command =~ m/(on|off)$/;
                    $directive->{explain_processing} = $1;
                }
                else
                {
                    my $olist = $mux_ini;
                    $olist =~ s/^.*\-\-\s*mvd//;
                    $directive->{mvd} = $olist;
                }
            }

            # TODO: check the logic here
            if ($mux_ini =~ m/select/i)
            {
                $sql_statement .= $mux_ini;
            }

            # TODO: load previous ini if concat aborted?
            # prune notices with segment info if they are duplicates
            if ($ini_s =~ m/^\s*(?:NOTICE|ERROR|HINT|DETAIL|WARNING)\:.*/)
            {
                $ini_s =~ s/\s+(?:\W)?(?:\W)?\(seg.*pid.*\)//;

                my $outsize = scalar(@outarr);

                $lastmsg = -1;

              L_checkfor:
                for my $jj (1..$outsize)
                {
                    my $checkstr = $outarr[$lastmsg];

                    #remove trailing spaces for comparison
                    $checkstr =~ s/\s+$//;

                    my $skinny = $ini;
                    $skinny =~ s/\s+$//;

                    # stop when no more notices
                    last L_checkfor
                        if ($checkstr !~ m/^\s*(?:NOTICE|ERROR|HINT|DETAIL|WARNING)\:/);

                    # discard this line if matches a previous notice
                    if ($skinny eq $checkstr)
                    {
                        if (0) # debug code
                        {
                            $ini = "DUP: " . $ini;
                            last L_checkfor;
                        }
                        $lastmsg = -1;
                        next L_bigwhile;
                    }
                    $lastmsg--;
                } # end for

            } # end if pruning notices

            # MPP-1492 allow:
            #  copy (select ...) to stdout
            #  \copy (select ...) to stdout
            # and special case these guys:
            #  copy test1 to stdout
            #  \copy test1 to stdout
            my $matches_copy_to_stdout = 0;
            if ($mux_ini =~ m/^(?:\\)?copy\s+(?:(?:\(select.*\))|\S+)\s+to stdout.*$/i)
            {
                $matches_copy_to_stdout = 1;
            }

            # Try to detect the beginning of result set, as printed by psql
            #
            # Examples:
            #
            #    hdr    
            # ----------
            #
            #  a | b 
            # ---+---
            #
            # The previous line should be the header. It should have a space at the
            # beginning and end. This line should consist of dashes and plus signs,
            # with at least three dashes for each column.
            #
            if (($matches_copy_to_stdout && $mux_ini !~ m/order by/i) ||
                (scalar(@outarr) > 1 && $outarr[-1] =~ m/^\s+.*\s$/ &&
                 $ini =~ m/^(?:(?:\-\-)(?:\-)+(?:\+(?:\-)+)*)$/))
                # special case for copy select
            { # sort this region

                $directive->{firstline} = $outarr[-1];

                if (exists($directive->{order}) ||
                    exists($directive->{mvd}))
                {
                    $directive->{sortlines} = $outarr[-1];
                }

                # special case for copy select
                if ($matches_copy_to_stdout)
                {
                    $copy_to_stdout_result = 1;
                    $sql_statement = "";
                }
                # special case for explain
               if (exists($directive->{explain}) &&
                   ($ini =~ m/^\s*(?:(?:\-\-)(?:\-)+(?:\+(?:\-)+)*)+\s*$/) &&
                   (scalar(@outarr) && $outarr[-1] =~ m/QUERY PLAN/))
                {
                    # ENGINF-88: fixup explain headers
                    $outarr[-1] = "QUERY PLAN\n";
                    $ini = ("_" x length($outarr[-1])) . "\n";
                }

                $getstatement = 0;

                for my $line (@outarr)
                {
                    print $atmsort_outfh $apref, $line;
                }
                @outarr = ();

                print $atmsort_outfh $apref, $ini;

                # If there is an ORDER BY in the query, then the results must
                # be in the order that we have memorized in the expected
                # output. Otherwise, the order of the rows is not
                # well-defined, so we sort them before comparing, to mask out
                # any differences in the order.
                #
                # This isn't foolproof, and will get fooled by ORDER BYs in
                # subqueries, for example. But it catches the commmon cases.
                if (defined($directive->{explain}))
                {
                    $has_order = 1; # Do not reorder EXPLAIN output
                }
                elsif (defined($sql_statement)
                    && length($sql_statement)
                    && !defined($directive->{order_none})
                    # multiline match
                    && ($sql_statement =~ m/select.*order.*by/is))
                {
                    # There was an ORDER BY. But if it was part of an
                    # "agg() OVER (ORDER BY ...)" or "WITHIN GROUP (ORDER BY
                    # ...)" construct, ignore it, because those constructs
                    # don't mean that the final result has to be in order.
                    my $t = $sql_statement;
                    $t =~ s/over\s*\(order\s+by/xx/isg;
                    $t =~ s/over\s*\((partition\s+by.*)?order\s+by/xx/isg;
                    $t =~ s/window\s+\w+\s+as\s+\((partition\s+by.*)?order\s+by/xx/isg;
                    $t =~ s/within\s+group\s*\((order\s+by.*)\)/xx/isg;

                    if ($t =~ m/order\s+by/is)
                    {
                        $has_order = 1; # so do *not* sort output
                    }
                    else
                    {
                        $has_order = 0; # need to sort query output
                    }
                }
                else
                {
                    $has_order = 0; # need to sort query output
                }
                $directive->{sql_statement} = $sql_statement;
                $sql_statement = '';

                $getrows = 1;
                if ($reprocess_mode)
                {
                        goto reprocess_row;
                }
                next;
            } # end sort this region
            if ($reprocess_mode)
            {
                    goto reprocess_row;
            }
        } # end finding SQL


L_push_outarr:

        push @outarr, $ini;

        # lastmsg < -1 means we found sequential block of messages like
        # "NOTICE|ERROR|HINT|DETAIL|WARNING", they might come from different
        # QEs, the order of output in QD is uncertain, so let's sort them to
        # aid diff comparison
        if ($lastmsg < -1)
        {
            my @msgblock = @outarr[$lastmsg..-1];
            @msgblock = sort @msgblock;
            splice @outarr, $lastmsg, abs $lastmsg, @msgblock;
            $lastmsg = -1;
        }

    } # end big while

    for my $line (@outarr)
    {
        print $atmsort_outfh $cpref, $line;
    }
} # end bigloop


# The arguments is the input filename. The output filename is returned as it
# is generated in this function to avoid races around the temporary filename
# creation.
sub run
{
    my $infname = shift;

    open my $infh,  '<', $infname  or die "could not open $infname: $!";
    my ($outfh, $outfname) = tempfile();

    if ($glob_debug)
    {

            close $outfh;
            open $outfh, '>', '/dev/stdout';
            $outfname = '';
    }

    run_fhs($infh, $outfh);

    close $infh;
    close $outfh;
    return $outfname;
}

# The arguments are input and output file handles
sub run_fhs
{
    my $infh = shift;
    my $outfh = shift;


    # loop over input file.
    atmsort_bigloop($infh, $outfh);
}

1;
