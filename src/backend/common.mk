#
# Common make rules for backend
#
# src/backend/common.mk
#

# When including this file, set OBJS to the object files created in
# this directory and SUBDIRS to subdirectories containing more things
# to build.

subsysfilename = objfiles.txt
subsysbitcodefilename = bitcodefiles.txt

SUBDIROBJS = $(SUBDIRS:%=%/$(subsysfilename))

SUBDIRBITCODES = $(SUBDIRS:%=%/$(subsysbitcodefilename))

# top-level backend directory obviously has its own "all" target
ifneq ($(subdir), src/backend)
all: $(subsysfilename)
# add bitcodefile to target all if llvm enabled
ifeq ($(with_llvm), yes)
all: $(subsysbitcodefilename)
endif
endif

SUBSYS.o: $(SUBDIROBJS) $(OBJS)
	$(LD) $(LDREL) $(LDOUT) $@ $^ $(LDOPTS)

objfiles.txt: Makefile $(SUBDIROBJS) $(OBJS)
# Don't rebuild the list if only the OBJS have changed.
	$(if $(filter-out $(OBJS),$?),( $(if $(SUBDIROBJS),cat $(SUBDIROBJS); )echo $(addprefix $(subdir)/,$(OBJS)) ) >$@,touch $@)

# Convert OBJS to BITCODES if BITCODES not defined (we have one-one mapping of bitcode/objs by default)
BITCODES ?= $(patsubst %.o,%.bc,$(OBJS))

bitcodefiles.txt: Makefile $(SUBDIRBITCODES) $(BITCODES)
# Don't rebuild the list if only the BITCODES have changed.
	$(if $(filter-out $(BITCODES),$?),( $(if $(SUBDIRBITCODES),cat $(SUBDIRBITCODES); )echo $(addprefix $(subdir)/,$(BITCODES)) ) >$@,touch $@)

# make function to expand objfiles.txt contents
expand_subsys = $(foreach file,$(1),$(if $(filter %/objfiles.txt,$(file)),$(patsubst ../../src/backend/%,%,$(addprefix $(top_builddir)/,$(shell cat $(file)))),$(file)))
# make function to expand bitcodefiles.txt contents
expand_subsys_bitcode = $(foreach file,$(1),$(if $(filter %/bitcodefiles.txt,$(file)),$(patsubst ../../src/backend/%,%,$(addprefix $(top_builddir)/,$(shell cat $(file)))),$(file)))

# Parallel make trickery
$(SUBDIROBJS): $(SUBDIRS:%=%-recursive) ;

$(SUBDIRBITCODES): $(SUBDIRS:%=%-recursive) ;

.PHONY: $(SUBDIRS:%=%-recursive)
$(SUBDIRS:%=%-recursive):
	$(MAKE) -C $(subst -recursive,,$@) all

$(call recurse,clean)
clean: clean-local
clean-local:
	rm -f $(subsysfilename) $(subsysbitcodefilename) $(OBJS) $(patsubst %.o,%.bc, $(OBJS))
	@if [ -d $(CURDIR)/test ]; then $(MAKE) -C $(CURDIR)/test clean; fi

$(call recurse,unittest-check)
unittest-check: unittest-check-local
unittest-check-local:
	@if [ -d $(CURDIR)/test ]; then $(MAKE) CFLAGES=-DUNITTEST -C $(CURDIR)/test check; fi

$(call recurse,coverage)
$(call recurse,install)
