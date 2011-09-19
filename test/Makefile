# Attempt to build the local version, and build the baseline if we can
# find it.

activebin = ../dist/test/bin/active
baselinebin = ../dist/test/bin/baseline
activedir = ../dist/test/active
baselinedir = ../dist/test/baseline
conf = ../dist/package.conf

all: .force
	echo 'TODO: Generic build target'
	false

#all: $(bindir)/active
#ifdef BASELINE
#baseline: $(bindir)/baseline
#endif

$(activebin)/%: dirs .force
	@echo -e "\tGHC\t$*"
	@ghc -package-conf $(conf) \
		-odir $(activedir) \
		-hidir $(activedir) \
		-stubdir $(activedir) \
		-o $(activebin)/$* \
		--make $*.hs

$(baselinebin)/%: dirs .force
	ghc -package-conf $(BASELINE)/$(conf) \
		-odir $(baselinedir) \
		-hidir $(baselinedir) \
		-stubdir $(baselinedir) \
		-o $(baselinebin)/$* \
		--make $*.hs

dirs:
	@echo -e '\tMKDIRS'
	@mkdir -p $(activebin) $(baselinebin) $(activedir) $(baselinedir)

.PHONY: .force all dirs
