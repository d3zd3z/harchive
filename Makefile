# Simple makefile.

all: .setup-config .force
	runhaskell Setup.hs build

dist: .setup-config .force
	runhaskell Setup.hs sdist

install: .setup-config all
	runhaskell Setup.hs install --inplace

register: .setup-config .force
	runhaskell Setup.hs register

unregister: .setup-config .force
	runhaskell Setup.hs unregister

sure: install
	${MAKE} -C tests
	./tests/qc

clean:
	runhaskell Setup.hs clean

.setup-config: Setup.hs harchive.cabal
	runhaskell Setup.hs configure --ghc --user

tags TAGS: .force
	rm -f tags TAGS
	find . -path './_darcs' -prune -o \
		'(' '(' -name '*.hs' -o -name '*.lhs' ')' -print ')' | \
		xargs hasktags
	env LC_ALL=C sort tags > tags_
	mv tags_ tags

#hash: Hash.hs
#	ghc --make -o hash Hash.hs
#
#scan: .force
#	ghc --make -o scan Scan.hs
#
#%.hs: %.hsc
#	hsc2hs $*.hsc

.force:
