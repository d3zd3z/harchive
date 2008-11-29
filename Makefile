# Simple makefile.

all: .setup-config .force
	runhaskell Setup.hs build

dist: .setup-config .force
	runhaskell Setup.hs sdist

install: .setup-config .force
	runhaskell Setup.hs install

register: .setup-config .force
	runhaskell Setup.hs register

unregister: .setup-config .force
	runhaskell Setup.hs unregister

clean:
	runhaskell Setup.hs clean

.setup-config: Setup.hs harchive.cabal
	runhaskell Setup.hs configure --ghc --user --prefix ~/cabal-code

#hash: Hash.hs
#	ghc --make -o hash Hash.hs
#
#scan: .force
#	ghc --make -o scan Scan.hs
#
#%.hs: %.hsc
#	hsc2hs $*.hsc

.force:
