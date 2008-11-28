# Simple makefile.

all: .setup-config .force
	runhaskell Setup.hs build

dist: .setup-config .force
	runhaskell Setup.hs sdist

clean:
	runhaskell Setup.hs clean

.setup-config: Setup.hs harchive.cabal
	runhaskell Setup.hs configure --ghc

#hash: Hash.hs
#	ghc --make -o hash Hash.hs
#
#scan: .force
#	ghc --make -o scan Scan.hs
#
#%.hs: %.hsc
#	hsc2hs $*.hsc

.force:
