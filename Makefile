all: dist/setup-config
	cabal build

# Cabal actually tries to detect changes.
dist/setup-config: harchive.cabal
	cabal configure

clean:
	cabal clean

.PHONY: all clean
