######################################################################
#
# Build and run the test framework.
# 
# For now, assume that the library has been built, and is still
# installed locally.

compiled: .force
	ghc --make Main.hs -o tests -Wall -fno-warn-missing-signatures

genwords: .force
	ghc --make GenWords.hs -o genwords

.PHONY: .force
