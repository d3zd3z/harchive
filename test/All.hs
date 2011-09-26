--  Framework for running all tests.

import Test.Framework (defaultMain, testGroup)
import Test.Framework.Providers.HUnit
import Test.HUnit

import HelloCheck
import LinuxDirCheck
import ChunkCheck
import ChunkIOCheck
import HashCheck
import BinPropCheck
import HashMapCheck
import HashMapFileCheck

main = defaultMain tests

tests = (:[]) $ testGroup "HUnit" $ hUnitTestToTests $ test [
   "Simple" ~: testHello,
   "Hashing" ~: test [
      "Basic" ~: hashCheck,
      "HashMap" ~: hashMapCheck,
      "HashMapFile" ~: hashMapFileCheck ],
   "Binprop" ~: binPropCheck,
   "Linux low level" ~: test [
      "Dirs" ~: testLinux ],
   "Chunks" ~: test [
      "Basic" ~: testChunk,
      "IO" ~: testChunkIO ]
   ]
{-
   testGroup "Simple" $ hUnitTestToTests testHello,
   testGroup "Linux Low level" $ hUnitTestToTests testLinux,
   testGroup "Chunks" $ hUnitTestToTests testChunk,
   testGroup "Chunk IO" $ hUnitTestToTests testChunkIO
   ]
-}
