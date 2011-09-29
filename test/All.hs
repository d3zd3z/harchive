--  Framework for running all tests.

import Test.Framework (defaultMain, testGroup)
import qualified Test.Framework as TF
import Test.Framework.Providers.HUnit
import Test.HUnit

import HelloCheck
import LinuxDirCheck
import ChunkCheck
import ChunkIOCheck
import FileIndexCheck
import HashCheck
import BinPropCheck
import HashMapCheck
import HashMapFileCheck

main :: IO ()
main = defaultMain tests

tests :: [TF.Test]
tests = (:[]) $ testGroup "HUnit" $ hUnitTestToTests $ test [
   "Simple" ~: testHello,
   "Hashing" ~: test [
      "Basic" ~: hashCheck,
      "HashMap" ~: hashMapCheck,
      "HashMapFile" ~: hashMapFileCheck ],
   "Binprop" ~: binPropCheck,
   "FileIndex" ~: fileIndexCheck,
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
