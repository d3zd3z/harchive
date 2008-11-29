----------------------------------------------------------------------
-- Quick check tests.

module Main where

import Test.HUnit
import Chunk
import qualified Data.ByteString.Lazy as L

import Control.Monad (when)
import System.Exit

main = do
   counts <- runTestTT tests
   when ((errors counts, failures counts) /= (0, 0)) $
      exitWith (ExitFailure 1)

tests = test [
   "lengthTests" ~: lengthTests,
   "compressionTests" ~: compressionTests ]

lengthTests = test [
   "Empty" ~: 0 ~?= chunkLength c1,
   "c2" ~: 5 ~?= chunkLength c2
   ]

compressionTests = test [
   "None 1" ~: Nothing ~?= chunkZData c1,
   "None 2" ~: Nothing ~?= chunkZData c2
   ]

c1 = stringToChunk "blob" ""
c2 = stringToChunk "blob" "Hello"
