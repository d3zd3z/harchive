----------------------------------------------------------------------
-- Test harness support

module Harness (
   runTests,
   module Test.HUnit
) where

import Test.HUnit
import System.Exit

runTests :: Test -> IO ()
runTests tests = do
   counts <- runTestTT tests
   if errors counts + failures counts == 0
      then exitSuccess
      else exitFailure
