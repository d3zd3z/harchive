----------------------------------------------------------------------
-- Verify linkage against the Hello module.

module Main where

import Hello
import Test.HUnit
import Harness

testHello :: Test
testHello = test [ "Hello" ~: message ~=? "Hello, world" ]

main :: IO ()
main = runTests testHello
