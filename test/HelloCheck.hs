----------------------------------------------------------------------
-- Verify linkage against the Hello module.

module HelloCheck (testHello) where

import Hello
import Test.HUnit

testHello :: Test
testHello = test [ "Hello" ~: message ~=? "Hello, world" ]
