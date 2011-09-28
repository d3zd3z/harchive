----------------------------------------------------------------------
-- Verify hashes.

module HashCheck (hashCheck) where

import Hash
import Test.HUnit
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L

hashCheck :: Test
hashCheck = test [
   -- These we just know.
   hashBlockLength ~?= 64,
   hashLength ~?= 20,

   "Hex conversions" ~: test [
      fromHex "c95ad0ce54f903e1568facb2b120ca9210f6778f" ~?=
         (RawHash $ B.pack [ 0xc9, 0x5a, 0xd0, 0xce, 0x54, 0xf9, 0x03, 0xe1, 0x56, 0x8f, 0xac,
           0xb2, 0xb1, 0x20, 0xca, 0x92, 0x10, 0xf6, 0x77, 0x8f ]),
      (toHex $ hashOf $ L.pack [ 65, 66, 67, 10 ]) ~?=
         ("c95ad0ce54f903e1568facb2b120ca9210f6778f") ],

   (hashOf $ L.pack [ 65, 66, 67, 10 ]) ~?= fromHex "c95ad0ce54f903e1568facb2b120ca9210f6778f",
   (hashOf $ B.pack [ 65, 66, 67, 10 ]) ~?= fromHex "c95ad0ce54f903e1568facb2b120ca9210f6778f",

   hashOf "abc" ~?= fromHex "a9993e364706816aba3e25717850c26c9cd0d89d"
   ]
