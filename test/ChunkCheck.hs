----------------------------------------------------------------------
-- Test chunk support.

module Main where

import qualified Data.ByteString.Lazy.Char8 as LChar (pack)
import qualified Data.ByteString as B

import System.Backup.Chunk
import Text.HexDump
import Hash

import GenWords
import Harness

main :: IO ()
main = runTests tests

tests :: Test
tests = test $ do
   let c1 = byteStringToChunk "blob" (LChar.pack "Sample chunk one")
   let h1 = B.pack [
         0x67, 0x40, 0x35, 0x7c, 0x3c, 0x44, 0x4c, 0xd5, 0x2f, 0x9c,
         0x9c, 0xf2, 0x51, 0x36, 0xa3, 0x12, 0x23, 0x76, 0xf7, 0xa2 ]
   h1 @=? toByteString (chunkHash c1)
   -- putStrLn $ show c1
   -- putStr $ hexDump $ lazify $ toByteString $ chunkHash c1
