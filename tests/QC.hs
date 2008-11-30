----------------------------------------------------------------------
-- Quick check tests.

module Main where

import TmpDir

import Chunk
import Chunk.IO
import HexDump
import Hash
import qualified Hash

import Test.HUnit
import qualified Data.ByteString.Lazy as L

import Control.Monad (when)
import System.Cmd
import System.Exit
import GenWords

main = do
   -- putStrLn $ show c3
   -- putStrLn . Hash.toHex . chunkHash $ c3
   -- putStrLn . showZData $ c3
   counts <- runTestTT tests
   when ((errors counts, failures counts) /= (0, 0)) $
      exitWith (ExitFailure 1)

tests = test [
   "lengthTests" ~: lengthTests,
   "compressionTests" ~: compressionTests,
   "hashTests" ~: hashTests,
   "simpleChunkIO" ~: simpleChunkIO ]

lengthTests = test [
   "Empty" ~: 0 ~?= chunkLength c1,
   "c2" ~: 5 ~?= chunkLength c2
   ]

compressionTests = test [
   "None 1" ~: Nothing ~?= chunkZData c1,
   "None 2" ~: Nothing ~?= chunkZData c2
   ]

hashTests = test [
   "Hash 1" ~: hexHash c1 ~=? "0fd0bcfb44f83e7d5ac7a8922578276b9af48746",
   "Hash 2" ~: hexHash c2 ~=? "51712bd6234e069e7e0b012a7c19e6e12a25d327" ]

-- IO test1.
simpleChunkIO = do
   withTmpDir $ \tmpDir -> do
      let name = tmpDir ++ "/cfile1.data"
      cfile <- openChunkFile name
      len <- chunkFileSize cfile
      len @?= 0
      let srcChunks = [ c1, c2, c3 ]
      offsets <- mapM (chunkWrite cfile) srcChunks
      newChunks <- mapM (chunkRead_ cfile) offsets
      (map chunkInfo srcChunks) @?= (map chunkInfo newChunks)

      o4 <- chunkWrite cfile c4
      n4 <- chunkRead_ cfile o4
      chunkInfo c4 @?= chunkInfo n4
      -- system $ "ls -l " ++ tmpDir
      -- system $ "hexdump -C " ++ name
      return ()

-- Information about a chunk that can be readily compared.
chunkInfo :: Chunk -> (String, L.ByteString, Hash, Int)
chunkInfo chunk =
   (chunkKind chunk,
      chunkData chunk,
      chunkHash chunk,
      chunkLength chunk)

c1 = stringToChunk "blob" ""
c2 = stringToChunk "blob" "Hello"

c3 = stringToChunk "blob" $ makePayload 1 256

c4 = stringToChunk "blob" $ makePayload 2 (256*1024)

hexHash :: Chunk -> String
hexHash = Hash.toHex . chunkHash

showZData :: Chunk -> String
showZData chunk =
   case chunkZData chunk of
      Nothing -> "Not compressed\n"
      Just z -> "Compressed:\n" ++ hexDump z

makeLBSPayload :: Int -> Int -> L.ByteString
makeLBSPayload num = L.pack . (map $ fromIntegral . fromEnum) . makePayload num
makePayload :: Int -> Int -> String
makePayload num length =
   take length (prefix ++ makeWords num)
   where
      prefix = show num ++ " "
