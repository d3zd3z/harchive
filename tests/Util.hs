----------------------------------------------------------------------
-- Test utilities.
----------------------------------------------------------------------

module Util (
   randomChunks,
   hexHash,
   makePayload,
   mustError,

   -- Unknown utility.
   showZData,
   makeLBSPayload
) where

import Hash
import Chunk
import HexDump
import GenWords

import qualified Data.ByteString.Lazy as L
import System.Random

import qualified System.IO.Error as E

randomChunks :: (Int, Int) -> Int -> [Chunk]
-- Generate a sequence of random chunks, seeding the random sequence
-- with 'seed', and using 'range' as the lo,hi size bounds on the
-- random sequence.
randomChunks range seed =
   map (stringToChunk "blob") payloads
   where
      g0 = mkStdGen seed
      r = randomRs range g0
      payloads = map (uncurry makePayload) $ zip [1..] r

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
makePayload num len =
   take len (prefix ++ makeWords num)
   where prefix = show num ++ " "

mustError :: IO a -> IO ()
-- Perform the action given, and assure that it fails.
mustError action = do
   result <- E.try action
   either (const $ return ())
      (const $ fail "mustError!")
      result
