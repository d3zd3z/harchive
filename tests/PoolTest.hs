----------------------------------------------------------------------
-- Testing of storage pools.
----------------------------------------------------------------------

module PoolTest (
   poolTests
) where

import Chunk
import Hash
import Util
import Pool.Memory

import qualified Data.ByteString as B

import Data.Bits
import Test.HUnit
import Control.Monad
import Data.Maybe

poolTests = test [
   "Pool 1" ~: poolTest1 ]

poolTest1 :: IO ()
poolTest1 = withMemoryPool exercisePool

exercisePool :: (ChunkReaderWriter p) => p -> IO ()
exercisePool pool = do
   let chunks = take 50 $ randomChunks (1024, 32768) 1

   forM_ chunks $ poolWriteChunk pool

   -- Verify that we can write the same chunks again.
   forM_ chunks $ poolWriteChunk pool

   forM_ chunks $ \chunk -> do
      let hash = chunkHash chunk

      present <- poolHashPresent pool hash
      present @? "Chunk present"

      kind <- poolChunkKind pool hash
      kind @=? Just "blob"

      -- Make sure the chunk is present.
      c2 <- poolReadChunk pool hash
      maybe (assert "No chunk") (const $ return ()) c2
      let c2' = fromJust c2
      hash @?= chunkHash c2'

      -- Also verify that it is not present with a different hash.
      let hash2 = tweakHash hash

      present2 <- poolHashPresent pool hash2
      (not present2) @? "Chunk not present"

      kind2 <- poolChunkKind pool hash2
      kind2 @=? Nothing

      c22 <- poolReadChunk pool hash2
      maybe (return ()) (const $ assert "Bad chunk found") c22

tweakHash :: Hash -> Hash
-- Change a single bit in the hash to make a hash that is likely to to
-- match anything.
tweakHash = byteStringToHash . B.pack . tweak . B.unpack . toByteString
   where
      tweak (x:xs) = (x `xor` 1) : xs
      tweak [] = []

