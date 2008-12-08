----------------------------------------------------------------------
-- Testing of storage pools.
----------------------------------------------------------------------

module PoolTest (
   poolTests
) where

import Chunk
import Hash
import TmpDir
import Util
import Pool.Memory
import Pool.Local

-- import Text.Printf

import System.Posix (getFileStatus, fileSize)
import System.FilePath ((</>))

import qualified Data.ByteString as B

import Data.Bits
import Data.List
import Test.HUnit
import Control.Monad
import Data.Maybe

import System.Cmd (system)
import System.Directory (getDirectoryContents)
import System.Exit

poolTests = test [
   "Memory Pool" ~: poolTestMemory,
   "Local Pool" ~: poolTestLocal,
   "Multi Pool Files" ~: multiFileTest,
   "Recover Single Pool File" ~: recoverSingleFile,
   "Recover multiple pool files" ~: recoverMultiple ]

poolTestMemory :: IO ()
poolTestMemory = withMemoryPool exercisePool

poolTestLocal :: IO ()
poolTestLocal = do
   withTmpDir $ \tmpDir -> do
      withLocalPool tmpDir exercisePool

multiFileTest :: IO ()
multiFileTest = do
   withTmpDir $ \tmpDir -> do
      let chunks = take 50 $ randomChunks (1024, 32768) 100

      withLocalPool tmpDir $ \pool -> do
	 setPoolLimit pool testLimit
	 limit <- getPoolLimit pool
	 limit @=? testLimit
	 poolFlush pool
      withLocalPool tmpDir $ \pool -> do
	 limit <- getPoolLimit pool
	 limit @=? testLimit

	 -- Make sure we can set it again.
	 setPoolLimit pool testLimit


	 forM_ chunks $ poolWriteChunk pool
	 poolFlush pool

      -- Verify that we have more than one data file.
      names_ <- getDirectoryContents tmpDir
      let names = filter (isPrefixOf "pool-data-") names_
      (length names > 1) @? "Must be more than one pool file"
      forM_ names $ \name -> do
	 let fullName = tmpDir </> name
	 stat <- getFileStatus $ fullName
	 let size = fromIntegral (fileSize stat) :: Int
	 (size <= testLimit) @? "File size exceeds test limit: " ++ show size
      -- printf "\nListing (%s):\n" (show names)
      -- system $ "ls -l " ++ tmpDir
      -- return ()

      -- Make sure we can read the chunks back.
      withLocalPool tmpDir $ \pool -> do
	 forM_ chunks $ \chunk -> do
	    let hash = chunkHash chunk
	    c2 <- poolReadChunk pool hash
	    maybe (assert "No chunk") (const $ return ()) c2
	    let c2' = fromJust c2
	    hash @?= chunkHash c2'

   where testLimit = 100 * 1024

recoverSingleFile :: IO ()
-- Test simple recovery of an existing database file.
recoverSingleFile = do
   withTmpDir $ \tmpDir -> do
      let chunks = randomChunks (1024, 32768) 200
      let write index =
	    withLocalPool tmpDir $ \pool -> do
	       poolWriteChunk pool $ chunks !! index
	       poolFlush pool

      write 0
      stashDatabase tmpDir
      write 1
      replaceDatabase tmpDir
      write 2

      withLocalPool tmpDir $ \pool -> do
	 forM_ (take 3 chunks) $ \chunk -> do
	    let hash = chunkHash chunk
	    c2 <- poolReadChunk pool hash
	    maybe (assert "No chunk") (const $ return ()) c2
	    let c2' = fromJust c2
	    hash @?= chunkHash c2'

recoverMultiple :: IO ()
-- Test recovery of multiple files.
recoverMultiple = do
   withTmpDir $ \tmpDir -> do
      let chunks = take 50 $ randomChunks (1024, 32768) 600

      let firstChunk = head chunks
      let (aChunks, bChunks) = splitAt 50 (tail $ take 101 chunks)

      withLocalPool tmpDir $ \pool -> do
	 setPoolLimit pool testLimit
	 poolWriteChunk pool firstChunk
	 poolFlush pool
      stashDatabase tmpDir
      withLocalPool tmpDir $ \pool -> do
	 forM_ aChunks $ poolWriteChunk pool
	 poolFlush pool
      replaceDatabase tmpDir
      withLocalPool tmpDir $ \pool -> do
	 forM_ (firstChunk : aChunks) $ \chunk -> do
	    let hash = chunkHash chunk
	    c2 <- poolReadChunk pool hash
	    maybe (assert "No chunk") (const $ return ()) c2
	    let c2' = fromJust c2
	    hash @?= chunkHash c2'

      -- Make sure we can write more.
      withLocalPool tmpDir $ \pool -> do
	 forM_ bChunks $ poolWriteChunk pool
	 poolFlush pool

      withLocalPool tmpDir $ \pool -> do
	 forM_ (firstChunk : aChunks ++ bChunks) $ \chunk -> do
	    let hash = chunkHash chunk
	    c2 <- poolReadChunk pool hash
	    maybe (assert "No chunk") (const $ return ()) c2
	    let c2' = fromJust c2
	    hash @?= chunkHash c2'

   where testLimit = 100 * 1024

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

-- To simulate partial writes, we make a copy of the database file,
-- and put it back after performing some more writing.
stashDatabase :: FilePath -> IO ()
-- Make a copy of the database file in the given directory.
stashDatabase dirName = do
   status <- system $ "cp " ++ (dirName </> "pool-info.sqlite3") ++ " " ++
      (dirName </> "pool-info.backup")
   status @=? ExitSuccess

replaceDatabase dirName = do
   status <- system $ "cp " ++ (dirName </> "pool-info.backup") ++ " " ++
      (dirName </> "pool-info.sqlite3")
   status @=? ExitSuccess

tweakHash :: Hash -> Hash
-- Change a single bit in the hash to make a hash that is likely to to
-- match anything.
tweakHash = byteStringToHash . B.pack . tweak . B.unpack . toByteString
   where
      tweak (x:xs) = (x `xor` 1) : xs
      tweak [] = []

