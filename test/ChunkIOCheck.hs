----------------------------------------------------------------------

module ChunkIOCheck (testChunkIO) where

import System.Backup.Chunk
import System.Backup.Chunk.IO

import qualified Control.Exception as E
import Control.Monad (forM_)
import qualified Data.ByteString.Lazy.Char8 as L8
import System.FilePath ((</>))
-- import System.Process (rawSystem)
import Text.Printf

import Test.HUnit hiding (path)
import TmpDir
import GenWords

testChunkIO :: Test
testChunkIO = test [
   "Read/Write" ~: basic,
   "Empty size" ~: testEmpty ]

basic :: IO ()
basic = withTmpDir $ \tmp -> do
   let sizes = [100,200..5000] ++ [32*1024, 64*1024 .. 256*1024]
   withChunkFile (tmp </> "data.dat") AppendMode $ \cf -> do
      forM_ sizes $ \i ->
         chunkWrite cf $ makeChunk i i
   -- rawSystem "/bin/ls" ["-l", tmp]
   withChunkFile (tmp </> "data.dat") ReadMode $ \cf -> do
      size <- chunkFileSize cf
      let iter pos (idx:idxs)
            | pos == size = error "File ended too early"
            | otherwise = do
               (chunk, nextPos) <- chunkRead cf pos
               chunkHash chunk @?= chunkHash (makeChunk idx idx)
               iter nextPos idxs
          iter pos [] | pos == size = return ()
          iter pos _ = error $ printf "Iteration error: pos=%d, size=%d" pos size
      iter 0 sizes
   return ()

-- Validate that we get a size of zero back from an non-existant chunk
-- file.
testEmpty :: IO ()
testEmpty = do
   withTmpDir $ \tmp -> do
   let name = tmp </> "empty.data"
   withChunkFile name AppendMode $ \cf -> do
      size <- chunkFileSize cf
      size @?= 0

withChunkFile :: FilePath -> IOMode -> (ChunkFile -> IO a) -> IO a
withChunkFile path mode =
   E.bracket (openChunkFile path mode) chunkClose

makeChunk :: Int -> Int -> Chunk
makeChunk seed = byteStringToChunk "blob" . L8.pack . makeString seed
