----------------------------------------------------------------------

module ChunkIOCheck (testChunkIO) where

import qualified Hash
import System.Backup.Chunk
import System.Backup.Chunk.IO

import qualified Control.Exception as E
import Control.Monad (forM_)
import qualified Data.ByteString.Lazy.Char8 as L8
import System.FilePath ((</>))
import System.Process (rawSystem)
import Text.Printf

import Test.HUnit
import TmpDir
import GenWords

testChunkIO :: Test
testChunkIO = test $ withTmpDir $ \tmp -> do
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

withChunkFile :: FilePath -> IOMode -> (ChunkFile -> IO a) -> IO a
withChunkFile path mode =
   E.bracket (openChunkFile path mode) chunkClose

makeChunk :: Int -> Int -> Chunk
makeChunk seed = byteStringToChunk "blob" . L8.pack . makeString seed
