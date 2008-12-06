----------------------------------------------------------------------
-- Directory Trees.
----------------------------------------------------------------------

module Tree (
   walk
) where

import Chunk
import Hash
import DecodeSexp
import Pool

import Data.Maybe (fromJust)
import System.FilePath
import Text.Printf
import Control.Monad.Reader

-- Let's see what we can do.  Starting with the hash of a directory,
-- let's recursively walk through it, printing the tree.  It's a
-- start.

walk :: ChunkReader p => p -> Hash -> IO ()
walk pool hash = doWalk pool "" hash

doWalk :: ChunkReader p => p -> FilePath -> Hash -> IO ()
doWalk pool base hash = do
   chunk <- liftM fromJust $ poolReadChunk pool hash
   case chunkKind chunk of
      "dir " -> walkDir pool base chunk
      k -> error $ "Implement walking for: " ++ k

walkDir :: ChunkReader p => p -> FilePath -> Chunk -> IO ()
walkDir pool base chunk = do
   forM_ (decodeMultiChunk chunk) $ \info -> do
      let fullName = base </> attrName info
      case attrKind info of
	 "DIR" -> do
	    printf "d %s\n" fullName
	    doWalk pool fullName (justField info "HASH")
	    printf "u %s\n" fullName
	 "REG" -> do
	    printf "- %s\n" fullName
	    walkReg pool fullName info
	 "LNK" ->
	    printf "l %s -> %s\n" fullName (justField info "LINK" :: String)
	 x -> printf "? %s (%s)\n" fullName x

walkReg :: ChunkReader p => p -> FilePath -> Attr -> IO ()
walkReg pool _path info = do
   kind <- liftM fromJust $ poolChunkKind pool (justField info "HASH")
   printf "  (kind = \"%s\")\n" kind
