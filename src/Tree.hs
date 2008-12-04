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
import Control.Monad (liftM, forM_)
import Text.Printf

-- Let's see what we can do.  Starting with the hash of a directory,
-- let's recursively walk through it, printing the tree.  It's a
-- start.

walk :: FilePath -> Hash -> PoolOp ()
walk base hash = do
   chunk <- liftM fromJust $ poolReadChunk hash
   case chunkKind chunk of
      "dir " -> walkDir base chunk
      k -> error $ "Implement walking for: " ++ k

walkDir :: FilePath -> Chunk -> PoolOp ()
walkDir base chunk = do
   forM_ (decodeMultiChunk chunk) $ \info -> do
      let fullName = base </> attrName info
      case attrKind info of
	 "DIR" -> do
	    liftIO $ printf "d %s\n" fullName
	    walk fullName (justField info "HASH")
	    liftIO $ printf "u %s\n" fullName
	 "REG" -> do
	    liftIO $ printf "- %s\n" fullName
	    walkReg fullName info
	 "LNK" -> liftIO $
	    printf "l %s -> %s\n" fullName (justField info "LINK" :: String)
	 x -> liftIO $ printf "? %s (%s)\n" fullName x

walkReg :: FilePath -> Attr -> PoolOp ()
walkReg path info = do
   chunk <- liftM fromJust $ poolReadChunk (justField info "HASH")
   liftIO $ printf "  (kind = \"%s\")\n" (chunkKind chunk)
