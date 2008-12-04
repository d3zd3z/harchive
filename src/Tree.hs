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

walk :: Pool a => a -> Hash -> IO ()
walk pool hash = runReaderT (doWalk "" hash) pool

doWalk :: Pool a => FilePath -> Hash -> ReaderT a IO ()
doWalk base hash = do
   pool <- ask
   chunk <- liftM fromJust $ liftIO $ poolReadChunk pool hash
   case chunkKind chunk of
      "dir " -> walkDir base chunk
      k -> error $ "Implement walking for: " ++ k

walkDir :: Pool a => FilePath -> Chunk -> ReaderT a IO ()
walkDir base chunk = do
   forM_ (decodeMultiChunk chunk) $ \info -> do
      let fullName = base </> attrName info
      case attrKind info of
	 "DIR" -> do
	    liftIO $ printf "d %s\n" fullName
	    doWalk fullName (justField info "HASH")
	    liftIO $ printf "u %s\n" fullName
	 "REG" -> do
	    liftIO $ printf "- %s\n" fullName
	    walkReg fullName info
	 "LNK" -> liftIO $
	    printf "l %s -> %s\n" fullName (justField info "LINK" :: String)
	 x -> liftIO $ printf "? %s (%s)\n" fullName x

walkReg :: Pool a => FilePath -> Attr -> ReaderT a IO ()
walkReg path info = do
   pool <- ask
   chunk <- liftM fromJust $ liftIO $ poolReadChunk pool (justField info "HASH")
   liftIO $ printf "  (kind = \"%s\")\n" (chunkKind chunk)
