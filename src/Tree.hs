----------------------------------------------------------------------
-- Directory Trees.
----------------------------------------------------------------------

module Tree (
   walk,
   TreeOp(..),
   module DecodeSexp
) where

import Chunk
import Hash
import DecodeSexp
import Pool

import Data.Maybe (fromJust)
import System.FilePath
import Control.Monad.Reader
import Control.Concurrent

-- Let's see what we can do.  Starting with the hash of a directory,
-- let's recursively walk through it, printing the tree.  It's a
-- start.

data TreeOp
   = TreeEOF
   | TreeEnter { treeOpPath :: String, treeOpAttr :: Attr }
   | TreeLeave { treeOpPath :: String, treeOpAttr :: Attr }
   | TreeLink { treeOpPath :: String, treeOpAttr :: Attr }
   | TreeOther { treeOpPath :: String, treeOpAttr :: Attr }
   | TreeReg {
      treeOpPath :: String, treeOpAttr :: Attr,
      treeOpKind :: String }

-- Note the assymetry of the root directory here.  The attributes of a
-- directory are stored outside of that directory in the parent, so
-- the root's attributes are stored in the backup record.

walk :: ChunkReader p => p -> Hash -> IO (IO TreeOp)
walk pool hash = do
   opBox <- newEmptyMVar
   _ <- forkIO $ do
      doWalk pool opBox "" hash
      putMVar opBox TreeEOF
   return $ takeMVar opBox

doWalk :: ChunkReader p => p -> MVar TreeOp -> FilePath -> Hash -> IO ()
doWalk pool opBox base hash = do
   chunk <- liftM fromJust $ poolReadChunk pool hash
   case chunkKind chunk of
      "dir " -> walkDir pool opBox base chunk
      k -> error $ "Implement walking for: " ++ k

walkDir :: ChunkReader p => p -> MVar TreeOp -> FilePath -> Chunk -> IO ()
walkDir pool opBox base chunk = do
   forM_ (decodeMultiChunk chunk) $ \info -> do
      let fullName = base </> attrName info
      case attrKind info of
	 "DIR" -> do
	    putMVar opBox $ TreeEnter fullName info
	    doWalk pool opBox fullName (justField info "HASH")
	    putMVar opBox $ TreeLeave fullName info
	 "REG" -> do
	    walkReg pool opBox fullName info
	 "LNK" -> do
	    putMVar opBox $ TreeLink fullName info
	 _ -> do
	    putMVar opBox $ TreeOther fullName info

walkReg :: ChunkReader p => p -> MVar TreeOp -> FilePath -> Attr -> IO ()
walkReg pool opBox path info = do
   kind <- liftM fromJust $ poolChunkKind pool (justField info "HASH")
   putMVar opBox $ TreeReg path info kind
