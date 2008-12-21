----------------------------------------------------------------------
-- Directory Trees.
----------------------------------------------------------------------

module Tree (
   walk, walkLazy, walkTree,
   walkHashes,
   forEachChunk,
   TreeOp(..),
   module Harchive.Store.Sexp
) where

import Chunk
import Hash
import Harchive.Store.Sexp
import Pool

import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString as B
import Data.Maybe (fromJust)
import System.FilePath
import Control.Monad.Reader
import Control.Concurrent

import System.IO.Unsafe

walkHashes :: ChunkReader p => p -> Hash -> (Hash -> IO ()) -> IO ()
-- |Walk through all of the hashes of the chunks involved in a
-- particular backup.  Reads only the data from the pool necessary to
-- determine all involved data (directories, and indirect blocks).
-- Calls 'act' on each hash involved.
walkHashes pool rootHash act = do
   act rootHash
   rootChunk_ <- poolReadChunk pool rootHash
   let rootChunk = fromJust rootChunk_
   case chunkKind rootChunk of
      "dir " -> subDir rootChunk
      ['d','i','r',n] | n >= '0' && n <= '9' -> do
	 mapM_ (\h -> walkHashes pool h act)
	    (indirectHashes $ chunkData rootChunk)
      k -> error $ "Implement walking for: " ++ k

   where
      subDir :: Chunk -> IO ()
      -- Iterate through the contents of a directory.
      subDir chunk = forM_ (decodeMultiChunk chunk) $ \info -> do
	 case attrKind info of
	    "DIR" ->
	       walkHashes pool (justField info "HASH") act
	    "REG" ->
	       regularFile (justField info "HASH")
	    _ -> return ()

      regularFile :: Hash -> IO ()
      -- Iterate over a regular file.
      regularFile hash = do
	 act hash
	 kind_ <- poolChunkKind pool hash
	 let kind = fromJust kind_
	 case kind of
	    "blob" -> return ()
	    ['i','n','d',n] | n >= '0' && n <= '9' -> do
	       payload_ <- poolReadChunk pool hash
	       let payload = fromJust payload_
	       mapM_ regularFile (indirectHashes $ chunkData payload)
	    k -> error $ "Implement walking for: " ++ k

-- |Call 'act' with each chunk of file data.
-- TODO: Better error handling.
forEachChunk :: ChunkReader p => p -> Hash -> (Chunk -> IO ()) -> IO ()
forEachChunk pool hash act = do
   loop hash
   where
      loop h = do
	 chunk <- liftM fromJust $ poolReadChunk pool h
	 case chunkKind chunk of
	    "blob" -> act chunk
	    ['i','n','d',n] | n >= '0' && n <= '9' -> do
	       mapM_ loop (indirectHashes $ chunkData chunk)
	    k -> error $ "Unknown chunk type in fileadta: " ++ k

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
   deriving (Show)

-- Note the assymetry of the root directory here.  The attributes of a
-- directory are stored outside of that directory in the parent, so
-- the root's attributes are stored in the backup record.

-- Walk the tree, calling the operation for each node.  TODO: The code
-- isn't really complicated enough to warrant forking.
walkTree :: ChunkReader p => p -> Hash -> (TreeOp -> IO ()) -> IO ()
walkTree pool hash action = do
   getNode <- walk pool hash
   loop getNode
   where
      loop getNode = do
	 item <- getNode
	 case item of
	    TreeEOF -> return ()
	    _ -> do
	       action item
	       loop getNode

----------------------------------------------------------------------
walkLazy :: ChunkReader p => p -> Hash -> IO [TreeOp]
-- Perform the walk, and return a list which will be evaluated lazily.
-- The list never contains a TreeEOF, since the end of list is
-- obvious.
walkLazy pool hash = do
   walker <- walk pool hash
   loop walker
   where
      loop walker = do
	 item <- walker
	 case item of
	    TreeEOF -> return []
	    x -> do
	       rest <- unsafeInterleaveIO $ loop walker
	       return $ x : rest

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

----------------------------------------------------------------------

indirectHashes :: L.ByteString -> [Hash]
-- Break a chunk of data consisting of hashes into chunk-sized pieces,
-- returning them in a list.
indirectHashes l | L.length l == 0 = []
indirectHashes l | L.length l < 20 =
   error "Indirect block is not a multiple of 20 bytes"
indirectHashes l = byteStringToHash x : indirectHashes xs
   where
      (xl, xs) = L.splitAt 20 l
      x = B.concat $ L.toChunks xl
