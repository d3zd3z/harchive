----------------------------------------------------------------------
-- Chunk I/O operations.
----------------------------------------------------------------------

module Chunk.IO (
   ChunkFile,
   openChunkFile,
   chunkRead,
   chunkClose,
   chunkFlush
) where

import Chunk
import System.IO
import Control.Concurrent.MVar
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import Data.Binary.Get
import Data.Word
import Control.Monad (unless)
import Hash

newtype ChunkFile = ChunkFile (MVar ChunkState)

data ChunkState = ChunkState {
   csPath :: String,
   handleState :: HandleState }

openChunkFile :: FilePath -> IO ChunkFile
openChunkFile path = do
   cs <- newMVar $ ChunkState { csPath = path, handleState = HClosed }
   return . ChunkFile $ cs

chunkClose :: ChunkFile -> IO ()
chunkClose (ChunkFile cfs) = do
   modifyMVar_ cfs $ \state -> do
      hstate' <- cfClose (handleState state)
      return $ state { handleState = hstate' }

chunkFlush :: ChunkFile -> IO ()
chunkFlush (ChunkFile cfs) = do
   withMVar cfs $ \state -> do
      cfFlush (handleState state)

chunkRead :: ChunkFile -> Int -> IO Chunk
chunkRead (ChunkFile cfs) offset = do
   modifyMVar cfs $ \state -> do
      (hstate', handle) <- getReadable (csPath state) (handleState state)
      chunk <- readChunk handle offset
      return $ (state { handleState = hstate' }, chunk)

-- Write the chunk to the file, returning the offset it was written
-- to.
{-
chunkWrite :: ChunkFile -> Chunk -> IO Int
chunkWrite (ChunkFile cfs) chunk = do
   modifyMVar cfs $ \state -> do
      (hstate', handle, pos) <- getWritable (csPath state) (handleState state)
      writeChunk handle chunk
-}

readChunk :: Handle -> Int -> IO Chunk
readChunk fd pos = do
   hSeek fd AbsoluteSeek (fromIntegral pos)
   rawHeader <- B.hGet fd 48
   runGet (getPayload fd) $ L.fromChunks [rawHeader]

----------------------------------------------------------------------
-- Returns a possible string, indicating an error, or Nothing if the
-- header is fine.
checkMagic :: Get Bool
checkMagic = do
   magic <- getBytes 16
   return $ magic == headerMagic

-- Parse the rest of the header.  The resulting IO will return the
-- full chunk.
getPayload :: Handle -> Get (IO Chunk)
getPayload fd = do
   goodMagic <- checkMagic
   if goodMagic
      then do
	 clen <- getWord32le
	 uclen <- getWord32le
	 kind <- getBytes 4
	 let kind' = (map $ toEnum . fromIntegral) . B.unpack $ kind
	 hash <- getBytes 20
	 return $ do
	    payload <- B.hGet fd (fromIntegral clen)
	    let lazyPayload = L.fromChunks [payload]
	    let
	       chunk = if uclen == 0xFFFFFFFF
		  then byteStringToChunk kind' lazyPayload
		  else zDataToChunk kind' lazyPayload (fromIntegral uclen)
	    -- TODO: Make this check optional.
	    unless (hash == (toByteString . chunkHash $ chunk)) $ do
	       ioError . userError $ "Hash mismatch"
	    return chunk
      else return (ioError . userError $ "Invalid magic number")

headerMagic :: B.ByteString
headerMagic = B.pack $ (map $ fromIntegral . fromEnum) "adump-pool-v1.1\n"

----------------------------------------------------------------------

-- Keeps track of the openness of file handles and such.
-- The handle is either closed, opened for read, or opened for write
-- (with the file pointer seeked to the end of the file).
data HandleState
   = HClosed
   | HReadable Handle
   | HWritable Handle Int

-- Get a readable Handle out of a HandleState, and return a new handle
-- state.
getReadable :: FilePath -> HandleState -> IO (HandleState, Handle)
getReadable path HClosed = do
   fd <- openBinaryFile path ReadMode
   return (HReadable fd, fd)
getReadable _ hs@(HReadable fd) = return (hs, fd)
getReadable path (HWritable fd _) = do
   hClose fd
   getReadable path HClosed

getWritable :: FilePath -> HandleState -> IO (HandleState, Handle, Int)
getWritable path HClosed = do
   fd <- openBinaryFile path ReadWriteMode
   hSeek fd SeekFromEnd 0
   pos <- hTell fd
   let pos' = fromIntegral pos
   return (HWritable fd pos', fd, pos')
getWritable _ hs@(HWritable fd pos) = return (hs, fd, pos)
getWritable path (HReadable fd) = do
   hClose fd
   getWritable path HClosed

cfClose :: HandleState -> IO HandleState
cfClose (HReadable fd) = do
   hClose fd
   return HClosed
cfClose (HWritable fd _) = do
   hClose fd
   return HClosed
cfClose hs = return hs

cfFlush :: HandleState -> IO ()
cfFlush (HWritable fd _) = hFlush fd
cfFlush _ = return ()

updatePos :: HandleState -> Int -> HandleState
updatePos (HWritable fd _) newPos = HWritable fd newPos
updatePos _ _ = error "Update position of non-write file"
