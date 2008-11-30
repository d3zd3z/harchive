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

newtype ChunkFile = ChunkFile (MVar ChunkState)

data ChunkState = ChunkState {
   csPath :: String,
   handleState :: HandleState }

openChunkFile :: FilePath -> IO ChunkFile
openChunkFile path = do
   cs <- newMVar $ ChunkState { csPath = path, handleState = HClosed }
   return . ChunkFile $ cs

chunkClose :: ChunkFile -> IO ()
chunkClose (ChunkFile state) = do
   modifyMVar_ state $ \cs -> do
      state' <- cfClose (handleState cs)
      return $ cs { handleState = state' }

chunkFlush :: ChunkFile -> IO ()
chunkFlush (ChunkFile state) = do
   withMVar state $ \cs -> do
      cfFlush (handleState cs)

chunkRead :: ChunkFile -> Int -> IO Chunk
chunkRead (ChunkFile state) offset = do
   modifyMVar state $ \cs -> do
      (state', handle) <- getReadable (csPath cs) (handleState cs)
      chunk <- readChunk handle offset
      return $ (cs { handleState = state' }, chunk)

readChunk :: Handle -> Int -> IO Chunk
readChunk fd pos = do
   hSeek fd AbsoluteSeek (fromIntegral pos)
   rawHeader <- B.hGet fd 48
   let header = runGet getHeader $ L.fromChunks [rawHeader]
   either (\_ -> ioError . userError $ "Error reading chunk") getData header
   where
      getData header = do
	 payload <- B.hGet fd (fromIntegral . hCLen $ header)
	 let lazyPayload = L.fromChunks [payload]
	 let uclen = hUCLen header
	 let kind = hKind header
	 if uclen == 0xFFFFFFFF
	    then return (byteStringToChunk kind lazyPayload)
	    else return (zDataToChunk kind lazyPayload (fromIntegral uclen))

----------------------------------------------------------------------
getHeader :: Get (Either String Header)
getHeader = do
   magic <- getBytes 16
   if magic /= headerMagic
      then return $ Left "Invalid header magic"
      else do
	 clen <- getWord32le
	 uclen <- getWord32le
	 kind <- getBytes 4
	 hash <- getBytes 20
	 return . Right $ Header {
	    hCLen = clen,
	    hUCLen = uclen,
	    hKind = (map $ toEnum . fromIntegral) . B.unpack $ kind,
	    hHash = hash }

headerMagic :: B.ByteString
headerMagic = B.pack $ (map $ fromIntegral . fromEnum) "adump-pool-v1.1\n"

data Header = Header {
   hCLen :: Word32,
   hUCLen :: Word32,
   hKind :: String,
   hHash :: B.ByteString }
   deriving (Eq, Show)

----------------------------------------------------------------------

{-
data ChunkFile = ChunkFile {
   getReadable :: IO (Handle, ChunkFile),
   getWritable :: IO (Handle, ChunkFile),
   getClosed :: IO ChunkFile }
-}

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

{-
openChunkFile :: FilePath -> ChunkFile
openChunkFile path = 
   ChunkFile { getReadable = readable, getWritable writable,
      getClosed = closed }
   where
      readable = do
-}
