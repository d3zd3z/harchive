----------------------------------------------------------------------
-- Chunk I/O operations.
----------------------------------------------------------------------

module Chunk.IO (
   ChunkFile,
   openChunkFile,
   chunkRead, chunkRead_,
   chunkWrite,
   chunkClose,
   chunkFlush,
   chunkFileSize
) where

import Chunk
import System.IO
import Control.Concurrent.MVar
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import Data.Binary.Get
import Data.Binary.Put
import Control.Monad (unless, liftM)
import Hash
import Data.Bits ((.&.))
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
chunkClose (ChunkFile cfs) = do
   modifyMVar_ cfs $ \state -> do
      hstate' <- cfClose (handleState state)
      return $ state { handleState = hstate' }

chunkFlush :: ChunkFile -> IO ()
chunkFlush (ChunkFile cfs) = do
   withMVar cfs $ \state -> do
      cfFlush (handleState state)

-- |Read a single chunk at the specified offset.  Returns the chunk,
-- and a guess as to the offset of the next chunk.
chunkRead :: ChunkFile -> Int -> IO (Chunk, Int)
chunkRead (ChunkFile cfs) offset = do
   modifyMVar cfs $ \state -> do
      (hstate', handle) <- getReadable (csPath state) (handleState state)
      chunkPos <- readChunk handle offset
      return $ (state { handleState = hstate' }, chunkPos)

-- |Like ChunkRead, but only returns the chunk.
chunkRead_ :: ChunkFile -> Int -> IO Chunk
chunkRead_ cfs = liftM fst . chunkRead cfs

-- |Write a single chunk to the end of the file.  Returns the offset
-- that the chunk was written to.
chunkWrite :: ChunkFile -> Chunk -> IO Int
chunkWrite (ChunkFile cfs) chunk = do
   modifyMVar cfs $ \state -> do
      (hstate', handle, pos) <- getWritable (csPath state) (handleState state)
      len <- writeChunk handle chunk
      let hstate'' = updatePos hstate' (pos + len)
      return $ (state { handleState = hstate'' }, pos )

chunkFileSize :: ChunkFile -> IO Int
-- |Determine the size of the file.  The file is closed at the end.
chunkFileSize (ChunkFile cfs) = do
   modifyMVar cfs $ \state -> do
      (hstate', handle) <- getReadable (csPath state) (handleState state)
      size <- hFileSize handle
      hstate'' <- cfClose hstate'
      return $ (state { handleState = hstate'' }, fromIntegral size)

----------------------------------------------------------------------
readChunk :: Handle -> Int -> IO (Chunk, Int)
readChunk fd pos = do
   hSeek fd AbsoluteSeek (fromIntegral pos)
   rawHeader <- B.hGet fd 48
   (chunk, len) <- runGet (getPayload fd) $ L.fromChunks [rawHeader]
   return (chunk, pos + 48 + len + (padLen 16 len))

writeChunk :: Handle -> Chunk -> IO Int
writeChunk fd chunk = do
   let item = runPut (putChunk chunk)
   L.hPut fd item
   return $ fromIntegral $ L.length item

-- Compute the number of bytes needed to pad 'value' to a 'padding'
-- boundary.  Formula from Hacker's Delight.
padLen :: Int -> Int -> Int
padLen padding value = (-value) .&. (padding - 1)

----------------------------------------------------------------------
-- Returns a possible string, indicating an error, or Nothing if the
-- header is fine.
checkMagic :: Get Bool
checkMagic = do
   magic <- getBytes 16
   return $ magic == headerMagic

-- Parse the rest of the header.  The resulting IO will return the
-- full chunk, and the number of bytes of payload read.
getPayload :: Handle -> Get (IO (Chunk, Int))
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
	    return (chunk, fromIntegral clen)
      else return (ioError . userError $ "Invalid magic number")

putChunk :: Chunk -> Put
putChunk chunk = do
   putByteString headerMagic
   let zData = chunkZData chunk
   let len = chunkLength chunk
   let (uclen, payload) = case zData of
	 Nothing -> (0xFFFFFFFF :: Word32, chunkData chunk)
	 Just z -> (fromIntegral len, z)
   putWord32le $ fromIntegral $ L.length payload
   putWord32le $ uclen
   putByteString $ B.pack $ (map $ fromIntegral . fromEnum) $ chunkKind chunk
   putByteString $ toByteString $ chunkHash chunk
   putLazyByteString payload
   putByteString $ B.replicate (padLen 16 $ fromIntegral $ L.length payload) 0

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
updatePos _ _ = error "Update position of non-writable file"
