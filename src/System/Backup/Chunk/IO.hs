----------------------------------------------------------------------
-- Chunk I/O
----------------------------------------------------------------------

module System.Backup.Chunk.IO (
   ChunkFile, IOMode(..),
   openChunkFile, chunkClose, chunkFlush, chunkFileSize,
   chunkRead, chunkRead_, chunkWrite
) where

import qualified Hash
import System.Backup.Chunk

import Control.Concurrent
import Control.Monad.Reader
import Control.Monad.State
import Data.Binary.Get
import Data.Binary.Put
import Data.Bits ((.&.))
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as B8
import qualified Data.ByteString.Lazy as L
import System.IO

data ChunkFile = ChunkFile {
   csPath :: String,
   csWrite :: Chunk -> ChunkIO Int,
   csHandleState :: MVar HandleState }

data HandleState
   = HClosed
   | HReadable Handle
   | HWritable Handle Int

type ChunkIO = ReaderT String (StateT HandleState IO)

runChunkIO :: ChunkFile -> ChunkIO a -> IO a
runChunkIO (ChunkFile { csHandleState = stateVar, csPath = path }) op =
   modifyMVar stateVar $ \st -> do
      (a, st') <- runStateT work st
      return (st', a)
   where work = runReaderT op path

openChunkFile :: FilePath -> IOMode -> IO ChunkFile
openChunkFile path mode = do
   hs <- newMVar HClosed
   let writer = makeWriter mode
   return ChunkFile { csPath = path, csWrite = writer, csHandleState = hs }

chunkClose :: ChunkFile -> IO ()
chunkClose cf = runChunkIO cf $ do
   hsClose
   put $ error "Attempt to use ChunkFile after close"

chunkFlush :: ChunkFile -> IO ()
chunkFlush = flip runChunkIO hsFlush

chunkFileSize :: ChunkFile -> IO Int
chunkFileSize = flip runChunkIO hsFileSize

chunkRead :: ChunkFile -> Int -> IO (Chunk, Int)
chunkRead cf = runChunkIO cf . hsReadChunk

chunkRead_ :: ChunkFile -> Int -> IO Chunk
chunkRead_ cf = fmap fst . chunkRead cf

chunkWrite :: ChunkFile -> Chunk -> IO Int
chunkWrite cf@(ChunkFile { csWrite = writer }) =
   runChunkIO cf . writer

makeWriter :: IOMode -> (Chunk -> ChunkIO Int)
makeWriter ReadMode = \_ -> error "Attempt to write to ReadOnly Chunk file"
makeWriter AppendMode = hsWriteChunk
makeWriter mode = error $ "Invalid open mode for ChunkFile: " ++ show mode

----------------------------------------------------------------------
-- Actual IO operations.

-- Read the chunk at the given offset, returning the chunk itself,
-- plus a guess as to where the next chunk should be.
hsReadChunk :: Int -> ChunkIO (Chunk, Int)
hsReadChunk pos = do
   fd <- getReadable
   liftIO $ hSeek fd AbsoluteSeek (fromIntegral pos)
   rawHeader <- liftIO $ B.hGet fd 48
   -- liftIO $ putStrLn $ hexDump $ lazify rawHeader
   (chunk, len) <- liftIO $ runGet (getPayload fd) $ L.fromChunks [rawHeader]
   return (chunk, pos + 48 + len + padLen 16 len)

hsWriteChunk :: Chunk -> ChunkIO (Int)
hsWriteChunk chunk = do
   (fd, oldPos) <- getWritable
   let item = runPut (putChunk chunk)
   liftIO $ L.hPut fd item
   let size = fromIntegral $ L.length item
   updatePos size
   return oldPos

-- Compute the number of bytes needed to pad 'value' to a 'padding'
-- boundary.  Formula from Hacker's Delight.
padLen :: Int -> Int -> Int
padLen padding value = (-value) .&. (padding - 1)

----------------------------------------------------------------------
-- Parse the header, and return an IO able to parse the entire rest of
-- the header as well.
getPayload :: Handle -> Get (IO (Chunk, Int))
getPayload fd = do
   goodMagic <- checkMagic
   if goodMagic then getRest
      else return (ioError $ userError "Invalid magic number")
   where
      getRest = do
         clen <- getWord32le
         uclen <- getWord32le
         kind <- fmap B8.unpack $ getBytes 4
         hash <- getBytes 20
         return $ do
            payload <- B.hGet fd (fromIntegral clen)
            let lazyPayload = L.fromChunks [payload]
            let
               chunk = if uclen == 0xFFFFFFFF
                  then byteStringToChunk kind lazyPayload
                  else zDataToChunk kind lazyPayload (fromIntegral uclen)
            -- TODO: Make this check optional.
            unless (hash == Hash.toByteString (chunkHash chunk)) $
               ioError $ userError "Hash mismatch"
            return (chunk, fromIntegral clen)

putChunk :: Chunk -> Put
putChunk chunk = do
   putByteString headerMagic
   let zData = chunkZData chunk
   let len = chunkLength chunk
   let (uclen, payload) = case zData of
         Nothing -> (0xFFFFFFFF, chunkData chunk)
         Just z -> (fromIntegral len, z)
   putWord32le $ fromIntegral $ L.length payload
   putWord32le uclen
   putByteString $ B8.pack $ chunkKind chunk
   putByteString $ Hash.toByteString $ chunkHash chunk
   putLazyByteString payload
   putByteString $ B.replicate (padLen 16 $ fromIntegral $ L.length payload) 0

checkMagic :: Get Bool
checkMagic = do
   magic <- getBytes 16
   return $ magic == headerMagic

headerMagic :: B.ByteString
headerMagic = B8.pack "adump-pool-v1.1\n"

----------------------------------------------------------------------
-- Keep track of the open state of file handles for chunks.  When the
-- handle is opened for write, we also track the position that the
-- file pointer is at (which should always be the end of the file).

-- Get a readable handle.
getReadable :: ChunkIO Handle
getReadable =
   get >>= makeOpen
   where
      makeOpen HClosed = do
         path <- ask
         fd <- liftIO $ openBinaryFile path ReadMode
         put $ HReadable fd
         return fd
      makeOpen (HReadable fd) = return fd
      makeOpen (HWritable fd _) = liftIO (hClose fd) >> makeOpen HClosed

-- Get a writable handle, as well as the current write offset.
getWritable :: ChunkIO (Handle, Int)
getWritable = get >>= makeOpen
   where
      makeOpen HClosed = do
         path <- ask
         fd <- liftIO $ openBinaryFile path ReadWriteMode
         liftIO $ hSeek fd SeekFromEnd 0
         pos <- liftIO $ hTell fd
         let pos' = fromIntegral pos
         put $ HWritable fd pos'
         return (fd, pos')
      makeOpen (HWritable fd pos) = return (fd, pos)
      makeOpen (HReadable fd) = liftIO (hClose fd) >> makeOpen HClosed

hsFlush :: ChunkIO ()
hsFlush = get >>= doFlush
   where
      doFlush (HWritable fd _) = liftIO (hFlush fd)
      doFlush _ = return ()

-- Make sure the handle is closed.
hsClose :: ChunkIO ()
hsClose = do
   get >>= doClose
   put HClosed
   where
      doClose (HWritable fd _) = liftIO (hClose fd)
      doClose (HReadable fd) = liftIO (hClose fd)
      doClose _ = return ()

-- Determine the file size.
hsFileSize :: ChunkIO Int
hsFileSize = get >>= doSize
   where
      doSize (HReadable fd) = fmap fromIntegral $ liftIO $ hFileSize fd
      doSize (HWritable _ pos) = return pos
      doSize HClosed = do
         -- If it's not opened, open it readable, determine the size,
         -- and close it.
         path <- ask
         liftIO $ do
            fd <- openBinaryFile path ReadMode
            size <- hFileSize fd
            hClose fd
            return $ fromIntegral size

updatePos :: Int -> ChunkIO ()
updatePos size = get >>= doUpdate
   where
      doUpdate (HWritable fd pos) = put $ HWritable fd (pos + size)
      doUpdate _ = error "Update position of non-writable file"
