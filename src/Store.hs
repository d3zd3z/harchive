----------------------------------------------------------------------
-- Management of the storage pool itself.
-- Copyright 2007, David Brown
--
-- This program is free software; you can redistribute it and/or modify it
-- under the terms of the GNU General Public License as published by the
-- Free Software Foundation; either version 2, or (at your option) any later
-- version.
--
-- This program is distributed in the hope that it will be useful, but
-- WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
-- Public License for more details.
--
-- You should have received a copy of the GNU General Public License along
-- with this program; if not, write to the Free Software Foundation, Inc.,
-- 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
--
----------------------------------------------------------------------
--
-- Please ask <hackage@davidb.org> if you are interested in another
-- license.  If pieces of this program are useful in other systems I
-- will be willing to release them under a freer license, but I want
-- the program as a whole to be covered under the GPL.
--
----------------------------------------------------------------------
--
-- This manages the contents of chunks of data in the storage pool
-- itself.  This module does not concern itself with the meaning of
-- these chunks, merely their existence.
--
-- This uses a few tables in the SQL database to track the particular
-- files.

module Store (
   StoragePool(),
   openStore,
   Blob(..),
   poolWriteBlob,
   poolReadBlob,
   poolClose,
   poolFlush
) where

import qualified Sqlite3
import System.IO
import Data.Binary
import Data.Bits
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import Data.Char (ord, chr)
import Control.Monad (when)
import Data.Int
import Control.Concurrent.MVar
import System.Posix (fileExist)
import Hash

-- Open a storage directory using the given call.
openStore :: String -> Sqlite3.Connection -> IO StoragePool
openStore basePath sql = do
   putStrLn $ "Opening store at " ++ basePath
   st0 <- newMVar $ IdleState
   return $ StoragePool st0 sql basePath

----------------------------------------------------------------------
-- Operations to perform

-- Write a single blob to the pool, returning it's (Int,Int) node and
-- offset.
poolWriteBlob :: StoragePool -> Blob -> IO (Int, Int)
poolWriteBlob pool@(StoragePool mST _ _) blob = do
   st <- takeMVar mST
   st' <- prepareWrite pool st
   let handle = thisHandle st'
   hSeek handle SeekFromEnd 0
   pos <- hTell handle
   putBlob handle blob
   putMVar mST st'
   return (thisFile st', fromIntegral pos)

-- Read a single blob from the pool.
poolReadBlob :: StoragePool -> Int -> Int -> IO Blob
poolReadBlob pool@(StoragePool mST _ _) node offset = do
   st <- takeMVar mST
   st' <- prepareRead pool st node
   let handle = thisHandle st'
   hSeek handle AbsoluteSeek (fromIntegral offset)
   blob <- getBlob handle
   putMVar mST st'
   return blob

-- Close up the pool.
poolClose :: StoragePool -> IO ()
poolClose (StoragePool mST _ _) = do
   st <- takeMVar mST
   st' <- closePool st
   putMVar mST st'

-- Flush any pending writes.
poolFlush :: StoragePool -> IO ()
poolFlush (StoragePool mST _ _) = do
   st <- takeMVar mST
   flushPool st
   putMVar mST st

----------------------------------------------------------------------
-- States that the storage manager can be in.
data StorageState
   = IdleState
   | ReadState { thisFile :: Int, thisHandle :: Handle }
   | WriteState { thisFile :: Int, thisHandle :: Handle }

-- State information of the storage pool.
data StoragePool = StoragePool {
   _spState :: MVar StorageState,
   spSql :: Sqlite3.Connection,
   spBaseName :: String }

-- Generates a name, and returns the last part, as well as the full
-- path.
genName :: StoragePool -> Int -> (String, String)
genName (StoragePool _ _ base) index =
   (name, base ++ "/" ++ name)
   where
      name = "file-" ++ pre ++ textual ++ ".data"
      pre = replicate (4 - length textual) '0'
      textual = show index

-- Find a fresh filename.
-- This does a linear search starting at 1.  When the backup becomes
-- large, this can end up taking a bit of time, so perhaps we could
-- implement a binary search at some point.
genFreshName :: StoragePool -> IO (String, String)
genFreshName pool =
   attempt 1
   where
      attempt n = do
         let (name, path) = genName pool n
         exists <- fileExist path
         if exists
            then attempt (n+1)
            else return $ (name, path)

-- Prepare to write.  If we are already writing, leave it that way,
-- unless this file is too large, in which case move on to the next
-- file.
prepareWrite :: StoragePool -> StorageState -> IO StorageState

prepareWrite pool IdleState = do
   (name, path) <- genFreshName pool
   h <- openBinaryFile path WriteMode
   Sqlite3.exec (spSql pool) "insert into poolfiles (path) values (?)"
      [ Sqlite3.BString name ]
   index <- Sqlite3.lastRowId (spSql pool)
   return $ WriteState (fromIntegral index) h

prepareWrite pool st@(WriteState _ handle) = do
   -- Check that the size hasn't exceeded the limit.  If it has, then
   -- go to the next file.
   size <- hFileSize handle
   if size > (640*1024*1024)
      then do
         st' <- closePool st
         prepareWrite pool st'
      else return st

prepareWrite pool st = do
   st' <- closePool st
   prepareWrite pool st'

-- Prepare to read from the given file.  Does nothing if this is the
-- file we are currently reading from, otherwise, closes the current
-- file, and opens the specified file.
prepareRead :: StoragePool -> StorageState -> Int -> IO StorageState
prepareRead pool IdleState num = do
   path <- Sqlite3.query (spSql pool)
      "select path from poolfiles where node = ?"
      [ Sqlite3.BInt64 $ fromIntegral num ] (error "Invalid path") $ \_ args ->
         case args of
            [ Sqlite3.BString p ] -> return $ p
            _ -> error "Unknown sql reply"
   let fullPath = (spBaseName pool) ++ "/" ++ path
   h <- openBinaryFile fullPath ReadMode
   return $ ReadState num h
prepareRead _ st@(ReadState nh _) num | nh == num = return st
prepareRead pool st num = do
   st' <- closePool st
   prepareRead pool st' num

-- Close any open file.
closePool :: StorageState -> IO StorageState
closePool st@IdleState = return st
closePool (ReadState _ handle) = do
   hClose handle
   return $ IdleState
closePool (WriteState _ handle) = do
   hClose handle
   return $ IdleState

-- Flush data to the write descriptor.
flushPool :: StorageState -> IO ()
flushPool (WriteState _ handle) = hFlush handle
flushPool _ = return ()

----------------------------------------------------------------------

-- Item to be stored in the pool.
data Blob = Blob {
   bHash :: Hash,
   bKind :: String,
   bUncompLen :: Int,
   bPayload :: B.ByteString }
   deriving (Eq, Show)

-- Magic header for files.
storeMagic :: B.ByteString
storeMagic = packString "Pool Chunk, v1.2"

-- Pack a string using Word8 representation of each character (not
-- UTF-8).
packString :: String -> B.ByteString
packString = B.pack . map (fromIntegral . ord)

-- Write a single blob to the current handle.
-- The blob is written to make it easier to find and read individual
-- pieces.
putBlob :: Handle -> Blob -> IO ()
putBlob handle (Blob (Hash hash) kind uncomplen payload) = do
   let kindLen = length kind
   when (B.length hash /= 20) $ fail "Incorrect hash length"
   when (kindLen > 255) $ fail "Kind too long"
   let
      chunks = [ storeMagic,
         hash,
         packInt32 $ uncomplen,
         packInt32 $ B.length payload,
         B.singleton (fromIntegral kindLen),
         packString kind,
         payload
         ]
   L.hPut handle . L.fromChunks $ chunks

-- Read a single blob from the current handle.
getBlob :: Handle -> IO Blob
getBlob handle = do
   magic <- B.hGet handle (B.length storeMagic)
   when (magic /= storeMagic) $ fail "Invalid magic in file"
   hash <- B.hGet handle 20

   uncompLenBS <- B.hGet handle 4
   let uncompLen = unpackInt32 uncompLenBS

   payloadLenBS <- B.hGet handle 4
   let payloadLen = unpackInt32 payloadLenBS

   kindLenCh <- hGetChar handle
   let kindLen = ord kindLenCh

   kindBS <- B.hGet handle kindLen
   let kind = map (chr . fromIntegral) . B.unpack $ kindBS

   payload <- B.hGet handle payloadLen
   return $ Blob (Hash hash) kind uncompLen payload

-- Pack a 32-bit value.
packInt32 :: Int -> B.ByteString
packInt32 val =
   B.pack [ fromIntegral (val) :: Word8,
      fromIntegral (val `shiftR` 8) :: Word8,
      fromIntegral (val `shiftR` 16) :: Word8,
      fromIntegral (val `shiftR` 32) :: Word8 ]

-- Unpack a 32-bit value.
unpackInt32 :: B.ByteString -> Int
unpackInt32 bs =
   fromIntegral $ unp 0 bytes
   where
      bytes = B.unpack bs
      unp _ [] = 0 :: Int32
      unp n (x:xs) = ((fromIntegral x) `shiftL` n) .|. (unp (n+8) xs)
