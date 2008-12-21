----------------------------------------------------------------------
-- Protocol between clients and the pool server.
----------------------------------------------------------------------

module Protocol.ClientPool (
   InitRequest(..),
   InitReply(..),
   PoolRequest(..),
   BackupListReply(..),
   RestoreReply(..),
   FileDataReply(..),
   sendMessage, receiveMessage
) where

import Chunk
import Harchive.Store.Sexp
import Hash
import Protocol.Packing
import Protocol.Attr ()
import Protocol.Chunk ()

import qualified Data.ByteString.Lazy as L
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.Word
import Data.Time

import System.IO

import Control.Monad (liftM)

-- From the perspective of this protocol description, a message sent
-- from the file client to the pool server are always considered to be
-- a 'Request', and a message sent from the pool server to the client
-- is a 'Reply'.  This remains the case even when the protocol becomes
-- asynchronous.

sendMessage :: (Binary a) => Handle -> a -> IO ()
-- Encode the message and send it (with a length indicator).  Does not
-- flush the handle when it is finished.
sendMessage handle item = do
   let packed = encode item
   let header = encode (fromIntegral $ L.length packed :: Word32)
   L.hPut handle header
   L.hPut handle packed

receiveMessage :: (Binary a) => Handle -> IO a
-- Receive a single message.
receiveMessage handle = do
   bHeader <- L.hGet handle 4
   let len = fromIntegral (decode bHeader :: Word32)
   packed <- L.hGet handle len
   return $ decode packed

----------------------------------------------------------------------
-- TODO: Use TH, or a pre-processor to generate all of this marshal
-- and unmarshal code.

-- Initialization requests.  The first communication must belong to
-- this type.
data InitRequest
   = RequestHello String
   deriving (Show)

data InitReply
   = ReplyHello
   deriving (Show)

-- TODO: Put type information here (protobuf style).
instance Binary InitRequest where
   put (RequestHello uuid) = do
      putPBInt (4096::Int)
      putString uuid

   get = do
      key <- getPBInt :: Get Int
      case key of
	 4096 -> do
	    uuid <- getString
	    return $ RequestHello uuid
	 _ -> error $ "Invalid request value: " ++ show key

instance Binary InitReply where
   put ReplyHello = putPBInt (4097::Int)

   get = do
      key <- getPBInt :: Get Int
      case key of
	 4097 -> return ReplyHello
	 _ -> error $ "Invalid reply value: " ++ show key

----------------------------------------------------------------------

-- Basic requests, once we have connected to a given pool.  Most of
-- these enter a different state for the replies.
data PoolRequest
   = RequestBackupList
   | RequestRestore Hash
   | RequestGoodbye

instance Binary PoolRequest where
   put RequestBackupList = putPBInt (4200::Int)
   put (RequestRestore hash) = do
      putPBInt (4201::Int)
      putByteString $ toByteString hash
   put RequestGoodbye = putPBInt (4299::Int)

   get = do
      key <- getPBInt :: Get Int
      case key of
	 4200 -> return RequestBackupList
	 4201 -> do
	    hash <- getByteString 20
	    return $ RequestRestore (byteStringToHash hash)
	 4299 -> return RequestGoodbye
	 _ -> error $ "Invalid backup request: " ++ show key

----------------------------------------------------------------------
-- Response from RequestBackupList
data BackupListReply
   = BackupListNode Hash String String UTCTime
   | BackupListDone

instance Binary BackupListReply where
   put (BackupListNode hash host volume date) = do
      putPBInt (4300::Int)
      putByteString $ toByteString hash
      putString host
      putString volume
      -- This ends up being fairly large, since UTC time uses
      -- picoseconds.
      putPBInt $ fromEnum $ utctDay date
      putPBInt $ fromEnum $ utctDayTime date
   put BackupListDone = putPBInt (4399::Int)

   get = do
      key <- getPBInt :: Get Int
      case key of
	 4300 -> do
	    hash <- getByteString 20
	    host <- getString
	    volume <- getString
	    day <- getPBInt
	    dayTime <- getPBInt
	    let date = UTCTime (toEnum day) (toEnum dayTime)
	    return $ BackupListNode (byteStringToHash hash) host volume date
	 4399 -> return BackupListDone
	 _ -> error $ "Invalid backup reply: " ++ show key

----------------------------------------------------------------------
-- Response from RequestRestore
data RestoreReply
   = RestoreEnter String Attr
   | RestoreLeave String Attr
   | RestoreOpen String Attr
   | RestoreDone

instance Binary RestoreReply where
   -- TODO: Factor these.
   put (RestoreEnter path atts) = do
      putPBInt (10::Int)
      putString path
      put atts
   put (RestoreLeave path atts) = do
      putPBInt (11::Int)
      putString path
      put atts
   put (RestoreOpen path atts) = do
      putPBInt (12::Int)
      putString path
      put atts
   put RestoreDone = putPBInt (19::Int)

   get = do
      key <- getPBInt :: Get Int
      case key of
	 10 -> do
	    path <- getString
	    atts <- get
	    return $ RestoreEnter path atts
	 11 -> do
	    path <- getString
	    atts <- get
	    return $ RestoreLeave path atts
	 12 -> do
	    path <- getString
	    atts <- get
	    return $ RestoreOpen path atts
	 19 -> return RestoreDone
	 _ -> error $ "Invalid restore reply: " ++ show key

-- Possible responses within a file.
data FileDataReply
   = FileDataChunk Chunk
   | FileDataDone

instance Binary FileDataReply where
   put FileDataDone = putPBInt (40::Int)
   put (FileDataChunk chunk) = do
      putPBInt (41::Int)
      put chunk

   get = do
      key <- getPBInt :: Get Int
      case key of
	 40 -> return FileDataDone
	 41 -> liftM FileDataChunk $ get
	 _ -> error $ "Invalid reply: " ++ show key
