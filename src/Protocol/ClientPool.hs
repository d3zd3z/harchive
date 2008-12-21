----------------------------------------------------------------------
-- Protocol between clients and the pool server.
----------------------------------------------------------------------

module Protocol.ClientPool (
   InitRequest(..),
   InitReply(..),
   PoolRequest(..),
   BackupListReply(..),
   sendMessage, receiveMessage
) where

import Hash
import Protocol.Packing

import qualified Data.ByteString.Lazy as L
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.Word
import Data.Time

import System.IO

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
