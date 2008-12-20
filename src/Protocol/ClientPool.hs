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

import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Char8 as BC (pack, unpack)
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.Bits
import Data.Word

import Control.Monad (liftM)

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

-- TODO: The Integer might be overkill here.
putPBInt :: Integral a => a -> Put
-- Output an integer in Protobuf integer format.
putPBInt num = do
   let num' = (fromIntegral num) :: Integer
   output num'
   loop $ num' `shiftR` 7
   where
      output x | x < 128 = putWord8 $ fromIntegral x .&. 127
      output x = putWord8 $ (fromIntegral x .&. 127) .|. 128

      loop x | x == 0  = return ()
      loop x = do
	 output x
	 loop $ x `shiftR` 7

getPBInt :: Integral a => Get a
-- Get a single integer.
getPBInt = do
   liftM fromIntegral $ accum 0 0
   where
      accum :: Integer -> Int -> Get Integer
      accum num shft = do
	 digit <- liftM fromIntegral getWord8
	 let num' = num .|. ((digit .&. 127) `shiftL` shft)
	 if digit < 128
	    then return num'
	    else accum num' (shft + 7)

putString :: String -> Put
putString str = do
   putPBInt (length str)
   putByteString (BC.pack str)

getString :: Get String
getString = do
   len <- getPBInt
   str <- getByteString len
   return $ BC.unpack str

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
   | RequestGoodbye

instance Binary PoolRequest where
   put RequestBackupList = putPBInt (4200::Int)
   put RequestGoodbye = putPBInt (4299::Int)

   get = do
      key <- getPBInt :: Get Int
      case key of
	 4200 -> return RequestBackupList
	 4299 -> return RequestGoodbye
	 _ -> error $ "Invalid backup request: " ++ show key

data BackupListReply
   = BackupListNode Hash String String String
   | BackupListDone

instance Binary BackupListReply where
   put (BackupListNode hash host volume date) = do
      putPBInt (4300::Int)
      putByteString $ toByteString hash
      putString host
      putString volume
      putString date
   put BackupListDone = putPBInt (4399::Int)

   get = do
      key <- getPBInt :: Get Int
      case key of
	 4300 -> do
	    hash <- getByteString 20
	    host <- getString
	    volume <- getString
	    date <- getString
	    return $ BackupListNode (byteStringToHash hash) host volume date
	 4399 -> return BackupListDone
	 _ -> error $ "Invalid backup reply: " ++ show key
