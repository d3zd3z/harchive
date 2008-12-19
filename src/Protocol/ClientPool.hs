----------------------------------------------------------------------
-- Protocol between clients and the pool server.
----------------------------------------------------------------------

module Protocol.ClientPool (
   Request(..),
   Reply(..),
   sendMessage, receiveMessage
) where

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

data Request
   = RequestHello String
   deriving (Show)

data Reply
   = ReplyHello
   deriving (Show)

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

-- TODO: Put type information here (protobuf style).
instance Binary Request where
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

instance Binary Reply where
   put ReplyHello = putPBInt (4097::Int)

   get = do
      key <- getPBInt :: Get Int
      case key of
	 4097 -> return ReplyHello
	 _ -> error $ "Invalid reply value: " ++ show key
