----------------------------------------------------------------------
-- Various encodings for the protocol.
----------------------------------------------------------------------

module Protocol.Packing (
   putPBInt, getPBInt,
   putString, getString
) where

import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.Bits
import qualified Data.ByteString.Char8 as BC (pack, unpack)
import Control.Monad (liftM)

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
