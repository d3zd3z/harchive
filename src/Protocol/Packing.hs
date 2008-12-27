{-# LANGUAGE GeneralizedNewtypeDeriving #-}
----------------------------------------------------------------------
-- Various encodings for the protocol.
----------------------------------------------------------------------

module Protocol.Packing (
   putPBInt, getPBInt,
   putString, getString,
   PackedInteger(..),
   PackedInt(..),
   PackedString(..),
) where

import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Data.Bits
import qualified Data.ByteString.Char8 as BC (pack, unpack)
import Control.Monad (liftM)

-- Wrappers for a few regular types to use the nicer encoding.
newtype PackedInteger = PackedInteger { unpackInteger :: Integer }
   deriving (Enum, Eq, Integral, Num, Ord, Read, Real, Show)

newtype PackedInt = PackedInt { unpackInt :: Int }
   deriving (Bounded, Enum, Eq, Integral, Num, Ord, Read, Real, Show)

newtype PackedString = PackedString { unpackString :: String }
   deriving (Eq, Ord, Read, Show)

instance Binary PackedInteger where
   put = putPBInt
   get = getPBInt

instance Binary PackedInt where
   put = putPBInt
   get = getPBInt

instance Binary PackedString where
   put = putString . unpackString
   get = liftM PackedString $ getString

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

-- Notice that these are NOT UTF-8, but just binary.  This is actually
-- intentional, since the posix API is already encoded in UTF-8.
putString :: String -> Put
putString str = do
   putPBInt (length str)
   putByteString (BC.pack str)

getString :: Get String
getString = do
   len <- getPBInt
   str <- getByteString len
   return $ BC.unpack str
