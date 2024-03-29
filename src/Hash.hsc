{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE TypeSynonymInstances #-}
{-# LANGUAGE FlexibleContexts #-}
----------------------------------------------------------------------
-- Hashing operators
-- Copyright 2007, 2009, David Brown
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

module Hash (
   Hash(..),
   -- Instances Eq, Show, Binary

   hashOf, hashOfIO,
   toHex, fromHex,
   byteStringToHash,
   getHashByte,

   hashBlockLength,
   hashLength
)where

import Data.ByteString.Internal (create, toForeignPtr)
import Data.Convertible.Text (ConvertSuccess, cs)
import Control.Monad (forM_, liftM)
import Foreign
import Foreign.C
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import Text.Show
import Text.Printf (printf)
import Data.Char (ord)
import Data.Binary
import Data.Binary.Put (putByteString)
import Data.Binary.Get (getByteString)

-- Binding to OpenSSL SHA1 library, which is significantly faster than
-- most other implementations.
#include <openssl/sha.h>

-- The hash result type is abstract.
newtype Hash = RawHash { unHash :: B.ByteString }
   deriving (Eq, Ord)

-- Return the given byte out of a hash.
getHashByte :: Int -> Hash -> Word8
getHashByte ofs h = B.index (unHash h) ofs

instance Show Hash where
   showsPrec _ h =
      showString "fromHex \"" .
      showString (toHex h) .
      showChar '"'

-- Construct a raw hash.
byteStringToHash :: B.ByteString -> Hash
byteStringToHash h | B.length h == fromIntegral hashLength = RawHash h
byteStringToHash _ = error "Invalid hash length"

toHex :: Hash -> String
toHex (RawHash h) = hexify h

fromHex :: String -> Hash
fromHex str =
   case unhexify str of
      h | B.length h == hashLength -> RawHash h
        | otherwise -> error $  "Hash must be " ++ show (hashLength * 2) ++ " hex digits"

-- Simple, compatible implementation.  Eventually change this to only
-- output the correct length bytes of the hash, instead of the full
-- amount.
instance Binary Hash where
   put (RawHash h) = putByteString h
   get = do
      res <- getByteString hashLength
      return $ RawHash res

hashLength :: Int
hashLength = (#const SHA_DIGEST_LENGTH)

hashBlockLength :: Int
hashBlockLength = (#const SHA_CBLOCK)

----------------------------------------------------------------------

-- Compute hashes of anything that convertible-text knows how to make
-- into a lazy byte string.
hashOf :: ConvertSuccess a L.ByteString => a -> Hash
hashOf = unsafePerformIO . hashOfIO . cs

hashOfIO :: L.ByteString -> IO Hash
hashOfIO bstr = do
   allocaBytes (#size SHA_CTX) $ \ctx -> do
   c_sha1Init ctx
   forM_ (L.toChunks bstr) $ \bStr -> do
      -- B.useAsCStringLen bStr $ \(bdata, blen) -> do

      unsafeUseAsCStringLen bStr $ \(bdata, blen) -> do
	 c_sha1Update ctx bdata blen

   -- Copy out the hash result.
   liftM RawHash $ create hashLength $ \p ->
      c_sha1Final p ctx

unsafeUseAsCString :: B.ByteString -> (CString -> IO a) -> IO a
-- Non-copying version of useAsCStringLen.  Can only be used if
-- the pointer is only read.
unsafeUseAsCString bs action = do
   let (fp, o, _) = toForeignPtr bs
   withForeignPtr fp $ \p -> do
      action (castPtr (p `plusPtr` o))

unsafeUseAsCStringLen :: B.ByteString -> (CStringLen -> IO a) -> IO a
unsafeUseAsCStringLen p f = do
   let (_, _, l) = toForeignPtr p
   unsafeUseAsCString p $ \cstr -> f (cstr, l)

----------------------------------------------------------------------
-- Generate a nice hex representation of a byte string.  Not
-- necessarily efficient, but is used only for debugging.
hexify :: B.ByteString -> String
hexify bytes =
   process $ B.unpack bytes
   where
      process [] = ""
      process (x:xs) =
         printf "%02x" (fromIntegral x :: Int) ++ process xs

-- Turn a hex representation back into a byte string.
unhexify :: String -> B.ByteString
unhexify src =
   B.pack $ process src
   where
      process [] = []
      process (a:b:rest) =
         (digit a `shiftL` 4) + digit b : process rest
      process [_] = error "Hex string has odd number of digits"

      digit = fromIntegral . digit'
      digit' x | x >= '0' && x <= '9' = ord x - ord '0'
               | x >= 'a' && x <= 'f' = ord x - ord 'a' + 10
               | x >= 'A' && x <= 'F' = ord x - ord 'A' + 10
               | otherwise = error "Invalid hex digit"

----------------------------------------------------------------------
-- Low level binding.
type Ctx = CString
foreign import ccall unsafe "openssl/sha.h SHA1_Init"
   c_sha1Init :: Ctx -> IO ()
foreign import ccall unsafe "openssl/sha.h SHA1_Update"
   c_sha1Update :: Ctx -> CString -> Int -> IO ()
foreign import ccall unsafe "openssl/sha.h SHA1_Final"
   c_sha1Final :: Ptr Word8 -> Ctx -> IO ()
