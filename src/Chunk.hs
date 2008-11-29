----------------------------------------------------------------------
-- Chunk manipulation
-- Copyright 2008, David Brown
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
-- Chunks are the fundamental unit of storage.  Each chunk has a
-- 4-character 'kind' (called a 'type' in some implementations), and 0
-- or more bytes of payload.  These chunks can be communicated and
-- stored in a compressed format.  Chunks are addressed by a unique
-- SHA1-hash of their kind and payload.
--
----------------------------------------------------------------------

module Chunk (
   Chunk,
   chunkKind, chunkData, chunkZData, chunkLength, chunkHash,

   byteStringToChunk, byteStringToChunkH,
   zDataToChunk, zDataToChunkH,

   stringToChunk
) where

import qualified Data.ByteString.Lazy as L
-- import qualified Data.ByteString as B
import Data.Maybe (fromMaybe)

-- We use the non-raw compression format, even though it takes more
-- space.  The raw version is not documented, so many language
-- bindings do not have the ability to access this format.
import qualified Codec.Compression.Zlib as Zlib
import Hash

data Chunk = Chunk {
   chunkKind :: String,
   chunkData :: L.ByteString,
   chunkLength :: Int,
   chunkZData :: Maybe L.ByteString,
   chunkHash :: Hash }

-- Constructing a chunk out of known uncompressed data.  If the hash
-- is known, it should be passed in.
byteStringToChunkH :: String -> L.ByteString -> Maybe Hash -> Chunk
byteStringToChunkH kind payload mHash =
   Chunk {
      chunkKind = kind,
      chunkData = payload,
      chunkLength = fromIntegral . L.length $ payload,
      chunkZData = tryCompress payload,
      chunkHash = fromMaybe (kpHash kind payload) mHash }

byteStringToChunk :: String -> L.ByteString -> Chunk
byteStringToChunk kind payload = byteStringToChunkH kind payload Nothing

-- Construct a chunk out of compressed data.  Compression always keeps
-- around the uncompressed length.
zDataToChunkH :: String -> L.ByteString -> Int -> Maybe Hash -> Chunk
zDataToChunkH kind payload len mHash =
   Chunk {
      chunkKind = kind,
      chunkData = unpayload,
      chunkLength = len,
      chunkZData = Just payload,
      chunkHash = fromMaybe (kpHash kind unpayload) mHash }
   where
      unpayload = Zlib.decompress payload

zDataToChunk :: String -> L.ByteString -> Int -> Chunk
zDataToChunk kind payload len = zDataToChunkH kind payload len Nothing

stringToChunk :: String -> String -> Chunk
stringToChunk kind payload =
   Chunk {
      chunkKind = kind,
      chunkData = rawPayload,
      chunkLength = length payload,
      chunkZData = tryCompress rawPayload,
      chunkHash = kpHash kind rawPayload }
   where
      rawPayload = stringToLazyByteString payload

stringToLazyByteString :: String -> L.ByteString
stringToLazyByteString = L.pack . (map $ fromIntegral . fromEnum)

kpHash :: String -> L.ByteString -> Hash
kpHash kind payload = hashOf (stringToLazyByteString kind `L.append` payload)

-- Try compressing the payload, returning Just the compressed data, or
-- Nothing, if the compressed data is larger than the uncompressed.
tryCompress :: L.ByteString -> Maybe L.ByteString
tryCompress payload =
   if L.length zpayload >= L.length payload
      then Nothing
      else Just zpayload
   where zpayload = Zlib.compressWith (Zlib.CompressionLevel 3) payload
