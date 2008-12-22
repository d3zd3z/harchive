----------------------------------------------------------------------
-- Marshaling for Chunk data.
----------------------------------------------------------------------

module Protocol.Chunk (
   -- instance Binary Chunk
) where

import Chunk
import Protocol.Packing

import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Char8 as BC (pack, unpack)
import Control.Monad (liftM)

-- The encoding is similar to the file encoding, but uses the packed
-- integers for size, and has no magic field.  Also, the hash is not
-- transferred, since it is usually not used.
instance Binary Chunk where
   put chunk = do
      putByteString (BC.pack $ chunkKind chunk)
      case chunkZData chunk of
	 Nothing -> do
	    putPBInt (0::Int)
	    putPBInt $ chunkLength chunk
	    putLazyByteString $ chunkData chunk
	 Just zdata -> do
	    putPBInt (1::Int)
	    putPBInt $ chunkLength chunk
	    putPBInt $ L.length zdata
	    putLazyByteString zdata

   get = do
      kind <- liftM BC.unpack $ getByteString 4
      code <- getPBInt :: Get Int
      case code of
	 0 -> do
	    len <- getPBInt
	    payload <- getLazyByteString len
	    return $ byteStringToChunk kind payload
	 1 -> do
	    len <- getPBInt
	    zlen <- getPBInt
	    zData <- getLazyByteString zlen
	    return $ zDataToChunk kind zData len
	 _ -> error $ "Invalid stream byte: " ++ show code

