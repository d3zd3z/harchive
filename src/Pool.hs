----------------------------------------------------------------------
-- Storage pools.
----------------------------------------------------------------------

module Pool (
   ChunkQuerier(..),
   ChunkReader(..),
   ChunkWriter(..),
   ChunkReaderWriter,
   emptyPool
) where

import Hash
import Chunk

-- |Something that can get information about a store of chunks, but not the
-- data payload.
class ChunkQuerier a where
   poolGetBackups :: a -> IO [Hash]
   poolChunkKind :: a -> Hash -> IO (Maybe String)
   poolHashPresent :: a -> Hash -> IO Bool

   poolHashPresent p hash = do
      info <- poolChunkKind p hash
      return $ maybe False (\_ -> True) info

-- |Something that can read the payload of the chunks in the store.
class (ChunkQuerier a) => ChunkReader a where
   poolReadChunk :: a -> Hash -> IO (Maybe Chunk)

-- |A class that can perform both reading and writing.  I don't
-- believe this is valid Haskell98, but works without enabling any
-- extensions in GHC.
class (ChunkReader a, ChunkWriter a) => ChunkReaderWriter a where {}

-- |Something that can store chunks.
class (ChunkQuerier a) => ChunkWriter a where
   -- |Write a single chunk to the storage pool.  It is safe to write
   -- a chunk that might already be present in the pool.
   poolWriteChunk :: a -> Chunk -> IO ()

newtype EmptyPool = EmptyPool ()

instance ChunkQuerier EmptyPool where
   poolGetBackups _ = return []
   poolChunkKind _ _ = return Nothing

instance ChunkReader EmptyPool where
   poolReadChunk _ _ = return Nothing

emptyPool :: EmptyPool
emptyPool = EmptyPool ()
