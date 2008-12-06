----------------------------------------------------------------------
-- Storage pools.
----------------------------------------------------------------------

module Pool (
   ChunkQuerier(..),
   ChunkReader(..),
   ChunkWriter(..),
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

-- |Something that can store chunks.
class (ChunkQuerier a) => ChunkWriter a where
   poolWriteChunk :: a -> Chunk -> IO ()

newtype EmptyPool = EmptyPool ()

instance ChunkQuerier EmptyPool where
   poolGetBackups _ = return []
   poolChunkKind _ _ = return Nothing

instance ChunkReader EmptyPool where
   poolReadChunk _ _ = return Nothing

emptyPool :: EmptyPool
emptyPool = EmptyPool ()
