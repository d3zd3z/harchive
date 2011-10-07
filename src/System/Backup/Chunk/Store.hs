{-# LANGUAGE Rank2Types #-}
-- Some class to represent objects that can store and retrieve chunks
-- by their hash.

module System.Backup.Chunk.Store (
   ChunkSource(..),
   ChunkStore(..),
   Store(..)
) where

import Hash
import System.Backup.Chunk

-- A ChunkSource is something where chunks can be looked up.
class ChunkSource a where

   -- Look up the given hash in the ChunkSource, returning it if
   -- present.
   lookup :: Hash.Hash -> a -> IO (Maybe Chunk)

   -- Get a list of backup chunks.
   getBackups :: a -> IO [Hash.Hash]

-- A ChunkStore is something that can also have chunks added to it.
class ChunkSource a => ChunkStore a where

   -- Flush any pending writes in the store.
   flush :: a -> IO ()

   -- Add a new chunk to this store.  The chunk should not already be
   -- present.
   insert :: a -> Chunk -> IO ()

-- Handy Rank2 store to avoid littering the code with Rank2Types.
newtype Store = Store {
   unStore :: ChunkStore a => a }
