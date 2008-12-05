----------------------------------------------------------------------
-- Storage pools.
----------------------------------------------------------------------

module Pool (
   Pool(..),
   emptyPool
) where

import Hash
import Chunk

class Pool a where
   poolGetBackups :: a -> IO [Hash]
   poolChunkKind :: a -> Hash -> IO (Maybe String)
   poolReadChunk :: a -> Hash -> IO (Maybe Chunk)
   poolHash :: a -> Hash -> IO Bool

   poolHash p hash = do
      info <- poolChunkKind p hash
      return $ maybe False (\_ -> True) info

newtype EmptyPool = EmptyPool ()

instance Pool EmptyPool where
   poolGetBackups _ = return []
   poolChunkKind _ _ = return Nothing
   poolReadChunk _ _ = return Nothing

emptyPool :: EmptyPool
emptyPool = EmptyPool ()
