----------------------------------------------------------------------
-- Storage pools.
----------------------------------------------------------------------

module Pool (
   Pool(..)
) where

import Hash
import Chunk

class Pool a where
   poolGetBackups :: a -> IO [Hash]
   poolReadChunk :: a -> Hash -> IO (Maybe Chunk)
