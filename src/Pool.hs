----------------------------------------------------------------------
-- Storage pools.
----------------------------------------------------------------------

module Pool (
   Pool(..)
) where

import Hash
import Chunk

data Pool = Pool {
   poolGetBackups :: IO [Hash],
   poolReadChunk :: Hash -> IO (Maybe Chunk) }
