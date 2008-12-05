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
   poolChunkKind :: Hash -> IO (Maybe String),
   poolReadChunk :: Hash -> IO (Maybe Chunk) }
