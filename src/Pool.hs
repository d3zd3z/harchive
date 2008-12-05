----------------------------------------------------------------------
-- Storage pools.
----------------------------------------------------------------------

module Pool (
   Pool(..),
   emptyPool
) where

import Hash
import Chunk

data Pool = Pool {
   poolGetBackups :: IO [Hash],
   poolChunkKind :: Hash -> IO (Maybe String),
   poolReadChunk :: Hash -> IO (Maybe Chunk),
   poolHas :: Hash -> IO Bool }

emptyPool :: Pool
-- A pool that has no data.
emptyPool = Pool {
   poolGetBackups = return [],
   poolChunkKind = \_ -> return Nothing,
   poolReadChunk = \_ -> return Nothing,
   poolHas = \_ -> return False }
