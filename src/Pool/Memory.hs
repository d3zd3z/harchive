----------------------------------------------------------------------
-- Memory resident pools.
----------------------------------------------------------------------
-- Useful for testing and such.

module Pool.Memory (
   withMemoryPool,
   MemoryPool,
   module Pool
) where

import Auth
import Chunk
import Hash
import Pool

import Data.Map (Map)
import qualified Data.Map as Map

import Control.Concurrent
import Control.Monad.State.Strict

newtype MemoryPool = MemoryPool { thePool :: MVar MemoryState }

withMemoryPool :: (MemoryPool -> IO a) -> IO a
withMemoryPool action = do
   uuid <- getUuid
   let state0 = MemoryState Map.empty uuid
   pool <- newMVar state0
   action $ MemoryPool pool

data MemoryState = MemoryState {
   chunks :: Map Hash Chunk,
   myUuid :: String }

type AtomicPoolOp a = StateT MemoryState IO a

atomicLift :: MemoryPool -> AtomicPoolOp a -> IO a
-- TODO: Centralize this definition, since it is the same as the one
-- in Pool.Local.
atomicLift pool action = do
   modifyMVar (thePool pool) $ \s -> do
      (x, s') <- runStateT action s
      return (s', x)

instance ChunkQuerier MemoryPool where

   poolGetBackups pool = atomicLift pool $ do
      cs <- gets chunks
      return $ Map.keys cs

   poolChunkKind pool hash = liftM (fmap chunkKind) $ poolReadChunk pool hash

   poolGetUuid pool = atomicLift pool $ do
      gets myUuid

instance ChunkReader MemoryPool where

   poolReadChunk pool hash = atomicLift pool $ do
      cs <- gets chunks
      return $ Map.lookup hash cs

instance ChunkWriter MemoryPool where

   poolWriteChunk pool chunk = atomicLift pool $ do
      state <- get
      let cs = chunks state
      let hash = chunkHash chunk
      -- We assume that if the hash is the same, then the chunk is as
      -- well.
      let cs' = Map.insert hash chunk cs
      put $ state { chunks = cs' }

   poolFlush _pool = return ()

instance ChunkReaderWriter MemoryPool where {}
