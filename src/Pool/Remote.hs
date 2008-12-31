----------------------------------------------------------------------
-- Remote pools.
----------------------------------------------------------------------

module Pool.Remote (
   RemotePool,
   withRemotePool
) where

import Auth
import Hash
import Pool
import Protocol.Chan
import Protocol.Control
import Protocol.Messages

import Control.Monad (liftM)
import Control.Concurrent.STM
import System.IO

data RemotePool = RemotePool {
   rpUuid :: UUID,
   rpMuxd :: MuxDemux,
   rpControl :: PChanWrite ControlMessage,
   rpPoolCommand :: PChanWrite PoolCommandMessage }

instance ChunkQuerier RemotePool where
   poolGetBackups = getBackups
   poolChunkKind = undefined
   poolHashPresent = undefined
   poolGetUuid = return . rpUuid

-- Open a connection to the peer described by 'peer', and connect to
-- the pool described by 'nick', and call 'action' on the remote pool.
withRemotePool :: ChanPeer -> String -> (RemotePool-> IO a) -> IO a
withRemotePool peer nick action = do
   muxd <- chanClient peer
   control <- makeClientControl muxd
   pools <- getPools muxd control
   result <- case lookup nick pools of
      Nothing -> do
         hPutStrLn stderr $ "Pool with nick " ++ nick ++
            " does not exist on server"
         fail "Unable to connect to pool"
      Just poolUuid -> do
         putStrLn $ "Using pool uuid: " ++ poolUuid
         withWriteChannel muxd PoolCommandChannel $ \poolCommand -> do
            atomically $ writePChan control $ ControlOpenPool poolUuid
            let rp = RemotePool {
               rpUuid = poolUuid,
               rpMuxd = muxd,
               rpControl = control,
               rpPoolCommand = poolCommand }
            action rp

   atomically $ writePChan control ControlGoodbye
   deregisterWriteChannel muxd ClientControlChannel
   return result

   -- TODO: There is a race with this kill.  The goodbye message
   -- might be received, and the socket closed before we have a
   -- chance to kill the receiver thread.  Killing this causes a
   -- message about a short-read exception in the demuxer thread.
   -- If we just exit, _often_ all the whole program will have a
   -- chance to exit before this can be printed, but it isn't
   -- reliable.  The proper fix is likely to detect the closed
   -- socket and clean up everything nicely.
   -- killMuxDemux muxd

getPools :: MuxDemux -> PChanWrite ControlMessage -> IO [(String, String)]
getPools muxd control = do
   withReadChannel muxd PoolListingChannel $ \poolChan -> do
      atomically $ writePChan control ControlListPools
      liftM (map pairList) $ pullMaybes poolChan

pairList :: PoolNodeMessage -> (String, String)
pairList (PoolNodeMessage nick uuid) = (nick, uuid)

----------------------------------------------------------------------

getBackups :: RemotePool -> IO [Hash]
getBackups rp = do
   withReadChannel (rpMuxd rp) PoolBackupListingChannel $ \listChan -> do
      atomically $ writePChan (rpPoolCommand rp) $ PoolCommandListBackups
      atomically $ readPChan listChan
