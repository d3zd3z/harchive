----------------------------------------------------------------------
-- Channel assignments
----------------------------------------------------------------------

module Protocol.Messages (
   ChannelAssignment(..),
   ControlMessage(..),
   PoolListingMessage, PoolNodeMessage(..),

   registerReadChannel, registerWriteChannel,
   deregisterReadChannel, deregisterWriteChannel,
   withReadChannel
) where

import Auth
import Protocol.Chan
import Protocol.Packing

import Control.Concurrent.STM
import Data.Binary
import Data.Binary.Put
import Data.Binary.Get

import qualified Control.Exception as E
import Control.Monad (liftM2)

----------------------------------------------------------------------

-- Register a channel that we will read from.
registerReadChannel :: (Binary a) => MuxDemux -> ChannelAssignment -> IO (PChanRead a)
registerReadChannel muxd chan = do
   (wChan, rChan) <- atomically makePChan
   atomically $ addDemuxerChannel (fromEnum chan) wChan (chanDemuxer muxd)
   return rChan

-- Deregister a read channel.
deregisterReadChannel :: MuxDemux -> ChannelAssignment -> IO ()
deregisterReadChannel muxd chan = do
   atomically $ removeDemuxerChannel (fromEnum chan) (chanDemuxer muxd)

-- Register a channel that we will write to.
registerWriteChannel :: (Binary a) => MuxDemux -> ChannelAssignment -> IO (PChanWrite a)
registerWriteChannel muxd chan = do
   (wChan, rChan) <- atomically makePChan
   atomically $ addMuxerChannel (fromEnum chan) rChan (chanMuxer muxd)
   return wChan

-- Deregister a write channel.
deregisterWriteChannel :: MuxDemux -> ChannelAssignment -> IO ()
deregisterWriteChannel muxd chan = do
   atomically $ removeMuxerChannel (fromEnum chan) (chanMuxer muxd)

-- Convenience handlers for these.
withReadChannel :: (Binary a) => MuxDemux -> ChannelAssignment ->
   (PChanRead a -> IO ()) -> IO ()
withReadChannel muxd chan action = do
   c <- registerReadChannel muxd chan
   E.finally (action c) (deregisterReadChannel muxd chan)

----------------------------------------------------------------------

data ChannelAssignment
   -- Channels from client to pool.
   = ClientControlChannel

   -- Channels from pool to client.
   | PoolListingChannel
   deriving (Show, Eq, Enum)

data ControlMessage
   = ControlShutdownServer
   | ControlHello
   | ControlGoodbye
   | ControlListPools
   deriving (Show)

instance Binary ControlMessage where
   put ControlShutdownServer = putWord8 0
   put ControlHello = putWord8 1
   put ControlGoodbye = putWord8 2
   put ControlListPools = putWord8 3
   get = getWord8 >>= \tag -> case tag of
      0 -> return ControlShutdownServer
      1 -> return ControlHello
      2 -> return ControlGoodbye
      3 -> return ControlListPools
      _ -> fail "Invalid ControlMessage encoding"

type PoolListingMessage = Maybe PoolNodeMessage
data PoolNodeMessage = PoolNodeMessage {
   poolNodeNick :: String,
   poolNodeUuid :: UUID }
   deriving (Show)

instance Binary PoolNodeMessage where
   put (PoolNodeMessage nick uuid) = putString nick >> putString uuid
   get = liftM2 PoolNodeMessage getString getString
