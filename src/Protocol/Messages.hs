----------------------------------------------------------------------
-- Channel assignments
----------------------------------------------------------------------

module Protocol.Messages (
   ChannelAssignment(..),
   ControlMessage(..),
   PoolListingMessage, PoolNodeMessage(..),

   registerReadChannel, registerWriteChannel
) where

import Auth
import Protocol.Chan
import Protocol.Packing

import Control.Concurrent.STM
import Data.Binary
import Data.Binary.Put
import Data.Binary.Get

import Control.Monad (liftM2)

----------------------------------------------------------------------

-- Register a channel that we will read from.
registerReadChannel :: (Binary a) => MuxDemux -> ChannelAssignment -> IO (PChanRead a)
registerReadChannel muxd chan = do
   (wChan, rChan) <- atomically makePChan
   atomically $ addDemuxerChannel (fromEnum chan) wChan (chanDemuxer muxd)
   return rChan

-- Register a channel that we will write to.
registerWriteChannel :: (Binary a) => MuxDemux -> ChannelAssignment -> IO (PChanWrite a)
registerWriteChannel muxd chan = do
   (wChan, rChan) <- atomically makePChan
   atomically $ addMuxerChannel (fromEnum chan) rChan (chanMuxer muxd)
   return wChan

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
   | ControlListPools
   deriving (Show)

instance Binary ControlMessage where
   put ControlShutdownServer = putWord8 0
   put ControlHello = putWord8 1
   put ControlListPools = putWord8 2
   get = getWord8 >>= \tag -> case tag of
      0 -> return ControlShutdownServer
      1 -> return ControlHello
      2 -> return ControlListPools
      _ -> fail "Invalid ControlMessage encoding"

type PoolListingMessage = Maybe PoolNodeMessage
data PoolNodeMessage = PoolNodeMessage {
   poolNodeNick :: String,
   poolNodeUuid :: UUID }
   deriving (Show)

instance Binary PoolNodeMessage where
   put (PoolNodeMessage nick uuid) = putString nick >> putString uuid
   get = liftM2 PoolNodeMessage getString getString
