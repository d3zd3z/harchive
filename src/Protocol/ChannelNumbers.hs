----------------------------------------------------------------------
-- Channel assignments
----------------------------------------------------------------------

module Protocol.ChannelNumbers (
   ChannelAssignments(..),
   ControlMessage(..),
   PoolListingMessage, PoolNodeMessage(..)
) where

import Auth
import Protocol.Packing

import Data.Binary
import Data.Binary.Put
import Data.Binary.Get

import Control.Monad (liftM2)

data ChannelAssignments
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
