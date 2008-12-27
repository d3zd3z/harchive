----------------------------------------------------------------------
-- The control channel is the first channel initially opened.  It
-- accepts a small set of messages that determine what operations the
-- server should perform.  All other results will be returned on other
-- channels.

module Protocol.Control (
   setupControlChannel, makeClientControl,
   sendHello, sendShutdown
) where

import Protocol.Chan

import Control.Concurrent
import Control.Concurrent.STM
import Data.Binary
import Data.Binary.Put
import Data.Binary.Get

-- Create the initial control channel message, and fork a thread to
-- listen to it's requests.
setupControlChannel :: MuxDemux -> ThreadId -> IO ()
setupControlChannel muxd rootThread = do
   (wChan, rChan) <- atomically makePChan
   atomically $ addDemuxerChannel 1 wChan (chanDemuxer muxd)
   forkIO $ controlLoop rChan rootThread
   return ()

controlLoop :: PChanRead ControlMessage -> ThreadId -> IO ()
controlLoop rChan rootThread = do
   message <- atomically $ readPChan rChan
   putStrLn $ "Message received: " ++ show message
   case message of
      ControlHello -> return ()
      ControlShutdownServer -> killThread rootThread
   controlLoop rChan rootThread

makeClientControl :: MuxDemux -> IO (PChanWrite ControlMessage)
makeClientControl muxd = do
   (wChan, rChan) <- atomically makePChan
   atomically $ addMuxerChannel 1 rChan (chanMuxer muxd)
   return wChan

sendHello :: PChanWrite ControlMessage -> IO ()
sendHello wChan = do
   atomically $ writePChan wChan ControlHello

-- This needs to wait for a response.
sendShutdown :: PChanWrite ControlMessage -> IO ()
sendShutdown wChan = do
   atomically $ writePChan wChan ControlShutdownServer

data ControlMessage
   = ControlShutdownServer
   | ControlHello
   deriving (Show)

instance Binary ControlMessage where
   put ControlShutdownServer = putWord8 0
   put ControlHello = putWord8 1
   get = getWord8 >>= \tag -> case tag of
      0 -> return ControlShutdownServer
      1 -> return ControlHello
      _ -> fail "Invalid ControlMessage encoding"
