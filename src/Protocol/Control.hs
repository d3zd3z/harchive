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
import Protocol.Messages

import Control.Concurrent
import Control.Concurrent.STM

type ControlHandler = ControlMessage -> IO ()

-- Create the initial control channel message, and fork a thread to
-- listen to it's requests.  The requests themselves are processed by
-- the handler.
setupControlChannel :: MuxDemux -> ControlHandler -> IO ()
setupControlChannel muxd handler = do
   rChan <- registerReadChannel muxd ClientControlChannel
   forkIO $ controlLoop rChan handler
   return ()

controlLoop :: PChanRead ControlMessage -> ControlHandler -> IO ()
controlLoop rChan handler = do
   message <- atomically $ readPChan rChan
   putStrLn $ "Message received: " ++ show message
   handler message
   controlLoop rChan handler

makeClientControl :: MuxDemux -> IO (PChanWrite ControlMessage)
makeClientControl muxd = do
   registerWriteChannel muxd ClientControlChannel

sendHello :: PChanWrite ControlMessage -> IO ()
sendHello wChan = do
   atomically $ writePChan wChan ControlHello

-- This needs to wait for a response.
sendShutdown :: PChanWrite ControlMessage -> IO ()
sendShutdown wChan = do
   atomically $ writePChan wChan ControlShutdownServer
