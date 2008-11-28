----------------------------------------------------------------------
-- Hdump pool server protocol.
-- Copyright 2007, David Brown
--
-- This program is free software; you can redistribute it and/or modify it
-- under the terms of the GNU General Public License as published by the
-- Free Software Foundation; either version 2, or (at your option) any later
-- version.
--
-- This program is distributed in the hope that it will be useful, but
-- WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
-- Public License for more details.
--
-- You should have received a copy of the GNU General Public License along
-- with this program; if not, write to the Free Software Foundation, Inc.,
-- 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
--
----------------------------------------------------------------------
--
-- Please ask <hackage@davidb.org> if you are interested in another
-- license.  If pieces of this program are useful in other systems I
-- will be willing to release them under a freer license, but I want
-- the program as a whole to be covered under the GPL.
--
----------------------------------------------------------------------

module Protocol (
   Request(..),
   Reply(..),
   Client(),

   serve,
   send,
   makeClient,
   MoreRequest(..),

   clientClose
) where

import Network
import System.IO
import Control.Monad (when)
import Control.Concurrent
import Control.Concurrent.MVar
import qualified Data.ByteString.Lazy as L
import Data.Binary
import Data.Int (Int64, Int32)
import Hash

versionText :: String
versionText = "Hdump 1.1.3"

data Request
   = ReqHello
   | ReqGoodbye
   | ReqHave Hash
   | ReqSave Hash String L.ByteString Int32
      -- [ReqHave hash kind payload uncomplen]
      -- the uncomplen is '-1' to indicate that the payload is not
      -- compressed, otherwise it is the length when decompressed.
      -- The hash must be the sha1 hash of the concatenation of the
      -- kind and the uncompressed payload.
   | ReqRetrieve Hash
      -- Request a piece of data.  Results in either RepNone, or
      -- RepData.
   | ReqMapping [String]
      -- Given a list of uuids, return a list of their mappings onto
      -- small device id values.
   | ReqPutCache Int Int64 L.ByteString
      -- Store a hunk of data associated with a device and inode
      -- number
   | ReqGetCache Int Int64
      -- Determine if a particular entry is in the cache.
      -- Results in RepCacheHave, or RepNone if not present.
   | ReqBackups
      -- Request a list of backups available.
   deriving (Eq, Show)

data Reply
   = RepHello
   | RepGoodbye
   | RepNone
   | RepHave String Int64 Int64
   | RepDone -- Finished saving data.
   | RepData String L.ByteString Int32
      -- [RepData kind payload uncomplen]
      -- uncomplen has same meaning as it does in ReqSave
   | RepMapping [Int]
      -- Return the mapping given before with the mapping for the same
      -- slots.
   | RepCacheHave L.ByteString
      -- Indicates a cached entry for a ReqGetCache request.
   | RepInvalid
   | RepBackups [Hash]
      -- Returns all of the backups we have accumulated.
   deriving (Eq, Show)

-- Begin listening on the given port number for requests.  For each
-- request received, call process with the request, and return the
-- resultant reply.  Currently, 'serve' never exist.
-- Forks off a thread to handle each client, but only process a single
-- request at a time.
serve :: PortNumber -> (Request -> IO Reply) -> IO ()
serve portNum process = do
   sock <- listenOn (PortNumber portNum)
   mRequest <- newEmptyMVar

   -- forkOS used so that the SQLite queries run in a real OS thread.
   forkIO (processor mRequest)
   wait sock mRequest
   where
      wait sock mRequest = do
         (child, _childHost, _childPort) <- accept sock
         forkIO (childProcess child mRequest)
         wait sock mRequest
      childProcess child mRequest = do
         mReply <- newEmptyMVar
         (seqno, request) <- rawGet child :: IO (Word32, Request)
         -- putStrLn $ "Client request: " ++ show request
         putMVar mRequest (request, mReply)
         reply <- takeMVar mReply
         rawSend child seqno reply

         -- Determine if we loop or exit.
         if reply == RepGoodbye
            then do
               hClose child
               putStrLn $ "Client exit"
            else childProcess child mRequest

      -- Thread that handles the processing requests
      processor mRequest = do
         -- putStrLn $ "Waiting for request"
         (request, mReply) <- takeMVar mRequest
         -- putStrLn $ "Got request: " ++ show request
         reply <- process request
         putMVar mReply reply
         processor mRequest

-- Send a request over a port.  The handle should either be a pipe, or
-- a socket connecting to the server.  Waits for the reply and returns
-- it.  Uses a fixed sequence number.  Not really all that useful.
send :: Handle -> Request -> Word32 -> IO Reply
send conn request seqno = do
   rawSend conn seqno request
   (seqno', reply) <- rawGet conn
   when (seqno /= seqno') $ fail "Sequence mismatch"
   return reply

-- Creates a pair of threads to handle communication with a client.
-- The result is an IO that can queue up requests, calling the given
-- callback with the result.  There is a statically determined limit
-- to the number of outstanding requests, and the caller will block
-- until the queue drains below this point.
data MoreRequest
   = Another (Request, Responder)
   | Finished
type Responder = Reply -> IO MoreRequest
type Client = Request -> Responder -> IO ()

makeClient :: String -> PortNumber -> IO Client
makeClient host port = do
   sock <- connectTo host (PortNumber port)
   helloReply <- send sock ReqHello 1
   when (helloReply /= RepHello) $ fail "Invalid reply to hello"

   let limit = 20  -- Arbitrary limit, not sure what is best.

   qlimit <- newQSemN limit
   writeQueue <- newChan
   readQueue <- newChan

   writeExit <- newEmptyMVar
   readExit <- newEmptyMVar

   let
      -- Writer thread.  Dequeues write requests, and queues up an
      -- expected reply.
      writer seqno = do
         -- putStrLn $ "Writer waiting, " ++ show seqno
         (req, process) <- readChan writeQueue
         -- putStrLn $ "writer: " ++ show req
         rawSend sock seqno req
         writeChan readQueue $ (process, seqno)
         if req == ReqGoodbye
            then putMVar writeExit ()
            else writer (seqno + 1)

      -- Reader thread.  Dequeues read requests, waits for result, and
      -- calls the callback.
      reader = do
         (process, seqno) <- readChan readQueue
         (seqno', reply) <- rawGet sock

         when (seqno /= seqno') $ fail "Sequence mismatch in reply"

         -- If this process wishes to enqueue another request, it can
         -- do so in it's reply.
         newRequest <- process reply
         case newRequest of
            Another (req, process') -> do
               -- It is not valid for the reply here to be a goodbye,
               -- so don't check for it.
               writeChan writeQueue $ (req, process')
            Finished -> signalQSemN qlimit 1
         if reply == RepGoodbye
            then do
               hClose sock
               putMVar readExit ()
            else reader

      requestor :: Request -> Responder -> IO ()
      requestor req process = do
         -- Requesting "goodbye", should request the entire queue, so
         -- make sure that everything has been processed, and we can
         -- safely exit.
         let myLimit = if req == ReqGoodbye then limit else 1
         waitQSemN qlimit myLimit
         writeChan writeQueue $ (req, process)
         when (req == ReqGoodbye) $ do
            takeMVar writeExit
            takeMVar readExit

   forkIO $ writer 2
   forkIO $ reader

   return requestor

-- Convenience functions for some of the simple requests.
clientClose :: Client -> IO ()
clientClose client =
   client ReqGoodbye $ \reply -> do
      when (reply /= RepGoodbye) $ fail "Invalid goodbye reply"
      return Finished

-- Raw send a request over a channel.
rawSend :: (Binary a) => Handle -> Word32 -> a -> IO ()
rawSend conn seqno packet = do
   let binPacket = encode packet
   let header = encode (L.length binPacket)
   L.hPut conn header
   L.hPut conn (encode seqno)
   L.hPut conn binPacket
   hFlush conn

-- Receive a raw request from the channel.
rawGet :: (Binary a) => Handle -> IO (Word32, a)
rawGet conn = do
   bHeader <- L.hGet conn 8
   let header = decode bHeader :: Int
   bSeqno <- L.hGet conn 4
   let seqno = decode bSeqno :: Word32
   binPacket <- L.hGet conn header
   return $ (seqno, decode binPacket)

instance Binary Request where
   put ReqHello = do
      put (1 :: Word8)
      put versionText
   put ReqGoodbye = put (2 :: Word8)
   put (ReqHave hash) = do
      put (3 :: Word8)
      put hash
   put (ReqSave hash kind payload uncomplen) = do
      put (4 :: Word8)
      put hash
      put kind
      put payload
      put uncomplen
   put (ReqRetrieve hash) = do
      put (5 :: Word8)
      put hash
   put (ReqMapping mapping) = do
      put (6 :: Word8)
      put mapping
   put (ReqPutCache dev ino info) = do
      put (7 :: Word8)
      put dev
      put ino
      put info
   put (ReqGetCache dev ino) = do
      put (8 :: Word8)
      put dev
      put ino
   put (ReqBackups) = do
      put (9 :: Word8)

   get = do
      t <- get :: Get Word8
      case t of
         1 -> do
            vers <- get :: Get String
            when (vers /= versionText) $ fail "Incorrect version in reply"
            return ReqHello
         2 -> return ReqGoodbye
         3 -> do
            hash <- get
            return $ ReqHave hash
         4 -> do
            hash <- get
            kind <- get
            payload <- get
            uncomplen <- get
            return $ ReqSave hash kind payload uncomplen
         5 -> do
            hash <- get
            return $ ReqRetrieve hash
         6 -> do
            mapping <- get
            return $ ReqMapping mapping
         7 -> do
            dev <- get
            ino <- get
            info <- get
            return $ ReqPutCache dev ino info
         8 -> do
            dev <- get
            ino <- get
            return $ ReqGetCache dev ino
         9 -> return ReqBackups
         _ -> fail "Unknown request"

instance Binary Reply where
   put RepHello = put (1 :: Word8)
   put RepGoodbye = put (2 :: Word8)
   put RepNone = put (3 :: Word8)
   put (RepHave kind node offset) = do
      put (4 :: Word8)
      put kind
      put node
      put offset
   put RepDone = put (5 :: Word8)
   put (RepData kind payload uncomplen) = do
      put (6 :: Word8)
      put kind
      put payload
      put uncomplen
   put (RepMapping mapping) = do
      put (7 :: Word8)
      put mapping
   put (RepCacheHave info) = do
      put (8 :: Word8)
      put info
   put (RepBackups set) = do
      put (9 :: Word8)
      put set
   put RepInvalid = put (255 :: Word8)

   get = do
      t <- get :: Get Word8
      case t of
         1 -> return RepHello
         2 -> return RepGoodbye
         3 -> return RepNone
         4 -> do
            kind <- get
            node <- get
            offset <- get
            return $ RepHave kind node offset
         5 -> return RepDone
         6 -> do
            kind <- get
            payload <- get
            uncomplen <- get
            return $ RepData kind payload uncomplen
         7 -> do
            mapping <- get
            return $ RepMapping mapping
         8 -> do
            info <- get
            return $ RepCacheHave info
         9 -> do
            set <- get
            return $ RepBackups set
         255 -> return RepInvalid
         _ -> fail $ "Unknown reply, code " ++ show t
