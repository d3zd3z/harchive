----------------------------------------------------------------------
-- Protocol channels.
----------------------------------------------------------------------

module Protocol.Chan (
   PChanWrite, PChanRead, PChanPair,
   makePChan,
   writePChan, readPChan,

   chanServer, chanClient,

   ChanMuxer, emptyMuxer, addMuxerChannel, removeMuxerChannel,
   ChanDemuxer, emptyDemuxer, addDemuxerChannel, removeDemuxerChannel,
   MuxDemux(..),
   killMuxDemux
) where

import Auth
import Server
import Protocol.Packing

import qualified Data.ByteString.Lazy as L
import Control.Concurrent.STM
import Control.Concurrent
import System.IO
import Data.Binary
import Data.Binary.Get
import Data.Binary.Put
import Control.Monad (when, unless)

import qualified Data.IntMap as IntMap
import Data.IntMap (IntMap)

-- To facilitate a richer communication between the various
-- participants in the backup, we implement what are called Protocol
-- Channels.  These are basically typed communication channels
-- (similar to Control.Concurrent.STM.TChan, except that the channels
-- have a queued write limit, and the data may be multiplexed over a
-- socket connection.
--
-- The channel mechanism also allows us to transparently hide where
-- the connections are made.
--
-- Each channel contains a specific kind of item.  It can be useful to
-- wrap it in Maybe to determine the end of a sequence, if this is
-- needed, or this can be one of the values.
--
-- Channels can carry any kind of payload that has a 'Binary'
-- instance.  The read and write ends of a channel can be
-- independently sent over a channel to another party.
--
-- Channels could carry corrupt-typed data if they are not
-- communicated with an instance of the same program on each end of
-- the connection.
----------------------------------------------------------------------
--
-- Each channel has a read and a write end (similar to a Unix pipe),
-- and they are unidirectional.  There is a 'flushed' flag that is
-- cleared by writing, and _not_ cleared by reading.  The read routine
-- will set flushed in the case after it has finished writing the data
-- to the network as long as no new data has been enqueued.

data PChannel a = PChannel {
   pchanVar :: TMVar a,
   pchanFlushed :: TVar Bool }
newtype PChanWrite a = PChanWrite { unWrite :: PChannel a }
newtype PChanRead a = PChanRead { unRead :: PChannel a }
type PChanPair a = (PChanWrite a, PChanRead a)

writePChan :: PChanWrite a -> a -> STM ()
writePChan p val = do
   putTMVar (pchanVar $ unWrite p) val
   writeTVar (pchanFlushed $ unWrite p) False

readPChan :: PChanRead a -> STM a
readPChan = takeTMVar . pchanVar . unRead

readPChanIsEmpty :: PChanRead a -> STM Bool
readPChanIsEmpty = isEmptyTMVar . pchanVar . unRead

makePChan :: STM (PChanPair a)
makePChan = do
   var <- newEmptyTMVar
   flushed <- newTVar True
   let chan = PChannel var flushed
   return $ (PChanWrite chan, PChanRead chan)

----------------------------------------------------------------------

data MuxDemux = MuxDemux {
   chanMuxer :: ChanMuxer,
   chanDemuxer :: ChanDemuxer,
   muxerThread :: ThreadId,
   demuxerThread :: ThreadId,
   muxDemuxHandle :: Handle }

-- Kill the threads associated with a muxer/demuxer.
killMuxDemux :: MuxDemux -> IO ()
killMuxDemux muxd = do
   killThread $ demuxerThread muxd
   killThread $ muxerThread muxd
   hClose $ muxDemuxHandle muxd

data ChannelMessage = ChannelMessage PackedInt L.ByteString
   deriving ({-! Binary !-})

-- The muxer contains a bunch of channel numbers and the Read part of
-- the channel, and encodes the messages over a single handle.

-- (Uses PolymorphicComponents)
data MuxReader = MuxReader {
  readFromMuxReader :: STM L.ByteString,
  isEmptyMuxReader :: STM Bool,
  flushedMuxReader :: TVar Bool }
type ChanMuxer = TVar (IntMap MuxReader)

-- Construct a read from a given channel.
makeMuxReader :: Binary a => PChanRead a -> MuxReader
makeMuxReader chan =
   MuxReader {
      readFromMuxReader = do
         msg <- readPChan chan
         return $ encode msg,
      isEmptyMuxReader = readPChanIsEmpty chan,
      flushedMuxReader = pchanFlushed $ unRead chan }

emptyMuxer :: STM ChanMuxer
emptyMuxer = newTVar (IntMap.empty)

addMuxerChannel :: Binary a => Int -> PChanRead a -> ChanMuxer -> STM ()
addMuxerChannel channel rchan mux = do
   old <- readTVar mux
   writeTVar mux $ IntMap.insert (fromIntegral channel) (makeMuxReader rchan) old

-- Remove the muxer channel.  Waits until the message has been
-- dequeued to be sent.  Note that if another thread is still
-- enqueuing messages, we may retry continually.
removeMuxerChannel :: Int -> ChanMuxer -> STM ()
removeMuxerChannel channel mux = do
   -- Simple version that doesn't wait (which we can't do yet).
   old <- readTVar mux
   let chan = IntMap.lookup channel old
   -- TODO: This can be expressed better.
   case chan of
      Nothing -> return ()
      Just ch -> do
         emptyp <- isEmptyMuxReader ch
         flushed <- readTVar $ flushedMuxReader ch
         unless (emptyp && flushed) retry
   writeTVar mux $ IntMap.delete channel old

-- Read messages from all of the appropriate channels taking the
-- messages and sending them over handle.
runMuxer :: ChanMuxer -> Handle -> IO ()
runMuxer mux han = do
   (channel, message, reader) <- atomically $ do
      mr <- readTVar mux
      foldl orElse retry $ map (tryRead) $ IntMap.toList mr
   let payload = encode $ ChannelMessage (PackedInt $ fromIntegral channel) message
   L.hPut han $ encode (fromIntegral $ L.length payload :: Word32)
   L.hPut han payload
   hFlush han

   -- Mark it as flushed if no interveining message has been enqueued.
   atomically $ do
      empty <- isEmptyMuxReader reader
      when empty $ writeTVar (flushedMuxReader reader) True

   runMuxer mux han
   where
      tryRead :: (IntMap.Key, MuxReader) -> STM (IntMap.Key, L.ByteString, MuxReader)
      tryRead (index, reader) = do
         msg <- readFromMuxReader reader
         return $ (index, msg, reader)

----------------------------------------------------------------------

-- The demuxer is a little more challenging.  Read channels must be
-- allocated and registered before any messages come in.

type DemuxWriter = L.ByteString -> STM ()
type ChanDemuxer = TVar (IntMap DemuxWriter)

emptyDemuxer :: STM ChanDemuxer
emptyDemuxer = newTVar (IntMap.empty)

addDemuxerChannel :: Binary a => Int -> PChanWrite a -> ChanDemuxer -> STM ()
addDemuxerChannel channel wchan mux = do
   old <- readTVar mux
   writeTVar mux $ IntMap.insert (fromIntegral channel) (makeDemuxWriter wchan) old

removeDemuxerChannel :: Int -> ChanDemuxer -> STM ()
removeDemuxerChannel channel mux = do
   old <- readTVar mux
   writeTVar mux $ IntMap.delete channel old

-- Construct a writer from a given channel.
makeDemuxWriter :: Binary a => PChanWrite a -> DemuxWriter
makeDemuxWriter chan msg = writePChan chan $ decode msg

-- Read messages from the handle, sending it to each appropriate
-- channel.
runDemuxer :: ChanDemuxer -> Handle -> IO ()
runDemuxer mux handle = do
   bHeader <- L.hGet handle 4
   packed <- L.hGet handle $ fromIntegral (decode bHeader :: Word32)
   case decode packed of
      ChannelMessage chan payload -> do
         wchan <- atomically $ do
            m <- readTVar mux
            return $ IntMap.lookup (unpackInt chan) m 
         maybe (warn $ "Unexpected channel: " ++ show chan)
            (\writer -> atomically $ writer payload)
            wchan
   runDemuxer mux handle
   where
      warn msg = hPutStrLn stderr $ "Warning: " ++ msg

----------------------------------------------------------------------

-- A function to lookup the secret for communication with a particular
-- peer.  Should return Nothing if the peer is not recognized, or Just
-- and the expected secret.
type SecretGetter = UUID -> IO (Maybe String)

-- Establish a sever listening on the given port.  For each new
-- connection, 'setupChannels' will be called with the muxer/demuxer
-- to allow the user of the server an opportunity to setup the server
-- channels.
chanServer :: Int -> UUID -> SecretGetter -> (MuxDemux -> IO ()) -> IO ()
chanServer port serverUuid getSecret setupChannels = do
   serve port $ \handle -> do
      secret <- idExchange handle serverUuid getSecret "server" "client"
      auth <- authInitiator secret
      valid <- runAuthIO handle handle auth
      putStrLn $ "valid: " ++ show valid

      muxer <- atomically emptyMuxer
      demuxer <- atomically emptyDemuxer
      muxId <- forkIO $ runMuxer muxer handle
      myId <- myThreadId
      setupChannels $ MuxDemux {
         chanMuxer = muxer,
         chanDemuxer = demuxer,
         muxerThread = muxId,
         demuxerThread = myId,
         muxDemuxHandle = handle }

      -- Become the demuxer.
      runDemuxer demuxer handle

-- Retrieve the secret for this peer.  Generates a fake secret if the
-- peer is unknown.
idExchange :: Handle -> UUID -> SecretGetter -> String -> String -> IO String
idExchange handle selfUuid getSecret selfName peerName = do
   hPutStrLn handle $ unwords [selfName, selfUuid]
   hFlush handle
   resp <- hGetLineSafe 128 handle
   case words resp of
      [n, peerUuid] | n == peerName -> do
         secret <- getSecret peerUuid
         maybe genNonce return secret
      _ -> fail "Invalid peer response"

-- Create a client.  Returns the MuxDemux with no active channels.
-- Forks two threads, one for muxing and one for demuxing.  Returns
-- once the communication has been authenticated.
chanClient :: String -> Int -> UUID -> SecretGetter -> IO MuxDemux
chanClient host port clientUuid getSecret = do
   client host port $ \handle -> do
      secret <- idExchange handle clientUuid getSecret "client" "server"
      auth <- authRecipient secret
      valid <- runAuthIO handle handle auth
      putStrLn $ "valid: " ++ show valid

      -- Construct the empty muxer, demuxer, and start the worker
      -- threads.
      muxer <- atomically emptyMuxer
      demuxer <- atomically emptyDemuxer
      muxId <- forkIO $ runMuxer muxer handle
      demuxId <- forkIO $ runDemuxer demuxer handle

      return $ MuxDemux {
         chanMuxer = muxer,
         chanDemuxer = demuxer,
         muxerThread = muxId,
         demuxerThread = demuxId,
         muxDemuxHandle = handle }

----------------------------------------------------------------------

-- GRRR, the Derive instances generate warnings.
-- It also isn't the best encoding.

instance Binary ChannelMessage where
   put (ChannelMessage index payload) = do
      return ()
      put index
      putPBInt $ L.length payload
      putLazyByteString payload

   get = do
      index <- get
      len <- getPBInt
      payload <- getLazyByteString len
      return $ ChannelMessage index payload
