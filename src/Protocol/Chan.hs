----------------------------------------------------------------------
-- Protocol channels.
----------------------------------------------------------------------

module Protocol.Chan (
   PChanWrite, PChanRead, PChanPair,
   makePChan,
   writePChan, readPChan,

   chanServer, chanClient,

   emptyMuxerIO
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
-- and they are unidirectional.

newtype PChanWrite a = PChanWrite { unWrite :: TMVar a }
newtype PChanRead a = PChanRead { unRead :: TMVar a }
type PChanPair a = (PChanWrite a, PChanRead a)

writePChan :: PChanWrite a -> a -> STM ()
writePChan p = putTMVar (unWrite p)

readPChan :: PChanRead a -> STM a
readPChan p = takeTMVar (unRead p)

makePChan :: STM (PChanPair a)
makePChan = do
   t <- newEmptyTMVar
   return $ (PChanWrite t, PChanRead t)

----------------------------------------------------------------------

data ChannelMessage = ChannelMessage PackedInt L.ByteString
   deriving ({-! Binary !-})

-- The muxer contains a bunch of channel numbers and the Read part of
-- the channel, and encodes the messages over a single handle.

-- (Uses PolymorphicComponents)
type MuxReader = STM L.ByteString
type ChanMuxer = TVar (IntMap MuxReader)

-- Construct a read from a given channel.
makeMuxReader :: Binary a => PChanRead a -> MuxReader
makeMuxReader chan = do
   msg <- readPChan chan
   return $ encode msg

emptyMuxerIO :: IO ChanMuxer
emptyMuxerIO = newTVarIO (IntMap.empty)

emptyMuxer :: STM ChanMuxer
emptyMuxer = newTVar (IntMap.empty)

addMuxerChannel :: Binary a => Int -> PChanRead a -> ChanMuxer -> STM ()
addMuxerChannel channel rchan mux = do
   old <- readTVar mux
   writeTVar mux $ IntMap.insert (fromIntegral channel) (makeMuxReader rchan) old

-- Read messages from all of the appropriate channels taking the
-- messages and sending them over handle.
runMuxer :: ChanMuxer -> Handle -> IO ()
runMuxer mux han = do
   (channel, message) <- atomically $ do
      mr <- readTVar mux
      foldl orElse retry $ map (tryRead) $ IntMap.toList mr
   let payload = encode $ ChannelMessage (PackedInt $ fromIntegral channel) message
   L.hPut han $ encode (fromIntegral $ L.length payload :: Word32)
   L.hPut han payload
   hFlush han
   runMuxer mux han
   where
      tryRead :: (IntMap.Key, MuxReader) -> STM (IntMap.Key, L.ByteString)
      tryRead (index, reader) = do
         msg <- reader
         return $ (index, msg)

----------------------------------------------------------------------

type DemuxWriter = L.ByteString -> STM ()
type ChanDemuxer = TVar (IntMap DemuxWriter)

emptyDemuxer :: STM ChanDemuxer
emptyDemuxer = newTVar (IntMap.empty)

addDemuxerChannel :: Binary a => Int -> PChanWrite a -> ChanDemuxer -> STM ()
addDemuxerChannel channel wchan mux = do
   old <- readTVar mux
   writeTVar mux $ IntMap.insert (fromIntegral channel) (makeDemuxWriter wchan) old

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

chanServer :: Int -> UUID -> SecretGetter -> IO ()
chanServer port serverUuid getSecret = do
   serve port $ \handle -> do
      secret <- idExchange handle serverUuid getSecret "server" "client"
      auth <- authInitiator secret
      valid <- runAuthIO handle handle auth
      putStrLn $ "valid: " ++ show valid

      -- Simple muxer, with simple control channel and message.
      (cWrite, cRead) <- atomically makePChan
      mux <- atomically emptyMuxer
      atomically $ addMuxerChannel 1 cRead mux
      atomically $ writePChan cWrite $ PackedString "Hello world"
      runMuxer mux handle

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

chanClient :: String -> Int -> UUID -> SecretGetter -> IO ()
chanClient host port clientUuid getSecret = do
   client host port $ \handle -> do
      secret <- idExchange handle clientUuid getSecret "client" "server"
      auth <- authRecipient secret
      valid <- runAuthIO handle handle auth
      putStrLn $ "valid: " ++ show valid

      (cWrite, cRead) <- atomically makePChan
      mux <- atomically emptyDemuxer
      atomically $ addDemuxerChannel 1 cWrite mux
      forkIO $ runDemuxer mux handle

      -- Wait for control messages and print them.
      loop cRead
   where
      loop chan = do
         msg <- atomically $ readPChan chan
         putStrLn $ "Read: " ++ unpackString msg
         loop chan

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
