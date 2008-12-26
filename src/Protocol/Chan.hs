----------------------------------------------------------------------
-- Protocol channels.
----------------------------------------------------------------------

module Protocol.Chan (
   PChanWrite, PChanRead, PChanPair,
   makePChan,
   writePChan, readPChan,

   chanServer, chanClient
) where

import Auth
import Server

import Control.Concurrent.STM
import System.IO

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
