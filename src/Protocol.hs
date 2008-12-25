{-# LANGUAGE GeneralizedNewtypeDeriving #-}
----------------------------------------------------------------------
-- Generalized protocols.
----------------------------------------------------------------------

module Protocol (
   Protocol(..),
   runProtocol, justProtocol,

   getLineP, putLineP,
   sendMessageP, flushP, receiveMessageP,

   liftIO, throwError, catchError, ask
) where

import qualified Data.ByteString.Lazy as L
import Data.Binary
import Control.Monad.Reader
import Control.Monad.Error
import System.IO

newtype Protocol a = Protocol {
   unProtocol :: ErrorT IOError (ReaderT Handle IO) a }
   deriving (Monad, MonadReader Handle, MonadIO, MonadError IOError)

-- Reverse arguments from normal so application is easier.
runProtocol :: Handle -> Protocol a -> IO (Either IOError a)
runProtocol h p = runReaderT (runErrorT $ unProtocol p) h

-- Similar to runProtocol, but propagate the exception to make
-- handling errors easier.
justProtocol :: Handle -> Protocol a -> IO a
justProtocol h p = do
   result <- runProtocol h p
   either ioError return result

-- Read a single line from the protocol (with a specified limit).
getLineP :: Int -> Protocol String
getLineP limit = do
   h <- ask
   ch <- liftIO $ hGetChar h
   case ch of
      '\r' -> getLineP limit
      '\n' -> return []
      _ -> do
	 rest <- getLineP (limit-1)
	 return $ ch : rest

-- Transmit a CR/LR terminated line over the protocol.
putLineP :: String -> Protocol ()
putLineP str = do
   h <- ask
   -- TODO: Once everybody uses this, put the '\r' back in.
   -- liftIO $ hPutStr h $ str ++ "\r\n"
   liftIO $ hPutStr h $ str ++ "\n"

-- Transmit a binary message over the protocol.
sendMessageP :: (Binary a) => a -> Protocol ()
sendMessageP item = do
   h <- ask
   let packed = encode item
   let header = encode (fromIntegral $ L.length packed :: Word32)
   liftIO $ do
      L.hPut h header
      L.hPut h packed

flushP :: Protocol ()
flushP = ask >>= liftIO . hFlush

-- Receive a single binary message from the protocol.
receiveMessageP :: (Binary a) => Protocol a
receiveMessageP = do
   h <- ask
   bHeader <- liftIO $ L.hGet h 4
   let len = fromIntegral (decode bHeader :: Word32)
   packed <- liftIO $ L.hGet h len
   return $ decode packed
