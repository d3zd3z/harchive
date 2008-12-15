----------------------------------------------------------------------
-- CRAM-SHA1 authentication.
----------------------------------------------------------------------

module Auth (
   UUID,
   genNonce, genUuid,
   authInitiator, authRecipient,
   runAuthIO,

   -- For testing.
   AuthState(..)
) where

import Hash

import qualified Codec.Binary.Base64 as Base64

import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import qualified Control.Exception as E
import Data.Bits
import System.IO

----------------------------------------------------------------------

authInitiator :: String -> IO AuthState
-- The initial communication needs to be IO, since it needs to get a
-- unique number from the environment.  The rest is pure.
authInitiator secret = do
   nonce <- genNonce
   let message = "challenge " ++ nonce
   return $ AuthMore (Just message) (initiatorResponse secret nonce)

authRecipient :: String -> IO AuthState
authRecipient secret = do
   nonce <- genNonce
   return $ AuthMore Nothing (recipientResponse secret nonce)

runAuthIO :: Handle -> Handle -> AuthState -> IO Bool
-- Run authentication given by AuthState over the given pair of
-- handles.  Returns true if the authentication succeeded.
runAuthIO _ _ AuthFail = return False
runAuthIO _ writer (AuthGood msg) = do
   maybe (return ()) (outputMessage writer) msg
   return True
runAuthIO reader writer (AuthMore msg next) = do
   maybe (return ()) (outputMessage writer) msg
   resp <- hGetLine reader  -- TODO: Use a bounded readline.
   runAuthIO reader writer (next resp)

outputMessage :: Handle -> String -> IO ()
-- Output a message to the handle.
outputMessage fd msg = do
   hPutStrLn fd msg
   hFlush fd

----------------------------------------------------------------------

-- Each step of the authentication results in an AuthState result.
-- 'AuthFail' indicates some type of authentication failure.
-- 'AuthGood' indicates we consider the other party to have
-- successfully authenticated.  However, if the String is present, it
-- must be sent to the other side to finish the authentication.
data AuthState
   = AuthGood (Maybe String)
   | AuthFail
   | AuthMore (Maybe String) (String -> AuthState)

recipientResponse :: String -> String -> String -> AuthState
-- Step 2.
-- Recipient receives message, sends recipient message.
recipientResponse secret recipientRandom challenge =
   case words challenge of
      ["challenge", initRandom] ->
	 AuthMore (Just message)
	    (recipientCheck secret initRandom recipientRandom)
	 where
	    message = "recipient " ++ recipientRandom ++ " " ++ summary
	    summary = hmac secret ("recipient" ++ initRandom ++ recipientRandom)
      _ -> AuthFail

initiatorResponse :: String -> String -> String -> AuthState
-- Step 3.
-- Initiator receives message, sends initiator message to auth itself
-- to the recipient.
initiatorResponse secret initRandom challenge =
   case words challenge of
      ["recipient", recipientRandom, rSummary] ->
	 if rSummary == goodRSummary
	    then AuthGood (Just message)
	    else AuthFail
	 where
	    goodRSummary = hmac secret ("recipient" ++ initRandom ++ recipientRandom)
	    iSummary = hmac secret ("initiator" ++ initRandom ++ recipientRandom)
	    message = "initiator " ++ iSummary
      _ -> AuthFail

recipientCheck :: String -> String -> String -> String -> AuthState
-- Step 4.
-- Recipient receives message, and verifies that the initiator is
-- valid.
recipientCheck secret initRandom recipientRandom response =
   case words response of
      ["initiator", iSummary] ->
	 if iSummary == goodISummary
	    then AuthGood Nothing
	    else AuthFail
	 where
	    goodISummary = hmac secret ("initiator" ++ initRandom ++ recipientRandom)
      _ -> AuthFail

----------------------------------------------------------------------
-- HMac.
-- TODO: This would be more efficient to capture the hash state and
-- reproduce it.  The extra copies of the hash state are likely to be
-- faster than the re-computation of the key info.

hmac :: String -> String -> String
-- Compute a keyed hmac using the given secret of the given payload.
-- The result will be base64 encoded.
hmac secret info =
   Base64.encode $ B.unpack result
   where
      result = toByteString $ hashOf outer
      outer = oKey `L.append` (L.fromChunks [innerHash])
      innerHash = toByteString $ hashOf inner
      inner = iKey `L.append` bData

      bData = L.pack $ (map $ fromIntegral . fromEnum) $ info
      bSecret = L.pack $ (map $ fromIntegral . fromEnum) $ secret

      oKey = L.pack $ map (xor 0x5c) $ L.unpack $ paddedSecret
      iKey = L.pack $ map (xor 0x36) $ L.unpack $ paddedSecret
      paddedSecret = bSecret `L.append` (L.replicate padLen 0)
      padLen = (-(L.length bSecret)) .&. fromIntegral (hashBlockLength - 1)

----------------------------------------------------------------------

genNonce :: IO String
-- Generate a Nonce appropriate for our application, or as a key.
genNonce = do
   bytes <- getOSRandomBytes 33
   return $ Base64.encode $ B.unpack bytes

getOSRandomBytes :: Int -> IO B.ByteString
-- Read a bytestring with a specified number of bytes of
-- system-generated pseudo-random data.
getOSRandomBytes count = do
   E.bracket (openBinaryFile "/dev/urandom" ReadMode) hClose $ \handle -> do
      B.hGet handle count

type UUID = String
genUuid :: IO UUID
-- TODO: This is very Linux dependent.
-- Use the kernel random uuid generator to generate a single random
-- uuid.
genUuid = do
   E.bracket (openFile "/proc/sys/kernel/random/uuid" ReadMode) hClose $ \handle -> do
      hGetLine handle
