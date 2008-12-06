----------------------------------------------------------------------
-- Test the authentication.
----------------------------------------------------------------------

module AuthTest where

import Auth

import Test.HUnit
import Control.Concurrent
import System.IO
import qualified System.Posix as P

authTests = test [
   "Auth good" ~: authGood ]

authGood :: IO ()
authGood = do
   i <- authInitiator "secret"
   r <- authRecipient "secret"
   (iOk, rOk) <- authInteract i r
   iOk @? "Initiator failed"
   rOk @? "Recipient failed"

   i2 <- authInitiator "secret1"
   r2 <- authInitiator "secret2"
   (iOk2, rOk2) <- authInteract i2 r2
   (not iOk2) @? "Initiator failed"
   (not rOk2) @? "Recipient failed"

authInteract :: AuthState -> AuthState -> IO (Bool, Bool)
-- Test the interaction between two authenticators.  Returns true if
-- the interaction is successful.
authInteract left right = do
   leftBox <- newEmptyMVar
   rightBox <- newEmptyMVar

   (arfd, awfd) <- P.createPipe
   (brfd, bwfd) <- P.createPipe

   runAuthClose leftBox arfd bwfd left
   runAuthClose rightBox brfd awfd right

   leftAnswer <- takeMVar leftBox
   rightAnswer <- takeMVar rightBox

   return (leftAnswer, rightAnswer)

runAuthClose :: MVar Bool -> P.Fd -> P.Fd -> AuthState -> IO ()
-- Run a thread on the authentation, closing the handles upon finish.
runAuthClose box readFd writeFd auth = do
   _ <- forkIO $ do
      readH <- P.fdToHandle readFd
      writeH <- P.fdToHandle writeFd
      answer <- runAuthIO readH writeH auth
      hClose readH
      hClose writeH
      putMVar box answer
   return ()
