----------------------------------------------------------------------
-- Progress monitoring.
----------------------------------------------------------------------
--
-- |The progress monitor tracks various counters and whatnot.  As a
-- simplification, all of the counters track with 'Integer'.

module Progress (
   boring
) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception (finally)
import Control.Monad (unless)
import System.IO (hFlush, hPutStr, stderr)

----------------------------------------------------------------------

-- Boring can be used to wrap an I/O operation that might take some
-- time.  If the operation takes enough time that the user might grow
-- impatient (and kill the program), this will print a brief message,
-- and then another at the end.  Note that this shouldn't be used if
-- the operation is going to print anything out.

data BoringDone = BoringDone | BoringDot

boring :: (IO a) -> IO a
boring action = do
   done <- newTVarIO False
   forkIO $ timer done "" "Working..." 300000
   finally action (atomically $ writeTVar done True)
   where
      timer done doneMsg contMsg pause = do
	 expire <- myRegisterDelay pause
	 state <- atomically $ waitTrue done BoringDone `orElse`
	    waitTrue expire BoringDot
	 case state of
	    BoringDone -> hPutStr stderr doneMsg
	    BoringDot -> do
	       hPutStr stderr contMsg
	       hFlush stderr
	       timer done "done\n" "." 1000000

-- Local version of registerDelay that works even without -threaded.
myRegisterDelay :: Int -> IO (TVar Bool)
myRegisterDelay delay = do
   notify <- newTVarIO False
   forkIO $ do
      threadDelay delay
      atomically $ writeTVar notify True
   return notify

-- retries until the given TVar is true, and then returns the given
-- value.
waitTrue :: TVar Bool -> a -> STM a
waitTrue tv x = do
   d <- readTVar tv
   unless d retry
   return x
