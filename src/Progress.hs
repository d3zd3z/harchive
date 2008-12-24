{-# LANGUAGE TypeSynonymInstances #-}
----------------------------------------------------------------------
-- Progress monitoring.
----------------------------------------------------------------------
--
-- |The progress monitor tracks various counters and whatnot.  As a
-- simplification, all of the counters track with 'Integer'.

module Progress (
   Counter, makeCounter,
   resetCounter, incrCounter,
   PathTracker, makePathTracker,
   setTrackerPath,

   startProgressMeter, stopProgressMeter,
   progressIO,

   Tracker(..),
   (+++), Trackerable(..),
   kb,

   boring
) where

import Control.Concurrent
import Control.Concurrent.STM
import Control.Exception (finally)
import Control.Monad (liftM, unless)
import System.Timeout
import System.IO (hFlush, stdout, hPutStr, stderr)

-- import Text.Printf (printf)

newtype Counter = Counter (MVar Integer)

makeCounter :: IO Counter
makeCounter = liftM Counter $ newMVar 0

resetCounter :: Counter -> IO ()
-- Reset a counter to zero.
resetCounter (Counter box) =
   modifyMVar_ box $ const $ return 0

incrCounter :: Integral a => Counter -> a -> IO ()
incrCounter (Counter box) inc =
   modifyMVar_ box $ \v ->
      return $! v + fromIntegral inc

-- A path tracker.
newtype PathTracker = PathTracker (MVar String)

makePathTracker :: String -> IO PathTracker
makePathTracker = liftM PathTracker . newMVar

setTrackerPath :: PathTracker -> String -> IO ()
setTrackerPath (PathTracker box) text =
   modifyMVar_ box $ \_ ->
      return text

----------------------------------------------------------------------

data ProgressMeter = ProgressMeter {
   pmTracker :: Tracker,
   pmCommand :: MVar Command }

data Command
   = Pause (MVar ()) (MVar ())
   | Stop (MVar ())

startProgressMeter :: Tracker -> IO ProgressMeter
startProgressMeter block = do
   command <- newEmptyMVar
   let pm = ProgressMeter {
	 pmTracker = block,
	 pmCommand = command }
   forkIO $ progressThread pm
   return pm

progressThread :: ProgressMeter -> IO ()
progressThread pm = do
   rendered <- renderTracker $ pmTracker pm
   let clear = do
	 putStr $ "\r\27[" ++ show (countElem '\n' rendered) ++ "A\27[0J"
   putStr $ rendered
   hFlush stdout
   cmd <- timeout 1000000 $ takeMVar $ pmCommand pm
   case cmd of
      Just (Stop done) -> do
	 -- Draw final updated values.
	 clear
	 rendered' <- renderTracker $ pmTracker pm
	 putStrLn $ rendered'
	 putMVar done ()
      Just (Pause paused cont) -> do
	 clear
	 putMVar paused ()
	 takeMVar cont
	 progressThread pm
      Nothing -> do
	 clear
	 progressThread pm

stopProgressMeter :: ProgressMeter -> IO ()
-- Clean up the progress meter.
stopProgressMeter pm = do
   done <- newEmptyMVar
   putMVar (pmCommand pm) $ Stop done
   takeMVar done

countElem :: (Eq a) => a -> [a] -> Int
-- Count the number of occurrences of 'a' in the list.
countElem elt = length . filter (==elt)

progressIO :: ProgressMeter -> IO a -> IO a
-- Perform the IO operation with the progress meter safely paused.
progressIO pm action = do
   paused <- newEmptyMVar
   cont <- newEmptyMVar
   putMVar (pmCommand pm) $ Pause paused cont
   takeMVar paused
   result <- action
   putMVar cont ()
   return result

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

----------------------------------------------------------------------

data Tracker
   = TrackString String
   | TrackCounter Counter
   | TrackKBCounter Counter
   | TrackPath PathTracker
   | TrackNest [Tracker]

class Trackerable t where
   toTracker :: t -> Tracker

instance Trackerable Counter where
   toTracker = TrackCounter

instance Trackerable Tracker where
   toTracker = id

instance Trackerable String where
   toTracker = TrackString

kb :: Counter -> Tracker
kb = TrackKBCounter

infixr 5 +++

(+++) :: (Trackerable a, Trackerable b) => a -> b -> Tracker
a +++ b = TrackNest [toTracker a, toTracker b]

renderTracker :: Tracker -> IO String
renderTracker (TrackString st) = return st
renderTracker (TrackCounter (Counter box)) =
   withMVar box $ \c ->
      return $ showNum 10 c
renderTracker (TrackKBCounter (Counter box)) =
   withMVar box $ \c ->
      return $ showNum 12 (c `div` 1024)
renderTracker (TrackPath (PathTracker box)) =
   withMVar box $ \c ->
      return $ lpad 65 $ reverse $ take 65 $ reverse c
renderTracker (TrackNest tracks) = do
   nodes <- mapM renderTracker tracks
   return $ concat nodes

showNum :: (Num a) => Int -> a -> String
showNum padding = pad padding . commaify . show

pad :: Int -> String -> String
pad len str = replicate (len - length str) ' ' ++ str

lpad :: Int -> String -> String
lpad len str = str ++ replicate (len - length str) ' '

commaify :: String -> String
-- Add commas in groups to a number.
commaify = reverse . add . reverse
   where
      add (a:b:c:d:xs) = a:b:c:',': add (d:xs)
      add xs = xs
