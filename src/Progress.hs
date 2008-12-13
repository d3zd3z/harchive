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
   TrackLine(..),
   TrackBlock(..),

   renderBlock
) where

import Control.Concurrent
import Control.Monad (liftM)
import System.Timeout
import System.IO (hFlush, stdout)

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
   pmTracker :: TrackBlock,
   pmCommand :: MVar Command }

data Command
   = Pause (MVar ()) (MVar ())
   | Stop (MVar ())

startProgressMeter :: TrackBlock -> IO ProgressMeter
startProgressMeter block = do
   command <- newEmptyMVar
   let pm = ProgressMeter {
	 pmTracker = block,
	 pmCommand = command }
   forkIO $ progressThread pm
   return pm

{-
      let
	 run = do
	    cmd <- timeout 1000000 $ takeMVar command
	    case cmd of
	       Just (Stop done) -> putStrLn "Stop" >> putMVar done ()
	       Nothing -> do
		  rendered <- renderBlock block
		  putStr $ unlines rendered
		  run
      in run
   return pm
-}

progressThread :: ProgressMeter -> IO ()
progressThread pm = do
   rendered <- renderBlock $ pmTracker pm
   let clear = do
	 putStr $ "\27[" ++ show (length rendered) ++ "A\27[0J"
   putStr $ unlines rendered
   hFlush stdout
   cmd <- timeout 1000000 $ takeMVar $ pmCommand pm
   case cmd of
      Just (Stop done) -> do
	 -- Draw final updated values.
	 clear
	 rendered' <- renderBlock $ pmTracker pm
	 putStr $ unlines rendered'
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

data Tracker
   = TrackString String
   | TrackCounter Counter
   | TrackKBCounter Counter
   | TrackPath PathTracker
   | TrackNest [Tracker]

newtype TrackLine = TrackLine Tracker
newtype TrackBlock = TrackBlock [TrackLine]

-- TODO: The haskelly thing would be to make some combining operators
-- that take these things and just take strings and counters and such
-- and assemble them properly.

-- renderedTracker :: TrackBlock -> IO String
-- renderedTracker tb = do

renderBlock :: TrackBlock -> IO [String]
renderBlock (TrackBlock block) =
   mapM renderLine block

renderLine :: TrackLine -> IO String
renderLine (TrackLine line) =
   liftM concat $ renderTracker line

renderTracker :: Tracker -> IO [String]
renderTracker (TrackString st) = return [st]
renderTracker (TrackCounter (Counter box)) =
   withMVar box $ \c ->
      return $ [showNum 10 c]
renderTracker (TrackKBCounter (Counter box)) =
   withMVar box $ \c ->
      return $ [showNum 12 (c `div` 1024)]
renderTracker (TrackPath (PathTracker box)) =
   withMVar box $ \c ->
      return $ [lpad 65 $ reverse $ take 65 $ reverse c]
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
