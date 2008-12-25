{-# LANGUAGE GeneralizedNewtypeDeriving #-}
----------------------------------------------------------------------
-- Progress meter.
----------------------------------------------------------------------

module Meter (
   Meter, GenMeter(..),
   runMeter, mString, mLeftPad, mRightPad, mAddCommas,

   makeCounter, resetCounter, incrCounter, meterCounter,
   makeRateCounter,

   Indicator,
   makeIndicator, runIndicator, stopIndicator, indicatorIO,
   runIndicatorUpdate,

   makeMeterCounter,

   module Control.Concurrent.STM,
   lift
) where

import qualified Data.DList as DList
import Data.DList (DList)
import Control.Concurrent
import Control.Concurrent.STM

import Control.Monad.Writer

import System.IO (hFlush, stdout)
import Text.Printf (printf)

type DString = DList Char

newtype GenMeter m a = GenMeter { unMeter :: WriterT DString m a }
   deriving (Monad, MonadWriter DString, MonadTrans)
type Meter a = GenMeter STM a

-- Hmm.  Not sure how to get rid of the warning about the "GenMeter"
-- constructor not being used.  It doesn't need to be explicitly used
-- because of the GeneralizedNewtypeDeriving, but it needs to be
-- specified, syntactically.  I don't really want to export the
-- constructor/destructor, but it's the only way I've found to get rid
-- of the warning.

-- Get the current representation of the meter (as a string)
runMeter :: Meter m -> STM String
runMeter = liftM DList.toList . execWriterT . unMeter

-- A meter consisting of a simple string.
mString :: String -> Meter ()
mString = tell . DList.fromList

-- Pad the given meter with spaces on the left to a given size.
mLeftPad :: Int -> Meter a -> Meter a
mLeftPad len = mCensor $ rpad len

-- Pad the given meter with spaces on the right to a given size.
mRightPad :: Int -> Meter a -> Meter a
mRightPad len = mCensor $ lpad len

-- Insert commas in groups of threes into a number
mAddCommas :: Meter a -> Meter a
mAddCommas = mCensor addCommas

----------------------------------------------------------------------
-- Utilities for working with TVars that are numbers.

makeCounter :: t -> IO (TVar t)
makeCounter = newTVarIO

resetCounter :: (Num t) => TVar t -> STM ()
resetCounter v = writeTVar v 0

incrCounter :: (Num t) => TVar t -> t -> STM ()
incrCounter v amount = do
   num <- readTVar v
   writeTVar v $! num + amount

-- Meter that shows a numeric counter, with commas of the given width.
meterCounter :: (Show t) => (t -> t) -> Int -> TVar t -> Meter ()
meterCounter op len v = do
   num <- lift $ readTVar v
   mLeftPad len $ mAddCommas $ mString $ show $ op num

-- Construct a more sophisticated counter.  Returns both a TVar and a
-- Meter that will display it.  The counter starts at zero, and prints
-- it's size after performing 'op' upon it.
makeMeterCounter :: (Integer -> Integer) -> String -> IO (TVar Integer, Meter ())
makeMeterCounter op post = do
   counter <- newTVarIO 0
   let meter = do
	 meterCounter op 12 counter
	 mString " "
	 mString post
   return $ (counter, meter)

-- Construct a meter that measures average rate of another counter.
-- Returns two meters and the update STM that needs to be given to
-- runIndicatorUpdate to keep update these meters.  The first meter is
-- a one-second instantaneous averate rate, and the second is a
-- decaying average.  This meter tries to detect resets.
-- The function is used to adjust the meter value for display (such as
-- dividing by 1024).
makeRateCounter :: (Double -> Double) -> TVar Integer
   -> IO (Meter (), Meter (), STM ())
makeRateCounter op counter = do
   tmp <- atomically $ readTVar counter
   lastValue <- newTVarIO $ tmp

   instRate <- newTVarIO (0.0::Double)
   avgRate <- newTVarIO (-1::Double)

   let
      update = do
	 tlast <- readTVar lastValue
	 cur <- readTVar counter
	 let theLast = cur `min` tlast

	 let inst = fromInteger (cur - theLast)
	 writeTVar instRate inst

	 oldAvg <- readTVar avgRate
	 if oldAvg < 0
	    then writeTVar avgRate inst
	    else writeTVar avgRate $ 0.9 * oldAvg + 0.1 * inst

	 writeTVar lastValue cur

      instMeter = do
	 inst <- lift $ readTVar instRate
	 mString $ printf "%8.1f" (op inst)

      avgMeter = do
	 avg <- lift $ readTVar avgRate
	 mString $ printf "%8.1f" (op avg)

   return (instMeter, avgMeter, update)

----------------------------------------------------------------------
-- Automatic display of a progress meter.

data Indicator = Indicator {
   -- The meter to print.
   iMeter :: Meter (),

   -- A string we could print to clear the indicator.
   iClear :: TVar (Maybe String),

   -- A lock on the meter.  True indicates that the meter should
   -- continue to be printed.  Once it is set to false, the meter
   -- shouldn't be printed.
   iLock :: TMVar Bool
   }

makeIndicator :: Meter () -> IO Indicator
makeIndicator meter = do
   clear <- newTVarIO Nothing
   lock <- newTMVarIO True
   return $ Indicator {
      iMeter = meter,
      iClear = clear,
      iLock = lock }

-- Runs the given indicator in the background.  The STM argument will
-- be invoked upon each update (close to once a second) and can be
-- used to update rate parameters and stuff.
runIndicatorUpdate :: STM () -> Indicator -> IO ()
runIndicatorUpdate update ind = do
   forkIO $ run
   return ()
   where
      run = do
	 threadDelay 1000000
	 atomically update
	 running <- atomically $ takeTMVar $ iLock ind
	 if running
	    then do
	       draw ind
	       atomically $ putTMVar (iLock ind) $ running
	       run
	    else
	       atomically $ putTMVar (iLock ind) $ running

runIndicator :: Indicator -> IO ()
runIndicator = runIndicatorUpdate (return ())

-- Stop the running indicator, clearing the display if indicated by
-- the boolean clearIt flag.
stopIndicator :: Indicator -> Bool -> IO ()
stopIndicator ind clearIt = do
   running <- atomically $ takeTMVar $ iLock ind
   when running $ do
      if clearIt
	 then clearIndicator ind
	 else draw ind
   atomically $ putTMVar (iLock ind) False

-- Clear the indicator, perform the IO, and restart the indicator.
-- Useful for printing messages and such.
indicatorIO :: Indicator -> IO a -> IO a
indicatorIO ind action = do
   running <- atomically $ takeTMVar $ iLock ind
   when running $ clearIndicator ind
   result <- action
   when running $ draw ind
   atomically $ putTMVar (iLock ind) running
   return result

-- Draw the indicator, clearing any previous indicator.
draw :: Indicator -> IO ()
draw ind = do
   (text, clear) <- atomically $ do
      text <- runMeter $ iMeter ind
      clear <- readTVar $ iClear ind
      writeTVar (iClear ind) $ Just $ clearText text
      return (text, clear)
   maybe (return ()) putStr clear
   putStr text
   hFlush stdout

-- Just clear the indicator.
clearIndicator :: Indicator -> IO ()
clearIndicator ind = do
   clear <- atomically $ do
      clear <- readTVar $ iClear ind
      writeTVar (iClear ind) Nothing
      return clear
   maybe (return ()) putStr clear

-- Return an ANSI sequence that will clear the display of the given
-- text.
clearText :: String -> String
clearText text =
   "\r" ++ (if count > 0 then upLines else "") ++ "\27[0J"
   where
      count = countElem '\n' text
      upLines = "\27[" ++ show count ++ "A"

{-
-- Show the given indicator.  Forks a thread to display the meter
-- periodically.
showIndicator :: (Meter a) -> IO Indicator
showIndicator meter = do
   token <- newTMVarIO True
   clearCommand <- newTVarIO Nothing

   return $ Indicator { cleanIO = clean token, stopIndicator = stop token }

   where
      clean token action = do
	 t <- atomically $ takeTMVar token
	 result <- action
	 atomically $ putTMVar token t
	 return result

      stop token = undefined

      run = undefined

      clear :: TVar (Maybe String) -> IO ()
      clear cmd = do
	 text <- atomically $ do
	    t <- readTVar cmd
	    writeTVar cmd Nothing
	    return t
	 maybe (return ()) (\t -> do
	    putStr t
	    hFlush stdout) text

      -- Print the meter, returning and action to clear it.
      showMeter :: TVar (Maybe String) -> IO String
      showMeter clearCommand = do
	 clear
	 text <- runMeter meter
	 putStr text
	 hFlush stdout
	 return $ "\r\27[" ++ show (countElem '\n' text) ++ "A\27[0J"
-}

countElem :: (Eq a) => a -> [a] -> Int
countElem elt = length . filter (==elt)

----------------------------------------------------------------------
rpad :: Int -> String -> String
rpad len str = replicate (len - length str) ' ' ++ str

lpad :: Int -> String -> String
lpad len str = str ++ replicate (len - length str) ' '

addCommas :: String -> String
addCommas = reverse . add . reverse
   where
      add (a:b:c:ds@(_:_)) = a:b:c:',':add (ds)
      add xs = xs

mCensor :: (String -> String) -> Meter a -> Meter a
mCensor op = censor (DList.fromList . op . DList.toList)
