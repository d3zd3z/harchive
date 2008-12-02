----------------------------------------------------------------------
-- Display an (optional) progress meter, of various levels of
-- verbosity.
-- Copyright 2007, David Brown
--
-- This program is free software; you can redistribute it and/or modify it
-- under the terms of the GNU General Public License as published by the
-- Free Software Foundation; either version 2, or (at your option) any later
-- version.
--
-- This program is distributed in the hope that it will be useful, but
-- WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
-- Public License for more details.
--
-- You should have received a copy of the GNU General Public License along
-- with this program; if not, write to the Free Software Foundation, Inc.,
-- 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
--
----------------------------------------------------------------------
--
-- Please ask <hackage@davidb.org> if you are interested in another
-- license.  If pieces of this program are useful in other systems I
-- will be willing to release them under a freer license, but I want
-- the program as a whole to be covered under the GPL.
--
----------------------------------------------------------------------

-- TODO: Coordinate better with status update so that we can update
-- immediately after printing a message.

module Status (

   statusToIO,
   StatusIO,

   startStatus,
   stopStatus,

   liftIO,
   cleanLiftIO,

   addDupedData, addSavedData, addFile, addSkippedFile,
   addSkippedData,
   addDirectory, setPath
) where

-- Note that all of the 'add' operations (as well as setPath) are
-- strict in their data argument.  Without this, this module makes it
-- fairly easy to create space leaks since the compute arguments are
-- often computed from large data blocks, even though they are
-- generally integers.

import MBox

import Data.Int
import Control.Concurrent
import Control.Monad.Reader

-- "Outer" monad, holding the MVar containing the atomic state.
type StatusIO a = MBox InternalState a

-- Monad representing IO operations that can happen atomically,
-- possibly modifying the state.
-- Not declared, since the type doesn't seem to be used yet.
-- type AtomicStatusOp a = AtomicOp InternalState a

type InternalState = Maybe Status

statusToIO :: Int -> StatusIO a -> IO a
statusToIO verbosity actions = do
   runMBox run Nothing
   where
      run = do
	 when (verbosity > 0) startStatus
	 answer <- actions
	 stopStatus
	 return answer

-- All of the counters we manage.
data Status = Status {
   sPath :: !String,
   sSkipped :: !Int64,
   sDuped :: !Int64,
   sSaved :: !Int64,
   sCompressed :: !Int64,
   sFiles :: !Int64,
   sSkippedFiles :: !Int64,
   sDirs :: !Int64,

   -- Have we just printed the information
   sPrinted :: !Bool
}

----------------------------------------------------------------------

-- Start the status monitor running (if not already).
startStatus :: StatusIO ()
startStatus = do
   mvar <- ask   -- Needed for forkIO below.
   atomicLift $ do
      state <- get
      case state of
	 Just _ -> return ()
	 Nothing -> do
	    put $ Just initialStatus
	    liftIO $ forkIO $ runReaderT printThread mvar
	    return ()

printThread :: StatusIO ()
printThread = do
   liftIO $ threadDelay 1000000   -- Portability concerns.
   more <- atomicLift $ do
      state <- get
      case state of
	 Nothing -> return False
	 Just st -> do
	    liftIO $ showStatus st
	    unless (sPrinted st) $ do
	       put $ Just $ st { sPrinted = True }
	    -- Recurse.
	    return True
   when more printThread

stopStatus :: StatusIO ()
stopStatus = do
   atomicLift $ do
      state <- get
      case state of
	 Nothing -> return ()
	 Just st -> liftIO $ showStatus st
      put Nothing

showStatus :: Status -> IO ()
showStatus status = do
   clearStatus status
   putStrLn "-----------------------------------------------------"
   putStrLn $ "saved: " ++ showNum 11 ((sSaved status) `div` 1024) ++
      "KB, skipped: " ++ showNum 11 ((sSkipped status) `div` 1024) ++
      "KB, duped: " ++ showNum 11 ((sDuped status) `div` 1024) ++
      "KB"
   putStrLn $
      " comp: " ++ showNum 11 (sCompressed status) ++
      "KB, files: " ++ showNum 8 (sFiles status) ++
      ", skip: " ++ showNum 8 (sSkippedFiles status) ++
      ", dirs: " ++ showNum 8 (sDirs status)
   putStrLn $ "path: " ++ prettyPath (sPath status)
   where
      prettyPath = lpad 65 . reverse . take 65 . reverse

clearStatus :: Status -> IO ()
clearStatus status | (sPrinted status) = putStr "\27[4A\27[0J"
                   | otherwise = return ()

-- Lift the IO operation into the StatusIO in such a way that the
-- progress meter will be cleared, and redrawn afterward.  Important
-- when lifting any IO that prints messages.
cleanLiftIO :: IO a -> StatusIO a
cleanLiftIO op = do
   atomicLift $ do
      state <- get
      case state of
	 Nothing -> liftIO op
	 Just st -> do
	    liftIO $ clearStatus st
	    result <- liftIO op
	    liftIO $ showStatus (st { sPrinted = False })
	    put $ Just $ st { sPrinted = True }
	    return result

-- Show a number nicely.
showNum :: (Num a) => Int -> a -> String
showNum padding = pad padding . commify . show

-- Add padding to a value.
pad :: Int -> String -> String
pad len str = replicate (len - length str) ' ' ++ str

lpad :: Int -> String -> String
lpad len str = str ++ replicate (len - length str) ' '

-- Add nice commas into a number.
commify :: String -> String
commify = reverse . add . reverse
   where
      add (a:b:c:d:rest) = a:b:c:',': add (d:rest)
      add x = x

----------------------------------------------------------------------
-- Various update entities.

addSavedData :: Int64 -> StatusIO ()
addSavedData count =
   withState $ \state -> state { sSaved = sSaved state + count }

addSkippedData :: Int64 -> StatusIO ()
addSkippedData count =
   withState $ \state -> state { sSkipped = sSkipped state + count }

addDupedData :: Int64 -> StatusIO ()
addDupedData count =
   withState $ \state -> state { sDuped = sDuped state + count }

addFile :: Int64 -> StatusIO ()
addFile count =
   withState $ \state -> state { sFiles = sFiles state + count }

addSkippedFile :: Int64 -> StatusIO ()
addSkippedFile count =
   withState $ \state -> state { sSkippedFiles = sSkippedFiles state + count }

addDirectory :: Int64 -> StatusIO ()
addDirectory count =
   withState $ \state -> state { sDirs = sDirs state + count }

setPath :: String -> StatusIO ()
setPath path =
   withState $ \state -> state { sPath = path }

-- Update state atomically and conveniently.
withState :: (Status -> Status) -> StatusIO ()
withState modifier = do
   atomicLift $ do
   status <- get
   case status of
      Nothing -> return ()
      Just st -> do
	 let st' = modifier st
	 -- Force the new state.  Not sure if this is necessary, since
	 -- we're using a strict state monad.
	 st' `seq` put $ Just st'

initialStatus :: Status
initialStatus = Status {
   sPath = "",
   sSkipped = 0,
   sDuped = 0,
   sSaved = 0,
   sCompressed = 0,
   sFiles = 0,
   sSkippedFiles = 0,
   sDirs = 0,
   sPrinted = False
}
