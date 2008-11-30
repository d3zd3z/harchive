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

module Status (
   State,
   start, stop,

   withIO,

   addDupedData, addSavedData, addFile, addSkippedFile,
   addSkippedData,
   addDirectory, setPath
) where

-- Note that all of the 'add' operations (as well as setPath) are
-- strict in their data argument.  Without this, this module makes it
-- fairly easy to create space leaks since the compute arguments are
-- often computed from large data blocks, even though they are
-- generally integers.

import Data.Int
import Control.Concurrent

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

type State = MVar (Maybe Status)

-- Start the progress meter, and return the state variable that is
-- handed around to everyone.
-- Verbosity levels:
--   0 - Don't print anything.
--   1 - Print basic counts each second

start :: Int -> IO State
start verbosity =
   case verbosity of
      0 -> newMVar Nothing
      1 -> do
         state <- newMVar $ Just initialStatus
         forkIO $ printer state
         return state
      _ -> error "Too much verbosity"
   where
      printer state = do
         threadDelay 1000000  -- Apparently this doesn't work on many platforms.
         status <- takeMVar state
         case status of
            Nothing -> putMVar state Nothing
            Just st -> do
               showStatus st
               let newSt = if sPrinted st then status else Just $ st { sPrinted = True }
               putMVar state newSt
               printer state

stop :: State -> IO ()
stop state = do
   status <- takeMVar state
   case status of
      Nothing -> return ()
      Just st -> do showStatus st
   putMVar state Nothing

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

-- Clear the status information, perform some IO, and then redraw the
-- status information.  Used to wrap around things that are going to
-- print messages so that they don't get stepped on by the status
-- message.
withIO :: State -> IO a -> IO a
withIO state op = do
   status <- takeMVar state
   case status of
      Nothing -> do
         result <- op
         putMVar state Nothing
         return result
      Just st -> do
         clearStatus st
         result <- op
         putMVar state $ Just (st { sPrinted = False })
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

addSavedData :: State -> Int64 -> IO ()
addSavedData st count =
   withState st $ \state -> state { sSaved = sSaved state + count }

addSkippedData :: State -> Int64 -> IO ()
addSkippedData st count =
   withState st $ \state -> state { sSkipped = sSkipped state + count }

addDupedData :: State -> Int64 -> IO ()
addDupedData st count =
   withState st $ \state -> state { sDuped = sDuped state + count }

addFile :: State -> Int64 -> IO ()
addFile st count =
   withState st $ \state -> state { sFiles = sFiles state + count }

addSkippedFile :: State -> Int64 -> IO ()
addSkippedFile st count =
   withState st $ \state -> state { sSkippedFiles = sSkippedFiles state + count }

addDirectory :: State -> Int64 -> IO ()
addDirectory st count =
   withState st $ \state -> state { sDirs = sDirs state + count }

setPath :: State -> String -> IO ()
setPath st path =
   withState st $ \state -> state { sPath = path }

withState :: State -> (Status -> Status) -> IO ()
withState state modifier = do
   status <- takeMVar state
   case status of
      Nothing -> putMVar state Nothing
      Just st -> do
	 let st' = modifier st
	 -- Force the new state.
	 st' `seq` putMVar state $ Just st'

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
