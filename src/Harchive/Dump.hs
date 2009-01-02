----------------------------------------------------------------------
-- Backup dumping.
----------------------------------------------------------------------

module Harchive.Dump (
   dumpDir
) where

import Meter

import qualified Control.Exception as E
import System.Linux.Directory
import Data.List (partition)

dumpDir :: FilePath -> IO ()
dumpDir path = do
   info <- getDirectoryInformation path
   let (dirs, nondirs) = partition isLinuxDir info
   putStrLn $ "dirs: " ++ show dirs
   putStrLn $ "nondirs: " ++ show nondirs

isLinuxDir :: LinuxDirInfo -> Bool
isLinuxDir (_, _, d) = d == dtDir

getDirectoryInformation :: FilePath -> IO [LinuxDirInfo]
getDirectoryInformation path = do
   E.bracket (openDirStream path) closeDirStream $ loop
   where
      loop dir = do
         ent <- linuxReadDirStream dir
         case ent of
            Nothing -> return []
            Just (".",_,_) -> loop dir
            Just ("..",_,_) -> loop dir
            Just info -> do
               rest <- loop dir
               return $ info:rest
