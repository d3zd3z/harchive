----------------------------------------------------------------------
-- Testing Linux directory operations.

module LinuxDirCheck (testLinux) where

import Test.HUnit

import System.Linux.Directory
import System.Posix.Files (getSymbolicLinkStatus, fileID)
import System.Posix.Types (FileID)
import System.FilePath ((</>))

import Control.Monad (liftM)
import Control.Exception (bracket)
import Data.Function (on)
import Data.List (sort, sortBy)

testLinux :: Test
testLinux = test $ do
   let base = "/bin"
   names <- bracket (openDirStream base) closeDirStream (getNames [])
   inodes <- mapM (getFileID base) names
   -- putStrLn "First Names"
   -- putStrLn $ show (zip names inodes)
   -- putStrLn "Second names"
   names2 <- bracket (openDirStream base) closeDirStream (linuxGetNames [])
   -- putStrLn $ show (sortBy (compare `on` fst) names2)
   zip names inodes @=? sortBy (compare `on` fst) names2

-- Extract the names in the given DirStream, eliminating "." and "..",
-- and then sorting the result.
getNames :: [String] -> DirStream -> IO [String]
getNames names str = do
   name <- readDirStream str
   case name of
      "" -> return (sort names)
      "." -> getNames names str
      ".." -> getNames names str
      _ -> getNames (name:names) str

linuxGetNames :: [(String, FileID)] -> DirStream -> IO [(String, FileID)]
linuxGetNames names str = do
   f@(name, id) <- linuxReadDirStream str
   case name of
      "" -> return names
      "." -> linuxGetNames names str
      ".." -> linuxGetNames names str
      _ -> linuxGetNames (f:names) str

-- Lookup an inode number using 'lstat'.
getFileID :: FilePath -> FilePath -> IO FileID
getFileID base name =
   liftM fileID (getSymbolicLinkStatus (base </> name))
