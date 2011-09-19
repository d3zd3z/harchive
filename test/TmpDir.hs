----------------------------------------------------------------------
-- Managing temporary directories
----------------------------------------------------------------------

module TmpDir (
   makeTmpDir,
   withTmpDir
) where

import Control.Exception (finally)
import System.IO.Error
import Data.Array
import System.Directory
import System.Posix hiding (createDirectory)
import System.Random

-- |Create a new temporary directory, returning it's name.
makeTmpDir :: IO String
makeTmpDir = do
   pid <- getProcessID
   let gen = mkStdGen $ fromIntegral pid
   let names = map makeName $ groupAt 10 $ randomLetters gen
   tryDir names

-- |Evalute 'action' with a newly created temp dir, which will be
-- removed once action completes.
withTmpDir :: (String -> IO a) -> IO a
withTmpDir action = do
   name <- makeTmpDir
   finally (action name) (removeDirectoryRecursive name)

-- Repeately try new names until we find one that works.
tryDir :: [FilePath] -> IO FilePath
tryDir [] = undefined
tryDir (x:xs) = do
   status <- try $ createDirectory x
   either (const $ tryDir xs) (const $ return x) status

makeName :: String -> FilePath
makeName = ("/tmp/test-" ++)

groupAt :: Int -> [a] -> [[a]]
groupAt _ [] = []
groupAt n ary = pre : groupAt n post
   where (pre, post) = splitAt n ary

randomLetters :: StdGen -> String
randomLetters gen =
   (letters ! idx) : randomLetters gen'
   where
      (idx, gen') = randomR (bounds letters) gen

letters :: Array Int Char
letters = listArray (1, length letterList) letterList

letterList :: String
letterList = ['A' .. 'Z'] ++ ['a' .. 'z'] ++ ['0' .. '9'] ++ "-+"
