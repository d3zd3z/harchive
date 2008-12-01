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
   tryDir gen

-- |Evalute 'action' with a newly created temp dir, which will be
-- removed once action completes.
withTmpDir :: (String -> IO a) -> IO a
withTmpDir action = do
   name <- makeTmpDir
   finally (action name) (removeDirectoryRecursive name)

-- Repeately try new names until we find one that works.
tryDir :: StdGen -> IO String
tryDir gen = do
   let (gen', name) = makeName gen
   status <- try $ createDirectory name
   either (const $ tryDir gen') (const $ return name) status

makeName :: StdGen -> (StdGen, String)
makeName gen =
   (gen', "/tmp/test-" ++ text)
   where
      (gen', text) = buildState randomLetter gen 10

buildState :: (a -> (a, b)) -> a -> Int -> (a, [b])
buildState _ st 0 = (st, [])
buildState gen st n =
   (stLast, x:xs)
   where
      (stNext, x) = gen st
      (stLast, xs) = buildState gen stNext (n-1)

randomLetter :: StdGen -> (StdGen, Char)
randomLetter gen =
   (gen', letters ! idx)
   where
      (idx, gen') = randomR (bounds letters) gen

letters :: Array Int Char
letters = listArray (1, length letterList) letterList

letterList :: String
letterList = ['A' .. 'Z'] ++ ['a' .. 'z'] ++ ['0' .. '9'] ++ "-+"
