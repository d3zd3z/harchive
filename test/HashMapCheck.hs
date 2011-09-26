----------------------------------------------------------------------
{-# LANGUAGE ScopedTypeVariables #-}

module HashMapCheck (hashMapCheck) where

import qualified Hash
import System.Backup.HashMap

import qualified Control.Exception as E
import qualified Data.Map as M
import Control.Monad (forM_, forM, unless)
import qualified Data.ByteString.Lazy.Char8 as LC
import qualified System.Posix as P
import Data.List (isPrefixOf, sort)
import System.Process (rawSystem)
import System.FilePath ((</>))

import Test.HUnit
import Harness
import TmpDir

hashMapCheck :: Test
hashMapCheck = test [
   "simple" ~: simple,
   "noDirty" ~: noDirty,
   "doesFlush" ~: doesFlush,
   "checkReadOnly" ~: checkReadOnly ]

simple :: IO ()
simple =
   withTmpDir $ \tmp -> do
   withHashMap tmp $ \hm -> do
      setProps hm
      setInts hm [1..ramLimit]
      checkInts hm [1..ramLimit]

   withHashMap tmp $ \hm -> do
      checkProps hm
      checkInts hm [1..ramLimit]

   if ramLimit < 200
      then do
         forM_ [1..23] $ \mult -> do
            let range = [mult*ramLimit + 1 .. (mult+1)*ramLimit]
            withHashMap tmp $ flip setInts range
            withHashMap tmp $ flip checkInts range

         withHashMap tmp $ flip setInts [1 .. 31*ramLimit]
         withHashMap tmp $ flip checkInts [1 .. 31*ramLimit]

         withHashMap tmp checkProps
      else
         putStrLn "Warning: ramLimit large, skipping extensive tests."

-- Verify no writes if not dirty.
noDirty :: IO ()
noDirty = withTmpDir $ \tmp -> do
   withHashMap tmp $ flip setInts [1..500]
   -- rawSystem "/bin/ls" ["-li", tmp]
   pre <- capture tmp indexPrefix
   withHashMap tmp $ flip setInts [1..500]
   -- rawSystem "/bin/ls" ["-li", tmp]
   post <- capture tmp indexPrefix
   pre @=? post
   withHashMap tmp $ flip setInts [1..502]
   -- rawSystem "/bin/ls" ["-li", tmp]
   post2 <- capture tmp indexPrefix
   -- putStrLn $ "Pre : " ++ show pre
   -- putStrLn $ "Post: " ++ show post2
   assertBool "Failed to change" $ pre /= post2

-- Verify that flush actually does something.
doesFlush :: IO ()
doesFlush = withTmpDir $ \tmp -> do
   withHashMap tmp $ flip setInts [1..100]
   state1 <- capture tmp indexPrefix
   withHashMap tmp $ \hm -> do
      setInts hm [101]
      state2 <- capture tmp indexPrefix
      assertBool "Changed before flush" $ state1 == state2
      runHashMap hm hmFlush
      state3 <- capture tmp indexPrefix
      assertBool "Not written by flush" $ state1 /= state3

checkReadOnly :: IO ()
checkReadOnly = withTmpDir $ \tmp -> do
   withHashMap tmp $ flip setInts [1..500]
   mustThrow $ openHashMap intValue tmp indexPrefix WriteMode
   E.bracket (openHashMap intValue tmp indexPrefix ReadMode) (flip runHashMap hmClose) $ \hm ->
      mustThrow $ setInts hm [501]

indexPrefix :: String
indexPrefix = "index-"

-- Invoke the exception, ensuring that it throws an error.  If it
-- doesn't, throw an assertion exception.
mustThrow :: IO a -> IO ()
mustThrow op = do
   (op >> assertFailure "operation didn't throw exception") `E.catch`
      \(e :: E.ErrorCall) -> do
         -- putStrLn $ "Expected exception: " ++ show e
         return ()

-- Perform 'op' over an opened hashmap, and close the hashmap.
withHashMap :: FilePath -> (HashMap Int -> IO a) -> IO a
withHashMap path =
   E.bracket
      (openHashMap intValue path indexPrefix AppendMode)
      (flip runHashMap hmClose)

-- Set the given integers in the hashmap.
setInts :: HashMap Int -> [Int] -> IO ()
setInts hm nums = forM_ nums (iAdd hm)

-- Check that the given integers are present in the hashMap.
checkInts :: HashMap Int -> [Int] -> IO ()
checkInts hm nums =
   forM_ nums $ \num ->
      iLookup hm num >>=
         assertBool ("Value in mapping: " ++ show num)

-- Set some test properties.
setProps :: HashMap Int -> IO ()
setProps hm =
   runHashMap hm $ hmModifyProperties (M.insert "sample1" "value1" . M.insert "sample2" "value2")

-- Check properties.
checkProps :: HashMap Int -> IO ()
checkProps hm = do
   props <- runHashMap hm hmGetProperties
   M.lookup "sample1" props @?= Just "value1"
   M.lookup "sample2" props @?= Just "value2"

iKey :: Int -> Hash.Hash
iKey = Hash.hashOf . LC.pack . show

iLookup :: HashMap Int -> Int -> IO Bool
iLookup hm index = do
   res <- runHashMap hm $ hmLookup (iKey index)
   case res of
      Nothing -> return False
      Just x -> if x == x then return True else error "iLookup key error"

iAdd :: HashMap Int -> Int -> IO ()
iAdd hm index =
   runHashMap hm $ hmInsert (iKey index) index

----------------------------------------------------------------------
-- For testing purposes, we can capture the files in the test
-- directory, along with their inode number.  Since the code writes
-- new files and never overwrites, inode changes will tell us which
-- files have been rewritten.

capture :: FilePath -> FilePath -> IO [(FilePath, P.FileID)]
capture path prefix = do
   names <- fmap (filter (isPrefixOf prefix)) $ listDir path
   stats <- mapM (P.getFileStatus . (path </>)) names
   return $ zip names (map P.fileID stats)

listDir :: FilePath -> IO [FilePath]
listDir path = E.bracket (P.openDirStream path) P.closeDirStream (getNames [])

-- Extract the names in the given DirStream, eliminating "." and "..",
-- and sort the result.
getNames :: [FilePath] -> P.DirStream -> IO [FilePath]
getNames names str = do
   name <- P.readDirStream str
   case name of
      "" -> return $ sort names
      "." -> getNames names str
      ".." -> getNames names str
      _ -> getNames (name:names) str
