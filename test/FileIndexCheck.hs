module FileIndexCheck (fileIndexCheck) where

import Data.Array
import Data.Bits ((.&.), xor)
import qualified Data.ByteString as B
import qualified Data.List as List
import qualified Data.Map as Map
import Data.Maybe (catMaybes)
import Data.Word (Word32)
import Hash
import System.Backup.Pool.FileIndex
import Test.HUnit
import TmpDir

-- For testing
{-
import Data.Binary
import Data.Binary.Put
import Data.Binary.Get
import Text.HexDump
import Data.Convertible.Text (cs)
-}

fileIndexCheck :: Test
fileIndexCheck = test [
   "Verify test framework" ~: frameworkTest,
   "Simple writing test" ~: ioTest,
   "Rewriting" ~: rewriteTest ]

frameworkTest :: Test
frameworkTest =
   let base = randomRamMap 100000 in
   checkIndexer base (ixToList base) ~=? []

ioTest :: IO ()
ioTest = do
   withTmpDir $ \dir -> do
   let name = dir ++ "/file1.idx"
   let base = randomRamMap 100000
   writeIndex name 42 base
   (ix2, psize) <- readIndex name
   psize @=? 42
   checkIndexer ix2 (ixToList base) @=? []

rewriteTest :: IO ()
rewriteTest = do
   withTmpDir $ \dir -> do
   let name = dir ++ "/file2.idx"
   let (part1, part2) = List.splitAt 50000 $ Map.toList $ randomRamMap 100000
   writeIndex name 50 (Map.fromList part1)
   (ix1, psize1) <- readIndex name
   psize1 @=? 50
   checkIndexer ix1 part1 @=? []

   -- Rewrite with no changes.
   writeIndex name 50 ix1
   (ix1', psize1') <- readIndex name
   psize1' @=? 50
   checkIndexer ix1' part1 @=? []

   -- Add in part2.
   let i2src = List.foldl' update ix1' part2
   writeIndex name 100 i2src
   (ix2, psize2) <- readIndex name
   psize2 @=? 100
   checkIndexer ix2 (part1 ++ part2) @=? []
   where
      update idx (k, v) = ixInsert k v idx

-- Make sure that all of the keys lookup correctly, and that invalid
-- keys don't.  Returns a list of failures, empty list being success.
checkIndexer :: Indexer a => a -> [(Hash.Hash, (Word32, String))] -> [String]
checkIndexer ixr asoc = catMaybes $ corrects ++ tweaked where
   check key expect =
      let got = ixLookup key ixr in
      if got == expect then Nothing
         else Just $ "Mismatch: key " ++ show key ++ " expect: " ++ show expect ++
               " got: " ++ show got

   corrects = [check key (Just val) | (key, val) <- asoc]
   tweaked  = [check (tweak key) Nothing | (key, _) <- asoc]

randomRamMap :: Int -> RamIndex
randomRamMap n =
   Map.fromList [ (hashOf x, (fromIntegral x, y)) | (x, y) <- zip [1 .. n] typeMap]

-- Randomly map the integers onto the blob kinds use.
blobKinds :: Array Int String
blobKinds = listArray (0, length items - 1) items
   where items = ["BLOB", "DIR ", "DIR0", "DIR1", "IND0", "IND1", "IND2", "BACK"]

typeMap :: [String]
typeMap = drop 10 $ map (blobKinds !) $ selected (rangeSize $ bounds blobKinds)

-- Returns a pseudorandomized list of Ints up to the given bound.
selected :: Int -> [Int]
selected bound = gen 1 where
   gen n = (n `mod` bound) : gen ((n * 1103515245 + 12345) .&. 0x7fffffff)

-- Tweak a hash by modifying the last byte.
tweak :: Hash.Hash -> Hash.Hash
tweak hashIn =
   let raw = Hash.unHash hashIn in
   let prefix = B.take 19 raw in
   let suffix = B.last raw in
   let newSuffix = suffix `xor` 1 in
   Hash.byteStringToHash $ B.snoc prefix newSuffix
