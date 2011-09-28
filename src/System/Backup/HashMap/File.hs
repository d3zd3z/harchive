----------------------------------------------------------------------
-- Hash maps.

module System.Backup.HashMap.File (
   putHashMap, putOrderedMap, makeLookup, hmToAscList, mapMerge
) where

import Control.Exception (assert)
import Control.Monad (forM_)
import Data.Binary.Put
import Data.Binary.Get
import Data.Function (on)
import Data.List (minimumBy)
import qualified Data.ByteString.Lazy as L
import qualified Data.Map as M

import qualified Hash

----------------------------------------------------------------------

-- The hash map associates a series of Hashes with a binary encoding
-- of the data associated with them.

putHashMap :: (a -> Put) -> M.Map Hash.Hash a -> Put
putHashMap putVal = putOrderedMap putVal . M.toAscList

putOrderedMap :: (a -> Put) -> [(Hash.Hash, a)] -> Put
putOrderedMap putVal m =
   forM_ m $ \(k, v) -> do
      putByteString $ Hash.unHash k
      putVal v

----------------------------------------------------------------------

-- Given a valueSize, and a getValue function, and a block of data,
-- make a function for querying data from it.
makeLookup :: Int -> Get a -> L.ByteString -> Hash.Hash -> Maybe a
makeLookup valueSize getValue raw key =
   search 0 (numNodes - 1)
   where
      hLen = Hash.hashLength
      totalLength = fromIntegral $ L.length raw
      numNodes = (totalLength `mod` stride == 0) `assert` (totalLength `div` stride)
      stride = valueSize + hLen
      elt n = runGet (getCell getValue) $ L.drop (fromIntegral (stride * n)) raw
      search low high
         | high < low = Nothing
         | node > key = search low (mid-1)
         | node < key = search (mid+1) high
         | otherwise = Just answer
            where
               mid = low + ((high - low) `div` 2)
               (node, answer) = elt mid

-- Read a single cell.
getCell :: Get a -> Get (Hash.Hash, a)
getCell getValue = do
   bytes <- getBytes Hash.hashLength
   value <- getValue
   return (Hash.byteStringToHash bytes, value)

-- Return all of the pairs (in order) from the mapping blob (similar
-- to M.toAscList).
hmToAscList :: Get a -> L.ByteString -> [(Hash.Hash, a)]
hmToAscList getValue = runGet getCells
   where
      getCells = do
         more <- isEmpty
         if more
            then return []
            else do
                  cell <- getCell getValue
                  rest <- getCells
                  return $ cell : rest

----------------------------------------------------------------------
-- Combine multiple sorted mappings.
-- This is a generalized merge over multiple streams.

mapMerge :: (Ord a) => [[(a, b)]] -> [(a, b)]
mapMerge = merge . filter (not . null)

-- Merge where none of the lists are empty.
merge :: (Ord a) => [[(a, b)]] -> [(a, b)]
merge [] = []
merge lists = case break ((== minElt) . fst . head) lists of
      (pre, (x:xs):post) -> x : mapMerge (pre ++ xs:post)
      _ -> error "Internal merge error"
   where
      minElt = fst $ head $ minimumBy (compare `on` (fst . head)) lists

----------------------------------------------------------------------
-- Debugging.

{-
import qualified Data.ByteString.Lazy.Char8 as LC
a, b, c :: [(Int, Int)]
a = [ (x,x) | x <- [1,3..10] ]
b = [ (x,x) | x <- [2,4..10] ]
c = [ (x,x) | x <- [3,6..12] ]

iKey :: Int -> Hash.Hash
iKey = Hash.hashOf . LC.pack . show

bigMap :: Int -> M.Map Hash.Hash Int
bigMap n = M.fromList [ (iKey x, x) | x <- [1..n] ]

tlookup :: Hash.Hash -> Maybe Int
tlookup = 
   makeLookup 4 getInt raw
   where
      raw = runPut $ putHashMap putInt $ bigMap 50

putInt :: Int -> Put
putInt = putWord32be . fromIntegral

getInt :: Get Int
getInt = getWord32be >>= (return . fromIntegral)
-}
