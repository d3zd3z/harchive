{-# LANGUAGE TypeSynonymInstances #-}
----------------------------------------------------------------------
-- Pool file index.
----------------------------------------------------------------------
-- Each pool file has an associated index file tha maps the offsets of
-- the contained hashes in that file.
----------------------------------------------------------------------

module System.Backup.Pool.FileIndex (
   Indexer(..),
   FileIndex,
   RamIndex,
   emptyIndex,
   PackedIndex,
   writeIndex,
   readIndex,

   isIndexDirty
) where

import Control.Applicative ((<$>))
import Control.Exception (assert)
import Control.Monad (forM_, mplus, replicateM, unless)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import qualified Data.Array.Unboxed as U
import Data.Binary
import Data.Binary.Put
import Data.Binary.Get
import Data.Convertible.Text (cs)
-- import Data.Word (Word32)
import Data.Map (Map)
import qualified Data.Map as Map
import qualified Hash
import qualified System.IO.Cautious as Cautious

-- Note that this code makes an assumption that Data.Map.Map keeps its
-- data sorted by key.  Another Map could be used, but the results
-- would have to be sorted.

indexMagic :: B.ByteString
indexMagic = cs "ldumpidx"

class Indexer a where
   -- Queries directly of each piece.  Faster than ixToList and
   -- extracting the desired value.
   ixKeys :: a -> [Hash.Hash]
   ixOffsets :: a -> [Word32]
   ixKinds :: a -> [String]

   -- Operations directly from lists
   ixToList :: a -> [(Hash.Hash, (Word32, String))]
   ixLookup :: Hash.Hash -> a -> Maybe (Word32, String)
   ixInsert :: Hash.Hash -> (Word32, String) -> a -> a

-- Merge the two pools.  Priority is given to the first if there are
-- duplicates.
ixMerge :: Indexer a => Indexer b => a -> b -> [(Hash.Hash, (Word32, String))]
ixMerge left right = merge (ixToList left) (ixToList right) where
   merge a [] = a
   merge [] b = b
   merge aall@(a@(akey, _):as) ball@(b@(bkey, _):bs)
      | akey < bkey = a : merge as ball
      | akey == bkey = a : merge as bs
      | otherwise   = b : merge aall bs

writeIndex :: Indexer a => FilePath -> Word32 -> a -> IO ()
writeIndex path poolSize idx = do
   let payload = runPut $ putIndex poolSize idx
   Cautious.writeFileL  path payload

-- As the data is built up, values are placed in a RAM index first.
-- This is just a simple map from hashes to an Offset/Kind pair.
type RamIndex = Map Hash.Hash (Word32, String)

emptyIndex :: FileIndex
emptyIndex = FileIndex {
   fiPacked = emptyPackedIndex,
   fiRam = Map.empty }

instance Indexer RamIndex where
   ixKeys = Map.keys
   ixOffsets = map fst . Map.elems
   ixKinds = map snd . Map.elems
   ixToList = Map.toList
   ixLookup = Map.lookup
   ixInsert = Map.insert

putIndex :: Indexer a => Word32 -> a -> Put
putIndex poolSize idx = do
   putHeader poolSize
   forM_ [putTop, putHashes, putOffsets, putKinds] $ \op ->
      op idx

putHeader :: Word32 -> Put
putHeader poolSize = do
   putByteString indexMagic
   putWord32le 3
   putWord32le $ poolSize

putTop :: Indexer a => a -> Put
putTop idx = do
   mapM_ putWord32le $ makeTop idx

putHashes :: Indexer a => a -> Put
putHashes idx = do
   forM_ (ixKeys idx) $ \k -> do
      putByteString $ Hash.unHash k

putOffsets :: Indexer a => a -> Put
putOffsets idx = do
   mapM_ putWord32le $ ixOffsets idx

-- Version 3 writes the kind table directly.
putKinds :: Indexer a => a -> Put
putKinds idx = do
   forM_ (ixKinds idx) $ \knd -> do
      let packed = cs knd
      assert (B.length packed == 4) $
        putByteString packed

-- TODO: Compress the kind table, e.g. version 4

-- The first 256 words of the file give the byte offset of the first
-- Hash that has a larger byte value.  For example, the first offset
-- is the index of the first hash that doesn't start with a 0x00 byte.
-- The 255th (last) will be the total number of entries.
makeTop :: Indexer a => a -> [Word32]
makeTop idx = walk 0 [0..255] firsts
   where
      firsts = map (Hash.getHashByte 0) $ ixKeys idx

      walk _ [] _ = []
      walk pos (_:as) [] = pos : walk pos as []
      walk pos aall@(a:as) ball@(b:bs)
         | a >= b = walk (pos+1) aall bs
         | otherwise = pos : walk pos as ball

----------------------------------------------------------------------
-- Reading the file index.

newtype Hashes = Hashes { unHashes :: B.ByteString }
newtype Offsets = Offsets { unOffsets :: OffsetInfo }
newtype Kinds = Kinds { unKinds :: B.ByteString }
type TopInfo = U.UArray Int Word32
type OffsetInfo = U.UArray Int Word32

data PackedIndex = PackedIndex {
   piTop :: TopInfo,
   piHashes :: Hashes,
   piOffsets :: Offsets,
   piKinds :: Kinds }

emptyPackedIndex :: PackedIndex
emptyPackedIndex = PackedIndex {
   piTop = U.listArray (0, 255) $ replicate 256 0,
   piHashes = Hashes B.empty,
   piOffsets = Offsets $ U.listArray (0, -1) [],
   piKinds = Kinds B.empty }

readPackedIndex :: FilePath -> IO (PackedIndex, Word32)
readPackedIndex path = do
   payload <- L.readFile path
   return $! runGet getIndex payload

getIndex :: Get (PackedIndex, Word32)
getIndex = do
   poolSize <- getHeader
   top <- getTops
   let len = fromIntegral $ top U.! 255
   hashes <- getHashes len
   ofs <- getOffsets len
   knd <- getKinds len
   return $! (PackedIndex {
      piTop = top,
      piHashes = hashes,
      piOffsets = ofs,
      piKinds = knd }, poolSize)

getHeader :: Get Word32
getHeader = do
   magic <- getByteString (B.length indexMagic)
   unless (magic == indexMagic) $ fail "Invalid magic in file"
   version <- getWord32le
   unless (version == 3) $ fail "Unsupported version in file"
   getWord32le

getTops :: Get TopInfo
getTops = do
   items <- replicateM 256 getWord32le
   return $! U.listArray (0, 255) items

getHashes :: Int -> Get Hashes
getHashes count = Hashes <$> getByteString (count * 20)

getOffsets :: Int -> Get Offsets
getOffsets count = do
   items <- replicateM count getWord32le
   return $! Offsets $ U.listArray (0, count-1) items

getKinds :: Int -> Get Kinds
getKinds count = Kinds <$> getByteString (count * 4)

-- Generalized extraction
extract :: Int -> Int -> B.ByteString -> B.ByteString
extract itemSpan index =
   B.take itemSpan . B.drop (itemSpan * index)

piLength :: PackedIndex -> Int
-- piLength = (`div` 20) . B.length . unHashes . piHashes
piLength idx = fromIntegral $ (piTop idx) U.! 255

getTop :: PackedIndex -> Int -> Int
getTop idx i
   | i < 0     = 0
   | otherwise = fromIntegral $ (piTop idx U.! i)

getHash :: PackedIndex -> Int -> Hash.Hash
getHash fi index = Hash.byteStringToHash $ extract 20 index $ unHashes $ piHashes fi

getOffset :: PackedIndex -> Int -> Word32
getOffset fi index = (unOffsets $ piOffsets fi) U.! index

getKind :: PackedIndex -> Int -> String
getKind fi index =
   let bytes = extract 4 index $ unKinds $ piKinds fi in
   cs bytes

-- Perform a binary search and return the index if it has been found.
findHash :: PackedIndex -> Hash.Hash -> Maybe Int
findHash fi key =
   loop (getTop fi (topByte - 1)) (getTop fi topByte - 1) where

      topByte = fromIntegral $ Hash.getHashByte 0 key
      loop low high =
         if low > high
            then Nothing
            else
               let mid = low + ((high - low) `div` 2) in
               case compare (getHash fi mid) key of
                  GT -> loop low (mid - 1)
                  LT -> loop (mid + 1) high
                  EQ -> Just mid

packedIndexLookup :: PackedIndex -> Hash.Hash -> Maybe (Word32, String)
packedIndexLookup fi key = do
   pos <- findHash fi key
   return (getOffset fi pos, getKind fi pos)

piWalk :: (PackedIndex -> Int -> a) -> PackedIndex -> [a]
piWalk op fi = [op fi x | x <- [0 .. piLength fi - 1]]

instance Indexer PackedIndex where
   ixKeys = piWalk getHash
   ixOffsets = piWalk getOffset
   ixKinds = piWalk getKind

   ixToList = piWalk (\fi pos -> (getHash fi pos, (getOffset fi pos, getKind fi pos)))
   ixLookup = flip packedIndexLookup
   ixInsert = error "Insert not supported for PackedIndex"

----------------------------------------------------------------------
-- A FileIndex is a combination of a packed index read from a file,
-- and a RamIndex to allow updates.

data FileIndex = FileIndex {
   fiPacked :: PackedIndex,
   fiRam    :: RamIndex }

readIndex :: FilePath -> IO (FileIndex, Word32)
readIndex path = do
   (packed, poolSize) <- readPackedIndex path
   return $ (FileIndex { fiPacked = packed, fiRam = Map.empty }, poolSize)

instance Indexer FileIndex where
   ixKeys = map (\ (k, _) -> k) . ixToList
   ixOffsets = map (\ (_, (o, _)) -> o) . ixToList
   ixKinds = map (\ (_, (_, k)) -> k) . ixToList

   ixToList fi = ixMerge (fiRam fi) (fiPacked fi)
   ixLookup key idx = ixLookup key (fiRam idx) `mplus` ixLookup key (fiPacked idx)
   ixInsert k v idx = idx { fiRam = ixInsert k v (fiRam idx) }

isIndexDirty :: FileIndex -> Bool
isIndexDirty idx = (fiRam idx) /= Map.empty
