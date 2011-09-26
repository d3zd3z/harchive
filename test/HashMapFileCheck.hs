----------------------------------------------------------------------

module HashMapFileCheck (hashMapFileCheck) where

import qualified Hash
import System.Backup.HashMap.File

import Test.HUnit
import Data.Bits (xor)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy.Char8 as LC
import qualified Data.ByteString.Lazy as L
import qualified Data.Map as M
import Data.Binary.Put (runPut, putWord32be, Put)
import Data.Binary.Get (Get, getWord32be)

hashMapFileCheck :: Test
hashMapFileCheck = test $ do
   let raw1 = makeMap [1..100]
   let look1 = lookuper raw1
   mapM_ (testKey look1) [1..100]
   let raw2 = makeMap [101..200]
   let look2 = lookuper raw2
   mapM_ (testKey look2) [101..200]
   let merged = mapMerge [hmToAscList getInt raw1, hmToAscList getInt raw2]
   let bigRaw = runPut $ putOrderedMap putInt merged
   let bigLook = lookuper bigRaw
   mapM_ (testKey bigLook) [1..200]

testKey :: (Hash.Hash -> Maybe Int) -> Int -> Assertion
testKey look key = do
   let k = iKey key
   look k @=? Just key
   look (hashMangle k) @=? Nothing

hashMangle :: Hash.Hash -> Hash.Hash
hashMangle = Hash.byteStringToHash . frob . Hash.toByteString
   where
      frob bs = B.snoc (B.init bs) (B.last bs `xor` 1)

iKey :: Int -> Hash.Hash
iKey = Hash.hashOf . LC.pack. show

makeMap :: [Int] -> L.ByteString
makeMap = runPut . putHashMap putInt . M.fromList . map (\x -> (iKey x, x))

lookuper :: L.ByteString -> Hash.Hash -> Maybe Int
lookuper = makeLookup 4 getInt

putInt :: Int -> Put
putInt = putWord32be . fromIntegral

getInt :: Get Int
getInt = fromIntegral `fmap` getWord32be
