----------------------------------------------------------------------
-- Binary property lists.
----------------------------------------------------------------------

module System.Backup.BinProp (
   getBinProp, putBinProp,
   makePropPrefix,
   encodeBinProp, decodeBinProp,
   Properties
) where

import Data.Bits
import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC
import qualified Data.ByteString.Lazy as L
import qualified Data.Map as M
import Data.Binary.Get
import Data.Binary.Put
import Data.Time (getCurrentTime, formatTime)
import System.Locale (defaultTimeLocale)

type Properties = M.Map String String

-- | Extract the binary properties from the beginning of this
-- ByteString, returning the property list contained in the
-- header and the remainder of the ByteString with the hashmap.
decodeBinProp :: L.ByteString -> (Properties, L.ByteString)
decodeBinProp src = (mapping, rest)
   where
      (mapping, rest, _) = runGetState getBinProp src 0

-- TODO: Do we need to support UTF-8 here?
getBinProp :: Get Properties
getBinProp = do
   len <- getWord32be
   payload <- getByteString $ fromIntegral len
   skip $ fromIntegral $ (-len - 4) .&. 4095
   let raw = M.fromList $ toPairs $ lines $ BC.unpack payload
   return raw

-- Index files have a fairly special restricted version where the
-- names are simple strings mapped to Int32's.
toPairs :: [String] -> [(String, String)]
toPairs [] = []
toPairs (('#':_):rs) = toPairs rs
toPairs (line:rs) = case break (== '=') line of
   (key, _:val) -> (key, val) : toPairs rs
   _ -> error "Invalid property"

----------------------------------------------------------------------
-- The Java properties file contains a timestamp, which is useful for
-- diagnosis.  This header can be prepended to the data if desired.
makePropPrefix :: IO String
makePropPrefix = do
   now <- getCurrentTime
   return $ formatTime defaultTimeLocale "#Harchive index file\n#%c\n" now

encodeBinProp :: String -> Properties -> L.ByteString
encodeBinProp prefix = runPut . putBinProp prefix

putBinProp :: String -> Properties -> Put
putBinProp prefix props = do
   let str = BC.pack $ prefix ++ encodeMap props
   let len = fromIntegral $ BC.length str
   let pad = (-len - 4) .&. 4095
   putWord32be len
   putByteString str
   putByteString $ B.replicate (fromIntegral pad) 0

-- Convert a mapping back to the strings containing the mapping.
-- TODO: This does no checking for escapes or other rules that are
-- used, so these need to be simple strings with no special characters
-- in them.
encodeMap :: Properties -> String
encodeMap = concatMap (\(k,v) -> k ++ "=" ++ v ++ "\n") . M.toList

-- Debug.
{-
gread :: IO (Properties, L.ByteString)
gread = do
   src <- L.readFile "data-index-0128"
   return $ decodeBinProp src
-}
