----------------------------------------------------------------------
-- HexDumping utility.
----------------------------------------------------------------------

module HexDump (hexDump) where

import qualified Data.ByteString.Lazy as L
import Data.List (intercalate)
import Data.Char (isPrint, isAscii, chr)
import Numeric (showHex)

-- Convert a chunk of bytestring data into a "nice" hex representation.
hexDump :: L.ByteString -> String
hexDump = hexDump' 0

hexDump' :: Integer -> L.ByteString -> String
hexDump' addr d | L.length d == 0 && addr > 0 = ""
hexDump' addr d =
   padHex 8 addr ++ "  " ++
      rpad 23 ' ' (hexify left) ++ "  " ++
      rpad 23 ' ' (hexify right) ++ "  |" ++
      rpad 8 ' ' (asciify left) ++ " " ++
      rpad 8 ' ' (asciify right) ++ "|\n" ++
      hexDump' (addr + 16) rest
   where
      (heads, rest) = L.splitAt 16 d
      nums = L.unpack heads
      (left, right) = splitAt 8 nums

      hexify = intercalate " " . map (padHex 2)
      asciify = map (safeChar . chr . fromIntegral)

padHex :: (Integral a) => Int -> a -> String
padHex len num = padding ++ hex
   where
      hex = showHex num ""
      padding = replicate (len - length hex) '0'

safeChar :: Char -> Char
safeChar c | isPrint c && isAscii c = c
safeChar _ = '.'

rpad :: Int -> Char -> String -> String
rpad len ch text = text ++ replicate (len - length text) ch
