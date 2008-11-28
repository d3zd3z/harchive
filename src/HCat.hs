----------------------------------------------------------------------
-- HCat - Dumping test main.
-- Copyright 2008, David Brown
----------------------------------------------------------------------

module Main (main) where

-- import Control.Monad (unless)
import Data.Char (chr, ord)
import System.Environment (getArgs)
import System.IO (openBinaryFile,
   IOMode(..),
   hClose)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import Data.Binary.Get
-- import Data.Int
import Data.Word

main :: IO ()
main = do
   args <- getArgs
   case args of
      [name] -> showFile name
      _ -> do
	 putStr $ "Usage: hcat filename\n"

showFile :: String -> IO ()
showFile path = do
   fd <- openBinaryFile path ReadMode
   content <- B.hGet fd 48
   let header = runGet getHeader $ L.fromChunks [content]
   putStr $ "Payload: " ++ show header ++ "\n"
   hClose fd

----------------------------------------------------------------------
-- Parsing of header.
-- This initial version uses the normal bytestring failure (which uses
-- error).  TODO: Make this more robust.

getHeader :: Get Header
getHeader = do
   magic <- getBytes 16
   if magic /= headerMagic
      then fail "Invalid header magic"
      else do
	 clen <- getWord32le
	 uclen <- getWord32le
	 kind <- getBytes 4
	 return $ Header {
	    hCLen = clen,
	    hUCLen = uclen,
	    hKind = asString kind }

headerMagic :: B.ByteString
headerMagic = B.pack $ (map $ fromIntegral . ord) "adump-pool-v1.1\n"

asString :: B.ByteString -> String
asString = (map $ chr . fromIntegral) . B.unpack

data Header = Header {
   hCLen :: Word32,
   hUCLen :: Word32,
   hKind :: String }
   deriving (Eq, Show)
