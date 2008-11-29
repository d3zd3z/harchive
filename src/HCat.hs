----------------------------------------------------------------------
-- HCat - Dumping test main.
-- Copyright 2008, David Brown
----------------------------------------------------------------------

module Main where

-- import Control.Monad (unless)
import Data.Char (chr, ord)
import qualified Control.Exception as E
import System.Environment (getArgs)
import System.IO (openBinaryFile,
   Handle,
   hSeek,
   SeekMode(..),
   IOMode(..),
   hClose)
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import Data.Binary.Get
import Data.Int
import Data.Word
import qualified Codec.Compression.Zlib as Zlib
-- import qualified System.Console.GetOpt
import qualified Database.HDBC as SQL
import Database.HDBC.Sqlite3
import DecodeSexp
import Codec.Binary.Base64 (decode)

import HexDump

import qualified Chunk()

main :: IO ()
main = do
   args <- getArgs
   case args of
      ("show":files) -> mapM_ showFile files
      ["dbinfo"] -> dbInfo
      _ -> do
	 putStr $ "Usage: command args\n"

----------------------------------------------------------------------
dbInfo :: IO ()
dbInfo = do
   db <- connectSqlite3 "pool/pool-info.sqlite3"
   putStr $ "Backups:\n"
   stmt <- SQL.quickQuery' db "select hash from backups" []
   let answer = map ((SQL.fromSql :: SQL.SqlValue -> B.ByteString) .  head) stmt
   let showem hash = do
	 putStr . hexDump . lazify $ hash
	 location <- lookupHash db hash
	 case location of
	    Nothing -> putStrLn "Not present"
	    Just (file, offset) -> do
	       putStrLn $ "At " ++ show file ++ ", " ++ show offset
	       chunk <- poolGetChunk file offset
	       putStr . hexDump . chunkData $ chunk
	       let bdata' = decodeSexp . chunkData $ chunk
	       let bdata = either (error . show) id bdata'
	       putStrLn . show $ bdata
	       let bhash = lookupString "HASH" bdata >>= decode
	       putStrLn . show $ (fmap B.pack $ bhash)
	 putStrLn "-------------------------"
   mapM_ showem answer
   SQL.disconnect db

lookupHash :: SQL.IConnection conn => conn -> B.ByteString -> IO (Maybe (Int, Int))
lookupHash conn hash = do
   answer <- SQL.quickQuery' conn
      ("select file, offset from hashes where hash = " ++
      (blobToSql hash)) []
   case answer of
      [[file, offset]] ->
	 return $ Just (SQL.fromSql file, SQL.fromSql offset)
      [] -> return Nothing
      _ -> fail "Multiple hash entries in database"

-- Ugh.  Nobody seems to actually use blobs in databases, very
-- strange.  To work around this, we can encode the blobs directly in
-- the insert and queries.  The replies do seem to survive with nulls
-- at least.
blobToSql :: B.ByteString -> String
blobToSql =
   ("X'"++) . (++"'") . concat . map (padHex 2) . B.unpack

----------------------------------------------------------------------

{-
main :: IO ()
main = do
   args <- getArgs
   case args of
      [name] -> showFile name
      _ -> do
	 putStr $ "Usage: hcat filename\n"
-}

poolPrefix, poolSuffix :: String
poolPrefix = "pool/pool-data-"
poolSuffix = ".data"

poolFile :: Int -> String
poolFile num =
   poolPrefix ++ num4 ++ poolSuffix
   where
      num4 = replicate (4 - length digits) '0' ++ digits
      digits = show num

poolGetChunk :: Int -> Int -> IO Chunk
poolGetChunk file offset = do
   fd <- openBinaryFile (poolFile file) ReadMode
   chunk <- readChunk fd (fromIntegral offset)
   hClose fd
   return chunk

showFile :: String -> IO ()
showFile path = do
   fd <- openBinaryFile path ReadMode
   chunk <- readChunk fd 0
   putStrLn $ "Chunk: " ++ show chunk
   hClose fd

readChunk :: Handle -> Integer -> IO Chunk
readChunk fd pos = do
   hSeek fd AbsoluteSeek pos
   rawHeader <- B.hGet fd 48
   let header = case runGet getHeader $ L.fromChunks [rawHeader] of
	 Left message -> error message
	 Right h -> h
   payload <- B.hGet fd (fromIntegral . hCLen $ header)
   let lazyPayload = L.fromChunks [payload]
   let uclen = hUCLen header
   if uclen == 0xFFFFFFFF
      then return $ Uncompressed (hKind header) lazyPayload
      else return $ Compressed (hKind header) lazyPayload
	    (fromIntegral uclen)

----------------------------------------------------------------------
-- A single Chunk of data from the pool.  Uncompressed is just a
-- 'kind' and the bytes of payload.  The Compressed version contains a
-- length of the uncompressed size.

data Chunk
   = Uncompressed String L.ByteString
   | Compressed String L.ByteString Int32

instance Show Chunk where
   show ch@(Uncompressed kind payload) =
      "Uncompressed \"" ++ kind ++ "\" " ++ show (L.length payload) ++
	 " bytes\n" ++ init (terseChunkHex ch)
   show ch@(Compressed kind payload len) =
      "Compressed \"" ++ kind ++ "\" " ++ show len ++
	 " bytes (" ++ show (L.length payload) ++ " compressed)\n" ++
	 init (terseChunkHex ch)

chunkKind :: Chunk -> String
chunkKind (Uncompressed kind _) = kind
chunkKind (Compressed kind _ _) = kind

chunkData :: Chunk -> L.ByteString
chunkData (Uncompressed _ p) = p
chunkData (Compressed _ p ulen) =
   E.assert (L.length p == fromIntegral ulen) $ payload
   where
      payload = Zlib.decompress p

-- Get a terse hex representation.
terseChunkHex :: Chunk -> String
terseChunkHex chunk =
   unlines header ++ trailer
   where
      linified = lines . hexDump $ chunkData chunk
      (header, trailer) = case linified of
	 (_:_:_:_:_:_) -> (take 4 linified, "....\n")
	 x -> (x, "")

----------------------------------------------------------------------
-- Parsing of header.
-- This initial version uses the normal bytestring failure (which uses
-- error).  TODO: Make this more robust.

getHeader :: Get (Either String Header)
getHeader = do
   magic <- getBytes 16
   if magic /= headerMagic
      then return $ Left "Invalid header magic"
      else do
	 clen <- getWord32le
	 uclen <- getWord32le
	 kind <- getBytes 4
	 return . Right $ Header {
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
