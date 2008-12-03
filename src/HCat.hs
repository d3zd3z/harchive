----------------------------------------------------------------------
-- HCat - Dumping test main.
-- Copyright 2008, David Brown
----------------------------------------------------------------------

module Main where

import System.Environment (getArgs)
import qualified Data.ByteString as B
import qualified Database.HDBC as SQL
import Database.HDBC.Sqlite3
import DecodeSexp
import Codec.Binary.Base64 (decode)

import HexDump

import Chunk
import Chunk.IO

main :: IO ()
main = do
   args <- getArgs
   case args of
      -- ("show":files) -> mapM_ showFile files
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
	       let bdata' = decodeAlist 0 . chunkData $ chunk
	       let bdata = either (error . show) id bdata'
	       putStrLn . show $ bdata
	       let bhash = lookupString "HASH" (snd bdata) >>= decode
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
   cfile <- openChunkFile (poolFile file)
   chunk <- chunkRead_ cfile offset
   chunkClose cfile
   return chunk
