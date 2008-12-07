----------------------------------------------------------------------
-- Local pool database management.
----------------------------------------------------------------------

module Pool.Local.DB (
   DB, fromSql, toSql,
   withDatabase,
   commit,
   setupSchema,

   query0, query1, query2, query3, queryN,

   blobToSql, hashToSql
) where

import HexDump (padHex)
import Hash

import Database.HDBC
import Database.HDBC.Sqlite3

import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString as B

import Control.Exception (assert, bracket)
import Control.Monad (forM_)
import Data.List (intercalate)

-- Export the Connection type.
type DB = Connection

blobToSql :: B.ByteString -> String
blobToSql = ("X'"++) . (++"'") . concat . map (padHex 2) . B.unpack

hashToSql :: Hash -> String
hashToSql = blobToSql . toByteString

----------------------------------------------------------------------
withDatabase :: FilePath -> (DB -> IO a) -> IO a
-- Evaluate 'action' with an SQLite3 database opened.
withDatabase fileName action =
   bracket (connectSqlite3 fileName) disconnect action

----------------------------------------------------------------------
query0 :: DB -> String -> [SqlValue] -> IO ()
-- Perform a query expecting no results.
query0 db query values = do
   rows <- quickQuery' db query values
   assert (length rows == 0) $
      return ()

query1 :: (SqlType a) => DB -> String -> [SqlValue] -> IO [a]
-- Perform a query where each row expects a single column result.
query1 db query values = do
   queryN db convert1 query values
   where
      convert1 [a] = fromSql a
      convert1 _ = error "Expecting 1 column in result"

query2 :: (SqlType a, SqlType b) =>
   DB -> String -> [SqlValue] -> IO [(a, b)]
-- Perform a query where each row expects two columns.
query2 db query values = do
   queryN db convert2 query values
   where
      convert2 [a, b] = (fromSql a, fromSql b)
      convert2 _ = error "Expecting 2 columns in result"

query3 :: (SqlType a, SqlType b, SqlType c) =>
   DB -> String -> [SqlValue] -> IO [(a, b, c)]
-- Perform a query where each row expects three columns.
query3 db query values = do
   queryN db convert3 query values
   where
      convert3 [a, b, c] = (fromSql a, fromSql b, fromSql c)
      convert3 _ = error "Expecting 3 columns in result"

queryN :: DB -> ([SqlValue] -> a) -> String -> [SqlValue] -> IO [a]
-- Generalized query with row conversion.  Calls the conversion
-- function on each row of the result.
queryN db convert query values = do
   rows <- quickQuery' db query values
   return $ map convert rows


----------------------------------------------------------------------
setupSchema :: DB -> IO ()
-- Check the schema of this database by trying to query for the config
-- value.
setupSchema db = do
   rows <- handleSql (const $ return Nothing) $ do
      r <- quickQuery' db
	 "select value from config where key = 'schema_hash'" []
      return $ Just r
   case rows of
      Nothing -> do
	 -- putStrLn "Creating schema"
	 createSchema db
      Just [] ->
	 -- Unexpected case.  Database has the row, but no schema_hash
	 -- added to it.  Probably some other database present.
	 fail "The database file appears unexpected"
      Just ((sHash:_):_) -> do
	 let hash = byteStringToHash $ fromSql $ sHash
	 if hash == schemaHash
	    then return ()
	    else fail "Schema hash mismatch, TODO: implement upgrade"
      _ -> fail "Unexpected query result"

createSchema :: DB -> IO ()
-- Create the initial database schema, asuming a blank slate.
createSchema db = do
   forM_ schema $ \item -> do
      quickQuery db item []
   query0 db ("insert into config values('schema_hash'," ++
      hashToSql schemaHash ++ ")") []
   commit db

-- TODO: The config schema needs to have a unique key.
schema :: [String]
schema = [
   -- The schema needs to match the ldump schema, exactly to avoid schema mismatches.
   "create table config (key text, value text)",
   "create table devmap (uuid text unique, dev integer primary key)",
   "create table dircache (pdev integer, pino integer,\n" ++
      "\t\tino integer, ctime integer, hash blob,\n" ++
      "\t\texpire integer)",
   "create index dircache_devino on dircache(pdev, pino)",
   "create table hashes(hash blob unique, kind text, file integer,\n" ++
      "\t\toffset integer)",
   "create index hashes_hash on hashes(hash)",
   "create table chunk_files(num integer unique primary key,\n" ++
      "\t\tsize integer)",
   "create table backups(hash blob)",
   "create trigger backup_trigger after insert on hashes\n" ++
      "\t\twhen new.kind = 'back'\n" ++
      "\tbegin\n" ++
      "\t\tinsert into backups values(new.hash);\n" ++
      "\tend" ]

schemaHash :: Hash
schemaHash = hashOf combined
   where
      combined = L.pack . (map $ fromIntegral . fromEnum) $ combinedString
      combinedString = intercalate ";" (schema ++ [""])
