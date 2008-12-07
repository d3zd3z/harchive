----------------------------------------------------------------------
-- Local pool database management.
----------------------------------------------------------------------

module Pool.Local.DB (
   DB, fromSql, toSql,
   withDatabase,
   commit,

   -- To be removed.
   SqlValue, quickQuery', SqlType,
   quickQuery, handleSql,

   schema, schemaHash,
   blobToSql, hashToSql
) where

import HexDump (padHex)
import Hash

import Database.HDBC as Sql
import Database.HDBC.Sqlite3 as Sql

import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString as B

import Control.Exception (bracket)
import Data.List (intercalate)

-- Export the Connection type.
type DB = Sql.Connection

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

-- TODO: Move the database stuff to a separate module possibly a
-- separate monad.
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
