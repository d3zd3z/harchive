----------------------------------------------------------------------
-- Local pool database management.
----------------------------------------------------------------------

module DB (
   DB, fromSql, toSql,
   withDatabase,
   commit,
   setupSchema, checkSchema, SchemaStatus(..),

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
-- Possible kinds of Schema Queries.
data SchemaStatus
   = EmptySchema
   | CorrectSchema
   | IncorrectSchema
   | OtherSchemaError

checkSchema :: DB -> [String] -> IO SchemaStatus
-- Check that the schema is correct.  Returns 'Left' with information,
-- or 'Right ()' if all is well.
checkSchema db schema = do
   rows <- handleSql (const $ return Nothing) $ do
      r <- quickQuery' db
	 "select value from config where key = 'schema_hash'" []
      return $ Just r
   return $ case rows of
      Nothing -> EmptySchema
      Just [] -> OtherSchemaError
      Just ((sHash:_):_) ->
	 let
	    hash = byteStringToHash $ fromSql $ sHash
	 in if hash == schemaHash schema
	       then CorrectSchema
	       else IncorrectSchema
      _ -> OtherSchemaError

setupSchema :: DB -> [String] -> IO ()
-- Check the schema of this database by trying to query for the config
-- value.
setupSchema db schema = do
   state <- checkSchema db schema
   case state of
      EmptySchema -> createSchema db schema
      IncorrectSchema -> fail "Schema hash mismatch, TODO: implement upgrade"
      CorrectSchema -> return ()
      OtherSchemaError -> fail "Unexpected query result"

createSchema :: DB -> [String] -> IO ()
-- Create the initial database schema, asuming a blank slate.
createSchema db schema = do
   forM_ schema $ \item -> do
      quickQuery db item []
   query0 db ("insert into config values('schema_hash'," ++
      hashToSql (schemaHash schema) ++ ")") []
   commit db

schemaHash :: [String] -> Hash
schemaHash schema = hashOf combined
   where
      combined = L.pack . (map $ fromIntegral . fromEnum) $ combinedString
      combinedString = intercalate ";" (schema ++ [""])
