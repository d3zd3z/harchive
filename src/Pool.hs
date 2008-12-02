{- # LANGUAGE PolymorphicComponents # -}
----------------------------------------------------------------------
-- Storage pools.
----------------------------------------------------------------------

module Pool (
   thump,
   StoragePool,
   runPool,
   liftIO
) where

import Hash
import HexDump
import Chunk.IO

import qualified Database.HDBC as SQL
import qualified Database.HDBC.Sqlite3 as SQL

import Data.IntMap (IntMap)
import qualified Data.IntMap as IntMap

import Numeric
import System.Directory
import System.FilePath

import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString as B

import Control.Concurrent
import Control.Exception (bracket)
import Control.Monad.Reader
import Control.Monad.State.Strict
import Data.List (intercalate)

type StoragePool a = ReaderT (MVar PoolState) IO a

-- The storage pool contains a directory of chunk files, and an SQLite
-- database.
data PoolState = PoolState {
   basePath :: String,
   -- Since we aren't dynamically choosing different databases,
   -- there's no need to deal with polymorphism here.
   -- connection :: forall a. SQL.IConnection a => a
   connection :: SQL.Connection,
   -- The currently accessible chunkfiles, indexed by cfile number.
   chunkFiles :: IntMap ChunkFile }

-- |Perform the given actions on the pool at the specified path.
runPool :: FilePath -> StoragePool a -> IO a
runPool path actions = do
   -- TODO: Verify the pool path.
   validatePath path
   let dbName = path </> databaseName
   bracket (SQL.connectSqlite3 dbName) SQL.disconnect $ \db -> do
      setupSchema db
      cf <- scanDataFiles path db
      let state0 = PoolState {
	 basePath = path,
	 connection = db,
	 chunkFiles = cf }
      mvar <- newMVar state0
      result <- runReaderT fullActions mvar
      return result
   where
      fullActions = actions

validatePath :: FilePath -> IO ()
-- Perform a series of validations on the directory specified for the
-- pool, in hopes of ensuring that this is really either a storage
-- pool, or a nice empty fresh directory.
validatePath path = do
   path' <- case reverse path of
      [] -> fail "Pool path argument must not be empty string"
      '/':p -> return $ reverse p
      _ -> return path
   isDir <- doesDirectoryExist path'
   unless isDir $ fail "Pool path argument does not name existing directory"

   -- The directory must either be empty, or contain the database
   -- file.
   contents <- getDirectoryContents path'
   case noDotty contents of
      [] -> return ()
      c | databaseName `elem` c -> return ()
      _ -> fail "Pool path specified is not empty, and doesn't appear to be a pool"

----------------------------------------------------------------------
noDotty :: [FilePath] -> [FilePath]
-- Eliminate Posix "." and ".." filenames from a directory list.
-- TODO: This will move elsewhere.
noDotty = filter $ \item -> item /= "." && item /= ".."

----------------------------------------------------------------------
scanDataFiles :: FilePath -> SQL.Connection -> IO (IntMap ChunkFile)
-- Scan the data directory for data files, verifying that they are in
-- the database, and perform recovery of the chunk information into
-- the database.  Returns the chunkfile map.
-- TODO: Recovery rather than just detection.
scanDataFiles path db = do
   cfileSizes <- queryCfiles db
   let expectedSizes = map Just cfileSizes ++ repeat Nothing
   let names = map (dataFileName path) [0..]
   cfiles <- whileM scanFile (zip expectedSizes names)
   return $ IntMap.fromList $ zip [0..] cfiles

scanFile :: (Maybe Int, FilePath) -> IO (Maybe ChunkFile)
-- Check a single pool data file, comparing it's size to the expected
-- size.  Returns False when we should stop scanning.
scanFile (expectedSize, path) = do
   present <- doesFileExist path
   case (present, expectedSize) of
      (False, Just _) -> fail $ "Missing pool file: " ++ path
      (False, Nothing) -> return Nothing
      (True, Nothing) -> fail $ "Handle pool file not in DB: " ++ path
      (True, Just size) -> do
	 cfile <- openChunkFile path
	 size' <- chunkFileSize cfile
	 chunkClose cfile
	 if size == size'
	    then return $ Just cfile
	    else fail $ "Pool size is wrong, need to recover: " ++ path

{-
whileM_ :: (a -> IO Bool) -> [a] -> IO ()
-- For each element in the list, invoke the action on it, stopping
-- when the action results in False.
whileM_ _ [] = return ()
whileM_ action (x:xs) = do
   continue <- action x
   if continue
      then whileM_ action xs
      else return ()
-}

whileM :: (a -> IO (Maybe b)) -> [a] -> IO [b]
-- Invoke the action on each element of the list, accumulating the
-- results as long as the action returns Just something.
whileM _ [] = return []
whileM action (x:xs) = do
   y <- action x
   case y of
      Nothing -> return []
      Just y' -> do
	 ys <- whileM action xs
	 return $ y' : ys

databaseName :: String
databaseName = "pool-info.sqlite3"

dataFileName :: FilePath -> Int -> FilePath
-- Construct the name of an individual datafile.
dataFileName base index =
   base </> "pool-data-" ++ padded ++ ".data"
   where
      padded = replicate (4 - length decimal) '0' ++ decimal
      decimal = showInt index ""

----------------------------------------------------------------------
queryCfiles :: SQL.Connection -> IO [Int]
-- Retrieve the sizes of each chunk file from the database.  Verifies
-- that the files are present and in order.
queryCfiles db = do
   rows <- SQL.quickQuery' db
      "select num, size from chunk_files order by num" []
   return $ flattenSizes 0 rows
   where
      flattenSizes :: Int -> [[SQL.SqlValue]] -> [Int]
      -- ensure that the query result from the database is sane.
      flattenSizes _ [] = []
      flattenSizes n ([n2, size]:xs) =
	 if n == n2'
	    then SQL.fromSql size : flattenSizes (n+1) xs
	    else error "Unexpected sequence in chunk_files table"
	 where n2' = SQL.fromSql n2
      flattenSizes _ _ = error "Unexpected result from chunk_files query"

----------------------------------------------------------------------
setupSchema :: SQL.Connection -> IO ()
-- Check the schema of this database by trying to query for the config
-- value.
setupSchema db = do
   rows <- SQL.handleSql (const $ return Nothing) $ do
      r <- SQL.quickQuery' db
	 "select value from config where key = 'schema_hash'" []
      return $ Just r
   case rows of
      Nothing -> do
	 putStrLn "Creating schema"
	 createSchema db
      Just [] ->
	 -- Unexpected case.  Database has the row, but no schema_hash
	 -- added to it.  Probably some other database present.
	 fail "The database file appears unexpected"
      Just ((sHash:_):_) -> do
	 let hash = byteStringToHash $ SQL.fromSql $ sHash
	 if hash == schemaHash
	    then return ()
	    else fail "Schema hash mismatch, TODO: implement upgrade"
      _ -> fail "Unexpected query result"

createSchema :: SQL.Connection -> IO ()
-- Create the initial database schema, asuming a blank slate.
createSchema db = do
   forM_ schema $ \item -> do
      SQL.quickQuery db item []
   SQL.quickQuery db ("insert into config values('schema_hash'," ++
      hashToSql schemaHash ++ ")") []
   SQL.commit db

blobToSql :: B.ByteString -> String
blobToSql = ("X'"++) . (++"'") . concat . map (padHex 2) . B.unpack

hashToSql :: Hash -> String
hashToSql = blobToSql . toByteString

----------------------------------------------------------------------
-- TODO: Move the database stuff to a separate module possibly a
-- separate monad.
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

-- Debugging stuff.
thump :: IO ()
thump = do
   db <- SQL.connectSqlite3 "pool/pool-info.sqlite3"
   answer <- SQL.quickQuery' db
      "select value from config where key = 'schema_hash'" []
   let clean = map ((SQL.fromSql :: SQL.SqlValue -> B.ByteString) . head) answer
   putStrLn $ "Answer: " ++ show clean
   putStrLn $ "Schema hash: " ++ toHex schemaHash
   SQL.disconnect db
