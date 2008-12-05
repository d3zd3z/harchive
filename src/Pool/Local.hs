----------------------------------------------------------------------
-- Local Storage pools.
----------------------------------------------------------------------

module Pool.Local (
   withLocalPool
) where

import Hash
import HexDump
import Chunk
import Chunk.IO
import Pool

import qualified Database.HDBC as SQL
import qualified Database.HDBC.Sqlite3 as SQL

import Data.IntMap (IntMap)
import qualified Data.IntMap as IntMap

import Numeric
import System.Directory
import System.FilePath

import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString as B

import Control.Exception (bracket)
import Control.Concurrent
import Data.List (intercalate)
import Control.Monad.State.Strict

type LocalPool = MVar PoolState

withLocalPool :: FilePath -> (Pool -> IO a) -> IO a
-- Open a pool in the local filesystem, calling 'action' with it as an
-- argument.
withLocalPool path action = do
   validatePath path
   let dbName = path </> databaseName
   bracket (SQL.connectSqlite3 dbName) SQL.disconnect $ \db -> do
      setupSchema db
      cf <- scanDataFiles path db
      let state0 = PoolState {
	 basePath = path,
	 connection = db,
	 chunkFiles = cf,
	 lastCached = NoCache }
      pool <- newMVar state0
      action $ Pool {
	 poolGetBackups = localPoolGetBackups pool,
	 poolChunkKind = localPoolChunkKind pool,
	 poolReadChunk = localPoolReadChunk pool,
	 poolHas = localPoolHas pool }

type AtomicPoolOp a = StateT PoolState IO a

atomicLift :: LocalPool -> AtomicPoolOp a -> IO a
-- Run the AtomicPoolOp state with the contents of the pool's MVar,
-- and put the result back into the MVar.
atomicLift pool action = do
   modifyMVar pool $ \s -> do
      (x, s') <- runStateT action s
      return (s', x)

-- The storage pool contains a directory of chunk files, and an SQLite
-- database.
data PoolState = PoolState {
   basePath :: String,
   -- Since we aren't dynamically choosing different databases,
   -- there's no need to deal with polymorphism here.
   -- connection :: forall a. SQL.IConnection a => a
   connection :: SQL.Connection,
   -- The currently accessible chunkfiles, indexed by cfile number.
   chunkFiles :: IntMap ChunkFile,
   -- Last Chunk we looked up in the database.
   lastCached :: CacheResult }

-- Each time we query the database, we cache the last result.
data CacheResult
   = NoCache
   | CacheHit Hash (Int, Int, String)
   | CacheMiss Hash

----------------------------------------------------------------------

-- Query the database to get a list of the backups that have been
-- performed.  These are not returned in any particular order.
localPoolGetBackups :: LocalPool -> IO [Hash]
localPoolGetBackups pool = do
   atomicLift pool $ do
      bs <- query1 "select hash from backups" []
      return $ map byteStringToHash bs

-- Reads a chunk from the pool, if present.
localPoolReadChunk :: LocalPool -> Hash -> IO (Maybe Chunk)
localPoolReadChunk pool hash = do
   atomicLift pool $ do
      place <- lookupHash hash
      maybe (return Nothing) getChunk place
      where
	 getChunk (file, offset, _) = do
	    cfs <- gets chunkFiles
	    let cf = cfs IntMap.! file
	    liftM Just $ liftIO $ chunkRead_ cf offset

localPoolChunkKind :: LocalPool -> Hash -> IO (Maybe String)
-- Return the kind of a given chunk in the storage pool, or Nothing if
-- it is not present.
localPoolChunkKind pool hash = do
   atomicLift pool $ do
      place <- lookupHash hash
      return $ fmap thrd $ place
      where
	 thrd (_, _, x) = x

localPoolHas :: LocalPool -> Hash -> IO Bool
localPoolHas pool hash = do
   info <- localPoolChunkKind pool hash
   return $ maybe False (\_ -> True) info

lookupHash :: Hash -> AtomicPoolOp (Maybe (Int, Int, String))
-- Determine the location of the specified hash in the database.
lookupHash hash = do
   state <- get
   let cache = lastCached state
   case cache of
      (CacheHit h pos) | h == hash -> return $ Just pos
      (CacheMiss h) | h == hash -> return Nothing
      _ -> do
	 place <- query3 ("select file, offset, kind from hashes \
		     \ where hash = " ++ hashToSql hash) []
	 let place2 = maybeOne place
	 let newCache = case place2 of
	       Nothing -> CacheMiss hash
	       Just pos -> CacheHit hash pos
	 put $ state { lastCached = newCache }
	 return place2

-- These are a little tedious, since we have to spell out the types.

query1 :: (SQL.SqlType a) => String -> [SQL.SqlValue] -> AtomicPoolOp [a]
-- Perform a query where each row expects a single column result.
query1 = queryN convert1
   where
      convert1 [a] = SQL.fromSql a
      convert1 _ = error "Expecting 1 column in result"

{-
query2 :: (SQL.SqlType a, SQL.SqlType b) =>
   String -> [SQL.SqlValue] -> AtomicPoolOp [(a, b)]
-- Perform a query where each row expects two columns.
query2 = queryN convert2
   where
      convert2 [a, b] = (SQL.fromSql a, SQL.fromSql b)
      convert2 _ = error "Expecting 2 columns in result"
-}

query3 :: (SQL.SqlType a, SQL.SqlType b, SQL.SqlType c) =>
   String -> [SQL.SqlValue] -> AtomicPoolOp [(a, b, c)]
-- Perform a query where each row expects three columns.
query3 = queryN convert3
   where
      convert3 [a, b, c] = (SQL.fromSql a, SQL.fromSql b, SQL.fromSql c)
      convert3 _ = error "Expecting 3 columns in result"

queryN :: ([SQL.SqlValue] -> a) -> String -> [SQL.SqlValue] -> AtomicPoolOp [a]
-- Perform a query where the given function is applied over each
-- resulting row to produce the result.
queryN convert query values = do
   db <- gets connection
   rows <- liftIO $ SQL.quickQuery' db query values
   return $ map convert rows

{-
onlyOne :: [a] -> a
-- Reduce a query result to a single value, generating an error if
-- there is more than one result row.
onlyOne [a] = a
onlyOne _ = error "Query is expecting a single result"
-}

maybeOne :: [a] -> Maybe a
-- Reduce a query to a single value, if there is one, Nothing if no
-- rows were returned, or an error if more than 1 was returned.
maybeOne [] = Nothing
maybeOne [a] = Just a
maybeOne _ = error "Query expects zero or one result"

----------------------------------------------------------------------

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
