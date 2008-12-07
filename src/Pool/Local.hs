----------------------------------------------------------------------
-- Local Storage pools.
----------------------------------------------------------------------

module Pool.Local (
   withLocalPool,
   LocalPool,
   getPoolLimit, setPoolLimit,
   module Pool
) where

import Hash
import Chunk
import Chunk.IO
import Pool
import Pool.Local.DB

import Data.IntMap (IntMap)
import qualified Data.IntMap as IntMap

import Numeric
import System.Directory
import System.FilePath

import Control.Concurrent
import Data.Maybe (fromMaybe)
import Control.Monad.State.Strict

newtype LocalPool = LocalPool { theLocalPool :: MVar PoolState }

withLocalPool :: FilePath -> (LocalPool -> IO a) -> IO a
-- Open a pool in the local filesystem, calling 'action' with it as an
-- argument.
withLocalPool path action = do
   validatePath path
   let dbName = path </> databaseName
   withDatabase dbName $ \db -> do
      setupSchema db
      storedLimit <- queryLimit db
      cf <- scanDataFiles path db
      let state0 = PoolState {
	 basePath = path,
	 connection = db,
	 chunkFiles = cf,
	 lastCached = NoCache,
	 -- Use the same default as the lisp code, if the limit has
	 -- not been set.
	 chunkFileLimit = fromMaybe (640 * 1024 * 1024) storedLimit }
      pool <- newMVar state0
      action $ LocalPool pool

type AtomicPoolOp a = StateT PoolState IO a

atomicLift :: LocalPool -> AtomicPoolOp a -> IO a
-- Run the AtomicPoolOp state with the contents of the pool's MVar,
-- and put the result back into the MVar.
atomicLift pool action = do
   modifyMVar (theLocalPool pool) $ \s -> do
      (x, s') <- runStateT action s
      return (s', x)

-- The storage pool contains a directory of chunk files, and an SQLite
-- database.
data PoolState = PoolState {
   basePath :: String,
   -- Since we aren't dynamically choosing different databases,
   -- there's no need to deal with polymorphism here.
   -- connection :: forall a. IConnection a => a
   connection :: DB,
   -- The currently accessible chunkfiles, indexed by cfile number.
   chunkFiles :: IntMap ChunkFile,
   -- Last Chunk we looked up in the database.
   lastCached :: CacheResult,
   -- Maximum size in bytes we strive for for a chunk file.
   chunkFileLimit :: Int }

-- Each time we query the database, we cache the last result.
data CacheResult
   = NoCache
   | CacheHit Hash (Int, Int, String)
   | CacheMiss Hash

----------------------------------------------------------------------

instance ChunkQuerier LocalPool where
   poolGetBackups = localPoolGetBackups
   poolChunkKind = localPoolChunkKind

instance ChunkReader LocalPool where
   poolReadChunk = localPoolReadChunk

instance ChunkWriter LocalPool where
   poolWriteChunk = localPoolWriteChunk
   poolFlush = localPoolFlush

instance ChunkReaderWriter LocalPool where {}

-- Query the database to get a list of the backups that have been
-- performed.  These are not returned in any particular order.
localPoolGetBackups :: LocalPool -> IO [Hash]
localPoolGetBackups pool = do
   atomicLift pool $ do
      db <- gets connection
      bs <- liftIO $ query1 db "select hash from backups" []
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

localPoolWriteChunk :: LocalPool -> Chunk -> IO ()
-- Write the specified chunk to the storage pool.
localPoolWriteChunk pool chunk = do
   atomicLift pool $ do
      let hash = chunkHash chunk
      place <- lookupHash hash
      maybe writeIt (const $ return ()) place
      where
	 writeIt = do
	    (num, cfile) <- prepareWrite $ chunkStoreEstimate chunk
	    offset <- liftIO $ chunkWrite cfile chunk
	    db <- gets connection
	    liftIO $ query0 db ("insert into hashes values (" ++
	       hashToSql (chunkHash chunk) ++
	       ", ?, ?, ?)")
	       [toSql $ chunkKind chunk,
		  toSql num, toSql offset]
	    newSize <- liftIO $ chunkFileSize cfile
	    liftIO $ query0 db "insert or replace into chunk_files values (?,?)"
	       [toSql num, toSql newSize]

localPoolFlush :: LocalPool -> IO ()
-- Make sure all data is written out.
localPoolFlush pool = do
   atomicLift pool $ do
      -- Order is important here.  First, close all of the chunk
      -- files, and then commit the database.
      cfs <- gets chunkFiles
      let cfsChunks = IntMap.elems cfs
      mapM_ (\c -> liftIO $ chunkClose c) cfsChunks

      db <- gets connection
      liftIO $ commit db

getPoolLimit :: LocalPool -> IO Int
getPoolLimit pool = do
   atomicLift pool $ do
      gets chunkFileLimit

setPoolLimit :: LocalPool -> Int -> IO ()
setPoolLimit pool limit = do
   atomicLift pool $ do
      modify $ \state -> state { chunkFileLimit = limit }
      db <- gets connection
      liftIO $ do
	 query0 db "delete from config where key = 'file_limit'" []
	 query0 db "insert into config values('file_limit',?)"
	    [toSql limit]

prepareWrite :: Int -> AtomicPoolOp (Int, ChunkFile)
-- Determine (or create) a chunkfile appropriate for writing a chunk
-- of the given size to.  Will update the state with the new chunkfile
-- in the event one is created.  Returns the chunkfile index, and the
-- new cfile.
prepareWrite size = do
   cfs <- gets chunkFiles
   case IntMap.size cfs of
      0 -> do
	 cfile <- makeNewCFile 0
	 return (0, cfile)
      n -> do
	 let cfile = cfs IntMap.! (n-1)
	 cfSize <- liftIO $ chunkFileSize cfile
	 limit <- gets chunkFileLimit
	 if cfSize + size + 64 < limit
	    then return (n-1, cfile)
	    else do
	       liftIO $ chunkClose cfile
	       newCFile <- makeNewCFile n
	       return (n, newCFile)

makeNewCFile :: Int -> AtomicPoolOp ChunkFile
-- Create a new chunkfile of the given index.
makeNewCFile index = do
   cfs <- gets chunkFiles
   path <- gets basePath
   let name = dataFileName path index
   cfile <- liftIO $ openChunkFile name
   let cfs' = IntMap.insert index cfile cfs
   modify $ \st -> st { chunkFiles = cfs' }
   return cfile

{-
localPoolHas :: LocalPool -> Hash -> IO Bool
localPoolHas pool hash = do
   info <- localPoolChunkKind pool hash
   return $ maybe False (\_ -> True) info
-}

lookupHash :: Hash -> AtomicPoolOp (Maybe (Int, Int, String))
-- Determine the location of the specified hash in the database.
lookupHash hash = do
   state <- get
   let cache = lastCached state
   case cache of
      (CacheHit h pos) | h == hash -> return $ Just pos
      (CacheMiss h) | h == hash -> return Nothing
      _ -> do
	 db <- gets connection
	 place <- liftIO $ query3 db ("select file, offset, kind from hashes " ++
	    "where hash = " ++ hashToSql hash) []
	 let place2 = maybeOne place
	 let newCache = case place2 of
	       Nothing -> CacheMiss hash
	       Just pos -> CacheHit hash pos
	 put $ state { lastCached = newCache }
	 return place2

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
scanDataFiles :: FilePath -> DB -> IO (IntMap ChunkFile)
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

queryCfiles :: DB -> IO [Int]
-- Retrieve the sizes of each chunk file from the database.  Verifies
-- that the files are present and in order.
queryCfiles db = do
   rows <- quickQuery' db
      "select num, size from chunk_files order by num" []
   return $ flattenSizes 0 rows
   where
      flattenSizes :: Int -> [[SqlValue]] -> [Int]
      -- ensure that the query result from the database is sane.
      flattenSizes _ [] = []
      flattenSizes n ([n2, size]:xs) =
	 if n == n2'
	    then fromSql size : flattenSizes (n+1) xs
	    else error "Unexpected sequence in chunk_files table"
	 where n2' = fromSql n2
      flattenSizes _ _ = error "Unexpected result from chunk_files query"

queryLimit :: DB -> IO (Maybe Int)
-- Query the database for a config entry that might define the size
-- limit of the pool files, and return it if found.
queryLimit db = do
   rows <- quickQuery' db
      "select value from config where key = 'file_limit'" []
   return $ (fmap fromSql) $ (fmap head) $ maybeOne rows

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
   quickQuery db ("insert into config values('schema_hash'," ++
      hashToSql schemaHash ++ ")") []
   commit db
