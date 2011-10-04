-- Managing a storage pool.

module System.Backup.Pool (
   Pool,
   openPool,
   closePool,

   -- For tests.
   decodePoolName
) where

import Control.Applicative ((<$>))
import Control.Concurrent (MVar, newMVar, modifyMVar_, withMVar)
import Control.Exception (tryJust)
import Control.Monad (ap, guard, when)
import Data.Char (isDigit)
import Data.IORef
import Data.Maybe (catMaybes, listToMaybe, maybeToList)
import Data.Word (Word32)
import Hash
import System.Backup.Chunk
import System.Backup.Chunk.IO
import System.Backup.Pool.FileIndex
import System.Backup.Pool.Metadata
import System.Directory
import System.FilePath ((</>))
import System.IO.Error (isDoesNotExistError)
import Text.Printf (printf)
import qualified System.Backup.Chunk.Store as Store
import qualified System.Log.Logger as Log

newtype Pool = Pool { unPool :: MVar PoolState }

openPool :: FilePath -> IO Pool
openPool path = do
   p <- makePoolState path
   return Pool `ap` newMVar p

withPool :: Pool -> (PoolState -> IO a) -> IO a
withPool p op = withMVar (unPool p) op

closePool :: Pool -> IO ()
closePool p = do
   modifyMVar_ (unPool p) $ \pool -> do
      files <- readIORef $ pfFiles pool
      mapM_ closeFileState files
      return $ error "Attempt to use closed pool"

instance Store.ChunkSource Pool where

   lookup key p = withPool p $ poolLookup key
   getBackups p = do
      withPool p $ \pool -> do
         safeGetBackups $ (pfBase pool) </> "metadata" </> "backups.txt"

instance Store.ChunkStore Pool where

   flush p = withPool p $ \pool -> do
      files <- readIORef $ pfFiles pool
      mapM_ flushFileState files

   insert p chunk = do
      withPool p $ \pool -> insertPool pool chunk

data PoolState = PoolState {
   pfBase :: FilePath,
   pfNewFile :: Bool,
   pfLimit   :: Word32,
   pfFiles   :: IORef [FileState],
   pfWritten :: IORef Bool }

makePoolFile :: FilePath -> Int -> FilePath
makePoolFile base num = base </> printf "pool-data-%04d.data" num

makePoolIndex :: FilePath -> Int -> FilePath
makePoolIndex base num = base </> printf "pool-data-%04d.idx" num

-- Do this to see info messages.
-- Log.updateGlobalLogger "System" (Log.setLevel Log.INFO)

----------------------------------------------------------------------

data FileState = FileState {
   fsIndex  :: FileIndex,
   fsNumber :: Int,
   fsBase   :: String,
   fsFile   :: ChunkFile }

-- Open the chunk file and possibly index.  Reindex the file if
-- necessary.
makeFileState :: FilePath -> Int -> IO FileState
makeFileState base num = do
   file <- openChunkFile (makePoolFile base num) AppendMode
   let indexName = makePoolIndex base num
   (index, ilen) <- safeReadIndex indexName
   cSize <- chunkFileSize file
   index' <- if cSize == ilen then return index else do
      reindexFile file indexName index ilen
   return $ FileState { fsIndex = index', fsFile = file, fsNumber = num, fsBase = base }

-- Close the given file state.
closeFileState :: FileState -> IO ()
closeFileState fs = do
   flushFileState fs
   chunkClose (fsFile fs)

-- Flush the given file state.
flushFileState :: FileState -> IO ()
flushFileState fs = do
   let idx = fsIndex fs
   let cfile = fsFile fs
   when (isIndexDirty idx) $ do
      size <- chunkFileSize cfile
      let name = makePoolIndex (fsBase fs) (fsNumber fs)
      writeIndex name size idx

-- Given a (possibly empty) partial index for the pool file, add the
-- rest of the file's data to the index, update the index file, and
-- return the result.
reindexFile :: ChunkFile -> FilePath -> FileIndex -> Word32 -> IO FileIndex
reindexFile cfile ipath index iSize = do
   logInfo $ "Reindexing for: " ++ show ipath
   cSize <- chunkFileSize cfile
   let loop idx pos = do
         case () of
            () | pos == cSize -> return $ idx
            () | pos > cSize -> error "Corrupted pool file"
            () | otherwise -> do
               -- TODO: Make a header-only read version of the chunk
               -- reader.
               (chunk, pos') <- chunkRead cfile pos
               let idx' = ixInsert (chunkHash chunk) (pos, chunkKind chunk) idx
               loop idx' pos'
   newIndex <- loop index iSize
   writeIndex ipath cSize newIndex
   fst <$> readIndex ipath

-- Try to read the file index, but if it doesn't exist, return an
-- empty one, with an offset of zero.
safeReadIndex :: FilePath -> IO (FileIndex, Word32)
safeReadIndex name = do
   r <- tryJust (guard . isDoesNotExistError) $ readIndex name
   return $ case r of
      Left _ -> (emptyIndex, 0)
      Right res -> res

----------------------------------------------------------------------

-- Safely read the list of backups.
safeGetBackups :: FilePath -> IO [Hash.Hash]
safeGetBackups name = do
   r <- tryJust (guard . isDoesNotExistError) $ getBackups name
   return $ case r of
      Left () -> []
      Right backs -> backs

----------------------------------------------------------------------

-- Lookup the given hash in the pool, returning the Chunk if it was
-- found.
poolLookup :: Hash.Hash -> PoolState -> IO (Maybe Chunk)
poolLookup key ps = do
   files <- readIORef $ pfFiles ps
   let mpos = listToMaybe $ concatMap (maybeToList . lookEntry) files
   case mpos of
      Nothing -> return Nothing
      Just (fs, pos) -> do
         let cfile = fsFile fs
         Just <$> chunkRead_ cfile pos
   where
      lookEntry :: FileState -> Maybe (FileState, Word32)
      lookEntry fs = do
         (pos, _) <- ixLookup key $ fsIndex fs
         return $ (fs, pos)

----------------------------------------------------------------------

insertPool :: PoolState -> Chunk -> IO ()
insertPool ps chunk = do
   ensureRoom ps (chunkLength chunk)
   (f:fs) <- readIORef (pfFiles ps)
   pos <- chunkWrite (fsFile f) chunk
   let ni = ixInsert (chunkHash chunk) (pos, chunkKind chunk) (fsIndex f)
   writeIORef (pfFiles ps) (f { fsIndex = ni } : fs)

-- Ensure there is room in the head chunk for this pool.
ensureRoom :: PoolState -> Int -> IO ()
ensureRoom ps size = do
   fs <- readIORef $ pfFiles ps
   case fs of
      [] -> makeNewPoolFile ps
      (f:_) -> do
         curSize <- chunkFileSize $ fsFile f
         let limit = pfLimit ps
         written <- readIORef $ pfWritten ps
         when ((pfNewFile ps && not written) || (curSize + fromIntegral size > limit)) $ do
            makeNewPoolFile ps
   writeIORef (pfWritten ps) True

-- Make a new pool file.
makeNewPoolFile :: PoolState -> IO ()
makeNewPoolFile ps = do
   files <- readIORef $ pfFiles ps
   let num = newNum files
   state <- makeFileState (pfBase ps) num
   writeIORef (pfFiles ps) (state : files)
   where
      newNum [] = 0
      newNum (FileState { fsNumber = n }:_) = n + 1

----------------------------------------------------------------------

-- Construct a new pool state out of an existing pool or an empty
-- directory.
makePoolState :: FilePath -> IO PoolState
makePoolState base = do
   valid <- validPoolDir base
   case valid of
      Left msg -> ioError $ userError msg
      Right pfiles -> do
         createDirectoryIfMissing False $ base </> "metadata"
         props <- safeGetProps base
         files <- mapM (makeFileState base) pfiles >>= newIORef
         written <- newIORef False
         return $ PoolState {
            pfBase = base,
            pfNewFile = getNewFile props,
            pfLimit = getLimit props,
            pfFiles = files,
            pfWritten = written }

safeGetProps :: FilePath -> IO Metadata
safeGetProps base = do
   let mfile = base </> "metadata" </> "props.txt"
   r <- tryJust (guard . isDoesNotExistError) $ readPropertyFile mfile
   case r of
      Left _ -> do
         uuid <- generateUUID
         let m = setUUID uuid emptyProperties
         writePropertyFile mfile m
         return m
      Right m -> return m

----------------------------------------------------------------------
-- Validate that the given pool directory is a valid directory.
-- We consider a pool directory to be valid if it is either empty,
-- save a possible 'metadata' subdirectory, or contains at least one
-- pool file.  Returns (Left msg) if the pool is invalid, or (Right
-- [String]) for a list of the pool file names in the directory.
validPoolDir :: FilePath -> IO (Either String [Int])
validPoolDir path = do
   isDir <- doesDirectoryExist path
   if not isDir then return (Left "Pool path is not a directory")
      else do
         names <- getDirectoryContents path
         return $ validPoolNames names

-- Validate a list of names, without the dots.
validPoolNames :: [FilePath] -> Either String [Int]
validPoolNames names =
   let names' = filter notDot names in
   let datas = catMaybes $ map decodePoolName names' in
   let hasMeta = "metadata" `elem` names' in
   if names' == [] || names' == ["metadata"] ||
         (not (null datas) && hasMeta)
      then Right datas
      else Left "Does not appear to be a valid pool directory"
   where
      notDot "." = False
      notDot ".." = False
      notDot _ = True

-- TODO: Decide how to handle missing or extra pool names.
-- It's probably better to allow more flexible pool names, but other
-- implementations of this code don't handle that yet.

-- Decode a pool filename into a file number, if valid, or Nothing if
-- invalid.
decodePoolName :: String -> Maybe Int
decodePoolName ['p','o','o','l','-','d','a','t','a','-',
                a,b,c,d,'.','d','a','t','a']
   | isDigit a && isDigit b && isDigit c && isDigit d
      = Just $ read [a,b,c,d]
decodePoolName _ = Nothing

----------------------------------------------------------------------
-- Advisory locking on the pool.
-- The Java/Scala version seems to use the F_SETLK fcntl, which we
-- have available through the posix binding.

-- Perform the given IO operation with the given named file
-- exclusively locked.
{- TODO: Add locking support
withLock :: FilePath -> IO a -> IO a
withLock path op = do
   fd <- openFd path ReadWrite (Just 0o644) defaultFileFlags
   setLock fd (WriteLock, AbsoluteSeek, 0, maxBound)
   finally op $ do
      setLock fd (Unlock, AbsoluteSeek, 0, maxBound)
-}

logInfo :: String -> IO ()
logInfo = Log.infoM "System.Backup.Pool"
