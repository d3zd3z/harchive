----------------------------------------------------------------------
-- Harchive driver.
----------------------------------------------------------------------

module Main (main) where

import System.Environment (getArgs)

import Chunk
import Chunk.IO
import Hash
import Status
import Progress
import Pool.Local
import Pool.Command

import Tree

import Control.Monad
import Data.Maybe
import Data.Time
import System.Locale
import Text.Printf

import Data.List
import Data.Ord

main :: IO ()
main = do
   args <- getArgs
   case args of
      ("check":files@(_:_)) ->
	 statusToIO 1 $ mapM_ runCheck files
	 -- mapM_ (statusToIO 1 . runCheck) files
      ("ncheck":files@(_:_)) -> newCheck files
      ("pool":xs) -> poolCommand xs
      ["list", path] -> withLocalPool path showBackups
      ["show", path, hash] -> withLocalPool path $ showOne (fromHex hash)
      ["walk", path, hash] -> withLocalPool path $ runWalk (fromHex hash)
      ["hashes", path, hash] -> withLocalPool path $ runHashes (fromHex hash)
      ["migrate", srcPath, dstPath, hash] ->
	 withLocalPool srcPath $ \srcPool ->
	    withLocalPool dstPath $ \dstPool ->
	       migrate srcPool dstPool (fromHex hash)
      _ ->
	 ioError (userError usage)

usage :: String
usage = "Usage: harchive check file ...\n"

----------------------------------------------------------------------

-- TODO: Limit display, and other such fancy things.

showBackups :: ChunkReader p => p -> IO ()
-- List the backups in the storage pool.
showBackups pool = do
   hashes <- poolGetBackups pool
   chunks <- mapM (poolReadChunk pool) hashes
   locally <- liftIO $ getCurrentTimeZone
   let chunks' = map fromJust chunks
   let chunkInfo = map decodeBackupInfo chunks'
   let info = zip hashes chunkInfo
   let sortedInfo = sortBy (comparing $ biEndTime . snd) info
   liftIO $ putStrLn (intercalate "\n" $ map (backupInfo locally) sortedInfo)

backupInfo :: TimeZone -> (Hash, BackupInfo) -> String
-- Nicely print the information about the backups.
backupInfo tz (hash, info) =
   toHex hash ++ " " ++
      lPad 10 (biHost info) ++ " " ++
      lPad 15 (biBackup info) ++ " " ++
      formatTime defaultTimeLocale "%F %H:%M" (utcToLocalTime tz $ biStartTime info)

lPad :: Int -> String -> String
-- Left padded and truncating version of the source string.
lPad n str = base ++ replicate (n - length base) ' '
   where base = take n str

data BackupInfo = BackupInfo {
   biHost, biDomain :: String,
   biBackup :: String,
   biStartTime, biEndTime :: UTCTime,
   biHash :: Hash,
   biInfo :: Attr }

decodeBackupInfo :: Chunk -> BackupInfo
decodeBackupInfo chunk =
   let
      getField :: (SexpType a) => String -> a
      getField = justField info
      info = decodeChunk chunk
   in
      BackupInfo {
	 biHost = getField "HOST",
	 biDomain = getField "DOMAIN",
	 biBackup = attrName info,
	 biStartTime = getField "START-TIME",
	 biEndTime = getField "END-TIME",
	 biHash = getField "HASH",
	 biInfo = info }

----------------------------------------------------------------------

showOne :: ChunkReader p => Hash -> p -> IO ()
showOne hash pool = do
   tz <- liftIO $ getCurrentTimeZone
   chunk <- liftM fromJust $ poolReadChunk pool hash
   let info = decodeBackupInfo chunk
   let sTime = formatTime defaultTimeLocale "%F %H:%M"
	 (utcToLocalTime tz $ biStartTime info)
   liftIO $ printf "Backup from %s:%s on %s\n" (biHost info)
      (biBackup info) sTime
   liftIO $ printf "Root = %s\n" (show $ biHash info)
   rootChunk <- liftM fromJust $ poolReadChunk pool (biHash info)
   liftIO $ mapM_ (printf "sexp = %s\n" . show) (decodeMultiChunk rootChunk)

----------------------------------------------------------------------

runWalk :: ChunkReader p => Hash -> p -> IO ()
runWalk hash pool = do
   chunk <- liftM fromJust $ poolReadChunk pool hash
   let info = decodeBackupInfo chunk
   nodes <- Tree.walkLazy pool $ biHash info
   forM_ nodes $ \op -> do
      case op of
	 TreeEOF -> printf "EOF\n"
	 TreeEnter {} -> do
	    printf "d %s\n" $ treeOpPath op
	 TreeLeave {} -> do
	    printf "u %s\n" $ treeOpPath op
	 TreeLink {} -> do
	    let attr = treeOpAttr op
	    printf "l %s -> %s\n" (treeOpPath op)
	       (justField attr "LINK" :: String)
	 TreeReg {} -> do
	    printf "- (%s) %s\n" (treeOpKind op) (treeOpPath op)
	 TreeOther {} -> do
	    printf "? %s\n" (treeOpPath op)

----------------------------------------------------------------------

runHashes :: ChunkReader p => Hash -> p -> IO ()
runHashes hash pool = do
   chunk <- liftM fromJust $ poolReadChunk pool hash
   let info = decodeBackupInfo chunk
   act hash
   walkHashes pool (biHash info) act
   where
      act h = do
	 putStrLn $ "Hash: " ++ show h

----------------------------------------------------------------------

migrate :: (ChunkReader srcPool, ChunkWriter dstPool) =>
   srcPool -> dstPool -> Hash -> IO ()
-- Migrate all data from srcPool to dstPool that is needed for the
-- backup referenced by hash.
migrate srcPool dstPool hash = do
   chunk <- liftM fromJust $ poolReadChunk srcPool hash
   let info = decodeBackupInfo chunk
   walkHashes srcPool (biHash info) $ \tmpHash -> do
      present <- poolHashPresent dstPool tmpHash
      unless present $ do
	 -- putStrLn $ "Hash: " ++ show tmpHash
	 tmpChunk <- liftM fromJust $ poolReadChunk srcPool tmpHash
	 poolWriteChunk dstPool tmpChunk
   poolWriteChunk dstPool chunk
   poolFlush dstPool

----------------------------------------------------------------------

runCheck :: FilePath -> StatusIO ()
runCheck path = do
   cleanLiftIO $ putStrLn $ "Checking: " ++ path

   cfile <- liftIO $ openChunkFile path
   size <- liftIO $ chunkFileSize cfile

   let
      process pos = do
	 (chunk, next) <- liftIO $ chunkRead cfile pos
	 addFile 1
	 addSavedData (fromIntegral . chunkStoreEstimate $ chunk)
	 if next < size
	    then process next
	    else return ()
   process 0

   liftIO $ chunkClose cfile

data CheckStatus = CheckStatus {
   csPath :: PathTracker,
   csBytes :: Counter,
   csTotal :: Counter,
   csChunks :: Counter,
   csTrack :: Tracker }

makeCheckProgress :: IO CheckStatus
makeCheckProgress = do
   path <- makePathTracker "???"
   bytes <- makeCounter
   total <- makeCounter
   chunks <- makeCounter
   let tracker =
	    "  " +++ kb bytes +++ " KBytes, " +++ kb total +++ " Ktotal, " +++
	    chunks +++ " chunks\n" +++
	    "   file: " +++ TrackPath path
   return $ CheckStatus { csPath = path, csBytes = bytes, csTotal = total,
      csChunks = chunks, csTrack = tracker }

newCheck :: [FilePath] -> IO ()
newCheck paths = do
   stat <- makeCheckProgress
   pm <- startProgressMeter $ csTrack stat

   forM_ paths $ \path -> do
      setTrackerPath (csPath stat) path
      resetCounter (csBytes stat)
      cfile <- openChunkFile path
      size <- chunkFileSize cfile
      let
	 process pos = do
	    (chunk, next) <- chunkRead cfile pos
	    incrCounter (csChunks stat) (1 :: Int)
	    let estimate = chunkStoreEstimate chunk
	    incrCounter (csBytes stat) estimate
	    incrCounter (csTotal stat) estimate
	    if next < size
	       then process next
	       else return ()
      process 0
      chunkClose cfile
   stopProgressMeter pm
