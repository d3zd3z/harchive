----------------------------------------------------------------------
-- Harchive driver.
----------------------------------------------------------------------

module Main (main) where

import System.Environment (getArgs)

import Chunk
import Chunk.IO
import DecodeSexp
import Hash
import Status
import Pool

import Data.Maybe
import Data.Time
import System.Locale

import Data.List
import Data.Ord

main :: IO ()
main = do
   args <- getArgs
   case args of
      ("check":files@(_:_)) ->
	 statusToIO 1 $ mapM_ runCheck files
	 -- mapM_ (statusToIO 1 . runCheck) files
      ["pool", path] -> runPool path $ return ()
      ["list", path] -> runPool path showBackups
      _ ->
	 ioError (userError usage)

usage :: String
usage = "Usage: harchive check file ...\n"

----------------------------------------------------------------------

-- TODO: Limit display, and other such fancy things.

showBackups :: PoolOp ()
-- List the backups in the storage pool.
showBackups = do
   hashes <- poolGetBackups
   chunks <- mapM poolReadChunk hashes
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
   biInfo :: Sexp }
decodeBackupInfo :: Chunk -> BackupInfo
decodeBackupInfo chunk =
   let
      host = getField "HOST"
      domain = getField "DOMAIN"
      backup = getField "BACKUP"
      startTime = getField "START-TIME"
      endTime = getField "END-TIME"
      getField key = maybe (error $ "field missing " ++ key) id $ lookupString key $ info
      info = either (\_msg -> error "Invalid parse") id infoE
      infoE = decodeSexp $ chunkData chunk
   in
      BackupInfo {
	 biHost = host, biDomain = domain,
	 biBackup = backup,
	 biStartTime = decodeUTC startTime, biEndTime = decodeUTC endTime,
	 biInfo = info }

decodeUTC :: String -> UTCTime
decodeUTC = readTime defaultTimeLocale "%FT%TZ"

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
