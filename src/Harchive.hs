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
      ["pool", path] -> runPool path $ return ()
      ["list", path] -> runPool path showBackups
      ["show", path, hash] -> runPool path $ showOne (fromHex hash)
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

showOne :: Hash -> PoolOp ()
showOne hash = do
   tz <- liftIO $ getCurrentTimeZone
   chunk <- liftM fromJust $ poolReadChunk hash
   let info = decodeBackupInfo chunk
   let sTime = formatTime defaultTimeLocale "%F %H:%M"
	 (utcToLocalTime tz $ biStartTime info)
   liftIO $ printf "Backup from %s:%s on %s\n" (biHost info)
      (biBackup info) sTime
   liftIO $ printf "Root = %s\n" (show $ biHash info)
   rootChunk <- liftM fromJust $ poolReadChunk (biHash info)
   liftIO $ mapM_ (printf "sexp = %s\n" . show) (decodeMultiChunk rootChunk)

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
