----------------------------------------------------------------------
-- Storage information pertaining to backups.
----------------------------------------------------------------------

module Harchive.Store.Backup (
   BackupInfo(..),
   decodeBackupInfo,
   getBackupInfo
) where

import Chunk
import Hash
import Harchive.Store.Sexp
import Pool

import Data.Time

data BackupInfo = BackupInfo {
   biHost, biDomain :: String,
   biBackup :: String,
   biStartTime, biEndTime :: UTCTime,
   biHash :: Hash,
   biInfo :: Attr }

getBackupInfo :: ChunkReader p => p -> Hash -> IO (Maybe BackupInfo)
getBackupInfo pool hash = do
   chunk <- poolReadChunk pool hash
   return $ decodeBackupInfo `fmap` chunk

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

