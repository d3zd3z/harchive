----------------------------------------------------------------------
-- Client side commands
----------------------------------------------------------------------

module Client.Command (
   clientCommand
) where

import Chunk
import Hash
import DB.Config
import Meter
import Protocol.ClientPool
import Protocol.Chan
import Harchive.Store.Backup
import Harchive.IO
import Pool
import Pool.Remote

import Control.Concurrent.STM
import qualified Data.ByteString.Lazy as L
import Data.List (sort, sortBy)
import Data.Ord (comparing)
import Data.Maybe (fromJust)
import Data.Time
import System.Locale
import Control.Monad
import System.IO
import System.Exit
import System.Console.GetOpt
import System.FilePath
import Text.Printf (printf)
import System.Directory

clientCommand :: [String] -> IO ()
clientCommand cmd = do
   let usageText = usageInfo topUsage topOptions
   case getOpt RequireOrder topOptions cmd of
      (opts, args, []) -> do
	 case args of
	    ["setup"] -> setup (getTopConfig opts)
	    ["pool", nick, uuid, host, port, secret] ->
	       addPool (getTopConfig opts) nick uuid host (read port) secret
	    ["hello", poolName] -> hello (getTopConfig opts) poolName
	    ["list", poolName] -> listBackups (getTopConfig opts) poolName id
	    ["list", "--short", poolName] -> listBackups (getTopConfig opts) poolName latestBackup
	    ["restore", poolName, hash, path] -> restoreBackup (getTopConfig opts) poolName hash path
	    _ -> do
	       hPutStrLn stderr $ usageText
	       exitFailure
      (_,_,errs) -> do
	 hPutStrLn stderr $ concat errs ++ usageText
	 exitFailure

data TopFlag = TopConfig String
   deriving Show

topOptions :: [OptDescr TopFlag]
topOptions =
   [Option ['c'] ["config"] (ReqArg TopConfig "FILE") "use FILE for config file"]

getTopConfig :: [TopFlag] -> String
getTopConfig [] = "/tmp/client-config.sqlite3"
getTopConfig (TopConfig name:_) = name

topUsage :: String
topUsage = "Usage: harchive client {options} command args...\n" ++
   "\n" ++
   "  commands:\n" ++
   "      setup     - Create client configuration\n" ++
   "      pool nick uuid host port secret\n" ++
   "      hello nick\n" ++
   "      list {--short} nick\n" ++
   "      restore nick hash path\n" ++
   "\n" ++
   "Options:\n"

----------------------------------------------------------------------
setup :: String -> IO ()
-- Setup the initial empty config file.
setup config = do
   configSetup schema config configMakeUuid

addPool :: String -> String -> String -> String -> Int -> String -> IO ()
addPool config nick uuid host port secret = do
   withConfig schema config $ \db -> do
      query0 db "insert into pools values(?,?,?,?,?)"
	 [toSql nick, toSql uuid, toSql host, toSql port, toSql secret]
      commit db

schema :: Schema
schema = [
   "create table config (key text unique primary key, value text)",
   "create table pools (nick text unique primary key, uuid text, " ++
      "host text, port number, secret)" ]

----------------------------------------------------------------------
hello :: String -> String -> IO ()
hello config nick = do
   withServer config nick $ \_ ->
      putStrLn $ "Client hello"

type BackupItem = (String, String, UTCTime, Hash)

listBackups :: String -> String -> ([BackupItem] -> [BackupItem]) -> IO ()
listBackups config nick shorten = do
   withServer config nick $ \pool -> do
      hashes <- poolGetBackups pool
      -- putStrLn $ "Hashes: " ++ show hashes
      chunkBoxes <- forM hashes $ poolAsyncReadChunk pool
      backups <- withListingMeter (length hashes) $ \counter -> do
         forM chunkBoxes $ \box -> do
            chunk <- liftM fromJust $ atomically $ takeTMVar box
            atomically $ incrementTVar counter
            let info = decodeBackupInfo chunk
            return (biHost info, biBackup info,
               biStartTime info, chunkHash chunk)
      showBackupList $ sortBy (comparing timeOf) $ shorten $ sort backups
   where
      timeOf (_, _, time, _) = time

withListingMeter :: Int -> (TVar Int -> IO a) -> IO a
withListingMeter total action = do
   (var, counterMeter) <- makeGoalCounter total "... "
   ind <- makeIndicator $ mString "Working: " >> counterMeter
   runIndicator ind
   result <- action var
   stopIndicator ind True
   return result

incrementTVar :: (Num a) => TVar a -> STM ()
incrementTVar tv = do
   old <- readTVar tv
   writeTVar tv (old + 1)

incrementTVarBy :: (Num a) => TVar a -> a -> STM ()
incrementTVarBy tv amount = do
   old <- readTVar tv
   writeTVar tv (old + amount)

-- Given a sorted list of backups, return only the newest backup of
-- each host/volume combination.
latestBackup :: [BackupItem] -> [BackupItem]
latestBackup ((h1, v1, _, _) : bb@(h2, v2, _, _) : xs)
   | h1 == h2 && v1 == v2   = latestBackup (bb : xs)
latestBackup (x:xs) = x : latestBackup xs
latestBackup [] = []

showBackupList :: [BackupItem] -> IO ()
showBackupList backups = do
   tz <- getCurrentTimeZone
   forM_ backups $ \ (host, volume, date, hash) -> do
      let local = utcToLocalTime tz date
      printf "%s %-10s %-20s %s\n" (toHex hash)
	 (take 10 host)
	 (take 20 volume)
	 (formatTime defaultTimeLocale "%F %H:%M" local)

----------------------------------------------------------------------
-- Restore a backup to the specified path.

data RestoreMeter = RestoreMeter {
   rmFileCount :: TVar Integer,
   rmTotalSize :: TVar Integer,
   rmPath :: TVar String,
   rmOldPaths :: TVar [String] }

pushPath :: RestoreMeter -> FilePath -> STM ()
pushPath (RestoreMeter { rmPath = path, rmOldPaths = oldPath }) newPath = do
   ps <- readTVar oldPath
   p <- readTVar path
   writeTVar path newPath
   writeTVar oldPath $ p:ps

popPath :: RestoreMeter -> STM ()
popPath (RestoreMeter { rmPath = path, rmOldPaths = oldPath }) = do
   (p:ps) <- readTVar oldPath
   writeTVar oldPath ps
   writeTVar path p

withRestoreMeter :: (RestoreMeter -> IO a) -> IO a
withRestoreMeter action = do
   pathTV <- newTVarIO "???"
   oldPathsTV <- newTVarIO $ repeat "."
   (totalTV, totalMeter) <- makeMeterCounter (`div` 1024) "KBytes "
   (instMeter, avgMeter, update) <- makeRateCounter (/ 1024) totalTV
   (countTV, countMeter) <- makeMeterCounter id "files "
   let meter = do
         mString "  "
         countMeter
         totalMeter
         mString " "
         instMeter >> mString " K/sec ("
         avgMeter >> mString " avg)\n"
         mString "   path: "
         p <- lift $ readTVar pathTV
         mString $ take 70 $ p ++ repeat ' '
   ind <- makeIndicator meter
   runIndicatorUpdate update ind
   result <- action $ RestoreMeter {
      rmFileCount = countTV,
      rmTotalSize = totalTV,
      rmPath = pathTV,
      rmOldPaths = oldPathsTV }
   stopIndicator ind False
   putStrLn ""
   return result

restoreBackup :: String -> String -> String -> String -> IO ()
restoreBackup config nick hash path = do
   withServer config nick $ \pool -> do
      withRestoreMeter $ \meter -> do
         remotePoolRestore pool (fromHex hash) (processRestore meter path)

processRestore :: RestoreMeter -> FilePath -> FileDataGetter -> RestoreReply -> IO ()
processRestore rm path _fdGet (RestoreEnter name _atts) = do
   let fullName = path </> name
   atomically $ pushPath rm fullName
   createDirectory $ fullName
processRestore rm path _fdGet (RestoreLeave name atts) = do
   atomically $ popPath rm
   setDirAtts (path </> name) atts
processRestore rm path fdGet (RestoreOpen name atts) = do
   let fullName = path </> name
   atomically $ incrementTVar (rmFileCount rm)
   withBinaryFile fullName WriteMode $ \desc -> do
      restoreFile rm desc fdGet
      hFlush desc
      setFileAtts fullName desc atts
processRestore _rm path _ (RestoreLink name atts) = do
   restoreSymLink (path </> name) atts
processRestore _rm path _ (RestoreOther name atts) = do
   restoreOther (path </> name) atts
processRestore _ _ _ _ = return ()

restoreFile :: RestoreMeter -> Handle -> FileDataGetter -> IO ()
restoreFile rm desc getter = do
   loop
   where
      loop = do
         msg <- getter
         case msg of
            FileDataChunk chunk -> do
               atomically $ incrementTVarBy (rmTotalSize rm) $
                  toInteger $ chunkLength chunk
               L.hPut desc $ chunkData chunk
               loop
            FileDataDone -> return ()

----------------------------------------------------------------------

-- TODO: XXX
withServer :: String -> String -> (RemotePool -> IO ()) -> IO ()
withServer config nick action = do
   withConfig schema config $ \db -> do
      uuid <- getJustConfig db "uuid"
      (host, port, secret) <- liftM onlyOne $
	 query3 db ("select host, port, secret from pools " ++
	    "where nick = ?") [toSql nick]
      -- TODO: Verify their identity not the pool.
      withRemotePool (ChanPeer host port uuid
            (const $ return $ Just secret)) nick $ action
