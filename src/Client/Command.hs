----------------------------------------------------------------------
-- Client side commands
----------------------------------------------------------------------

module Client.Command (
   clientCommand
) where

import Auth
import Hash
import DB.Config
import Server
import Protocol.ClientPool
import Protocol

import Data.List (sort, sortBy)
import Data.Ord (comparing)
import Data.Time
import System.Locale
import Control.Monad
import System.IO
import System.Exit
import System.Console.GetOpt
import Text.Printf (printf)

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
   withServer config nick $ \_db _handle -> do
      putStrLn $ "Client hello"

type BackupItem = (String, String, UTCTime, Hash)

listBackups :: String -> String -> ([BackupItem] -> [BackupItem]) -> IO ()
listBackups config nick shorten = do
   withServer config nick $ \_db handle -> do
      status <- runProtocol handle $ do
	 sendMessageP $ RequestBackupList
	 flushP
	 backups <- getBackupList
	 liftIO $ showBackupList $ sortBy (comparing timeOf) $ shorten $ sort backups
	 sendMessageP $ RequestGoodbye
	 flushP
      either failure return status
   where
      failure err = do
	 putStrLn $ "listing failure: " ++ show err
	 error "Failure"
      timeOf (_, _, time, _) = time

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

-- Retrieve the list of backups from the server.  The order of the
-- tuple is carefully chosen for sorting purposes, currently: Host,
-- Volume, Date, and finally hash.
getBackupList :: Protocol [BackupItem]
getBackupList = do
   entry <- receiveMessageP
   case entry of
      BackupListNode hash host volume date -> do
	 rest <- getBackupList
	 return $ (host, volume, date, hash) : rest
      BackupListDone -> return []

----------------------------------------------------------------------

withServer :: String -> String -> (DB -> Handle -> IO a) -> IO a
withServer config nick action = do
   withConfig schema config $ \db -> do
      uuid <- getJustConfig db "uuid"
      (host, port, poolUuid, secret) <- liftM onlyOne $
	 query4 db ("select host, port, uuid, secret from pools " ++
	    "where nick = ?") [toSql nick]
      client host port $ \handle -> do
	 status <- runProtocol handle $ do
	    idExchange uuid
	    auth <- liftIO $ authRecipient secret
	    valid <- authProtocol auth
	    -- liftIO $ putStrLn $ "Valid: " ++ show valid
	    unless valid $ fail "Authentication failure"
	    sendMessageP $ RequestHello poolUuid
	    flushP
	    _resp <- receiveMessageP :: Protocol InitReply
	    return () -- Only one possible message.
	    -- liftIO $ putStrLn $ "Reply: " ++ show resp
	 either failure return status
	 action db handle
   where
      failure err = do
	 putStrLn $ "Client failure: " ++ show err
	 error "Client aborted"

idExchange :: UUID -> Protocol String
idExchange clientUuid = do
   req <- getLineP 80
   case words req of
      ["server", serverUuid] -> do
	 putLineP $ "client " ++ clientUuid
	 flushP
	 return serverUuid
      _ -> fail "Invalid message from server"
