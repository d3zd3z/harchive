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
	    ["list", poolName] -> listBackups (getTopConfig opts) poolName
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
   "      list nick\n" ++
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

listBackups :: String -> String -> IO ()
listBackups config nick = do
   withServer config nick $ \_db handle -> do
      status <- runProtocol handle $ do
	 sendMessageP $ RequestBackupList
	 flushP
	 getBackupList
	 sendMessageP $ RequestGoodbye
	 flushP
      either failure return status
   where
      failure err = do
	 putStrLn $ "listing failure: " ++ show err
	 error "Failure"

getBackupList :: Protocol ()
getBackupList = do
   entry <- receiveMessageP
   case entry of
      BackupListNode hash host volume date -> do
	 liftIO $ printf "%s %s %s %s\n" (toHex hash) host volume date
	 getBackupList
      BackupListDone -> return ()

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
	    liftIO $ putStrLn $ "Valid: " ++ show valid
	    unless valid $ fail "Authentication failure"
	    sendMessageP $ RequestHello poolUuid
	    flushP
	    resp <- receiveMessageP :: Protocol InitReply
	    liftIO $ putStrLn $ "Reply: " ++ show resp
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
