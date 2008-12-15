----------------------------------------------------------------------
-- Client side commands
----------------------------------------------------------------------

module Client.Command (
   clientCommand
) where

import Auth
import DB.Config
import Server

import Control.Monad
import System.Directory
import System.IO
import System.Exit
import System.Console.GetOpt

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
	    _ -> do
	       hPutStrLn stderr $ usageText
	       exitFailure
      (_,_,errs) -> do
	 hPutStrLn stderr $ concat errs ++ usageText
	 exitFailure

die :: String -> IO ()
die message = do
   hPutStrLn stderr message
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
   "\n" ++
   "Options:\n"

----------------------------------------------------------------------
setup :: String -> IO ()
-- Setup the initial empty config file.
setup config = do
   doesFileExist config >>= \e -> when e $
      die $ "Config file '" ++ config ++ "' already exists."
   withDatabase config $ \db -> do
      setupSchema db schema
      uuid <- genUuid
      query0 db "insert into config values('uuid',?)"
	 [toSql uuid]
      commit db

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
   withConfig schema config $ \db -> do
      uuid <- liftM onlyOne $
	 query1 db "select value from config where key = 'uuid'" []
      (host, port, secret) <- liftM onlyOne $
	 query3 db ("select host, port, secret from pools " ++
	    "where nick = ?") [toSql nick]
      client host port $ \handle -> do
	 _ <- initialHello handle uuid
	 auth <- authRecipient secret
	 valid <- runAuthIO handle handle auth
	 putStrLn $ "Valid: " ++ show valid

initialHello :: Handle -> UUID -> IO String
initialHello handle clientUuid = do
   req <- hSafeGetLine handle 80
   case words req of
      ["server", serverUuid] -> do
	 hPutStrLn handle $ "client " ++ clientUuid
	 hFlush handle
	 return serverUuid
      _ -> error "Invalid message from server"
