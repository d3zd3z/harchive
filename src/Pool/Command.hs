----------------------------------------------------------------------
-- Pool commands.
----------------------------------------------------------------------

module Pool.Command (
   poolCommand
) where

import Auth
import DB.Config
import Pool.Local
import Server
import Protocol.ClientPool

import Control.Monad (unless, forM_, liftM)

import Data.List (foldl')
import System.IO
import System.Exit
import System.Console.GetOpt
import Text.Printf (printf)

poolCommand :: [String] -> IO ()
poolCommand cmd = do
   let usageText = usageInfo topUsage topOptions
   case getOpt RequireOrder topOptions cmd of
      (opts, args, []) -> do
	 case args of
	    ["info"] -> showInfo (getTopConfig opts)
	    ["setup"] -> setup (getTopConfig opts)
	    ["add", nick, path] -> addPool (getTopConfig opts) nick path
	    ["client", uuid] -> clientGenerate (getTopConfig opts) uuid
	    ["client", uuid, secret] ->
	       clientAdd (getTopConfig opts) uuid secret
	    ["serve"] -> startServer (getTopConfig opts)
	    _ -> do
	       hPutStrLn stderr $ usageText
      (_,_,errs) -> do
	 hPutStrLn stderr $ concat errs ++ usageText
	 exitFailure

data TopFlag = TopConfig String
   deriving Show

topOptions :: [OptDescr TopFlag]
topOptions =
   [Option ['c'] ["config"]  (ReqArg TopConfig "FILE")  "use FILE for config file"]

getTopConfig :: [TopFlag] -> String
getTopConfig [] = "/tmp/pool-config.sqlite3"
getTopConfig (TopConfig name:_) = name
-- getTopConfig (_:xs) = getTopConfig xs

topUsage :: String
topUsage = "Usage: harchive pool {options} command args...\n" ++
   "\n" ++
   "  commands:\n" ++
   "      info     - Show pool configuration\n" ++
   "      setup    - Create pool configuration\n" ++
   "      add {nick} {path} - Add an existing pool to configuration\n" ++
   "      client uuid [secret] - Add a client.  Generates a secret if not given.\n" ++
   "\n" ++
   "Options:\n"

----------------------------------------------------------------------

showInfo :: String -> IO ()
showInfo config = do
   withConfig schema config $ \db -> do
      npu <- query3 db "select nick, path, uuid from pools" []
      printf "Pools:\n"
      let poolWidth = foldl' max 4 $ map (\(u,_,_) -> length u) npu
      printf "  %-*s UUID                                 Path\n"
	 poolWidth "Nick"
      forM_ npu $ \(nick, path, uuid) ->
	 printf "  %-*s %s %s\n" poolWidth
	    (nick :: String) (uuid :: String) (path :: String)
      printf "\nClients:\n"
      cl <- query1 db "select uuid from clients" []
      forM_ cl $ \c -> printf "  %s\n" (c :: String)

setup :: String -> IO ()
-- Setup the initial empty config file.
setup config = do
   configSetup schema config $ \db -> do
      configMakeUuid db
      setConfig db "port" (8932::Int)
      -- run db "insert into config values('port', 8933)" []
      -- return ()

addPool :: String -> String -> String -> IO ()
addPool config nick path = do
   withConfig schema config $ \db -> do
      uuid <- withLocalPool path poolGetUuid
      query0 db "insert into pools values(?, ?, ?)"
	 [toSql nick, toSql path, toSql uuid]
      commit db

clientGenerate :: String -> String -> IO ()
clientGenerate config uuid = do
   withConfig schema config $ \db -> do
      secret <- genNonce
      query0 db "insert into clients values (?,?)"
	 [toSql uuid, toSql secret]
      commit db
      putStrLn $ "Client " ++ uuid ++ ", secret: \"" ++ secret ++ "\""

clientAdd :: String -> String -> String -> IO ()
clientAdd config uuid secret = do
   withConfig schema config $ \db -> do
      query0 db "insert into clients values (?,?)"
	 [toSql uuid, toSql secret]
      commit db
      putStrLn $ "Client added"

schema :: [String]
schema = [
   "create table config (key text unique primary key, value text)",
   "create table pools (nick text unique primary key, path text, uuid text)",
   "create table clients (uuid text unique primary key, secret text)" ]

----------------------------------------------------------------------

startServer :: String -> IO ()
startServer config = do
   withConfig schema config $ \db -> do
      port <- getJustConfig db "port"
      serverUuid <- getJustConfig db "uuid"
      serve port $ \handle -> do
	 secret <- initialHello handle db serverUuid
	 auth <- authInitiator secret
	 valid <- runAuthIO handle handle auth
	 unless valid $ error "Client not authenticated"
	 putStrLn $ "Client authenticated"

	 msg <- receiveMessage handle :: IO Request
	 putStrLn $ "Message: " ++ show msg
	 sendMessage handle ReplyHello
	 hFlush handle

initialHello :: Handle -> DB -> UUID -> IO String
initialHello handle db serverUuid = do
   hPutStrLn handle $ "server " ++ serverUuid
   hFlush handle
   resp <- hSafeGetLine handle 80
   case words resp of
      ["client", clientUuid] -> do
	 secret <- liftM maybeOne $ query1 db "select secret from clients where uuid = ?"
	    [toSql clientUuid]
	 maybe fakeSecret return secret
      _ -> error "Invalid client response"
   where
      fakeSecret = genNonce
