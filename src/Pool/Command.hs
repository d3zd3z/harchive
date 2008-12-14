----------------------------------------------------------------------
-- Pool commands.
----------------------------------------------------------------------

module Pool.Command (
   poolCommand
) where

import DB
import Pool.Local

import Control.Monad (unless, when, forM_)

import System.IO
import System.Exit
import System.Console.GetOpt
import System.Directory (doesFileExist)
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
	    _ -> do
	       hPutStrLn stderr $ usageText
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
   "      add nick path - Add an existing pool to configuration\n" ++
   "\n" ++
   "Options:\n"

----------------------------------------------------------------------

withConfig :: String -> (DB -> IO ()) -> IO ()
withConfig config action = do
   doesFileExist config >>= \e -> unless e $
      die $ "Config file '" ++ config ++ "' not present.\n" ++
	 "Use 'setup' command to create and/or specify --config"
   withDatabase config $ \db -> do
      ss <- checkSchema db schema
      case ss of
	 CorrectSchema -> action db
	 _ -> die $
	    "Config file '" ++ config ++ "' is not correct."

showInfo :: String -> IO ()
showInfo config = do
   withConfig config $ \db -> do
      npu <- query3 db "select nick, path, uuid from pools" []
      forM_ npu $ \(nick, path, uuid) ->
	 printf "%-15s %s %s\n"
	    (nick :: String) (uuid :: String) (path :: String)

setup :: String -> IO ()
-- Setup the initial empty config file.
setup config = do
   doesFileExist config >>= \e -> when e $
      die $ "Config file '" ++ config ++ "' already exists."
   withDatabase config $ \db -> do
      setupSchema db schema

addPool :: String -> String -> String -> IO ()
addPool config nick path = do
   withConfig config $ \db -> do
      uuid <- withLocalPool path poolGetUuid
      query0 db "insert into pools values(?, ?, ?)"
	 [toSql nick, toSql path, toSql uuid]
      commit db

schema :: [String]
schema = [
   "create table config (key text unique primary key, value text)",
   "create table pools (nick text unique primary key, path text, uuid text)" ]
