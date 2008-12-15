----------------------------------------------------------------------
-- Database management of config files.
----------------------------------------------------------------------

module DB.Config (
   withConfig,
   configSetup,
   configMakeUuid,
   module DB
) where

import Auth
import DB

import Control.Monad
import System.Exit
import System.Directory
import System.IO

withConfig :: Schema -> String -> (DB -> IO ()) -> IO ()
-- Ensure that the named 'configPath' file is properly setup as a
-- database file with the correct schema.  If so, the action is run
-- with the opened database.
withConfig schema configPath action = do
   doesFileExist configPath >>= \e -> unless e $
      die $ "Config file '" ++ configPath ++ "' not present.\n" ++
	 "Use 'setup' command to create and/or specify --config"
   withDatabase configPath $ \db -> do
      ss <- checkSchema db schema
      case ss of
	 CorrectSchema -> action db
	 _ -> die $
	    "Config file '" ++ configPath ++ "' is not correct."

configSetup :: Schema -> String -> (DB -> IO ()) -> IO ()
-- Initialize an initial configuration based on the specified schema
-- at the specified path.  The schema will be installed into the
-- database.  The action will then be invoked to perform any
-- additional initialization.
configSetup schema configPath action = do
   doesFileExist configPath >>= \e -> when e $
      die $ "Config file '" ++ configPath ++ "' already exists."
   withDatabase configPath $ \db -> do
      setupSchema db schema
      action db
      commit db

configMakeUuid :: DB -> IO ()
-- Generate a new UUID, and add it to the config table.
configMakeUuid db = do
   uuid <- genUuid
   run db "insert into config values('uuid', ?)" [toSql uuid]
   return ()

die :: String -> IO ()
die message = hPutStrLn stderr message >> exitFailure
