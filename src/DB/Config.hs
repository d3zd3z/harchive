----------------------------------------------------------------------
-- Database management of config files.
----------------------------------------------------------------------

module DB.Config (
   withConfig,
   configSetup,
   configMakeUuid,

   setConfig, getConfig, getJustConfig,

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
   setConfig db "uuid" uuid
   -- run db "insert into config values('uuid', ?)" [toSql uuid]
   -- return ()

setConfig :: (SqlType v) => DB -> String -> v -> IO ()
-- Set a configuration item to a particular value.  Fails if the item
-- already exists.
setConfig db key value = do
   run db "insert into config values(?,?)" [toSql key, toSql value]
   return ()

getConfig :: (SqlType a) => DB -> String -> IO (Maybe a)
-- Query for a particular config item.
getConfig db key = do
   liftM maybeOne $ query1 db "select value from config where key = ?" [toSql key]

getJustConfig :: (SqlType a) => DB -> String -> IO a
-- Like getConfig, but raises an exception if the key is not present.
getJustConfig db key = do
   val <- getConfig db key
   maybe (error $ "Config file missing key: " ++ key) return val

die :: String -> IO ()
die message = hPutStrLn stderr message >> exitFailure
