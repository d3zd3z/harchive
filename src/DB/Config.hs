----------------------------------------------------------------------
-- Database management of config files.
----------------------------------------------------------------------

module DB.Config (
   withConfig,
   module DB
) where

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

die :: String -> IO ()
die message = hPutStrLn stderr message >> exitFailure
