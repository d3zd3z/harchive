----------------------------------------------------------------------
-- Device ID mapping.
-- Copyright 2007, David Brown
--
-- This program is free software; you can redistribute it and/or modify it
-- under the terms of the GNU General Public License as published by the
-- Free Software Foundation; either version 2, or (at your option) any later
-- version.
--
-- This program is distributed in the hope that it will be useful, but
-- WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
-- Public License for more details.
--
-- You should have received a copy of the GNU General Public License along
-- with this program; if not, write to the Free Software Foundation, Inc.,
-- 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
--
----------------------------------------------------------------------
--
-- Please ask <hackage@davidb.org> if you are interested in another
-- license.  If pieces of this program are useful in other systems I
-- will be willing to release them under a freer license, but I want
-- the program as a whole to be covered under the GPL.
--
----------------------------------------------------------------------
--
-- Because systems such as LVM can cause device ID's to change from
-- one mount to another, we prefer to track filesystems by the UUID of
-- the underlying filesystem.
--
-- This starts to become fairly system specific.  The code here has
-- been developed and tested on a fairly recent Linux system, and uses
-- the 'e2fsprogs' package utility 'blkid' to generate listing of
-- UUIDs of active filesystems.
--
-- As configured on this system, the 'blkid' program maintains a cache
-- of this information, which it prints out whenever run as a
-- non-superuser.  This means that normal user invocation could become
-- out of date.
--
-- The parsing of the device ID database will fail if the output
-- format of 'blkid' changes.

module Devid (
   DevMapping,
   getDevMapping
) where

import Prelude hiding (catch)

import Control.Exception (catch)
import Control.Monad (when, forM)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe (catMaybes)
import Text.ParserCombinators.Parsec
import System.Exit
import System.IO
import System.Process
import System.Posix

type DevMapping = Map DeviceID String

-- Program to run to get device mapping information.
blkidProgram :: String
blkidProgram = "/sbin/blkid"

getDevMapping :: IO DevMapping
getDevMapping = do
   (_childIn, childOut, _childErr, child) <- runInteractiveCommand blkidProgram
   contents <- hGetContents childOut
   let mapping0 = parse devIdInfo blkidProgram contents
   -- putStrLn $ "parse: " ++ show parsed
   let
      mapping1 = case mapping0 of
         Left message -> error $ show message
         Right m -> m

   -- Process the mapping.
   mapping2 <- forM mapping1 $ \(name, atts) ->
      catch
         (do
            if Map.member "UUID" atts
               then do
                  -- putStrLn $ "Lookup: " ++ name
                  stats <- getFileStatus name
                  when (not $ isBlockDevice stats) $ fail "Not a block device"
                  return $ Just (specialDeviceID stats, atts Map.! "UUID")
               else return Nothing)
         $ \exn -> do
            putStrLn $ "Exception: " ++ show exn
            return Nothing

   let mapping = Map.fromList . catMaybes $ mapping2

   code <- waitForProcess child
   when (code /= ExitSuccess) $ fail $ "Failure to invoke " ++ blkidProgram ++ ": " ++ show code

   return mapping

----------------------------------------------------------------------
-- Parser for the output of blkid.

devIdInfo :: GenParser Char () [(String, Map String String)]
devIdInfo = do
   many singleDevice

singleDevice :: GenParser Char () (String, Map String String)
singleDevice = do
   device <- many1 (alphaNum <|> char '/' <|> char '-' <|> char '_' <|> char '.')
   char ':'
   skipMany (char ' ')
   tag <- many devTag
   newline
   return (device, Map.fromList tag)

devTag :: GenParser Char () (String, String)
devTag = do
   tag <- many1 (letter <|> char '_')
   char '='
   char '"'
   value <- many $ noneOf ['"']
   char '"'
   skipMany (char ' ')
   return $ (tag, value)
