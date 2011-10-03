module System.Backup.Pool.Metadata (
   Metadata,
   getUUID,
   setUUID,
   getNewFile,
   getLimit,
   generateUUID,

   getBackups,
   saveBackups,

   -- From JavaProperties
   readPropertyFile,
   writePropertyFile,
   emptyProperties
) where

import Control.Applicative ((<$>))
import Data.Word (Word32)
import qualified Hash
import qualified Data.Map as Map
import Text.JavaProperties
import System.IO.Cautious as Cautious

type Metadata = JavaProperties

getUUID :: Metadata -> Maybe String
getUUID = Map.lookup "uuid"

setUUID :: String -> Metadata -> Metadata
setUUID = Map.insert "uuid"

-- These are configurable by the user, but not ever written by the
-- code.
getNewFile :: Metadata -> Bool
getNewFile m = Map.lookup "newfile" m == Just "true"

-- The default is hardcoded to fit 1 per CD, 7 per dvd, etc.
getLimit :: Metadata -> Word32
getLimit m = case Map.lookup "limit" m of
   Just x  -> read x
   Nothing -> 640 * 1024 * 1024

-- Generate a new UUID.  Currently, this is a very linux-specific
-- implementation.
generateUUID :: IO String
generateUUID = (head . lines) <$> readFile "/proc/sys/kernel/random/uuid"

----------------------------------------------------------------------
-- Read in the list of backups from the named text file.
getBackups :: FilePath -> IO [Hash.Hash]
getBackups name = map Hash.fromHex <$> lines <$> readFile name

-- Write the list of backups to the named text file, safely.
saveBackups :: FilePath -> [Hash.Hash] -> IO ()
saveBackups path = Cautious.writeFile path . unlines . map Hash.toHex
