----------------------------------------------------------------------
-- Harchive driver.
----------------------------------------------------------------------

module Main (main) where

import System.Environment (getArgs)

import Chunk
import Chunk.IO
import DecodeSexp
import Hash
import Status
import Pool

import Data.List
import Data.Ord

main :: IO ()
main = do
   args <- getArgs
   case args of
      ("check":files@(_:_)) ->
	 statusToIO 1 $ mapM_ runCheck files
	 -- mapM_ (statusToIO 1 . runCheck) files
      ["pool", path] -> runPool path $ return ()
      ["list", path] -> runPool path showBackups
      _ ->
	 ioError (userError usage)

usage :: String
usage = "Usage: harchive check file ...\n"

----------------------------------------------------------------------

-- TODO: Limit display, and other such fancy things.

showBackups :: PoolOp ()
-- List the backups in the storage pool.
showBackups = do
   hashes <- poolGetBackups
   chunks <- mapM poolReadChunk hashes
   let chunks' = map (maybe (error "Chunk missing") id) chunks
   let chunkInfo = map (decodeSexp . chunkData) chunks'
   let chunkInfo' = map (either (\_msg -> error $ "Invalid data: ") id) chunkInfo
   let info = zip hashes chunkInfo'
   let sortedInfo = sortBy (comparing $ dateOf . snd) info
   liftIO $ putStrLn (intercalate "\n" $ map backupInfo sortedInfo)
   where
      dateOf = maybe (error "field missing") id . lookupString "END-TIME"

backupInfo :: (Hash, Sexp) -> String
-- Nicely print the information about the backups.
backupInfo (hash, info) =
   toHex hash ++ " " ++
      lPad 10 (mustLookupString "HOST" info) ++ " " ++
      lPad 15 (mustLookupString "BACKUP" info) ++ " " ++
      mustLookupString "START-TIME" info
   where
      mustLookupString key = maybe (error "field missing") id . lookupString key

lPad :: Int -> String -> String
-- Left padded and truncating version of the source string.
lPad n str = base ++ replicate (n - length base) ' '
   where base = take n str

----------------------------------------------------------------------

runCheck :: FilePath -> StatusIO ()
runCheck path = do
   cleanLiftIO $ putStrLn $ "Checking: " ++ path

   cfile <- liftIO $ openChunkFile path
   size <- liftIO $ chunkFileSize cfile

   let
      process pos = do
	 (chunk, next) <- liftIO $ chunkRead cfile pos
	 addFile 1
	 addSavedData (fromIntegral . chunkStoreEstimate $ chunk)
	 if next < size
	    then process next
	    else return ()
   process 0

   liftIO $ chunkClose cfile
