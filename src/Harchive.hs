----------------------------------------------------------------------
-- Harchive driver.
----------------------------------------------------------------------

module Main (main) where

import System.Environment (getArgs)

import Chunk
import Chunk.IO
import Status

main :: IO ()
main = do
   args <- getArgs
   case args of
      ("check":files@(_:_)) -> mapM_ runCheck files
      _ ->
	 ioError (userError usage)

usage :: String
usage = "Usage: harchive check file ...\n"

----------------------------------------------------------------------

runCheck :: FilePath -> IO ()
runCheck path = do
   putStrLn $ "Checking: " ++ path
   monitor <- start 1

   cfile <- openChunkFile path
   size <- chunkFileSize cfile

   let
      process pos = do
	 (chunk, next) <- chunkRead cfile pos
	 addFile monitor 1
	 addSavedData monitor (fromIntegral . chunkStoreEstimate $ chunk)
	 if next < size
	    then process next
	    else return ()
   process 0

   chunkClose cfile
   stop monitor
