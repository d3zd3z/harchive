----------------------------------------------------------------------
-- Harchive driver.
----------------------------------------------------------------------

module Main (main) where

import System.Environment (getArgs)

-- import Chunk
import Chunk.IO

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
   cfile <- openChunkFile path
   (chunk, next) <- chunkRead cfile 0
   putStrLn $ show chunk
   putStrLn $ show next
   chunkClose cfile
