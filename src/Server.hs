----------------------------------------------------------------------
-- General network server.
----------------------------------------------------------------------

module Server (
   serve
) where

import Auth
import Network
import System.IO

serve :: Int -> IO ()
-- Listen on the given port for network connections.
serve port = do
   server <- listenOn (PortNumber $ fromIntegral port)
   (child, childHost, childPort) <- accept server
   putStrLn $ "child " ++ show child ++ ", host: " ++ show childHost ++
      ", port: " ++ show childPort
   uuid <- genUuid
   hPutStrLn child $ "harchive version=0.99 uuid=" ++ uuid
   hFlush child
   line <- hSafeGetLine child 20
   putStrLn $ "Child wrote: " ++ line
   hClose child

hSafeGetLine :: Handle -> Int -> IO String
-- Read a single line from input, with a character limit.  It is an
-- error to exceed the limit.
hSafeGetLine _ n | (n <= 0) = error "Input length limit reached."
hSafeGetLine h n = do
   ch <- hGetChar h
   if ch == '\n'
      then return []
      else do
	 rest <- hSafeGetLine h (n-1)
	 return $ ch : rest
