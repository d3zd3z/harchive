----------------------------------------------------------------------
-- General network server.
----------------------------------------------------------------------

module Server (
   serve, client,
   hSafeGetLine
) where

-- import Auth
import Network
import System.IO
import Control.Concurrent
import Control.Exception

import System.Posix (installHandler, Handler(..), sigPIPE)

-- TODO: Make a way for a particular client command to terminate the
-- server.

-- Listen for connections on the given port, running 'action' with the
-- child handle in a new thread for each connection.
serve :: Int -> (Handle -> IO ()) -> IO ()
serve port action = do
   installHandler sigPIPE Ignore Nothing
   server <- listenOn (PortNumber $ fromIntegral port)
   let
      main = do
	 (child, childHost, childPort) <- accept server
	 putStrLn $ "child " ++ show child ++ ", host: " ++ show childHost ++
	    ", port: " ++ show childPort
	 forkIO $ finally (action child) (hClose child)
	 main
   main

-- Connect to a server.
client :: String -> Int -> (Handle -> IO a) -> IO a
client host port action = do
   connectTo host (PortNumber $ fromIntegral port) >>= action

hSafeGetLine :: Handle -> Int -> IO String
-- Read a single line from input, with a character limit.  It is an
-- error to exceed the limit.
hSafeGetLine _ n | (n <= 0) = error "Input length limit reached."
hSafeGetLine h n = do
   ch <- hGetChar h
   case ch of
      '\r' -> hSafeGetLine h n
      '\n' -> return []
      _ -> do
	 rest <- hSafeGetLine h (n-1)
	 return $ ch : rest
