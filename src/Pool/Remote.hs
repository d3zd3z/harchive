----------------------------------------------------------------------
-- Remote pool access.
----------------------------------------------------------------------

module Pool.Remote (
) where

data RemotePool = RemotePool ()

data RemotePoolInfo = (String, String, 

-- Connect to the remote pool associated with the 'nick' in the given
-- config file.
withRemotePool :: String -> String -> (RemotePool -> IO a) -> IO a
withRemotePool configPath nick action = do
   withConfig schema config
