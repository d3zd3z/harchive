----------------------------------------------------------------------
-- Pool commands.
----------------------------------------------------------------------

module Pool.Command (
   poolCommand
) where

import Auth
import Chunk
import Hash
import DB.Config
import Pool.Local
import Tree
import Harchive.Store.Backup
import Protocol.Chan
import Protocol.Control
import Protocol.Messages

import Control.Monad (forM_, liftM)

import Control.Concurrent
import Control.Concurrent.STM (atomically)
import Data.List (foldl')
import Data.Maybe (fromJust)
import System.IO
import System.Exit
import System.Console.GetOpt
import Text.Printf (printf)

poolCommand :: [String] -> IO ()
poolCommand cmd = do
   let usageText = usageInfo topUsage topOptions
   case getOpt RequireOrder topOptions cmd of
      (opts, args, []) -> do
	 case args of
	    ["info"] -> showInfo (getTopConfig opts)
	    ["setup"] -> setup (getTopConfig opts)
	    ["add", nick, path] -> addPool (getTopConfig opts) nick path
	    ["client", uuid] -> clientGenerate (getTopConfig opts) uuid
	    ["client", uuid, secret] ->
	       clientAdd (getTopConfig opts) uuid secret
	    ["serve"] -> startServer (getTopConfig opts)
	    _ -> do
	       hPutStrLn stderr $ usageText
      (_,_,errs) -> do
	 hPutStrLn stderr $ concat errs ++ usageText
	 exitFailure

data TopFlag = TopConfig String
   deriving Show

topOptions :: [OptDescr TopFlag]
topOptions =
   [Option ['c'] ["config"]  (ReqArg TopConfig "FILE")  "use FILE for config file"]

getTopConfig :: [TopFlag] -> String
getTopConfig [] = "/tmp/pool-config.sqlite3"
getTopConfig (TopConfig name:_) = name
-- getTopConfig (_:xs) = getTopConfig xs

topUsage :: String
topUsage = "Usage: harchive pool {options} command args...\n" ++
   "\n" ++
   "  commands:\n" ++
   "      info     - Show pool configuration\n" ++
   "      setup    - Create pool configuration\n" ++
   "      add {nick} {path} - Add an existing pool to configuration\n" ++
   "      client uuid [secret] - Add a client.  Generates a secret if not given.\n" ++
   "\n" ++
   "Options:\n"

----------------------------------------------------------------------

showInfo :: String -> IO ()
showInfo config = do
   withConfig schema config $ \db -> do
      npu <- query3 db "select nick, path, uuid from pools" []
      printf "Pools:\n"
      let poolWidth = foldl' max 4 $ map (\(u,_,_) -> length u) npu
      printf "  %-*s UUID                                 Path\n"
	 poolWidth "Nick"
      forM_ npu $ \(nick, path, uuid) ->
	 printf "  %-*s %s %s\n" poolWidth
	    (nick :: String) (uuid :: String) (path :: String)
      printf "\nClients:\n"
      cl <- query1 db "select uuid from clients" []
      forM_ cl $ \c -> printf "  %s\n" (c :: String)

setup :: String -> IO ()
-- Setup the initial empty config file.
setup config = do
   configSetup schema config $ \db -> do
      configMakeUuid db
      setConfig db "port" (8933::Int)
      -- run db "insert into config values('port', 8933)" []
      -- return ()

addPool :: String -> String -> String -> IO ()
addPool config nick path = do
   withConfig schema config $ \db -> do
      uuid <- withLocalPool path poolGetUuid
      query0 db "insert into pools values(?, ?, ?)"
	 [toSql nick, toSql path, toSql uuid]
      commit db

clientGenerate :: String -> String -> IO ()
clientGenerate config uuid = do
   withConfig schema config $ \db -> do
      secret <- genNonce
      query0 db "insert into clients values (?,?)"
	 [toSql uuid, toSql secret]
      commit db
      putStrLn $ "Client " ++ uuid ++ ", secret: \"" ++ secret ++ "\""

clientAdd :: String -> String -> String -> IO ()
clientAdd config uuid secret = do
   withConfig schema config $ \db -> do
      query0 db "insert into clients values (?,?)"
	 [toSql uuid, toSql secret]
      commit db
      putStrLn $ "Client added"

schema :: [String]
schema = [
   "create table config (key text unique primary key, value text)",
   "create table pools (nick text unique primary key, path text, uuid text)",
   "create table clients (uuid text unique primary key, secret text)" ]

----------------------------------------------------------------------

startServer :: String -> IO ()
startServer config = do
   rootThread <- myThreadId
   withConfig schema config $ \db -> do
      port <- getJustConfig db "port"
      serverUuid <- getJustConfig db "uuid"
      chanServer (ChanPeer "any" port serverUuid (lookupSecret db))
         (setupChannels db rootThread)

lookupSecret :: DB -> UUID -> IO (Maybe UUID)
lookupSecret db clientUUID =
   liftM maybeOne $ query1 db "select secret from clients where uuid = ?"
      [toSql clientUUID]

setupChannels :: DB -> ThreadId -> MuxDemux -> IO ()
setupChannels db rootThread muxd = do
   putStrLn "Setup Channels"
   setupControlChannel muxd $ \message ->
      case message of
         ControlHello -> return ()
         ControlShutdownServer -> killThread rootThread
         ControlListPools -> sendPools db muxd
         ControlGoodbye -> killMuxDemux muxd
         ControlOpenPool uuid -> openPool db uuid muxd

sendPools :: DB -> MuxDemux -> IO ()
sendPools db muxd = do
   wChan <- registerWriteChannel muxd PoolListingChannel
   rows <- query2 db "select nick, uuid from pools" []
   forM_ rows $ \(nick, uuid) ->
      atomically $ writePChan wChan $ Just $ PoolNodeMessage nick uuid
   atomically $ writePChan wChan Nothing
   deregisterWriteChannel muxd PoolListingChannel

openPool :: DB -> UUID -> MuxDemux -> IO ()
openPool db uuid muxd = do
   path <- liftM onlyOne $ query1 db
      "select path from pools where uuid = ?"
      [toSql uuid]

   -- TODO: need cleanup here.
   rChan <- registerReadChannel muxd PoolCommandChannel
   readChunkChan <- registerWriteChannel muxd PoolReadChunkChannel
   _ <- forkIO $ withLocalPool path $ \pool -> runPoolCommand $ PoolRunState {
      prsPoolCommandChannel = rChan,
      prsMuxDemux = muxd,
      prsPool = pool,
      prsReadChunks = readChunkChan }
   return ()

data PoolRunState = PoolRunState {
   prsPoolCommandChannel :: PChanRead PoolCommandMessage,
   prsMuxDemux :: MuxDemux,
   prsPool :: LocalPool,
   prsReadChunks :: PChanWrite (Maybe Chunk) }

runPoolCommand :: PoolRunState -> IO ()
runPoolCommand prs = do
   msg <- atomically $ readPChan (prsPoolCommandChannel prs)
   -- putStrLn $ "Pool message received: " ++ show msg
   let pool = prsPool prs
   case msg of
      PoolCommandListBackups -> do
         hashes <- poolGetBackups pool
         withWriteChannel (prsMuxDemux prs) PoolBackupListingChannel $ \listChan ->
            atomically $ writePChan listChan hashes
      PoolCommandReadChunk hash -> do
         chunk <- poolReadChunk pool hash
         atomically $ writePChan (prsReadChunks prs) chunk
      PoolCommandRestore hash -> do
         withWriteChannel (prsMuxDemux prs) RestoreChannel $ \restoreChan -> do
            restoreBackup pool restoreChan (prsMuxDemux prs) hash

   runPoolCommand prs

----------------------------------------------------------------------

-- TODO: the file data can be a Maybe.
restoreBackup :: (ChunkReader p) => p -> PChanWrite RestoreReply ->
   MuxDemux -> Hash -> IO ()
restoreBackup pool restoreChan muxd hash = do
   -- TODO: Error handling.
   info <- liftM fromJust $ getBackupInfo pool hash
   walkTree pool (biHash info) $ \node -> do
      -- putStrLn $ "node: " ++ show node
      case node of
         TreeEnter path atts -> do
            atomically $ writePChan restoreChan $ RestoreEnter path atts
         TreeLeave path atts -> do
            atomically $ writePChan restoreChan $ RestoreLeave path atts
         TreeReg path atts _ -> do
            atomically $ writePChan restoreChan $ RestoreOpen path atts
            withWriteChannel muxd FileDataChannel $ \fileDataChan -> do
               forEachChunk pool (justField atts "HASH") $ \chunk -> do
                  atomically $ writePChan fileDataChan $ FileDataChunk chunk
               atomically $ writePChan fileDataChan $ FileDataDone
         TreeLink path atts -> do
            atomically $ writePChan restoreChan $ RestoreLink path atts
         TreeOther path atts -> do
            atomically $ writePChan restoreChan $ RestoreOther path atts
         _ -> putStrLn $ "TODO: " ++ show node
   atomically $ writePChan restoreChan $ RestoreLeave "." (biInfo info)
   atomically $ writePChan restoreChan $ RestoreDone
