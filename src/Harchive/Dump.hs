----------------------------------------------------------------------
-- Backup dumping.
----------------------------------------------------------------------

module Harchive.Dump (
   dumpDir
) where

import Meter

import Data.List (sortBy)
import Data.Ord (comparing)
import qualified Control.Exception as E
import System.Linux.Directory
import Data.List (partition)
import System.FilePath ((</>))
import Control.Monad.Reader
import Control.Concurrent
import System.Posix
import System.IO.Error (try)

data FullNode
   = DirNode { fnName :: FilePath,
      _fnStats :: FileStatus,
      _fnChildren :: [FullNode] }
   | OtherNode { fnName :: FilePath,
      _fnStats :: FileStatus }

data DumpInfo = DumpInfo {
   diDirIncrement :: Integer -> STM (),
   diOtherIncrement :: Integer -> STM (),
   diWarner :: Warner }

type DirIO a = ReaderT DumpInfo IO a

dumpDir :: FilePath -> IO ()
dumpDir path = do
   _tree <- withDumpMeter $ getTree path
   return ()

data Warner = Warner (TChan String) (TVar Bool)

-- Create and start an IO thread to nicely print warnings.
makeWarner :: Indicator -> IO Warner
makeWarner ind = do
   chan <- newTChanIO
   stop <- newTVarIO False
   let
      run :: IO ()
      run = do
         next <- atomically (finish `orElse` process)
         next
      finish = do
         s <- readTVar stop
         unless s retry
         return $ return ()
      process = do
         msg <- readTChan chan
         return $ do
            indicatorIO ind $ putStrLn $ "Warning: " ++ msg
            run
   forkIO run
   return $ Warner chan stop

enqueueWarning :: Warner -> String -> IO ()
enqueueWarning (Warner chan _) = atomically . writeTChan chan

stopWarner :: Warner -> IO ()
stopWarner (Warner chan stop) = atomically $ do
   empty <- isEmptyTChan chan
   unless empty retry
   writeTVar stop True

warn :: String -> DirIO ()
warn msg = do
   warner <- asks diWarner
   liftIO $ enqueueWarning warner msg

-- Catch any IO exception, turning the result into a warning.
-- Arguments are similar to 'maybe'.  The first is the zero value, to
-- return in the exception case.  Otherwise the result of the
-- second action is returned.  In the warning case, registers a
-- warning about the exception.
warnDirIO :: a -> DirIO a -> DirIO a
warnDirIO zero action = do
   r <- ask
   result <- liftIO $ try $ runReaderT action r
   either (\exn -> warn (show exn) >> return zero) return result

withDumpMeter :: (DirIO a) -> IO a
withDumpMeter action = do
   (dirCount, dirMeter) <- makeMeterCounter id "dirs, "
   (otherCount, otherMeter) <- makeMeterCounter id "nondirs "
   (totalCount, totalMeter) <- makeMeterCounter id "total "
   let meter = do
         mString "Scanning tree: "
         dirMeter
         otherMeter
         totalMeter
   ind <- makeIndicator meter
   runIndicator ind
   warner <- makeWarner ind
   let
      incDir = incrementWithTotal dirCount otherCount totalCount
      incOther = incrementWithTotal otherCount dirCount totalCount
   result <- runReaderT action $ DumpInfo incDir incOther warner
   stopWarner warner
   stopIndicator ind False
   putStrLn ""
   return result

-- Ugh, this doesn't work with XFS.

getTree :: FilePath -> DirIO [FullNode]
getTree path = do
   incrementDirs
   info <- warnDirIO [] $ liftIO $ getDirectoryInformation path
   -- TODO: Sort by inode number.
   stats <- forM (sortBy (comparing dirInfoInode) info) $ \n -> do
      let fullName = path </> dirInfoName n
      st <- liftIO $ getSymbolicLinkStatus fullName
      return (n, st)

   let (dirs, nondirs) = partition (isDirectory . snd) stats
   incrementOthersBy (length nondirs)
   children <- forM dirs $ \(d, st) -> do
      children <- getTree $ path </> dirInfoName d
      return $ DirNode (dirInfoName d) st children
   return $ sortBy (comparing fnName) $
      children ++ map makeOtherNode nondirs
   where
      makeOtherNode (info, stats) = OtherNode (dirInfoName info) stats

-- Increment a by 'inc', adding to the current value of 'b', to update
-- the total 'c'.
incrementWithTotal :: (Num n) => TVar n -> TVar n -> TVar n -> n -> STM ()
incrementWithTotal a b total inc = do
   aval <- readTVar a
   bval <- readTVar b
   writeTVar a (aval + inc)
   writeTVar total (aval + bval + inc)

incrementDirs :: DirIO ()
incrementDirs = do
   bump <- asks diDirIncrement
   liftIO $ atomically $ bump 1

incrementOthersBy :: Int -> DirIO ()
incrementOthersBy amount = do
   bump <- asks diOtherIncrement
   liftIO $ atomically $ bump $ toInteger amount

getDirectoryInformation :: FilePath -> IO [LinuxDirInfo]
getDirectoryInformation path = do
   E.bracket (openDirStream path) closeDirStream $ loop
   where
      loop dir = do
         ent <- linuxReadDirStream dir
         case ent of
            Nothing -> return []
            Just (LinuxDirInfo { dirInfoName = "." }) -> loop dir
            Just (LinuxDirInfo { dirInfoName = ".." }) -> loop dir
            Just info -> do
               rest <- loop dir
               return $ info:rest
