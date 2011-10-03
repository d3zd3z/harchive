module PoolCheck (tester) where

import Control.Exception (finally)
import Errors
import Test.HUnit
import TmpDir
import System.FilePath ((</>))

-- import qualified System.Backup.Chunk.Store as Store
import System.Backup.Pool

tester :: Test
tester = test [
   "Creation" ~: noPool ]

-- Verify that various problems with creating pools are handled.
noPool :: IO ()
noPool = withTmpDir $ \tmp -> do
   let name = tmp </> "p1"
   mustThrowUser $ withPool name $ \_ -> return ()

withPool :: FilePath -> (Pool -> IO a) -> IO a
withPool p op = do
   pool <- openPool p
   finally (op pool) $ closePool pool