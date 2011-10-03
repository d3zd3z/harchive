module PoolCheck (tester) where

import Control.Exception (finally, tryJust)
import Control.Monad (guard)
import Test.HUnit
import TmpDir
import System.FilePath ((</>))
import System.IO.Error (isUserError)

-- import qualified System.Backup.Chunk.Store as Store
import System.Backup.Pool

tester :: Test
tester = test [
   "Creation" ~: noPool ]

-- Verify that various problems with creating pools are handled.
noPool :: IO ()
noPool = withTmpDir $ \tmp -> do
   let name = tmp </> "p1"
   mustThrow $ withPool name $ \_ -> return ()

withPool :: FilePath -> (Pool -> IO a) -> IO a
withPool p op = do
   pool <- openPool p
   finally (op pool) $ closePool pool

-- Fail if the test doesn't throw an exception.
-- TODO: Allow exception to be specified.
mustThrow :: IO a -> IO ()
mustThrow op = do
   r <- tryJust (guard . isUserError) op
   case r of
      Left _ -> return ()
      Right _ -> assertFailure "Failed to throw exception"

{-
mustThrow :: Exception e => (e -> Bool) -> IO a -> IO ()
mustThrow testE op = do
   op `catch` ok
   assertFailure "Failed to throw exception"
   where
      ok e | testE e = return ()
      ok e = assertFailure $ "Unexpected exception thrown" ++ show e
-}
