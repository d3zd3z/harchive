module Errors (
   mustThrow,
   mustThrowUser
) where

import Control.Exception (Exception, tryJust)
import Control.Monad (guard)
import System.IO.Error (isUserError)
import Test.HUnit

-- Fail if this doesn't throw the exception specified by the given
-- guard.
mustThrow :: Exception e => (e -> Maybe b) -> IO a -> IO ()
mustThrow testE op = do
   r <- tryJust testE op
   case r of
      Left _ -> return ()
      Right _ -> assertFailure "Failed to throw exception"

mustThrowUser :: IO a -> IO ()
mustThrowUser = mustThrow (guard . isUserError)
