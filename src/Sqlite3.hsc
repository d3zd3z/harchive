----------------------------------------------------------------------
-- Sqlite3 binding.
-- Copyright 2007, David Brown
--
-- This program is free software; you can redistribute it and/or modify it
-- under the terms of the GNU General Public License as published by the
-- Free Software Foundation; either version 2, or (at your option) any later
-- version.
--
-- This program is distributed in the hope that it will be useful, but
-- WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
-- Public License for more details.
--
-- You should have received a copy of the GNU General Public License along
-- with this program; if not, write to the Free Software Foundation, Inc.,
-- 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
--
----------------------------------------------------------------------
--
-- Please ask <hackage@davidb.org> if you are interested in another
-- license.  If pieces of this program are useful in other systems I
-- will be willing to release them under a freer license, but I want
-- the program as a whole to be covered under the GPL.
--
----------------------------------------------------------------------
--
-- This is not intended to be a general purpose binding.  I'm using
-- Sqlite3 very different than typical databases.  Most things are
-- blobs.

module Sqlite3 (
   Connection,
   connect,
   query, exec,
   withTransaction,
   lastRowId,
   BoundParameter(..)
) where

import Prelude hiding (catch)
-- import Data.Dynamic
import Control.Monad (when, forM_, forM)
import Foreign
import Foreign.C
import qualified Data.ByteString as B
import Data.ByteString (ByteString)
import qualified Control.Exception
import Control.Exception (throwIO, catch)

-- Sqlite3 raises this particular exception on error.
-- data Sqlite3Error = Sqlite3Error String
--    deriving (Eq, Show, Typeable)

#include <sqlite3.h>

type Connection = Sqlite3

connect :: String -> IO Connection
connect path =
   alloca $ \pCon ->
   withCString path $ \pPath -> do
   res <- sqlite3_open pPath pCon
   con <- peek pCon
   checkResult con res
   return con

-- The various types that can be passed in a parameters.
data BoundParameter
   = BString String
   | BInt64 Int64
   | BByteString ByteString
   deriving (Eq, Show)

bindParameter :: BoundParameter -> Connection -> Stmt -> Int -> IO ()
bindParameter (BString parm) con statement index =
   withCStringLen parm $ \(pParm, lenParm) -> do
   res <- sqlite3_bind_text statement index pParm lenParm
      (intPtrToPtr (#const SQLITE_TRANSIENT))
   checkResult con res
bindParameter (BInt64 parm) con statement index = do
   res <- sqlite3_bind_int64 statement index parm
   checkResult con res
bindParameter (BByteString parm) con statement index = do
   B.useAsCStringLen parm $ \(pParam, lenParam) -> do
   res <- sqlite3_bind_blob statement index pParam lenParam
      (intPtrToPtr (#const SQLITE_TRANSIENT))
   checkResult con res

-- Wrap the operation in a transaction.  If the operation completes,
-- then the transaction will be committed.  If an exception is raised,
-- the transaction will be rolled back, and the exception re-raised.
withTransaction :: Connection -> (IO a) -> IO a
withTransaction con action = do
   exec con "begin transaction" []
   Control.Exception.catch actionFull $ \exc -> do
      exec con "rollback" []
      throwIO exc
   where
      actionFull = do
         result <- action
         exec con "commit" []
         return result

-- Perform a simple query that isn't expected to return any data.  The
-- list of arguments will be bound to the '?' parameters in the query
-- (if there are any).
exec :: Connection -> String -> [BoundParameter] -> IO ()
exec con qtext parms = do
   query con qtext parms () $ \_ _ -> fail "Unexpected query result"

-- Perform a query, Calling the handler on each of the results.
query :: Connection -> String -> [BoundParameter] -> a -> (a -> [BoundParameter] -> IO a) -> IO a
query con qtext parms state0 handler =
   alloca $ \pStatement ->
   alloca $ \pTail ->
   withCStringLen qtext $ \(pQuery, lenQuery) -> do
   res <- sqlite3_prepare con pQuery lenQuery pStatement pTail
   checkResult con res

   -- For now check that the entire query was consumed.
   tailP <- peek pTail
   when (minusPtr tailP pQuery /= lenQuery) $ fail "Length mismatch"

   statement <- peek pStatement

   -- Bind the parameters.
   forM_ (zip [1..] parms) $ \(index, parm) -> do
      (bindParameter parm) con statement index

   state' <- steps statement state0

   sqlite3_finalize statement >>= checkResult con

   -- Run the machine.
   return state'
   where
      steps statement state = do
         res <- sqlite3_step statement
         case res of
            (#const SQLITE_DONE) -> return state
            (#const SQLITE_ROW)  -> do
               state' <- handleRow statement state
               steps statement state'
            _                    -> do
               checkResultIs (#const SQLITE_DONE) con res
               return state

      handleRow statement state = do
         count <- sqlite3_column_count statement
         -- putStrLn $ "Got row " ++ show count ++ " columns"
         columns <- forM [0..count-1] $ \(index) -> do
            kind <- sqlite3_column_type statement index
            -- putStrLn $ "  col " ++ show index ++ " = " ++ show kind
            item <- case kind of
               (#const SQLITE_INTEGER) -> do
                  iVal <- sqlite3_column_int64 statement index
                  return $ BInt64 iVal
               (#const SQLITE_TEXT) -> do
                  pText <- sqlite3_column_text statement index
                  text <- peekCString pText
                  return $ BString text
               (#const SQLITE_BLOB) -> do
                  pText <- sqlite3_column_blob statement index
                  len <- sqlite3_column_bytes statement index
                  text <- B.copyCStringLen (pText, len)
                  return $ BByteString text
               _ -> do error "Unknown column type"
            -- putStrLn $ "    value = " ++ show item
            return item
         -- putStrLn $ "   data = " ++ show columns
         handler state columns

-- Return the rowid of the last insert.
lastRowId :: Connection -> IO Int64
lastRowId = sqlite3_last_insert_rowid

-- Check that a result is SQLITE_OK, and raise an exception if not.
checkResult :: Connection -> Int -> IO ()
checkResult = checkResultIs (#const SQLITE_OK)

checkResultIs :: Int -> Connection -> Int -> IO ()
checkResultIs expected con res =
   when (res /= expected) $ do
      pMsg <- sqlite3_errmsg con
      msg <- peekCString pMsg
      fail $ "Sqlite3 error (" ++ show res ++ "):" ++ msg

-- Low level binding.
type Sqlite3 = Ptr ()
type Stmt = Ptr ()
foreign import ccall "sqlite3.h" sqlite3_open :: CString -> (Ptr Sqlite3) -> IO Int
foreign import ccall "sqlite3.h" sqlite3_errmsg :: Sqlite3 -> IO CString
foreign import ccall "sqlite3.h" sqlite3_prepare :: Sqlite3 -> CString -> Int ->
   (Ptr Stmt) -> (Ptr CString) -> IO Int
foreign import ccall "sqlite3.h" sqlite3_finalize :: Stmt -> IO Int
foreign import ccall "sqlite3.h" sqlite3_step :: Sqlite3 -> IO Int
foreign import ccall "sqlite3.h" sqlite3_bind_text :: Stmt -> Int -> CString -> Int ->
   Ptr () -> IO Int
foreign import ccall "sqlite3.h" sqlite3_bind_blob :: Stmt -> Int -> CString -> Int ->
   Ptr () -> IO Int
foreign import ccall "sqlite3.h" sqlite3_bind_int64 :: Stmt -> Int -> Int64 -> IO Int
foreign import ccall "sqlite3.h" sqlite3_column_count :: Stmt -> IO Int
foreign import ccall "sqlite3.h" sqlite3_column_type :: Stmt -> Int -> IO Int
foreign import ccall "sqlite3.h" sqlite3_column_int64 :: Stmt -> Int -> IO Int64
foreign import ccall "sqlite3.h" sqlite3_column_text :: Stmt -> Int -> IO CString
foreign import ccall "sqlite3.h" sqlite3_column_bytes :: Stmt -> Int -> IO Int
foreign import ccall "sqlite3.h" sqlite3_column_blob :: Stmt -> Int -> IO CString
foreign import ccall "sqlite3.h" sqlite3_last_insert_rowid :: Sqlite3-> IO Int64
