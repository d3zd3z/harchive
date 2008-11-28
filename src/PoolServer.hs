----------------------------------------------------------------------
-- The pool server.
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

module Main where

import qualified Sqlite3
import qualified Protocol
import qualified Store
import System.Environment
import Hash
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString as B

main :: IO ()
main = do
   args <- getArgs
   case args of
      ["serve", path] -> serve path
      _ -> error "Usage: hpool serve path"

serve :: String -> IO ()
serve path = do
   sql <- Sqlite3.connect $ path ++ "/" ++ "index.db"
   pool <- Store.openStore path sql
   Sqlite3.exec sql "begin transaction" []
   let
      haveQuery (Hash hash) = do
         reply <- Sqlite3.query sql
            "select kind, node, offset from blobs where hash = ?"
            [ Sqlite3.BByteString hash ] Protocol.RepNone $ \_ args ->
               case args of
                  [ Sqlite3.BString kind, Sqlite3.BInt64 node, Sqlite3.BInt64 offset ] ->
                     return $ (Protocol.RepHave kind node offset)
                  _ -> error "Unknown sql reply"
         -- putStrLn $ "Have reply: " ++ show reply
         return reply

      saveQuery hash kind lPayload uncomplen = do
         let payload = B.concat $ L.toChunks lPayload
         (node, offset) <- Store.poolWriteBlob pool (
            Store.Blob {
               Store.bHash = hash,
               Store.bKind = kind,
               Store.bUncompLen = fromIntegral uncomplen,
               Store.bPayload = payload })
         Sqlite3.exec sql
            "insert into blobs (hash, kind, node, offset) values (?, ?, ?, ?)"
            [ Sqlite3.BByteString $ toByteString hash,
               Sqlite3.BString kind,
               Sqlite3.BInt64 $ fromIntegral node,
               Sqlite3.BInt64 $ fromIntegral offset ]
         return Protocol.RepDone

      retrieveQuery (Hash hash) = do
         answer <- Sqlite3.query sql
            "select kind, node, offset from blobs where hash = ?"
            [ Sqlite3.BByteString hash] Protocol.RepNone $ \_ args ->
               case args of
                  [ Sqlite3.BString kind, Sqlite3.BInt64 node, Sqlite3.BInt64 offset ] -> do
                     (Store.Blob _hash' _kind' uncomplen payload) <-
                        Store.poolReadBlob pool (fromIntegral node) (fromIntegral offset)
                     -- TODO: Verify that what we got back is right.
                     let lPayload = L.fromChunks [payload]
                     return $ Protocol.RepData kind lPayload (fromIntegral uncomplen)
                  _ -> fail "Unknown sql reply"
         -- putStrLn $ "Retrieved: " ++ show answer
         return answer

      mapQuery uuid = do
         result <- Sqlite3.query sql
            "select dev from devmap where uuid = ?" [ Sqlite3.BString uuid ]
            Nothing $ \_ args ->
               case args of
                  [ Sqlite3.BInt64 smallid ] -> return $ Just $ smallid
                  _ -> fail "Unknown sql reply"
         case result of
            Nothing -> do
               Sqlite3.exec sql "insert into devmap (uuid) values (?)"
                  [ Sqlite3.BString uuid ]
               Sqlite3.lastRowId sql
            Just smallid -> return smallid

      putCacheQuery dev ino lInfo = do
         let info = B.concat $ L.toChunks lInfo
         Sqlite3.exec sql "insert or replace into cache (dev, ino, info) values (?, ?, ?)"
            [ Sqlite3.BInt64 $ fromIntegral dev,
               Sqlite3.BInt64 $ ino,
               Sqlite3.BByteString info ]
         return Protocol.RepDone

      getCacheQuery dev ino = do
         result <- Sqlite3.query sql
            "select info from cache where dev = ? and ino = ?"
            [ Sqlite3.BInt64 $ fromIntegral dev, Sqlite3.BInt64 ino ]
            Nothing $ \_ args ->
               case args of
                  [ Sqlite3.BByteString info ] -> return $ Just $ info
                  _ -> fail "Unknown sql reply"
         case result of
            Nothing -> return $ Protocol.RepNone
            Just info -> do
               let lInfo = L.fromChunks [info]
               return $ Protocol.RepCacheHave lInfo

      getBackupsQuery = do
         result <- Sqlite3.query sql
            "select hash from blobs where kind = 'backup'" []
            [] $ \xs args ->
               case args of
                  [ Sqlite3.BByteString x ] -> return $ (Hash x):xs
                  _ -> fail "Unknown sql reply"
         return $ Protocol.RepBackups result

      goodbyeQuery = do
         -- Commit the transaction so that these results are saved.
         putStrLn $ "Begin flush/commit"
         Store.poolFlush pool
         Sqlite3.exec sql "commit" []
         putStrLn $ "End flush/commit"
         Sqlite3.exec sql "begin transaction" []
         return Protocol.RepGoodbye

   Protocol.serve 7195 $ \req -> do
      -- putStrLn $ "Reqeust: " ++ show req
      case req of
         Protocol.ReqHello -> return Protocol.RepHello
         Protocol.ReqGoodbye -> goodbyeQuery
         (Protocol.ReqHave hash) -> haveQuery hash
         (Protocol.ReqSave hash kind blob complen) -> saveQuery hash kind blob complen
         (Protocol.ReqRetrieve hash) -> retrieveQuery hash
         (Protocol.ReqMapping uuids) -> do
            -- putStrLn $ "mapping: " ++ show uuids
            smallids <- mapM mapQuery uuids
            -- putStrLn $ "map to : " ++ show smallids
            return $ Protocol.RepMapping $ map fromIntegral smallids
         (Protocol.ReqPutCache dev ino info) -> putCacheQuery dev ino info
         (Protocol.ReqGetCache dev ino) -> getCacheQuery dev ino
         Protocol.ReqBackups -> getBackupsQuery
