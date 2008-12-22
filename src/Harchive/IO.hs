----------------------------------------------------------------------
-- Filesystem IO for harchive.
----------------------------------------------------------------------

module Harchive.IO (
   setFilePreCloseAtts, setFilePostCloseAtts, setDirAtts
) where

import Harchive.Store.Sexp

import qualified Control.Exception as E
import System.IO
import System.Posix

-- TODO: Detect and handle sparse files.
-- TODO: Open files for reading on Linux with NO_ATIME set.

setFilePreCloseAtts :: Handle -> Attr -> IO ()
setFilePreCloseAtts handle atts = do
   fd <- handleToFd handle -- Note: Closes handle.
   -- putStrLn $ "Atts: " ++ show atts
   whenMaybe (field atts "UID" :: Maybe Integer) $ \uid ->
      whenMaybe (field atts "GID" :: Maybe Integer) $ \gid -> do
	 failable $ setFdOwnerAndGroup fd (fromInteger uid) (fromInteger gid)
   whenMaybe (field atts "MODE" :: Maybe Integer) $ \mode -> do
      setFdMode fd (fromInteger mode)
   return ()

setFilePostCloseAtts :: FilePath -> Attr -> IO ()
setFilePostCloseAtts name atts = do
   whenMaybe (field atts "MTIME" :: Maybe Integer) $ \mtime' -> do
      let mtime = fromInteger mtime'
      setFileTimes name mtime mtime

setDirAtts :: FilePath -> Attr -> IO ()
setDirAtts name atts = do
   whenMaybe (field atts "UID" :: Maybe Integer) $ \uid ->
      whenMaybe (field atts "GID" :: Maybe Integer) $ \gid -> do
	 failable $ setOwnerAndGroup name (fromInteger uid) (fromInteger gid)
   whenMaybe (field atts "MODE" :: Maybe Integer) $ \mode -> do
      setFileMode name (fromInteger mode)
   whenMaybe (field atts "MTIME" :: Maybe Integer) $ \mtime' -> do
      let mtime = fromInteger mtime'
      setFileTimes name mtime mtime

whenMaybe :: Maybe a -> (a -> IO ()) -> IO ()
whenMaybe Nothing _ = return ()
whenMaybe (Just a) op = op a

failable :: IO () -> IO ()
failable = E.handle (const $ return ())
