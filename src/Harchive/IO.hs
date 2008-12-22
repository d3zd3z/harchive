----------------------------------------------------------------------
-- Filesystem IO for harchive.
----------------------------------------------------------------------

module Harchive.IO (
   setFileAtts, setDirAtts, restoreSymLink
) where

import Harchive.Store.Sexp

import qualified Control.Exception as E
import System.IO
import System.Posix

-- TODO: Detect and handle sparse files.
-- TODO: Open files for reading on Linux with NO_ATIME set.

-- Set the attributes on the file.  The handle will be closed by this
-- operation, since that is needed to set some of the attributes.
-- The handleToFd closes the Handle, so the Posix descriptor needs to
-- be closed as well.
setFileAtts :: FilePath -> Handle -> Attr -> IO ()
setFileAtts name handle atts = do
   fd <- handleToFd handle
   -- putStrLn $ "Atts: " ++ show atts
   whenMaybe (field atts "UID" :: Maybe Integer) $ \uid ->
      whenMaybe (field atts "GID" :: Maybe Integer) $ \gid -> do
	 failable $ setFdOwnerAndGroup fd (fromInteger uid) (fromInteger gid)
   whenMaybe (field atts "MODE" :: Maybe Integer) $ \mode -> do
      setFdMode fd (fromInteger mode)
   closeFd fd
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

-- Restore a symlink.  There is some trickery here to getting the
-- permissions correct.
restoreSymLink :: FilePath -> Attr -> IO ()
restoreSymLink name atts = do
   let newMode = maybe 0644 id (field atts "MODE" :: Maybe Integer)
   oldMode <- setFileCreationMask $ fromInteger newMode
   (flip E.finally) (setFileCreationMask oldMode) $ do
      whenMaybe (field atts "LINK") $ \target -> do
	 createSymbolicLink target name
   whenMaybe (field atts "UID" :: Maybe Integer) $ \uid ->
      whenMaybe (field atts "GID" :: Maybe Integer) $ \gid -> do
	 failable $ setSymbolicLinkOwnerAndGroup name (fromInteger uid) (fromInteger gid)

whenMaybe :: Maybe a -> (a -> IO ()) -> IO ()
whenMaybe Nothing _ = return ()
whenMaybe (Just a) op = op a

failable :: IO () -> IO ()
failable = E.handle (const $ return ())
