----------------------------------------------------------------------
-- Filesystem IO for harchive.
----------------------------------------------------------------------

module Harchive.IO (
   setFileAtts, setDirAtts, restoreSymLink, restoreOther
) where

import Harchive.Store.Sexp

import qualified Control.Exception as E
import qualified System.IO.Error as Error
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
   setUidGid (setFdOwnerAndGroup fd) atts
   setMode (setFdMode fd) atts
   closeFd fd
   setTimes name atts

setDirAtts :: FilePath -> Attr -> IO ()
setDirAtts name atts = do
   setUidGid (setOwnerAndGroup name) atts
   setMode (setFileMode name) atts
   setTimes name atts

-- Restore a symlink.  There is some trickery here to getting the
-- permissions correct.
restoreSymLink :: FilePath -> Attr -> IO ()
restoreSymLink name atts = do
   let newMode = maybe 0644 id (field atts "MODE" :: Maybe Integer)
   withCreationMask (fromInteger newMode) $ do
      whenMaybe (field atts "LINK") $ \target -> do
	 createSymbolicLink target name
   setUidGid (setSymbolicLinkOwnerAndGroup name) atts

-- Restore "other" things, such as device nodes, fifos and the likes.
restoreOther :: FilePath -> Attr -> IO ()
restoreOther name atts = do
   case attrKind atts of
      "CHR" -> restoreDevice name characterSpecialMode atts
      "BLK" -> restoreDevice name blockSpecialMode atts
      "FIFO" -> restoreFifo name atts
      kind -> putStrLn $ "Skipping \"" ++ kind ++ "\" node: " ++ name

restoreDevice :: FilePath -> FileMode -> Attr -> IO ()
restoreDevice name kind atts = do
   whenMaybe (field atts "MODE" :: Maybe Integer) $ \mode ->
      whenMaybe (field atts "RDEV" :: Maybe Integer) $ \rdev -> do
	 withCreationMask 0 $ do
	    failable $ createDevice name
	       (fromInteger mode `unionFileModes` kind)
	       (fromInteger rdev)
   setUidGid (setOwnerAndGroup name) atts
   setTimes name atts

restoreFifo :: FilePath -> Attr -> IO ()
restoreFifo name atts = do
   whenMaybe (field atts "MODE" :: Maybe Integer) $ \mode ->
      withCreationMask 0 $ do
	 createNamedPipe name (fromInteger mode)
   setUidGid (setOwnerAndGroup name) atts
   setTimes name atts

withCreationMask :: FileMode -> IO () -> IO ()
withCreationMask mask action = do
   oldMode <- setFileCreationMask mask
   E.finally action (setFileCreationMask oldMode)

-- Use the given function to set the name and attributes for the node.
setUidGid :: (UserID -> GroupID -> IO ()) -> Attr -> IO ()
setUidGid op atts = do
   whenMaybe (field atts "UID" :: Maybe Integer) $ \uid ->
      whenMaybe (field atts "GID" :: Maybe Integer) $ \gid -> do
	 failable $ op (fromInteger uid) (fromInteger gid)

-- Use the given function to set the mode attributes of the given
-- entity.
setMode :: (FileMode -> IO ()) -> Attr -> IO ()
setMode op atts = do
   whenMaybe (field atts "MODE" :: Maybe Integer) $ \mode -> do
      op (fromInteger mode)

setTimes :: FilePath -> Attr -> IO ()
setTimes name atts =
   whenMaybe (field atts "MTIME" :: Maybe Integer) $ \mtime' -> do
      let mtime = fromInteger mtime'
      setFileTimes name mtime mtime

whenMaybe :: Maybe a -> (a -> IO ()) -> IO ()
whenMaybe Nothing _ = return ()
whenMaybe (Just a) op = op a

failable :: IO () -> IO ()
failable action = Error.catch action (const $ return ())
