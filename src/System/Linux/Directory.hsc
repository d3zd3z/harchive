----------------------------------------------------------------------
-- vim: set filetype=haskell :
--
-- Directory reading optimized in Linux.
----------------------------------------------------------------------

module System.Linux.Directory (
   module System.Posix.Directory,
   linuxReadDirStream,
   LinuxDirInfo,
   DirFileKind, dtUnknown, dtFifo, dtChr, dtDir, dtBlk, dtReg, dtLnk,
   dtSock, dtWht
) where

-- Make sure we use the 64-bit version.
#define _FILE_OFFSET_BITS 64
#include <dirent.h>

import System.Posix.Directory hiding (DirStream)
import qualified System.Posix.Directory as D

import System.Posix.Types
import System.Posix.Internals
import Foreign
import Foreign.C
import Unsafe.Coerce

type LinuxDirInfo = (FilePath, FileID, DirFileKind)

-- |The Linux Directory operation returns additional information
-- (filetype and inode number).  There are several useful things to do
-- with this data, including traversing only directories, and sorting
-- the nodes by inode number before statting the nodes.  On some
-- filesystem types (ext3 specifically), this sort can drastically
-- reduce disk thrashing on the stat.
linuxReadDirStream :: D.DirStream -> IO (Maybe LinuxDirInfo)
linuxReadDirStream posixDir = do
   let (LinuxDirStream dirp) = unsafeCoerce posixDir
   alloca $ \ptr_dEnt -> loop dirp ptr_dEnt
   where
      loop dirp ptr_dEnt = do
         resetErrno
         r <- readdir dirp ptr_dEnt
         if r == 0
            then do
               dEnt <- peek ptr_dEnt
               if dEnt == nullPtr
                  then return Nothing
                  else do
                     name <- (peekCString $ dName dEnt)
                     ino <- dIno dEnt
                     kind <- dType dEnt
                     freeDirEnt dEnt
                     return $ Just (name, ino, DirFileKind $ fromIntegral kind)
            else do
               errno <- getErrno
               if (errno == eINTR) then loop dirp ptr_dEnt else do
               let (Errno eo) = errno
               if (eo == end_of_dir)
                  then return Nothing
                  else throwErrno "linuxReadDirStream"

newtype LinuxDirStream = LinuxDirStream (Ptr CDir)

newtype DirFileKind = DirFileKind CInt
   deriving (Eq, Show)

{-
dOff = #peek struct dirent, d_off
-}

dType :: Ptr CDirent -> IO CChar
dType = #peek struct dirent, d_type

dIno :: Ptr CDirent -> IO FileID
dIno = (#peek struct dirent, d_ino)

dName :: Ptr CDirent -> CString
dName = #ptr struct dirent, d_name

#enum DirFileKind, DirFileKind, \
   DT_UNKNOWN, \
   DT_FIFO, \
   DT_CHR, \
   DT_DIR, \
   DT_BLK, \
   DT_REG, \
   DT_LNK, \
   DT_SOCK, \
   DT_WHT
