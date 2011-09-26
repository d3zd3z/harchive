{-# LANGUAGE ForeignFunctionInterface #-}
-- vim: set ft=haskell:

#include <dirent.h>

module System.Linux.Directory (
   module System.Posix.Directory,
   linuxReadDirStream
) where

import System.Posix.Directory
import System.Posix.Types
import Foreign
import Foreign.C
import Unsafe.Coerce

-- The DirStream is just a newtype of a pointer, but it is hidden.

newtype LDirStream = LDirStream (Ptr CDir)
type CDir = ()
type CDirent = ()

-- Like @readDirStream@, but returns the inode number with the name in
-- a tuple.  This is basically copied from the
-- System.Posix.Directory.readDirStream and modified to also fetch the
-- inode number.
linuxReadDirStream :: DirStream -> IO (FilePath, FileID)
linuxReadDirStream rawDS =
   alloca $ \ptr_dEnt -> loop ptr_dEnt
   where
      (LDirStream dirp) = unsafeCoerce rawDS
      loop ptr_dEnt = do
         resetErrno
         r <- c_readdir dirp ptr_dEnt
         if (r == 0)
            then do
               dEnt <- peek ptr_dEnt
               if (dEnt == nullPtr)
                  then return ([], -1)
                  else do
                     entry <- (d_name dEnt >>= peekCString)
                     fid <- (#peek struct dirent, d_ino) dEnt
                     c_freeDirEnt dEnt
                     return (entry, fid)
            else do
               errno <- getErrno
               if (errno == eINTR) then loop ptr_dEnt else do
                  let (Errno eo) = errno
                  if (eo == 0)
                     then return ([], -1)
                     else throwErrno "linuxReadDirStream"

-- These appear to have to just be duplicated.
foreign import ccall unsafe "__hscore_readdir"
   c_readdir :: Ptr CDir -> Ptr (Ptr CDirent) -> IO CInt

foreign import ccall unsafe "__hscore_free_dirent"
   c_freeDirEnt :: Ptr CDirent -> IO ()

foreign import ccall unsafe "__hscore_d_name"
   d_name :: Ptr CDirent -> IO CString
