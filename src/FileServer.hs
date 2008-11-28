----------------------------------------------------------------------
-- Simple file server main.
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

module Main (main) where

import System.Time
import qualified Network.BSD
import Data.Char (ord)
import Hash
import qualified Protocol
import System.Environment
import System.IO
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import qualified Codec.Compression.Zlib.Raw as Zlib
import Control.Monad (unless, when, forM_, forM)
import System.Console.GetOpt
import Network (PortNumber)
import Data.Bits ((.|.))
import Control.Concurrent
import Data.Int (Int32, Int64)
import qualified Control.Exception as E
import qualified Status
import Data.Maybe (catMaybes)
import Devid

-- Used for directory stuff.
import Control.Exception (bracket)
import Data.List (sort, partition, lookup)
import System.Posix
import qualified Data.Map as Map
import Data.Map (Map)
import Data.Binary
import Attributes

main :: IO ()
main = do
   args <- getArgs
   case getOpt Permute options args of
      -- Put a single or set of files.
      (opts, ("put":files@(_:_)),[]) -> do
         withServer opts $ \manager -> do
            forM_ files $ \file -> do
               hash <- blobifyFile file manager
               putStrLn $ show hash ++ " " ++ file

      -- Retrieve individual files.
      (opts, ("get":files@(_:_:_)),[]) -> do
         let pairs = pairup files
         withServer opts $ \manager -> do
            forM_ pairs $ \(hashStr, file) -> do
               let hash = fromHex hashStr
               restoreFile manager hash file

      -- Dump a directory.
      (opts, ["dump", path],[]) -> do
         withServer opts $ \manager -> do
            hash <- dumpDir path manager
            Status.withIO (myStatus manager) $
               putStrLn $ "root hash: " ++ show hash

      -- Show available backups.
      (opts, ["catalog"],[]) ->
         withServer opts $ \manager -> do
            showCatalog manager

      -- List a previously done dump.
      (opts, ["list", hash],[]) ->
         withServer opts $ \manager -> do
            listDir manager (fromHex hash)

      -- Restore a backup to the specified directory.
      (opts, ["restore", hash, path],[]) ->
         withServer opts $ \manager -> do
            restoreDir manager (fromHex hash) path

      (_,_,errs) ->
         ioError (userError (concat errs ++ usageInfo header options))
   return ()
   where
      header = "Usage: hfile [OPTION...] put file ...\n" ++
               "                         get hash file ...\n" ++
               "                         dump dir ...\n" ++
               "                         catalog\n" ++
               "                         list hash\n" ++
               "                         restore hash destPath"

      pairup (a:b:rest) = (a,b) : pairup rest
      pairup [_] = error "Must have an even number of arguments"
      pairup [] = []

withServer :: [Flag] -> (Manager -> IO ()) -> IO ()
withServer opts thunk = do
   let host = foldl getHost "localhost" opts
   let port = foldl getPort 7195 opts
   printer <- Status.start (foldl getVerbose 0 opts)
   client <- Protocol.makeClient host port
   mapping <- getDevMapping
   localMapping <- convertMapping client mapping
   let
      -- Send off a 'have' request for a particular blob.  Keep the
      -- data around until we get a reply, to determine if we need to
      -- send the blob itself to the server.
      blobWriter blob@(Uncompressed kind buf) = do
         let hash = hashOf $ L.append (packString kind) buf
         client (Protocol.ReqHave hash) $ \reply ->
            case reply of
               (Protocol.RepHave _kind _node _offset) -> do
                  Status.addDupedData printer (fromIntegral $ L.length buf)
                  return Protocol.Finished
               Protocol.RepNone -> do
                  let
                     (newbuf, uncomplen) = case tryCompress blob of
                        (Uncompressed _ n) -> (n, -1)
                        (Compressed _ n uc) -> (n, uc)
                  Status.addSavedData printer (fromIntegral $ L.length buf)
                  -- TODO: Add compression ration information.
                  let savedata = Protocol.ReqSave hash kind newbuf (fromIntegral uncomplen)
                  return $ Protocol.Another (savedata, savedone)
               _ -> fail "Invalid server reponse"
         return hash
      blobWriter blob@(Compressed _ _ _) = do
         putStrLn $ "Warning, shouldn't be seeing a compressed blob here"
         blobWriter . tryCompress $ blob

      -- Send off a retrieve request for a particular blob, sticking
      -- it into the destination MVar.  Sticks in 'Nothing' if not
      -- present, or 'Just Blob' if it is.
      blobReader hash result = do
         client (Protocol.ReqRetrieve hash) $ \reply -> do
            case reply of
               (Protocol.RepNone) -> putMVar result Nothing
               (Protocol.RepData kind payload uncomplen) ->
                  putMVar result $ Just (makeBlob kind payload uncomplen)
               _ -> fail "Invalid server response"
            return Protocol.Finished

      myPutCache dev ino info = do
         client (Protocol.ReqPutCache dev ino info) $ \reply -> do
            case reply of
               (Protocol.RepDone) -> return Protocol.Finished
               _ -> fail "Invalid server response"

      myGetCache dev ino = do
         box <- newEmptyMVar
         client (Protocol.ReqGetCache dev ino) $ \reply -> do
            case reply of
               (Protocol.RepNone) -> putMVar box Nothing
               (Protocol.RepCacheHave info) -> putMVar box $ Just info
               _ -> fail "Invalid server response"
            return Protocol.Finished
         return box

      myGetBackups = do
         box <- newEmptyMVar
         client Protocol.ReqBackups $ \reply -> do
            case reply of
               (Protocol.RepBackups set) -> putMVar box set
               _ -> fail "Invalid server response"
            return Protocol.Finished
         takeMVar box

      doLogWarning msg =
         Status.withIO printer (putStrLn $ "Warning: " ++ msg)

      -- Simple, static exclusions.
      -- TODO: Obviously needs to be stored and computed.
      myExclusions entry = elem entry exclusionList

      -- Given a response from the server, make an appropriate blob
      -- out of it.
      makeBlob kind payload uncomplen | uncomplen == -1 =
         Uncompressed kind payload
                                      | otherwise =
         Compressed kind payload uncomplen

      -- Finish a save request.
      savedone reply = do
         -- putStrLn $ "Done saving"
         when (reply /= Protocol.RepDone) $ fail "Invalid reply from server"
         return Protocol.Finished

      manager = Manager {
         writeBlob = blobWriter,
         readBlob = blobReader,
         myStatus = printer,
         devMap = localMapping,
         putCache = myPutCache,
         getCache = myGetCache,
         getBackups = myGetBackups,
         isExcluded = myExclusions,
         logWarning = doLogWarning }

   thunk manager
   Status.stop printer
   Protocol.clientClose client

   where
      getHost _ (Host x) = x
      getHost x _ = x

      getPort _ (Port x) = x
      getPort x _ = x

      getVerbose n Verbose = n + 1
      getVerbose n _ = n

data Flag
   = Host String
   | Port PortNumber
   | Verbose
   deriving (Eq, Show)

options :: [OptDescr Flag]
options = [
   Option ['h'] ["host"] (ReqArg Host "HOST") "host HOST",
   Option ['p'] ["port"]
      (ReqArg (Port . fromIntegral . (read :: String -> Int)) "PORT")
      "port PORT",
   Option ['v'] ["verbose"] (NoArg Verbose) "Increase verbosity" ]

packString :: String -> L.ByteString
packString = L.pack . map (fromIntegral . ord)

----------------------------------------------------------------------
-- A simple exclusion list.
-- TODO: This shouldn't be a bunch of static things, but it at least
-- let's me test the exclusion code.
-- These are absolute paths, built up from the command line, and there
-- isn't any fuzzy matching (yet), so if there is a trailing slash on
-- the argument, there will be a doubled slash in the pathname.
--
-- Also, at this point, only directories can be excluded.  The
-- directory itself will still be backed up, but it will have no
-- children in the backup.
exclusionList :: [String]
exclusionList = [
   -- Exclude in both the snapshot directory and the base dir.
   "/tmp",
   "/mnt/snap/root/tmp",
   "/mnt/snap/root64/tmp",
   "/var/tmp",
   "/mnt/snap/var/tmp",
   "/mnt/snap/var64/tmp",
   "/usr/portage/distfiles",
   "/mnt/snap/usr/portage/distfiles",
   "/mnt/snap/usr64/portage/distfiles",

   -- Files on my laptop.
   "/mnt/root/tmp",
   "/mnt/root/usr/portage/distfile"
   ]

----------------------------------------------------------------------
-- Management.  Coordinates all of the work that is happening here.
data Manager = Manager {
   writeBlob :: Blob -> IO Hash,
   -- Make sure this blob is saved away, and return the 'hash' we can
   -- later use to reference it.

   readBlob :: Hash -> (MVar (Maybe Blob)) -> IO (),

   -- Put a cache entry for the given directory.
   putCache :: Int -> Int64 -> L.ByteString -> IO (),

   -- Read a cache entry for the given directory.
   getCache :: Int -> Int64 -> IO (MVar (Maybe L.ByteString)),

   -- Get the backups that have been done.
   getBackups :: IO [Hash],

   -- Is the specified pathname excluded from the backup.
   isExcluded :: String -> Bool,

   -- Status update variable
   myStatus :: Status.State,

   -- Log a warning message
   logWarning :: String -> IO (),

   devMap :: DeviceID -> Int
}

----------------------------------------------------------------------
-- Mapping conversion.
--
-- Convert the DeviceID to UUID mapping returned from the Devid info,
-- and lookup/add each entry into the database.
-- The mappings are kept on the server, and requested each time.
-- It would also be possible to only request mappings as they were
-- needed.

convertMapping :: Protocol.Client -> DevMapping -> IO (DeviceID -> Int)
convertMapping client srcMapping = do
   let (keys, uuids) = unzip . Map.toAscList $ srcMapping
   smallidsM <- newEmptyMVar
   client (Protocol.ReqMapping uuids) $ \response -> do
      case response of
         Protocol.RepMapping smallids -> do
            putMVar smallidsM smallids
            return Protocol.Finished
         _ -> fail "Unknown response from server"
   smallids <- takeMVar smallidsM
   let myMap = Map.fromAscList (zip keys smallids)
   return $ (myMap Map.!)

----------------------------------------------------------------------
-- A single blob/chunk from the database.
-- First two fields are the 'kind' and the blob itself.  The
-- compressed version also contains a count of the size when
-- uncompressed.
data Blob
   = Uncompressed String L.ByteString
   | Compressed String L.ByteString Int32
   deriving (Show)

-- Compress (if necessary), and return the kind and payload of the
-- blob.
blobData :: Blob -> (String, L.ByteString)
blobData (Uncompressed kind payload) = (kind, payload)
blobData (Compressed kind zPayload uncomplen) =
   E.assert (L.length payload == fromIntegral uncomplen) $ (kind, payload)
   where
      payload = Zlib.decompress zPayload

-- Try compressing a blob.  Returns the original blob if the
-- compression was not helpful, otherwise returns the compressed data.
tryCompress :: Blob -> Blob
tryCompress (Uncompressed kind src) =
   if L.length compressed > L.length src
      -- TODO: Learn about the precedence here.
      then (Uncompressed kind src)
      else (Compressed kind compressed (fromIntegral (L.length src)))
   where
      compressed = Zlib.compressWith (Zlib.CompressionLevel 2) src
tryCompress x = x

----------------------------------------------------------------------
-- Processing directories.
--
-- Wander through the directory, dumping out the files found, and
-- archiving away the directories as well.

-- The top level stats out, and dumps the root of the tree, and then
-- writes out a record describing this particular dump.
-- The top-level directory must be readable, so don't trap any errors
-- here.
dumpDir :: FilePath -> Manager -> IO Hash
dumpDir path manager = do
   startTime <- getClockTime
   myHost <- Network.BSD.getHostName

   rootStat <- getSymbolicLinkStatus path
   unless (isDirectory rootStat) $ fail "Root of backup must be a directory"
   rootHash <- walkDir manager (deviceID rootStat) path rootStat

   -- Encode the backup set entry for this.
   let
      info = BackupInfo {
         backupTime = startTime,
         backupHost = myHost,
         backupRootAttribute = Att path (fileKind rootStat) $
            Map.fromList (("hash", H rootHash) : statusToMap rootStat)
         }
      encodedInfo = encode info

   rootKey <- writeBlob manager $ Uncompressed "backup" encodedInfo

   return rootKey

-- Recursively walk down a given directory tree, Scans in directory
-- contents.
walkDir :: Manager -> DeviceID -> FilePath -> FileStatus -> IO Hash
walkDir manager rootDev path stat = do
   -- Get the names of all of the entries in this directory.
   -- Only descend this directory if it is on the same device.
   -- TODO: This could be an option.
   names <- if (rootDev == deviceID stat && not (isExcluded manager path))
      then bracket (openDirStream path) closeDirStream (getNames [])
      else return []

   -- Stat all of the names that we can.  We ignore anything we can't
   -- stat (after logging a message).
   nameStats1 <- mapM safeStat names
   let nameStats2 = catMaybes nameStats1

   -- To allow time back for queries from the server, files and
   -- directories need to be handled specially.  Everything else can
   -- be handled by the regular processing.
   let (dirs, nameStats3) = partition (isDirectory . snd) nameStats2
   let (files, nameStats4) = partition (isRegularFile . snd) nameStats3
   let (symlinks, nameStats5) = partition (isSymbolicLink . snd) nameStats4
   let (devices, others) = partition (\ (_, x) -> isBlockDevice x || isCharacterDevice x) nameStats5

   -- Send off a query for any cached information about this
   -- directory.
   cacheQueryBox <- sendCacheQuery

   -- While waiting for that response, recursively descend children
   -- directories.  We keep alive all data associated with the deepest
   -- directory tree in the system.
   dirHashes <- mapM safeWalkDir dirs

   Status.setPath (myStatus manager) path

   symlinkInfo <- mapM safeReadLink symlinks
   let deviceInfo = map getDeviceInfo devices

   -- Wait for the results of the cache query.  Done as late as
   -- possible to give the query more time to come back.
   cachedInfo <- getCacheQueryResult cacheQueryBox
   fileHashes <- mapM (safeHashFileCached cachedInfo) files

   let
      attributes = zipWith encodeCommon dirs dirHashes ++
         zipWith encodeCommon files fileHashes ++
         zipWith encodeCommon symlinks symlinkInfo ++
         zipWith encodeCommon devices deviceInfo ++
         zipWith encodeCommon others (repeat [])

   -- TMP: Print out some stuff to reduce warnings.
   -- putStrLn $ "in " ++ path
   -- putStrLn $ "   atts: " ++ show attributes

   encodedSelf <- encodedDir attributes

   -- Write out a cache entry for this directory.
   writeCacheEntry $ zip files fileHashes

   Status.addDirectory (myStatus manager) 1

   return encodedSelf

   where
      fullPath = ((path ++ "/") ++)

      -- Stat a given name, returning a Maybe of the name and the stat
      -- information.
      safeStat :: FilePath -> IO (Maybe (FilePath, FileStatus))
      safeStat name = do
         E.handle (\_ -> return Nothing) $ do
            stats <- getSymbolicLinkStatus $ fullPath name
            return $ Just (name, stats)

      -- Safely walk down a new directory.
      safeWalkDir :: (FilePath, FileStatus) -> IO [(String, Field)]
      safeWalkDir (childName, childStat) = do
         E.handle dirError $ do
            childHash <- walkDir manager rootDev (fullPath childName) childStat
            return $ [("hash", H childHash)]
         where
            dirError _ = do
               logWarning manager $ "Warning: Unable to read dir" ++ fullPath childName
               -- Decisions.  We could also just not include a hash.
               emptyHash <- encodedDir []
               return $ [("hash", H emptyHash)]

      -- Safely determine the hash of a specified file.
      safeHashFile :: (FilePath, FileStatus) -> IO [(String, Field)]
      safeHashFile (childName, _childStat) = do
         E.handle fileError $ do
            childHash <- blobifyFile (fullPath childName) manager
            return $ [("hash", H childHash)]
         where
            fileError _ = do
               logWarning manager $ "Warning: Unable to read file" ++ fullPath childName
               emptyHash <- writeBlob manager $ Uncompressed "blob" L.empty
               return $ [("hash", H emptyHash)]

      -- Determine if the file is in the cache, and either return the
      -- cached hash, or call safeHashFile.
      safeHashFileCached :: CacheInfoMap -> (FilePath, FileStatus) -> IO [(String, Field)]
      safeHashFileCached cache child@(_cname, cstat) = do
         let ctime = fromIntegral $ fromEnum $ statusChangeTime cstat
         let mtime = fromIntegral $ fromEnum $ modificationTime cstat
         let ino = fromIntegral $ fileID cstat
         case Map.lookup ino cache of
            Nothing -> do
               -- putStrLn $ "miss  " ++ fullPath cname
               safeHashFile child
            Just (ct, mt, hash)
               | ct == ctime && mt == mtime -> do
                  Status.addSkippedFile (myStatus manager) 1
                  Status.addSkippedData (myStatus manager)
                     (fromIntegral . fileSize $ cstat)
                  -- putStrLn $ "hit   " ++ fullPath cname
                  return $ [("hash", H hash)]
               | otherwise -> do
                  -- putStrLn $ "stale " ++ fullPath cname
                  safeHashFile child

      -- Safely read the targets of a symlink.
      safeReadLink :: (FilePath, FileStatus) -> IO [(String, Field)]
      safeReadLink (childName, _childStat) = do
         E.handle linkError $ do
            linkTarget <- readSymbolicLink $ fullPath childName
            return [("link", S linkTarget)]
         where
            linkError _ = do
               logWarning manager $ "Warning: Unable to read symlink" ++ fullPath childName
               return [("link", S "???")]

      -- Extract out the special device info.
      getDeviceInfo :: (FilePath, FileStatus) -> [(String, Field)]
      getDeviceInfo (_childName, childStat) =
         [("rdev", I rdev)]
         where
            rdev = fromIntegral $ fromEnum $ specialDeviceID childStat

      -- Encode the common attributes of the given file, adding in any
      -- special attributes as needed.
      encodeCommon :: (FilePath, FileStatus) -> [(String, Field)] -> Attribute
      encodeCommon (childName, childStat) extraAtts =
         Att childName (fileKind childStat) fields
         where
            fields = Map.fromList $ extraAtts ++ statusToMap childStat

      -- Encode the results of a directory into a blob, send it, and
      -- return the hash of the result.
      encodedDir :: [Attribute] -> IO Hash
      encodedDir atts = do
         let eAtts = encode atts
         writeBlob manager $ Uncompressed "dir" eAtts

      -- Write a cache entry out for this directory.  Includes all
      -- entries that we have a "hash" value for.
      writeCacheEntry :: [((FilePath, FileStatus), [(String, Field)])] -> IO ()
      writeCacheEntry files = do
         let
            indices :: CacheInfo
            indices = catMaybes $ map offile files
            cacheData = compressor $ encode indices
            compressor = Zlib.compressWith (Zlib.CompressionLevel 2)
            offile ((_name, childStat), atts) =
               case lookup "hash" atts of
                  Nothing -> Nothing
                  Just hash -> Just (
                     fromIntegral $ fromEnum $ fileID childStat :: Integer,
                     (fromIntegral $ fromEnum $ statusChangeTime childStat :: Integer,
                     fromIntegral $ fromEnum $ modificationTime childStat :: Integer,
                     forceH hash))
            forceH (H val) = val
            forceH _ = error "Incorrect hash type"
         putCache manager (devMap manager $ deviceID stat)
            (fromIntegral $ fileID stat)
            cacheData

      -- Send a request off for a cache entry, returning the mbox that
      -- getCacheQueryResult can use to get this information later.
      sendCacheQuery = do
         getCache manager dev inum
         where
            dev = devMap manager $ deviceID stat
            inum = fromIntegral $ fileID stat

      -- Wait for the result of a cache query, decode it, and map a
      -- mapping for lookups.
      getCacheQueryResult box = do
         info <- takeMVar box
         let
            indices = case info of
               Nothing -> []
               Just b -> do
                  decode $ Zlib.decompress b :: CacheInfo
         return $ Map.fromList indices

-- Information written to the cache structure.  Typed here to make
-- sure we encode and decode the same type.
type CacheInfo = [(Integer, (Integer, Integer, Hash))]
type CacheInfoMap = Map Integer (Integer, Integer, Hash)

-- TODO: Need to store attributes for the root directory.
data BackupInfo = BackupInfo {
   backupTime :: ClockTime,
   backupHost :: String,
   backupRootAttribute :: Attribute }
   deriving (Show)

instance Binary BackupInfo where
   put (BackupInfo (TOD t _) h at) = do
      put t
      put h
      put at
   get = do
      t <- get
      h <- get
      at <- get
      return $ BackupInfo (TOD t 0) h at

-- Extract the names in the given DirStream, eliminating "." and "..",
-- and sorting the result.
getNames :: [String] -> DirStream -> IO [String]
getNames names str = do
   name <- readDirStream str
   case name of
      "" -> return (sort names)
      "." -> getNames names str
      ".." -> getNames names str
      _ -> getNames (name:names) str

-- Turn a FileStatus into a Map of associations describing the data.
-- These are all represented as Integers.
statusToMap :: FileStatus -> [(String, Field)]
statusToMap status = [
      ("dev", I . fromIntegral . fromEnum $ deviceID status),
      ("ino", I . fromIntegral $ fileID status),
      ("nlink", I . fromIntegral $ linkCount status),
      ("uid", I . fromIntegral $ fileOwner status),
      ("gid", I . fromIntegral $ fileGroup status),
      ("size", I . fromIntegral $ fileSize status),
      ("mtime", I . fromIntegral . fromEnum $ modificationTime status),
      ("ctime", I . fromIntegral . fromEnum $ statusChangeTime status),
      ("perm", I . fromIntegral $ fileMode status)
      ]

-- Turn a FileKind into a string description of the kind.
fileKind :: FileStatus -> String
fileKind st
   | isBlockDevice st = "blk"
   | isCharacterDevice st = "chr"
   | isNamedPipe st = "fifo"
   | isRegularFile st = "reg"
   | isDirectory st = "dir"
   | isSymbolicLink st = "lnk"
   | isSocket st = "sock"
   | otherwise = "???"

----------------------------------------------------------------------
-- Show available bacukps.
showCatalog :: Manager -> IO ()
showCatalog manager = do
   set <- getBackups manager
   setBoxes <- forM set $ \hash -> do
      box <- newEmptyMVar
      readBlob manager hash box
      return box
   setResults <- forM setBoxes $ \box -> do
      blob <- takeMVar box
      case blob of
         -- TODO: This shouldn't fail.
         Nothing -> fail "Missing blob"
         Just x -> return $ snd $ blobData x
   let fullSet = map decode setResults :: [BackupInfo]

   forM_ (zip set fullSet) $ \ (hash, info) -> do
      let (Att base _ _) = backupRootAttribute info
      localTime <- toCalendarTime (backupTime info)
      putStrLn $ show hash ++ pad 8 (backupHost info) ++ " " ++
         formatCalendarTime (error "Notvisible") "%Y%m%d-%H%M" localTime ++
         " " ++ base

   -- putStrLn $ "Backups: " ++ show (zip set fullSet)

-- Lookup a specific backup.
lookupBackup :: Manager -> Hash -> IO Hash
lookupBackup manager bhash = do
   box <- newEmptyMVar
   readBlob manager bhash box
   blob1 <- takeMVar box
   let
      blob = case blob1 of
         Nothing -> error "Missing blob"
         Just x -> snd $ blobData x
   let info = decode blob :: BackupInfo
   let (Att base _ atts) = backupRootAttribute info
   let (H hash) = atts Map.! "hash"
   putStrLn $ "Backup of host: " ++ (backupHost info) ++
      ", dir: " ++ base
   putStrLn $ "       on " ++ show (backupTime info)
   return $ hash

----------------------------------------------------------------------
-- Directory listings.
--
-- Again, proof of concept, but not necessarily efficient.
listDir :: Manager -> Hash -> IO ()
listDir manager hash = do
   rootHash <- lookupBackup manager hash
   listing "." rootHash
   where
      listing path h = do
         boxM <- newEmptyMVar
         readBlob manager h boxM
         dir <- takeMVar boxM
         case dir of
            Nothing -> error $ "Directory not found: " ++ show hash
            Just blob -> do
               let (kind, payload) = blobData blob
               when (kind /= "dir") $ fail "hash is not a directory"
               let atts = decode payload :: [Attribute]
               forM_ atts $ entry path
      entry path (Att name kind fields) = do
         let fullName = path ++ "/" ++ name
         putStrLn $ pad 4 kind ++ " " ++ fullName
         when (kind == "dir") $ do
            case fields Map.! "hash" of
               (H hash') -> listing fullName hash'
               _ -> error "Invalid field"

pad :: Int -> String -> String
pad len str = replicate (len - length str) ' ' ++ str

----------------------------------------------------------------------
-- Restoring a backup.

restoreDir :: Manager -> Hash -> FilePath -> IO ()
restoreDir manager bHash rootPath = do
   rootHash <- lookupBackup manager bHash
   putStrLn $ "Restoring " ++ show rootHash ++ " to " ++ rootPath
   linksM <- newMVar Map.empty :: IO (MVar (Map Int64 LinkTracker))
   restoreRunner manager rootPath rootHash linksM

restoreRunner :: Manager -> FilePath -> Hash
   -> MVar (Map Int64 LinkTracker) -> IO ()
restoreRunner manager rootPath rootHash linksM = do
   restore rootPath rootHash
   -- Create the mapping that will keep track of hardlinks.  Any
   -- regular file that has a link count > 1 will be put into this
   -- map.

   where
      restore path h = do
         boxM <- newEmptyMVar
         readBlob manager h boxM
         dir <- takeMVar boxM
         case dir of
            Nothing -> error $ "Directory not found: " ++ show h
            Just blob -> do
               let (kind, payload) = blobData blob
               when (kind /= "dir") $ fail "hash is not of a directory"
               let atts = decode payload :: [Attribute]
               forM_ atts $ entry path

      entry path (Att name kind fields) = do
         let fullName = path ++ "/" ++ name
         let hash = forceH (fields Map.! "hash")
         let linkTarget = forceS (fields Map.! "link")
         let ino = fromIntegral $ forceI (fields Map.! "ino")
         let uid = fromIntegral $ forceI (fields Map.! "uid")
         let gid = fromIntegral $ forceI (fields Map.! "gid")
         let perm = fromIntegral $ forceI (fields Map.! "perm")
         let mtime = fromIntegral $ forceI (fields Map.! "mtime")
         let nlink = fromIntegral $ forceI (fields Map.! "nlink")
         let rdev = fromIntegral $ forceI (fields Map.! "rdev")
         let
            restorePerms isLink = do
               let
                  chown = if isLink
                     then setSymbolicLinkOwnerAndGroup
                     else setOwnerAndGroup
               E.handle (\exn -> logWarning manager $ show exn) $
                  chown fullName uid gid
               unless isLink $ setFileMode fullName (toEnum perm)
               unless isLink $ setFileTimes fullName mtime mtime
         case kind of
            "dir" -> do
               createDirectory fullName dirMode
               -- putStrLn $ "mkdir " ++ fullName
               restore fullName hash
               restorePerms False
            "reg" -> do
               links <- takeMVar linksM

               -- Have we seen this node before?
               case Map.lookup ino links of
                  Nothing -> do
                     -- Not seen yet.
                     restoreFile manager hash fullName
                     restorePerms False

                     let newLinks = if nlink > 1
                           then
                              let
                                 newTrack = LinkTracker { ltPath = fullName,
                                    ltCount = 1,
                                    ltNlink = nlink } in
                              Map.insert ino newTrack links
                           else links
                     putMVar linksM newLinks

                  Just track -> do
                     -- Yes, it's found, so link it.
                     -- putStrLn $ "link " ++ (ltPath track) ++ " to " ++ fullName
                     createLink (ltPath track) fullName
                     -- restorePerms False  -- Shouldn't be needed.

                     -- Increment the count to keep track of how many
                     -- we've visited.
                     let newTrack = track { ltCount = ltCount track + 1 }
                     putMVar linksM (Map.insert ino newTrack links)

               -- putStrLn $ "reg " ++ fullName ++ " <- " ++ hexify hash
            "lnk" -> do
               -- putStrLn $ "link " ++ linkTarget ++ " to " ++ fullName
               createSymbolicLink linkTarget fullName
               restorePerms True
            "blk" -> do
               createDevice fullName (toEnum perm) rdev
               restorePerms False
            "chr" -> do
               createDevice fullName (toEnum perm) rdev
               restorePerms False
            x -> do
               putStrLn $ "not restored: " ++ x ++ ": " ++ fullName

      forceH (H x) = x
      forceH _ = error "Invalid type"

      forceS (S x) = x
      forceS _ = error "Invalid type"

      forceI (I x) = x
      forceI _ = error "Invalid type"

      dirMode = stdFileMode .|. ownerExecuteMode .|.
         groupExecuteMode .|. otherExecuteMode

data LinkTracker = LinkTracker {
   ltPath :: String,    -- Path of first file using this link.
   ltNlink :: Int,      -- Value of nlink field from first time.
   ltCount :: Int }     -- How many we have restored this time.

----------------------------------------------------------------------
-- Blobification of files.
--
-- Read file into segments, passing each 'blob' and chunk to 'handleBlob'.
blobifyFile :: FilePath -> Manager -> IO Hash
blobifyFile path manager = do
   handle <- openBinaryFile path ReadMode
   contents <- L.hGetContents handle
   let chunks = chopUp (fromIntegral blobSize) contents
   hashes <- forM chunks $ \x -> writeBlob manager $ Uncompressed "blob" x
   oneHash <- collapse hashes
   Status.addFile (myStatus manager) 1
   case oneHash of
      [] -> do
         hash <- (writeBlob manager) $ Uncompressed "blob" L.empty
         return hash
      [x] -> return x
      _ -> fail "Collapse didn't work"
   where
      -- Collapse a list of blobs down, progressively, into lists of
      -- chunks until we have a simple result.
      collapse :: [Hash] -> IO [Hash]
      collapse [] = return []
      collapse [x] = return [x]
      collapse xs = do
         let (left, right) = splitAt chunkSize xs
         let leftPieces = map (\ (Hash h) -> h) left
         hash <- (writeBlob manager) $ Uncompressed "chunk" (L.fromChunks leftPieces)
         right' <- collapse right
         collapse (hash : right')

-- Chop a Lazy.ByteString into a series of 'size' chunks.
chopUp :: Int64 -> L.ByteString -> [L.ByteString]
chopUp _ x | x == L.empty = []
chopUp size x =
   case L.splitAt size x of
      (left, right) -> left : chopUp size right

-- Limits on the size of blobs and chunks.  For debugging, it can be
-- made very small.
blobSize, chunkSize :: Int
(blobSize, chunkSize) = if True
   then (256*1024, 10000)
   else (1024, 50)

----------------------------------------------------------------------
-- Restoring a file.
--
-- This first-pass implementation isn't very efficient, since it waits
-- for each reply before sending another request.  With a large
-- database, this can be quite a bottleneck.
--
-- TODO: Read ahead.  This is challenging to get right, since we still
-- need to write the file in order.
restoreFile :: Manager -> Hash -> FilePath -> IO ()
restoreFile manager hash path = do
   handle <- openBinaryFile path WriteMode
   -- putStrLn $ "Restoring: " ++ path ++ " (" ++ hexify hash ++ ")"
   let
      -- Read 'count' blobs from the gotM, process it as necessary, and
      -- then put () into the doneM.
      reader :: Int -> MVar (Maybe Blob) -> MVar () -> IO ()
      reader count gotM doneM = do
         sequence_ $ replicate count $ do
            blobq <- takeMVar gotM
            case blobq of
               Just blob -> do
                  case blobData blob of
                     ("blob", payload) -> do
                        L.hPut handle payload
                     ("chunk", chunklist) ->
                        readChunks chunklist
                     _ ->
                        fail $ "Unknown blob type from server"
               Nothing ->
                  -- TODO: this shouldn't be fatal.
                  fail $ "Unable to read back data"
         putMVar doneM ()

      -- Go through a sequence of chunks, and read in their contents
      -- as well.
      -- TODO: This implementation waits for every response, so the
      -- restore will be rediculously slow.
      readChunks :: L.ByteString -> IO ()
      readChunks chunks = do
         -- let count = B.length chunks `div` 20
         doChunk chunks
         where
            doChunk chunk = do
               let (cHead, cTail) = L.splitAt 20 chunk
               when (L.length cHead /= 20) $ fail "Invalid chunk multiple"
               -- putStrLn $ "Restore chunk: " ++ hexify cHead
               chunkM <- newEmptyMVar
               doneM <- newEmptyMVar
               (readBlob manager) (Hash (B.concat $ L.toChunks cHead)) chunkM
               reader 1 chunkM doneM
               takeMVar doneM
               when (L.length cTail > 0) $ doChunk cTail

   firstM <- newEmptyMVar
   firstDoneM <- newEmptyMVar
   (readBlob manager) hash firstM
   reader 1 firstM firstDoneM
   takeMVar firstDoneM

   hClose handle
