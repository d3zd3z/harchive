----------------------------------------------------------------------
-- Hash Maps.
----------------------------------------------------------------------

module System.Backup.HashMap (
   ValueInfo(..), intValue,
   HashMap,

   openHashMap, IOMode(..),
   runHashMap, hmLookup, hmInsert, hmClose, hmFlush,
   hmModifyProperties, hmGetProperties,

   ramLimit,

   -- Temporary exports to eliminate warnings about symbols not used.
   -- Should be eliminated once everything is eliminated.
   ciLookup, ciAscList, readIndexFile,
) where

import Control.Concurrent
import Data.Bits
import Data.Binary.Put
import Data.Binary.Get
import qualified Data.ByteString as B
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.Char8 as LC
import Data.Char (isDigit)
import Data.List (sortBy, stripPrefix)
import qualified Data.Map as M
import Data.Maybe (mapMaybe, isJust)
import Data.Monoid
import System.Directory
import System.FilePath ((</>))
import System.IO (IOMode(..))

import Control.Monad.Reader
import Control.Monad.State

import qualified Hash
import System.Backup.BinProp
import System.Backup.HashMap.File

-- | A given Hash Map maps hashes to a particular target datatype,
-- which must have an associated encoder.  A particular target type
-- must be binary encodable into a fixed-size number of bytes.
-- This can't be represented as a class because 'valueSize' depends
-- only on the type, and not a specific value of the type.
data ValueInfo a = ValueInfo {
   valueSize :: Int,
   valuePut :: a -> Put,
   valueGet :: Get a
}

-- Some useful ValueInfo's.
intValue :: ValueInfo Int
intValue = ValueInfo 4 (putWord32be . fromIntegral) (fmap fromIntegral getWord32be)

----------------------------------------------------------------------
-- The HashMap is an efficient append-only equivalent of 
-- M.Map Hash.Hash a, associated with a particular 'ValueInfo a'.
--
-- At any given time, there are [0..ramLimit-1] mappings kept in a
-- M.Map in memory.  Once this fills up, it will be flushed to
-- 'index-0001', and the RAM index will represent the data to be
-- contained in 'index-0002'.  What this means is that any given time,
-- there will be a series of files [1..n] with various of these files
-- combined via the hanoi combining algorithm described below.  There
-- will also possibly be an empty index file numbered (n+1) that holds
-- the entries that will be cached in RAM.

newtype HashMap v = HashMap (HRead v)

-- Reader info for a hashmap.
data HRead v = HRead {
   hmState :: MVar (HState v),
   hmValueInfo :: ValueInfo v,
   hmMakeName :: Int -> FilePath,
   hmDoInsert :: Hash.Hash -> v -> HashMapIO v () }

-- State that is mutated, stored in hmState of the Reader.
data HState v = HState {
   hsMap :: M.Map Hash.Hash v,
   hsIndexes :: [CachedIndex v],
   hsProperties :: Properties,
   hsNext :: Int,
   hsDirty :: Bool }

type HashMapIO v = ReaderT (HRead v) (StateT (HState v) IO)

runHashMap :: HashMap v -> HashMapIO v a -> IO a
runHashMap (HashMap hm@(HRead { hmState = stateVar })) op =
   modifyMVar stateVar $ \state -> do
      (a, state') <- runStateT work state
      return (state', a)
   where
      work = runReaderT op hm

openHashMap :: ValueInfo v -> FilePath -> FilePath -> IOMode -> IO (HashMap v)
openHashMap vinfo path prefix mode = do
   inserter <- makeInsert mode
   let namer = makeName path prefix
   (indexFiles, tmpFiles) <- getIndexFiles path prefix
   when (tmpFiles /= []) $
      error $ "tmp files present: " ++ show (map (path </>) tmpFiles)

   (props, lastCombine) <- getProps namer indexFiles
   (curMap, indexes, next) <- readMaps namer vinfo lastCombine indexFiles

   let state = HState { hsMap = curMap, hsNext = next,
      hsIndexes = indexes, hsProperties = props,
      hsDirty = False }
   stateVar <- newMVar state
   return $ HashMap HRead {
      hmState = stateVar,
      hmValueInfo = vinfo,
      hmMakeName = makeName path prefix,
      hmDoInsert = inserter }

-- Read the properties from the newest index file.
getProps :: (Int -> FilePath) -> [Int] -> IO (Properties, Int)
getProps _ [] = return (M.empty, 0)
getProps namer (a:_) = do
   props <- readIndexProperties (namer a)
   unless (M.lookup "version" props == Just "1.0") $
      error "File properties is not version 1.0"
   case M.lookup "index.lastCombine" props of
      Just n -> return (props, read n)
      Nothing -> error "Index file does not contain index.lastCombine property"

-- From a given 'lastCombine' value and a list of index files present,
-- read in the index files appropriately.  If lastCombine is equal to
-- the highest index found, then the map should be empty, and next one
-- greater than this (this only normally happens on improper close).
-- When the highest index is one greater, then this highest index
-- represents the map data and next value.
readMaps :: (Int -> FilePath) -> ValueInfo v -> Int -> [Int] ->
   IO (M.Map Hash.Hash v, [CachedIndex v], Int)
readMaps _ _ 0 [] = return (M.empty, [], 1)
readMaps namer vinfo lastCombine (a:as)
   | lastCombine == a  = do
      checkIndexes lastCombine (a:as)
      cache <- mapM (readIndexFile vinfo . namer) as
      return (M.empty, cache, lastCombine + 1)
   | lastCombine == a-1  = do
      checkIndexes lastCombine as
      hd <- fmap M.fromList $ fmap ciAscList $ readIndexFile vinfo (namer a)
      cache <- mapM (readIndexFile vinfo . namer) as
      return (hd, cache, a)
readMaps _ _ lst idx = error $ "Invalid index files present: " ++ 
   "last=" ++ show lst ++ ", idx=" ++ show idx

checkIndexes :: Int -> [Int] -> IO ()
checkIndexes lastCombine idx =
   unless (idx == indexPresent lastCombine) $
      error $ "Incorrect index files, expecting " ++ show (indexPresent lastCombine) ++
         " but found " ++ show idx

-- | Lookup a single key from the map.
hmLookup :: Hash.Hash -> HashMapIO v (Maybe v)
hmLookup key = do
   m <- gets hsMap
   idx <- gets hsIndexes
   let ram = First $ M.lookup key m
   let idxEnt = map (First . flip ciLookup key) idx
   return $ getFirst $ mconcat (ram : idxEnt)

-- | Add the Hash to value mapping to the HashMap, if it isn't already
-- present.
hmInsert :: Hash.Hash -> v -> HashMapIO v ()
hmInsert key val = do
   ins <- asks hmDoInsert
   ins key val

-- Construct the insert function appropriate for the IO mode.
makeInsert :: IOMode -> IO (Hash.Hash -> v -> HashMapIO v ())
makeInsert mode =
   case mode of
      ReadMode -> return $ \_key _val -> error "Attempt to update ReadOnly HashMap"
      AppendMode -> return $ \key val -> do
         already <- fmap isJust $ hmLookup key
         unless already $ do
            map1 <- gets hsMap
            when (M.size map1 >= ramLimit) doCombine
            modify $ \st ->
               st { hsMap = M.insert key val $ hsMap st, hsDirty = True }
      _ -> error $ "Invalid insert mode: " ++ show mode

-- Combine the current index with zero or more nodes.
doCombine :: HashMapIO v ()
doCombine = do
   next <- gets hsNext
   idx <- gets hsIndexes
   -- liftIO $ putStrLn $ "Combining for " ++ show next ++ " count=" ++ show (combineCount next)
   let (pre, post) = splitAt (combineCount next) idx
   mapItems <- fmap M.toAscList $ gets hsMap
   let idxItems = map ciAscList pre
   let items = mapMerge (mapItems : idxItems)
   modify $ \st -> st { hsMap = M.empty, hsNext = next + 1, hsIndexes = post }
   writeNext items next

   vinfo <- asks hmValueInfo
   namer <- asks hmMakeName
   newTop <- liftIO $ readIndexFile vinfo (namer next)
   modify $ \st -> st { hsIndexes = newTop:post }

   -- Delete old files.
   -- liftIO $ putStrLn $ "Deleting: " ++ show (map ciPath pre)
   forM_ pre $ liftIO . removeFile . ciPath

hmClose :: HashMapIO v ()
hmClose = do
   hmFlush
   put $ error "HashMap is closed"

hmFlush :: HashMapIO v ()
hmFlush = do
   dirty <- gets hsDirty
   when dirty $ do
      items <- fmap M.toAscList $ gets hsMap
      next <- gets hsNext
      writeNext items next
      modify $ \st -> st { hsDirty = False }

-- | Modify the properties associated with this hashmap.
hmModifyProperties :: (Properties -> Properties) -> HashMapIO v ()
hmModifyProperties f = modify $ \st -> st { hsProperties = f $ hsProperties st }

-- | Retrieve the properties from the HashMap.
hmGetProperties :: HashMapIO v Properties
hmGetProperties = gets hsProperties

-- Write the contents of the map to a file.
writeNext :: [(Hash.Hash, v)] -> Int -> HashMapIO v ()
writeNext items index = do
   next <- gets hsNext
   vinfo <- asks hmValueInfo
   maker <- asks hmMakeName
   oprops <- gets hsProperties
   let props = M.insert "version" "1.0" $ (M.insert "index.lastCombine" $ show (next-1)) oprops
   liftIO $ writeIndexFile vinfo (maker index) props items
   modify $ \st -> st { hsMap = M.empty }

ramLimit :: Int
ramLimit = if True then 37000 else 100

----------------------------------------------------------------------
-- File scanning.

-- The location of the index is represented as a pair of a directory
-- name, and a file prefix.  The index files themselves will occupy
-- files of the form "path" </> prefix ++ "nnnn" where 'n' is a
-- 4 digit number.
-- For compability with jpool, the index files use 0-based numbers,
-- even though

indexToName :: FilePath -> Int -> FilePath
indexToName prefix index = prefix ++ pad ++ shown
   where
      shown = show index
      pad = replicate (4 - length shown) '0'

-- Construct a full pathname.
makeName :: FilePath -> FilePath -> Int -> FilePath
makeName path prefix index = path </> indexToName prefix index

nameToIndex :: FilePath -> FilePath -> Maybe Int
nameToIndex prefix name = stripPrefix prefix name >>= safeNumber

-- Is this name a left over temporary file?
isTmpName :: FilePath -> FilePath -> Bool
isTmpName prefix = maybe False tmpSuffix . stripPrefix prefix

tmpSuffix :: FilePath -> Bool
tmpSuffix (a:".tmp") | isDigit a = True
tmpSuffix (a:rs) | isDigit a = tmpSuffix rs
tmpSuffix _ = False

-- Convert a string that matches >= 4 decimal digits into an Int.
safeNumber :: String -> Maybe Int
safeNumber str =
   if length str >= 4 && all isDigit str
      then Just $ read str
      else Nothing

-- Given a path, return a list of the present index files, and a list
-- of all of the names in the path that are left behind tmp files.
getIndexFiles :: FilePath -> FilePath -> IO ([Int], [FilePath])
getIndexFiles path prefix = do
   names <- getDirectoryContents path
   let indexes = sortBy (flip compare) $ mapMaybe (nameToIndex prefix) names
   let tmps = filter (isTmpName prefix) names
   return (indexes, tmps)

-- Contents of a cached index.
data CachedIndex a = CachedIndex {
   ciPath :: String,
   ciProperties :: Properties,
   ciLookup :: Hash.Hash -> Maybe a,
   ciAscList :: [(Hash.Hash, a)]
}

-- Retrieve the contents of the named file.  The Unlike "normal" lazy
-- ByteStrings, the entire file is read into a strict ByteString, and
-- then wrapped.  Because the index data is accessed randomly, reading
-- it in lazily would cause walks of the Chunk spine of the lazy data.
readIndexFile :: ValueInfo a -> FilePath -> IO (CachedIndex a)
readIndexFile vinfo path = do
   sdata <- B.readFile path
   let ldata = L.fromChunks [sdata]
   let (props, payload) = decodeBinProp ldata
   return CachedIndex {
      ciPath = path,
      ciProperties = props,
      ciLookup = makeLookup (valueSize vinfo) (valueGet vinfo) payload,
      ciAscList = hmToAscList (valueGet vinfo) payload
   }

-- | Retrieve only the properties from an index file.  The file is
-- read lazily, so that only enough to retrieve the properties is
-- needed.
readIndexProperties :: FilePath -> IO Properties
readIndexProperties path = do
   ldata <- L.readFile path
   let (props, _) = decodeBinProp ldata
   return props

writeIndexFile :: ValueInfo a -> FilePath -> Properties -> [(Hash.Hash, a)] -> IO ()
writeIndexFile vinfo path props items = do
   let tmpPath = path ++ ".tmp"
   prefix <- makePropPrefix
   let payload = runPut (putBinProp prefix props >> putOrderedMap (valuePut vinfo) items)
   L.writeFile tmpPath payload
   -- TODO: fsync
   renameFile tmpPath path

----------------------------------------------------------------------
-- To create a balance between reducing the number of files, and the
-- number of times we have to merge files together, use a "Towers of
-- Hanoi" type of algorithm to determine when to combine the current
-- index with previous files into a larger index.  Numbering the index
-- files from one, the default rule is to not combine any values.
-- Then if the (n%2)==0, consider combining with the previous index
-- file.  If (n%4)==0 consider combining with the previous 2 index
-- files, and so on.  Always use the rule that gives the largest
-- combination.

-- These functions return zero-based versions of these operators.

-- | For a given index number, return how many entries should be
-- combined.
combineCount :: Int -> Int
combineCount = trailingZeros

-- | For a given index, return a set of index files that should be
-- present.  The index itself will always be in the list.
indexPresent :: Int -> [Int]
indexPresent 0 = []
indexPresent n = n : indexPresent (n .&. complement (lowestOneBit n))

-- | Number of trailing zeros in an int.  This isn't used particularly
-- frequently, so we're not as worried about efficiency.  Hacker's
-- Delight has some fascinating algorithms for doing this quickly,
-- however.
trailingZeros :: Int -> Int
trailingZeros 0 = error "Zero has an unbounded number of trailing zeros"
trailingZeros nn = tz nn 0
   where
      tz n x
         | (n .&. 1) == 1    = x
         | otherwise         = tz (n `shiftR` 1) (x+1)

-- | Return the lowest set one bit in a number.
lowestOneBit :: Int -> Int
lowestOneBit x = x .&. (-x)
