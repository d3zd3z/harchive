module PoolCheck (tester) where

import Control.Exception (finally)
import Control.Monad (zipWithM_)
import Data.List (nub, sort)
import Data.Maybe (catMaybes)
import Errors
import GenWords
import Test.HUnit
import TmpDir
import System.Directory (createDirectory)
import System.FilePath ((</>))

import System.Backup.Chunk
import qualified System.Backup.Chunk.Store as Store

-- import qualified System.Backup.Chunk.Store as Store
import System.Backup.Pool

tester :: Test
tester = test [
   "Creation" ~: noPool,
   "Read/Write" ~: readWrite ]

-- Verify that various problems with creating pools are handled.
noPool :: IO ()
noPool = withTmpDir $ \tmp -> do
   let name = tmp </> "p1"
   mustThrowUser $ withPool name $ \_ -> return ()
   createDirectory name
   withPool name $ \_ -> return ()
   withPool name $ \p -> do
      backs <- Store.getBackups p
      backs @=? []

-- Verify chunks written can be read back.
readWrite :: IO ()
readWrite = withTmpDir $ \tmp -> do
   let fullItems = makeChunks 100
   let (itemsA, itemsB) = splitAt 20 fullItems
   withPool tmp $ \pool -> do
      mapM_ (Store.insert pool) itemsA
      verifyPool pool itemsA
      return ()

   withPool tmp $ \pool -> do
      verifyPool pool itemsA
      mapM_ (Store.insert pool) itemsB
      verifyPool pool fullItems

   withPool tmp $ \pool -> do
      verifyPool pool fullItems

-- TODO: Test writing backups.

-- Verify that the given chunks are present.
verifyPool :: Pool -> [Chunk] -> IO ()
verifyPool pool chunks = do
   answer <- mapM (\ch -> Store.lookup (chunkHash ch) pool) chunks
   length answer @=? length chunks
   zipWithM_ chunkEq chunks (catMaybes answer)

newtype EqChunk = EqChunk Chunk
   deriving Show
instance Eq EqChunk where
   (EqChunk a) == (EqChunk b) = (chunkHash a) == (chunkHash b)

chunkEq :: Chunk -> Chunk -> IO ()
chunkEq a b = do
   (chunkHash a) @=? (chunkHash b)
   (chunkKind a) @=? (chunkKind b)
   (chunkData a) @=? (chunkData b)

withPool :: FilePath -> (Pool -> IO a) -> IO a
withPool p op = do
   pool <- openPool p
   finally (op pool) $ closePool pool

-- For testing, we want a nice variety of sizes, to test various
-- boundary conditions, we will use powers of two up to the largest
-- chunk we will use (256k)
sizes :: [Int]
sizes = sort $ nub $ concatMap (\s -> let base = 2 ^ s in [base-1, base, base+1]) [0 :: Int ..18]

-- Like above sizes, but also have the list as large as desired, using
-- a fixed size after the boundary tests.
nSizes :: Int -> [Int]
nSizes n = take n $ sizes ++ repeat 4096

makeChunks :: Int -> [Chunk]
makeChunks n = zipWith makeChunk [1..] (nSizes n)
   where makeChunk index len = stringToChunk "blob" $ makeString index len
