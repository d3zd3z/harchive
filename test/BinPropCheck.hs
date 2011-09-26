----------------------------------------------------------------------
-- Testing binary property encoding.

module BinPropCheck (binPropCheck) where

import Data.Bits ((.&.))
import qualified Data.ByteString.Lazy.Char8 as LC
import qualified Data.Map as M
import System.Random

import Test.HUnit
import System.Backup.BinProp
import Text.Printf

binPropCheck :: Test
binPropCheck = test $ do
   roundTrip 1
   roundTrip 400
   roundTrip 10000

roundTrip :: Int -> IO ()
roundTrip n = do
   let prop = makeProps n
   prefix <- makePropPrefix
   let enc = encodeBinProp prefix prop
   (LC.length enc .&. 4095) @=? 0
   let (prop', trailer) = decodeBinProp enc
   LC.length trailer @=? 0
   prop @?= prop'

-- Generate 'n' props.
makeProps :: Int -> M.Map String String
makeProps n =
   M.fromList $ zipWith (curry id)
      [ printf "property%d" x | x <- [1..n] ] (map show (randoms $ mkStdGen 1 :: [Int]))
