module TestJavaProperties (tester) where

import Data.Char (isLetter)
import qualified Data.Map as Map
import GenWords
import Test.HUnit
import Text.JavaProperties
import TmpDir
import System.FilePath ((</>))

tester :: Test
tester = test [
   "Write and read-back properties" ~: propTest ]

propTest :: IO ()
propTest = do
   withTmpDir $ \tmp -> do
   let name = tmp </> "myfile.txt"
   let p1 = testProps 150
   writePropertyFile name p1
   p2 <- readPropertyFile name
   (p1 == p2) @? "Property reread mismatch"

-- TODO: Test for, and support non-simple characters.

testProps :: Int -> JavaProperties
testProps count =
   Map.fromList $ map (\n -> (getWord n, makeString n n)) [1..count]

getWord :: Int -> String
getWord = fst . span isLetter . makeWords
