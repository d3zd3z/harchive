module PropertiesCheck (tester) where

import Data.ByteString (ByteString)
import Data.Char (isLetter)
import qualified Data.Map as Map
import GenWords
import Test.HUnit
import Text.Properties
import Text.Properties.JavaXml
import Text.Properties.Node
import TmpDir
import System.FilePath ((</>))

tester :: Test
tester = test [
   "Write and read-back properties" ~: propTest,
   "XML translation" ~: xmlTest,
   "Node translation" ~: nodeTest ]

propTest :: IO ()
propTest = do
   withTmpDir $ \tmp -> do
   let name = tmp </> "myfile.txt"
   let p1 = testProps 150
   writePropertyFile name p1
   p2 <- readPropertyFile name
   (p1 == p2) @? "Property reread mismatch"

xmlTest :: IO ()
xmlTest = do
   m1 <- javaXmlToProperties sample
   let mOrig = simpleProp
   m1 @?= mOrig
   buf <- propertiesToJavaXml mOrig :: IO ByteString
   m2 <- javaXmlToProperties buf
   m2 @?= mOrig

simpleProp :: Properties
simpleProp = Map.fromList [
      ("_date", "1317421304162"),
      ("key", "value"),
      ("kind", "snapshot"),
      ("hash", "cb32d5ed1eb8ec56f7157b0b7fac4b656c8e62cc") ]

nodeTest :: IO ()
nodeTest = do
   let p0 = (Map.insert "_kind" "simple" simpleProp)
   n1 <- propertiesToNode p0 :: IO ByteString
   p1 <- nodeToProperties n1
   p1 @?= p0

-- This is captured out of the output of the Scala implementation.
sample :: String
sample = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n\
   \<!DOCTYPE properties SYSTEM \"http://java.sun.com/dtd/properties.dtd\">\n\
   \<properties>\n\
   \<comment>Backup</comment>\n\
   \<entry key=\"_date\">1317421304162</entry>\n\
   \<entry key=\"key\">value</entry>\n\
   \<entry key=\"kind\">snapshot</entry>\n\
   \<entry key=\"hash\">cb32d5ed1eb8ec56f7157b0b7fac4b656c8e62cc</entry>\n\
   \</properties>\n"

-- TODO: Test for, and support non-simple characters.

testProps :: Int -> Properties
testProps count =
   Map.fromList $ map (\n -> (getWord n, makeString n n)) [1..count]

getWord :: Int -> String
getWord = fst . span isLetter . makeWords
