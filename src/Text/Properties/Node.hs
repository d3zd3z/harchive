{-# LANGUAGE Arrows #-}
{-# LANGUAGE FlexibleContexts #-}

module Text.Properties.Node where

import Text.Properties.Types
import Data.Convertible.Text (ConvertSuccess, cs)
import qualified Data.Map as Map
import Text.XML.HXT.Core

-- Convert an XML node format to a set of properties.
-- nodeToProperties
nodeToProperties :: ConvertSuccess a String => a -> IO Properties
nodeToProperties inp = do
   tree <- runX $ constA (cs inp) >>> decode
   mapping <- runX $ constL tree >>> decodeMap
   [kind] <- runX $ constL tree >>> decodeKind
   return $ Map.fromList $ ("_kind", kind) : mapping

propertiesToNode :: ConvertSuccess String a => Properties -> IO a
propertiesToNode props = do
   let kind = props Map.! "_kind"
   let props' = "_kind" `Map.delete` props
   text <- runX $ encoder kind (Map.toList props')
   return $ cs $ header ++ concat text

decode :: IOSArrow String XmlTree
decode = proc input -> do
   readFromString [] -< input

decodeMap :: ArrowXml a => a XmlTree (String, String)
decodeMap = proc input -> do
   nodes <- hasName "/" /> hasName "node" /> hasName "entry" -< input
   keys <- getAttrValue "key" -< nodes
   text <- deep isText >>> getText -< nodes
   returnA -< (keys, text)

decodeKind :: ArrowXml a => a XmlTree String
decodeKind = proc input -> do
   node <- hasName "/" /> hasName "node" -< input
   getAttrValue "kind" -< node

header :: String
header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"

encoder :: ArrowXml a => String -> [(String, String)] -> a XmlTree String
encoder kind props =
   selem "root" [] >>> root [] [
      mkElement (mkName "node") (constA kind >>> attr "kind" mkText) (
         constL props >>> encodePairs)
      ] >>>
   writeDocumentToString []

encodePairs :: ArrowXml a => a (String, String) XmlTree
encodePairs =
   mkelem "entry" [ attr "key" (arr fst >>> mkText) ] [ arr snd >>> mkText ]
