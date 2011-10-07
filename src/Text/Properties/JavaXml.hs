{-# LANGUAGE Arrows #-}
{-# LANGUAGE FlexibleContexts #-}

module Text.Properties.JavaXml (
   javaXmlToProperties,
   propertiesToJavaXml
) where

-- TODO: Figure out how to parse the XML without having to strip off
-- the DOCTYPE.
-- TODO: Maybe, figure out how to insert convenient newlines in
-- document (indentDoc in HXT is a good start).

import Text.Properties (Properties)
import Data.Convertible.Text (ConvertSuccess, cs)
import Data.List (isPrefixOf)
import qualified Data.Map as Map
import Text.XML.HXT.Core

-- This has to run in IO because the XML parsing may need external
-- references (even though we try to keep that from happening).
javaXmlToProperties :: ConvertSuccess a String => a -> IO Properties
javaXmlToProperties inp = do
   let body = removePrefix $ cs inp
   -- let body = cs inp
   items <- runX $ constA body >>> decoder
   return $ Map.fromList items

propertiesToJavaXml :: ConvertSuccess String a => Properties -> IO a
propertiesToJavaXml props = do
   text <- runX (encoder $ Map.toList props)
   return $ cs $ header ++ concat text

-- Remove the xml and DOCTYPE headers, since these cause the XML
-- parser to try and query these from the outside world.  This isn't
-- really very fast.
removePrefix :: String -> String
removePrefix [] = []
removePrefix text | "<properties" `isPrefixOf` text = text
removePrefix (_:as) = removePrefix as

-- Convert the mapping into an XML document itself.  Converts an XML
-- property document into a list of key/value pairs.
decoder :: IOSArrow String (String, String)
decoder = proc input -> do
   doc <- readFromString [] -< input
   nodes <- hasName "/" /> hasName "properties" /> hasName "entry" -< doc
   keys <- getAttrValue "key" -< nodes
   text <- deep isText >>> getText -< nodes
   returnA -< (keys, text)

encoder :: ArrowXml a => [(String, String)] -> a XmlTree String
encoder props =
   selem "root" [] >>> root [] [
      mkElement (mkName "properties") (catA []) (
         mkelem "comment" [] [ txt "Backup" ] <+>
         (constL props >>> encodePairs))
      ] >>>
   writeDocumentToString []

encodePairs :: ArrowXml a => a (String, String) XmlTree
encodePairs =
   mkelem "entry" [ attr "key" (arr fst >>> mkText) ] [ arr snd >>> mkText ]

header :: String
header = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n\
   \<!DOCTYPE properties SYSTEM \"http://java.sun.com/dtd/properties.dtd\">\n"

-- If we ever figure out how to give the properties to the parser so
-- it doesn't need to retrieve them, and can use a pre-processed one,
-- do so.  Until then, this is unused.
{-
propertiesDTD :: String
propertiesDTD = "\
   \<!--\n\
   \   Copyright 2006 Sun Microsystems, Inc.  All rights reserved.\n\
   \  -->\n\
   \<!-- DTD for properties -->\n\
   \<!ELEMENT properties ( comment?, entry* ) >\n\
   \<!ATTLIST properties version CDATA #FIXED \"1.0\">\n\
   \<!ELEMENT comment (#PCDATA) >\n\
   \<!ELEMENT entry (#PCDATA) >\n\
   \<!ATTLIST entry key CDATA #REQUIRED>\n"

config :: [SysConfig]
config = [withSysAttr "http://java.sun.com/dtd/properties.dtd" propertiesDTD]
-}

