{-# LANGUAGE FlexibleContexts #-}
-- Parser for Java Properties files.

module Text.Properties (
   Properties,
   readPropertyFile,
   writePropertyFile,
   emptyProperties
) where

-- I haven't found an actual spec for the flat format that Java uses
-- to write it's property file.  For harchive, we really only have to
-- read the simle ISO8859-1 key/value pairs that are written.

import Prelude hiding (takeWhile)

import Control.Applicative hiding (many)
import Data.Attoparsec.Lazy ()
import Data.Attoparsec.Char8
import qualified Data.Attoparsec as AP
import qualified Data.ByteString as B
import Data.Convertible.Text (cs)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Time (formatTime, getZonedTime)
import System.Locale (defaultTimeLocale)
import qualified System.IO.Cautious as Cautious

type Properties = Map String String

emptyProperties :: Properties
emptyProperties = Map.empty

readPropertyFile :: FilePath -> IO Properties
readPropertyFile path = do
   text <- B.readFile path
   let p = fullParse pProperties text
   case p of
      Left msg -> error msg
      Right r -> return $ Map.fromList r

writePropertyFile :: FilePath -> Properties -> IO ()
writePropertyFile path props = do
   now <- getZonedTime
   let stamp = formatTime defaultTimeLocale "%c" now
   -- TODO: Properly escape the key and value.
   let out = "#Harchive metadata properties" : ("#" ++ stamp) : textProps
   Cautious.writeFile path $ unlines out
   where
      fmtProp :: (String, String) -> String
      fmtProp (k, v) = k ++ "=" ++ v

      textProps :: [String]
      textProps = map fmtProp $ Map.toList props

-- Like parseOnly in AttoParsec, but actually correct.
fullParse :: Parser a -> B.ByteString -> Either String a
fullParse m s = fullParse' (parse m s) True

fullParse' :: Result a -> Bool -> Either String a
fullParse' r more =
   case r of
      Fail rest context msg -> Left $ show msg ++ " " ++ show context ++ " " ++ show rest
      Done rest r' -> if not (B.null rest)
         then Left $ "Text left over after parse: " ++ show rest
         else Right r'
      Partial k -> if more then fullParse' (k B.empty) False else
         Left $ "Parser didn't finish"

pProperties :: Parser [(String, String)]
pProperties = do
   many pProperty

pProperty :: Parser (String, String)
pProperty = do
   _ <- many comment
   key <- many1 letter_ascii <* char '='
   value <- AP.takeWhile1 (not . isEndOfLine) <* endOfLine
   return (cs key, cs value)

comment :: Parser ()
comment = char '#' *> AP.takeWhile (not . isEndOfLine) *> endOfLine *> return ()
