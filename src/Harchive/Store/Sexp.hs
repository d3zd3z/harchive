{-# LANGUAGE TypeSynonymInstances #-}
----------------------------------------------------------------------
-- Primitive Sexp decoder.
----------------------------------------------------------------------
--
-- Ldump stores several things in the database as limited lisp
-- expressions.  Basically, they are always a single parenthesized
-- list, of keyword value pairs where the values are of a few limited
-- types (string, and numeric).

module Harchive.Store.Sexp (
   decodeAttr, decodeChunk,
   decodeMultiAttr, decodeMultiChunk,
   Attr(..),
   SexpType(..),
   attrField, field, justField,

   decodeAlist, decodeAlists,
   Alist, SexpValue(..),
   lookupString, lookupInteger

   -- instance Binary Attr
) where

import Chunk
import Hash

import Data.Binary
import Protocol.Packing

import Control.Monad (forM_, liftM)
import qualified Codec.Binary.Base64 as Base64
import qualified Data.ByteString as B

import Data.Maybe (fromJust)
import Text.ParserCombinators.Parsec
import qualified Data.ByteString.Lazy as L
import qualified Data.ByteString.Lazy.Char8 as LChar (unpack)
import Numeric (readDec)
import System.Locale
import Data.Time

-- Attributes associated with various things that are backed up.
data Attr = Attr {
   attrKind :: String, -- What category of thing.
   attrName :: String, -- Name of this entity.
   attrAttrs :: Mapping }
   deriving (Show)
type Mapping = [(String, SexpValue)]

decodeChunk :: Chunk -> Attr
-- Decode the payload of the chunk as a single Attr, generating an
-- error if there is a parse problem.
decodeChunk =
   either (\msg -> error $ "Unable to parse one Attr: " ++ show msg) id .
      decodeAttr . chunkData

decodeMultiChunk :: Chunk -> [Attr]
decodeMultiChunk =
   either (\msg -> error $ "Unable to parse multiple Attrs: " ++ show msg) id .
      decodeMultiAttr . chunkData

decodeAttr :: L.ByteString -> Either ParseError Attr
decodeAttr = parse attrParser "data" . LChar.unpack

decodeMultiAttr :: L.ByteString -> Either ParseError [Attr]
decodeMultiAttr = parse multiAttrParser "data" . LChar.unpack

attrField :: Attr -> String -> Maybe SexpValue
attrField a key =
   lookup key $ attrAttrs a

field :: (SexpType a) => Attr -> String -> Maybe a
-- field a key = fmap fromValue (field a key)
field a = fmap fromValue . attrField a

justField :: (SexpType a) => Attr -> String -> a
-- Same as field, but causes an error if the attribute isn't present.
justField a = fromJust . field a

-- A class for convenient access to fields.
class SexpType a where
   fromValue :: SexpValue -> a

instance SexpType String where
   fromValue (SVString s) = s
   fromValue _ = error "fromValue: Item not a string"

instance SexpType Integer where
   fromValue (SVInteger i) = i
   fromValue _ = error "fromValue: Item not an integer"

instance SexpType Hash where
   -- Hashes are stored as base64-encoded strings.
   fromValue (SVString s) = byteStringToHash $ B.pack $ fromJust $ Base64.decode s
   fromValue _ = error "fromValue: Item not a hash"

instance SexpType UTCTime where
   -- Time is stored in ISO format
   fromValue (SVString s) = readTime defaultTimeLocale "%FT%TZ" s
   fromValue _ = error "fromValue: Item is not time"

attrParser :: Parser Attr
attrParser = do
   a <- oneAttr
   eof
   return a

multiAttrParser :: Parser [Attr]
multiAttrParser = do
   a <- many oneAttr
   eof
   return a

oneAttr :: Parser Attr
oneAttr = do
   spaces >> char '('
   kind <- spaces >> keyword
   name <- spaces >> realString
   attrs <- many $ do
      sk <- spaces >> keyword
      sv <- spaces >> value
      return (sk, sv)
   spaces >> char ')'
   return $ Attr { attrKind = kind, attrName = name, attrAttrs = attrs }

decodeAlist :: Int -> L.ByteString -> Either ParseError ([SexpValue], Alist)
-- Parse a single, simple sexp, and return the first 'n' of the items
-- as the first of the result pair, and treat the rest of the sexp as
-- a list of keyword, value pairs, making an alist.
decodeAlist n = parse (alist n) "data" . LChar.unpack

decodeAlists :: Int -> L.ByteString -> Either ParseError [([SexpValue], Alist)]
-- Parse concatenated alists as in 'decodeAlist', returning the result
-- as a list.  The same amount is consumed from each item.
decodeAlists n = parse (alists n) "data" . LChar.unpack

newtype Alist = Alist { getAlist :: [(String, SexpValue)] }
   deriving (Show)

-- Lookups of expected values.  Causes an error if the item is
-- present, but of the wrong type.
lookupString :: String -> Alist -> Maybe String
lookupString str = fmap svString . lookup str . getAlist

lookupInteger :: String -> Alist -> Maybe Integer
lookupInteger str = fmap svInteger . lookup str . getAlist

alists :: Int -> Parser [([SexpValue], Alist)]
alists n = many (alist n)

alist :: Int -> Parser ([SexpValue], Alist)
alist n = do
   char '('
   prefix <- count n (spaces >> value)
   sets <- many $ do
      spaces
      k <- keyword
      spaces
      v <- value
      return (k, v)
   char ')'
   return $ (prefix, Alist sets)

-- Assumes (for now) that keywords consist strictly of alphaNum's
keyword :: Parser String
keyword = do
   char ':'
   many kwchar

kwchar :: Parser Char
kwchar = alphaNum <|> char '-'

value :: Parser SexpValue
value = do
   vstring <|> vkeyword <|> vinteger

realString :: Parser String
realString = do
   char '\"'
   -- TODO: Handle escapes.
   text <- many (noneOf "\\\"")
   char '\"'
   return text

vstring, vinteger, vkeyword :: Parser SexpValue
vstring = do
   x <- realString
   return $ SVString x

vinteger = do
   text <- many1 digit
   return $ SVInteger (fst . head . readDec $ text)

vkeyword = do
   char ':'
   name <- many kwchar
   return $ SVKeyword name

data SexpValue
   = SVString { svString :: String }
   | SVInteger { svInteger :: Integer }
   | SVKeyword { svKeyword :: String }
   deriving (Eq, Show)

----------------------------------------------------------------------

instance Binary Attr where
   put attr = do
      putString $ attrKind attr
      putString $ attrName attr
      forM_ (attrAttrs attr) $ \(k,v) -> do
	 putWord8 1
	 putString k
	 put v
      putWord8 0

   get = do
      kind <- getString
      name <- getString
      mapping <- getMapping
      return $ Attr kind name mapping

instance Binary SexpValue where
   put (SVString str) = do
      putWord8 1
      putString str
   put (SVInteger int) = do
      putWord8 2
      putPBInt int
   put (SVKeyword str) = do
      putWord8 3
      putString str

   get = do
      kind <- getWord8
      case kind of
	 1 -> liftM SVString $ getString
	 2 -> liftM SVInteger $ getPBInt
	 3 -> liftM SVKeyword $ getString
	 _ -> error $ "Invalid stream byte: " ++ show kind

getMapping :: Get [(String, SexpValue)]
getMapping = do
   mark <- getWord8
   case mark of
      1 -> do
	 key <- getString
	 v <- get
	 rest <- getMapping
	 return $ (key, v):rest
      0 -> return []
      _ -> error $ "Invalid stream byte: " ++ show mark
