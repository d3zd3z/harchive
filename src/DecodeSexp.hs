----------------------------------------------------------------------
-- Primitive Sexp decoder.
----------------------------------------------------------------------
--
-- Ldump stores several things in the database as limited lisp
-- expressions.  Basically, they are always a single parenthesized
-- list, of keyword value pairs where the values are of a few limited
-- types (string, and numeric).

module DecodeSexp (
   decodeAlist, decodeAlists,
   Alist, SexpValue(..),
   lookupString, lookupInteger
) where

import Text.ParserCombinators.Parsec
import qualified Data.ByteString.Lazy as L
import Numeric (readDec)

decodeAlist :: Int -> L.ByteString -> Either ParseError ([SexpValue], Alist)
-- Parse a single, simple sexp, and return the first 'n' of the items
-- as the first of the result pair, and treat the rest of the sexp as
-- a list of keyword, value pairs, making an alist.
decodeAlist n = parse (alist n) "data" . asString

decodeAlists :: Int -> L.ByteString -> Either ParseError [([SexpValue], Alist)]
-- Parse concatenated alists as in 'decodeAlist', returning the result
-- as a list.  The same amount is consumed from each item.
decodeAlists n = parse (alists n) "data" . asString

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

vstring, vinteger, vkeyword :: Parser SexpValue
vstring = do
   char '\"'
   -- TODO: Handle escapes.
   text <- many (noneOf "\"")
   char '\"'
   return $ SVString text

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

asString :: L.ByteString -> String
asString = map (toEnum . fromIntegral) . L.unpack
