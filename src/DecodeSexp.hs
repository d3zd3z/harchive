----------------------------------------------------------------------
-- Primitive Sexp decoder.
----------------------------------------------------------------------
--
-- Ldump stores several things in the database as limited lisp
-- expressions.  Basically, they are always a single parenthesized
-- list, of keyword value pairs where the values are of a few limited
-- types (string, and numeric).

module DecodeSexp (
   decodeSexp, decodeSexps,
   Sexp, SexpValue(..),
   svString, svInteger,
   lookupString, lookupInteger
) where

import Text.ParserCombinators.Parsec
import qualified Data.ByteString.Lazy as L
import Numeric (readDec)

decodeSexp :: L.ByteString -> Either ParseError Sexp
decodeSexp = parse sexp "data" . asString

decodeSexps :: L.ByteString -> Either ParseError [Sexp]
decodeSexps = parse sexps "data" . asString

newtype Sexp = Sexp { getSexp :: [(String, SexpValue)] }
   deriving (Show)

-- Lookups of expected values.  Causes an error if the item is
-- present, but of the wrong type.
lookupString :: String -> Sexp -> Maybe String
lookupString str = fmap svString . lookup str . getSexp

lookupInteger :: String -> Sexp -> Maybe Integer
lookupInteger str = fmap svInteger . lookup str . getSexp

sexps :: Parser [Sexp]
sexps = many sexp

sexp :: Parser Sexp
sexp = do
   char '('
   sets <- many $ do
      spaces
      k <- keyword
      spaces
      v <- value
      return (k, v)
   char ')'
   return $ Sexp sets

-- Assumes (for now) that keywords consist strictly of alphaNum's
keyword :: Parser String
keyword = do
   char ':'
   many kwchar

kwchar :: Parser Char
kwchar = alphaNum <|> char '-'

value :: Parser SexpValue
value = do
   vstring <|> vinteger

vstring, vinteger :: Parser SexpValue
vstring = do
   char '\"'
   -- TODO: Handle escapes.
   text <- many (noneOf "\"")
   char '\"'
   return $ SVString text

vinteger = do
   text <- many digit
   return $ SVInteger (fst . head . readDec $ text)

data SexpValue
   = SVString String
   | SVInteger Integer
   deriving (Eq, Show)

svString :: SexpValue -> String
svString (SVString s) = s
svString _ = error "Expecting sexp string"

svInteger :: SexpValue -> Integer
svInteger (SVInteger i) = i
svInteger _ = error "Expecting sexp integer"

asString :: L.ByteString -> String
asString = map (toEnum . fromIntegral) . L.unpack
