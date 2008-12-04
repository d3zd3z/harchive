----------------------------------------------------------------------
-- Primitive Sexp decoder.
----------------------------------------------------------------------
--
-- |Ldump stores several things in the database as limited lisp
-- expressions.  Basically, they are always a single parenthesized
-- list, of keyword value pairs where the values are of a few limited
-- types (string, and numeric).

module DecodeSexp (
   -- $valuelist
   Transformer, TransformFunction,
   StateTransform(..),
   decodeValueList,

   decodeAlist, decodeAlists,
   Alist, SexpValue(..),
   lookupString, lookupInteger
) where

import Text.ParserCombinators.Parsec
import qualified Data.ByteString.Lazy as L
import Numeric (readDec)
import Data.Maybe

----------------------------------------------------------------------

-- $valuelist
-- Many of the fields in the ldump store are stored textually as list
-- sexps.  A specialized format has the first two fields as a keyword
-- representing the kind of the item, and a string giving some value
-- (usually a path).  The remaining items in the list are pairs of
-- keywords and values, the values always having expected types.

-- The parser is driven by a generating function, which takes a kind
-- and a value, and returns a function that transforms state.
data StateTransform st
   = SXString (String -> st -> st)
   | SXInteger (Integer -> st -> st)

-- The state transformer should take the kind and the value, and
-- return a state transforming function, as well as the initial state.
type TransformFunction st = String -> StateTransform st
type Transformer st = String -> String -> (TransformFunction st, st)

decodeValueList :: Transformer st -> L.ByteString ->
      Either ParseError st
-- Attempt to parse the given bytestring using the specified
-- transformer function.
decodeValueList xform = runParser (valueList xform) Nothing "data" . asString

valueList :: Transformer st -> CharParser (Maybe st) st
valueList xform = do
   spaces >> char '('
   kind <- spaces >> keyword
   info <- spaces >> rawString
   let (handler, st0) = xform kind info
   setState $ Just st0
   skipMany $ do
      k <- spaces >> keyword
      v <- spaces >> value
      case (handler k, v) of
	 (SXString f, SVString x) -> updateState $ Just . f x . fromJust
	 (SXInteger f, SVInteger x) -> updateState $ Just . f x . fromJust
	 (SXInteger _, SVString _) -> unexpected $ "String, expecting integer"
	 (SXString _, SVInteger _) -> unexpected $ "Integer, expecting string"
	 _ -> unexpected $ "Bad field type (implement keywords)"
   char ')'
   stN <- getState
   return $ fromJust stN

----------------------------------------------------------------------

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
keyword :: CharParser st String
keyword = do
   char ':'
   many kwchar

kwchar :: CharParser st Char
kwchar = alphaNum <|> char '-'

value :: CharParser st SexpValue
value = do
   vstring <|> vkeyword <|> vinteger

rawString :: CharParser st String
rawString = do
   char '\"'
   -- TODO: Handle escapes.
   text <- many (noneOf "\\\"")
   char '\"'
   return text

vstring, vinteger, vkeyword :: CharParser st SexpValue
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
