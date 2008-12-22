----------------------------------------------------------------------
-- Marshaling for Sexp Attr data.
----------------------------------------------------------------------

module Protocol.Attr (
   -- instance Binary Attr
) where

import Harchive.Store.Sexp
import Protocol.Packing

import Data.Binary

import Control.Monad (forM_, liftM)

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
	 value <- get
	 rest <- getMapping
	 return $ (key, value):rest
      0 -> return []
      _ -> error $ "Invalid stream byte: " ++ show mark
