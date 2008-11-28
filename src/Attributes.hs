----------------------------------------------------------------------
-- File attribute storage.
-- Copyright 2007, David Brown
--
-- This program is free software; you can redistribute it and/or modify it
-- under the terms of the GNU General Public License as published by the
-- Free Software Foundation; either version 2, or (at your option) any later
-- version.
--
-- This program is distributed in the hope that it will be useful, but
-- WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU General
-- Public License for more details.
--
-- You should have received a copy of the GNU General Public License along
-- with this program; if not, write to the Free Software Foundation, Inc.,
-- 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
--
----------------------------------------------------------------------
--
-- Please ask <hackage@davidb.org> if you are interested in another
-- license.  If pieces of this program are useful in other systems I
-- will be willing to release them under a freer license, but I want
-- the program as a whole to be covered under the GPL.
--
----------------------------------------------------------------------
--
-- Every entity stored in a directory contains various attributes that
-- give enough information to restore that entity.
--
-- To keep this code general, and to allow additional information to
-- be added later (HFS attributes, extended attributes, Posix ACLs,
-- etc), most of this information is going to be represented as simple
-- name/value pairs, with a simple typed value (for now).
--
-- There are two attributes associated with each entity that are
-- required.  One is the name that this entity takes in this
-- directory, and the other is the 'kind' of the entity.  Both of
-- these are represented as strings.
--
-- In order to store the attributes, we must be able to serialize and
-- deserialize the attributes of a directory into a ByteString so that
-- they can be stored and later retrieved.
--
-- This package is able to fill in/convert all of the attributes that
-- can be determined strictly from the stat information.  Other
-- information (symbolic link targets, file hashes and such) will have
-- to be added externally.

module Attributes (
   Attribute(..),
   Field(..)
   -- instance Binary
) where

import qualified Data.Map as Map
import Data.Map (Map)
import qualified Data.ByteString as B
import Data.Binary
import Hash (Hash)

data Attribute = Att String String (Map String Field)
   deriving (Eq, Show)
data Field
   = I Integer
   | S String
   | B B.ByteString
   | H Hash
   deriving (Eq, Show)

instance Binary Attribute where
   get = do
      name <- get
      kind <- get
      fields <- get
      return $ Att name kind (Map.fromAscList fields)
   put (Att name kind asoc) = do
      let fields = Map.toAscList asoc
      put name
      put kind
      put fields

instance Binary Field where
   get = do
      t <- get :: Get Word8
      case t of
         1 -> get >>= return . I
         2 -> get >>= return . S
         3 -> get >>= return . B
         4 -> get >>= return . H
         _ -> fail "Unknown code"
   put (I value) = do
      put (1 :: Word8)
      put value
   put (S value) = do
      put (2 :: Word8)
      put value
   put (B value) = do
      put (3 :: Word8)
      put value
   put (H value) = do
      put (4 :: Word8)
      put value

