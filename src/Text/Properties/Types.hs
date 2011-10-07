module Text.Properties.Types (
   Properties,
   emptyProperties
) where

import Data.Map (Map)
import qualified Data.Map as Map

type Properties = Map String String

emptyProperties :: Properties
emptyProperties = Map.empty

