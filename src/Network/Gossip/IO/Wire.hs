-- |
-- Copyright   : 2018 Monadic GmbH
-- License     : BSD3
-- Maintainer  : kim@monadic.xyz, team@monadic.xyz
-- Stability   : experimental
-- Portability : non-portable (GHC extensions)
--
module Network.Gossip.IO.Wire (WireMessage (..)) where

import           Codec.Serialise (Serialise)
import           Data.Text (Text)
import           GHC.Generics (Generic)

data WireMessage p =
      WirePayload p
    | WireGoaway (Maybe Text)
    deriving (Generic)

instance Serialise p => Serialise (WireMessage p)
