-- |
-- Copyright   : 2018-2019 Monadic GmbH
-- License     : BSD3
-- Maintainer  : kim@monadic.xyz, team@monadic.xyz
-- Stability   : experimental
-- Portability : non-portable (GHC extensions)
--
module Network.Gossip.IO.Protocol
    ( ProtocolMessage(..)
    )
where

import qualified Network.Gossip.HyParView as H
import qualified Network.Gossip.Plumtree as P


import           Codec.Serialise (Serialise)
import           Data.Hashable (Hashable)
import           GHC.Generics (Generic)

data ProtocolMessage n =
      ProtocolPlumtree  (P.RPC n)
    | ProtocolHyParView (H.RPC n)
    deriving (Eq, Generic)

instance (Eq n, Hashable n, Serialise n) => Serialise (ProtocolMessage n)
