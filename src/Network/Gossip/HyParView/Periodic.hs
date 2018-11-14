-- |
-- Copyright   : 2018 Monadic GmbH
-- License     : BSD3
-- Maintainer  : kim@monadic.xyz, team@monadic.xyz
-- Stability   : experimental
-- Portability : non-portable (GHC extensions)
--
module Network.Gossip.HyParView.Periodic
    ( Config
    , defaultConfig
    , withPeriodic
    ) where

import qualified Network.Gossip.HyParView as H

import           Control.Concurrent (threadDelay)
import           Control.Concurrent.Async
                 ( Async
                 , Concurrently(..)
                 , async
                 , uninterruptibleCancel
                 )
import           Control.Exception.Safe (bracket)
import           Control.Monad (forever)
import           Data.Hashable (Hashable)
import           Data.Time.Clock (NominalDiffTime)

data Config = Config
    { confShuffleInterval         :: NominalDiffTime
    , confRandomPromotionInterval :: NominalDiffTime
    }

defaultConfig :: Config
defaultConfig = Config
    { confShuffleInterval         = 10
    , confRandomPromotionInterval = 5
    }

newtype Periodic = Periodic (Async ())

new :: (Eq n, Hashable n)
    => Config
    -> (H.HyParView n () -> IO ())
    -> IO Periodic
new Config { confShuffleInterval, confRandomPromotionInterval } run =
    fmap Periodic . async . runConcurrently $
        Concurrently shuffle <> Concurrently promote
  where
    shuffle = forever $ sleep confShuffleInterval         *> run H.shuffle
    promote = forever $ sleep confRandomPromotionInterval *> run H.promoteRandom

destroy :: Periodic -> IO ()
destroy (Periodic t) = uninterruptibleCancel t

withPeriodic
    :: (Eq n, Hashable n)
    => Config
    -> (H.HyParView n () -> IO ())
    -> (Periodic -> IO a)
    -> IO a
withPeriodic cfg run = bracket (new cfg run) destroy

--------------------------------------------------------------------------------

sleep :: NominalDiffTime -> IO ()
sleep t = threadDelay $ toSeconds t * 1000000
  where
    toSeconds = round @Double . realToFrac
