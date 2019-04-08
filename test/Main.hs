module Main (main) where

import qualified Test.Network.Gossip.Broadcast as Broadcast
import qualified Test.Network.Gossip.Membership as Membership

import           Control.Monad (unless)
import           System.Exit (exitFailure)

main :: IO ()
main = do
    success <-
        and <$> sequence
            [ Broadcast.tests
            , Membership.tests
            ]

    unless success exitFailure
