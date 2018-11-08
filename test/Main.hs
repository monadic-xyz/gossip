module Main (main) where

import qualified Network.Gossip.Test.Broadcast as Broadcast
import qualified Network.Gossip.Test.Membership as Membership

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
