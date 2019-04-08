module Test.Network.Gossip.Assert (allEqual) where

import           Data.List.NonEmpty (NonEmpty(..))

allEqual :: Eq a => NonEmpty a -> Bool
allEqual (x :| xs) = all (== x) xs
