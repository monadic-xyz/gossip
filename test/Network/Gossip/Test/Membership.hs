{-# LANGUAGE OverloadedStrings #-}

module Network.Gossip.Test.Membership (tests) where

import           Network.Gossip.HyParView

import           Network.Gossip.Test.Gen
                 ( Contacts
                 , InfiniteListOf(..)
                 , LinkState(..)
                 , NodeId
                 , SplitMixSeed
                 , renderInf
                 )
import qualified Network.Gossip.Test.Gen as Gen
import           Network.Gossip.Test.Helpers

import qualified Algebra.Graph.AdjacencyMap as Alga
import           Control.Concurrent (threadDelay)
import           Control.Monad.Trans.Class (lift)
import           Data.Bifunctor (second)
import           Data.Foldable (for_)
import qualified Data.HashSet as Set
import           Data.IORef (IORef, atomicModifyIORef', newIORef)
import           Data.List (uncons)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Traversable (for)
import           System.Random (randomR, split)
import           System.Random.SplitMix (SMGen, seedSMGen')

import           Hedgehog hiding (eval)
import qualified Hedgehog.Gen as Gen

data Network = Network
    { netNodes     :: Map NodeId Node
    , netTaskQueue :: TaskQueue
    , netPRNG      :: IORef SMGen
    , netLinkState :: IORef (InfiniteListOf LinkState)
    }

newtype Node = Node { nodeEnv :: Env NodeId }

tests :: IO Bool
tests = checkParallel $ Group "Gossip.Membership"
    [ ("prop_disconnected",       propDisconnected)
    , ("prop_circular_connected", propCircularConnected)
    , ("prop_connected",          propConnected)
    , ("prop_network_delays",     propNetworkDelays)
    ]

propDisconnected :: Property
propDisconnected = property $ do
    seed  <- forAll Gen.splitMixSeed
    boot  <- forAll $ Gen.disconnectedContacts Gen.defaultNetworkBounds
    links <-
        forAllWith (renderInf 10) $
            Gen.prune $ Gen.infiniteListOf (pure Fast)
    activeDisconnected seed boot links

propCircularConnected :: Property
propCircularConnected = property $ do
    seed  <- forAll Gen.splitMixSeed
    boot  <- forAll $ Gen.circularContacts Gen.defaultNetworkBounds
    links <-
        forAllWith (renderInf 10) $
            Gen.prune $ Gen.infiniteListOf (pure Fast)
    activeConnected seed boot links

propConnected :: Property
propConnected = property $ do
    seed  <- forAll Gen.splitMixSeed
    boot  <- forAll $ Gen.connectedContacts Gen.defaultNetworkBounds
    links <-
        forAllWith (renderInf 10) $
            Gen.prune $ Gen.infiniteListOf (pure Fast)
    activeConnected seed boot links

propNetworkDelays :: Property
propNetworkDelays = property $ do
    seed  <- forAll Gen.splitMixSeed
    boot  <- forAll $ Gen.connectedContacts Gen.defaultNetworkBounds
    links <-
        forAllWith (renderInf 10) $
            Gen.prune $ Gen.infiniteListOf (Gen.linkState 100)
    activeConnected seed boot links

-- | Bootstrap the protocol with the respective contacts given by 'Contacts',
-- and assert the network of active views converged to a connected state.
activeConnected
    :: SplitMixSeed
    -> Contacts
    -> InfiniteListOf LinkState
    -> PropertyT IO ()
activeConnected seed boot links = do
    peers <- lift $ runNetwork (seedSMGen' seed) boot links
    annotateShow $ passiveNetwork peers
    assert $ isConnected (activeNetwork peers)

-- | Like 'propActiveConnected', but assert that the network converges to a
-- disconnected state.
--
-- This exists to suppress output which 'Test.Tasty.ExpectedFailure.expectFail'
-- would produce, and also because there's no point in letting hedgehog shrink
-- on failure.
activeDisconnected
    :: SplitMixSeed
    -> Contacts
    -> InfiniteListOf LinkState
    -> PropertyT IO ()
activeDisconnected seed boot links = do
    peers <- lift $ runNetwork (seedSMGen' seed) boot links
    annotateShow $ passiveNetwork peers
    assert $ not $ isConnected (activeNetwork peers)

--------------------------------------------------------------------------------

activeNetwork :: [(NodeId, Peers NodeId)] -> [(NodeId, [NodeId])]
activeNetwork = map (second (Set.toList . active))

passiveNetwork :: [(NodeId, Peers NodeId)] -> [(NodeId, [NodeId])]
passiveNetwork = map (second (Set.toList . passive))

isConnected :: [(NodeId, [NodeId])] -> Bool
isConnected adj =
    case Alga.dfsForest (Alga.stars adj) of
        [_] -> True
        _   -> False

--------------------------------------------------------------------------------

runNetwork
    :: SMGen
    -> Contacts
    -> InfiniteListOf LinkState
    -> IO [(NodeId, Peers NodeId)]
runNetwork rng boot links = do
    nodes <- Map.fromList <$> initNodes rng init'
    rng'  <- newIORef rng
    tq    <- newTaskQueue
    lnks  <- newIORef links
    let network = Network
            { netNodes     = nodes
            , netTaskQueue = tq
            , netPRNG      = rng'
            , netLinkState = lnks
            }

    for_ (zip (Map.elems nodes) contacts) $ \(node, contacts') ->
        runMembership network node $ joinAny contacts'

    fmap Map.toList . for nodes $ \node ->
        runMembership network node getPeers'
  where
    (init', contacts) = unzip boot

initNodes :: SMGen -> [NodeId] -> IO [(NodeId, Node)]
initNodes rng ns =
    for ns $ \nid -> do
        env <- new nid defaultConfig rng
        pure (nid, Node env)

runMembership
    :: Network
    -> Node
    -> HyParView NodeId a
    -> IO a
runMembership network node ma = runHyParView (nodeEnv node) ma >>= eval
  where
    eval = \case
        ConnectionOpen to k ->
            k (Right (mkConn to)) >>= eval

        SendAdHoc rpc k -> do
            onNode network (rpcRecipient rpc) $ receive rpc
            k >>= eval

        NeighborUp   _ k -> k >>= eval
        NeighborDown _ k -> k >>= eval

        Done a -> pure a

    mkConn to = Connection
        { connSend  = onNode network to . receive
        , connClose = pure ()
        }

onNode :: Network -> NodeId -> HyParView NodeId () -> IO ()
onNode network n ma =
    for_ (Map.lookup n (netNodes network)) $ \node ->
        let
            go         = runMembership network node ma
            delayed by = scheduleTask (netTaskQueue network) $
                             threadDelay (by * 1000) *> go
         in do
            link <-
                atomicModifyIORef' (netLinkState network) $ \l ->
                    case uncons (fromInfiniteListOf l) of
                        Just (x, xs) -> (InfiniteListOf xs, x)
                        Nothing      -> error "Ran out of link states"
            case link of
                Fast      -> go
                Slow  ms  -> delayed ms
                Flaky l u -> delayed . fst . randomR (l, u)
                         =<< atomicModifyIORef' (netPRNG network) split
                Down      -> pure ()
