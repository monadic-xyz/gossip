{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE TemplateHaskell            #-}
{-# LANGUAGE TypeFamilies               #-}

module Test.Network.Gossip.Membership (tests) where

import           Network.Gossip.HyParView

import           Test.Network.Gossip.Gen
                 ( Contacts
                 , InfiniteListOf(..)
                 , LinkState(..)
                 , MockNodeId
                 , MockPeer(..)
                 , SplitMixSeed
                 , renderInf
                 )
import qualified Test.Network.Gossip.Gen as Gen
import           Test.Network.Gossip.Helpers

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
import           Lens.Micro.Extras (view)
import           System.Random (randomR, split)
import           System.Random.SplitMix (SMGen, seedSMGen')

import           Hedgehog hiding (eval)
import qualified Hedgehog.Gen as Gen

data Network = Network
    { netNodes     :: Map MockNodeId Node
    , netTaskQueue :: TaskQueue
    , netPRNG      :: IORef SMGen
    , netLinkState :: IORef (InfiniteListOf LinkState)
    }

newtype Node = Node { nodeEnv :: Env MockPeer }

tests :: IO Bool
tests = checkParallel $$discover

prop_disconnected :: Property
prop_disconnected = property $ do
    seed  <- forAll Gen.splitMixSeed
    boot  <- forAll $ Gen.disconnectedContacts Gen.defaultNetworkBounds
    links <-
        forAllWith (renderInf 10) $
            Gen.prune $ Gen.infiniteListOf (pure Fast)
    activeDisconnected seed boot links

prop_circularConnected :: Property
prop_circularConnected = property $ do
    seed  <- forAll Gen.splitMixSeed
    boot  <- forAll $ Gen.circularContacts Gen.defaultNetworkBounds
    links <-
        forAllWith (renderInf 10) $
            Gen.prune $ Gen.infiniteListOf (pure Fast)
    activeConnected seed boot links

prop_connected :: Property
prop_connected = property $ do
    seed  <- forAll Gen.splitMixSeed
    boot  <- forAll $ Gen.connectedContacts Gen.defaultNetworkBounds
    links <-
        forAllWith (renderInf 10) $
            Gen.prune $ Gen.infiniteListOf (pure Fast)
    activeConnected seed boot links

prop_networkDelays :: Property
prop_networkDelays = property $ do
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

-- | Like 'activeConnected', but assert that the network converges to a
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

activeNetwork :: [(MockNodeId, Peers MockPeer)] -> [(MockNodeId, [MockNodeId])]
activeNetwork = map (second (map (view peerNodeId) . Set.toList . active))

passiveNetwork :: [(MockNodeId, Peers MockPeer)] -> [(MockNodeId, [MockNodeId])]
passiveNetwork = map (second (map (view peerNodeId) . Set.toList . passive))

isConnected :: [(MockNodeId, [MockNodeId])] -> Bool
isConnected adj =
    case Alga.dfsForest (Alga.stars adj) of
        [_] -> True
        _   -> False

--------------------------------------------------------------------------------

runNetwork
    :: SMGen
    -> Contacts
    -> InfiniteListOf LinkState
    -> IO [(MockNodeId, Peers MockPeer)]
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
        runMembership network node $
            joinAny (map (\nid -> (Just nid, nid)) contacts')

    fmap Map.toList . for nodes $ \node ->
        runMembership network node getPeers'
  where
    (init', contacts) = unzip boot

initNodes :: SMGen -> [MockNodeId] -> IO [(MockNodeId, Node)]
initNodes rng ns =
    for ns $ \nid -> do
        env <- new (MockPeer nid) defaultConfig rng
        pure (nid, Node env)

runMembership
    :: Network
    -> Node
    -> HyParView MockPeer a
    -> IO a
runMembership network node ma = runHyParView (nodeEnv node) ma >>= eval
  where
    eval = \case
        ConnectionOpen addr _ k ->
            k (Right (mkConn (MockPeer addr))) >>= eval

        SendAdHoc rpc k -> do
            onNode network (view peerAddr $ rpcRecipient rpc) $ receive rpc
            k >>= eval

        NeighborUp   _ k -> k >>= eval
        NeighborDown _ k -> k >>= eval

        Done a -> pure a

    mkConn to = Connection
        { connPeer  = to
        , connSend  = onNode network (view peerAddr to) . receive
        , connClose = pure ()
        }

onNode :: Network -> MockNodeId -> HyParView MockPeer () -> IO ()
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
