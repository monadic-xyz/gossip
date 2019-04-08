{-# LANGUAGE OverloadedStrings #-}

module Test.Network.Gossip.Broadcast (tests) where

import           Network.Gossip.Plumtree

import           Test.Network.Gossip.Assert (allEqual)
import           Test.Network.Gossip.Gen
                 ( Contacts
                 , InfiniteListOf(..)
                 , LinkState(..)
                 , MockNodeId
                 , renderInf
                 )
import qualified Test.Network.Gossip.Gen as Gen
import           Test.Network.Gossip.Helpers

import           Control.Concurrent (threadDelay)
import           Control.Exception.Safe (throwString)
import           Control.Monad (void)
import           Data.ByteString (ByteString)
import           Data.Foldable (for_, traverse_)
import qualified Data.HashSet as Set
import           Data.IORef (IORef, atomicModifyIORef', newIORef, readIORef)
import           Data.List (uncons)
import           Data.List.NonEmpty (NonEmpty, nonEmpty, (<|))
import qualified Data.List.NonEmpty as NonEmpty
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Traversable (for)
import           Prelude hiding (round)
import           System.Random (randomR, split)
import           System.Random.SplitMix (SMGen, seedSMGen')

import           Hedgehog hiding (eval)
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range

tests :: IO Bool
tests = checkParallel $ Group "Gossip.Broadcast"
    [ ("prop_atomic_connected",      propAtomicConnected)
    , ("prop_atomic_network_delays", propAtomicNetworkDelays)
    ]

propAtomicConnected :: Property
propAtomicConnected = property $ do
    seed   <- forAll $ Gen.prune Gen.splitMixSeed
    boot   <- forAll $ Gen.connectedContacts Gen.defaultNetworkBounds
    links  <-
        forAllWith (renderInf 10) $
            Gen.prune $ Gen.infiniteListOf (pure Fast)
    bcasts <- forAll $ genBroadcasts boot
    atomicBroadcast (seedSMGen' seed) boot links bcasts

propAtomicNetworkDelays :: Property
propAtomicNetworkDelays = property $ do
    seed   <- forAll $ Gen.prune Gen.splitMixSeed
    boot   <- forAll $ Gen.connectedContacts Gen.defaultNetworkBounds
    links  <-
        forAllWith (renderInf 10) $
            Gen.prune $ Gen.infiniteListOf (Gen.linkState 50)
    bcasts <- forAll $ genBroadcasts boot
    atomicBroadcast (seedSMGen' seed) boot links bcasts

-- | Assert atomicity of broadcasts.
--
-- A broadcast is said to be atomic if it (eventually) reaches all nodes.
atomicBroadcast
    :: SMGen
    -> Contacts
    -> InfiniteListOf LinkState
    -> NonEmpty BroadcastMessage
    -> PropertyT IO ()
atomicBroadcast rng contacts links bcasts = do
    stores <-
        evalIO $ do
            network <- initNetwork rng contacts links
            -- Los geht's
            traverse_ (castbroad network) bcasts
            -- Heal network
            atomicModifyIORef' (netLinkState network) . const $
                (InfiniteListOf $ repeat Fast, ())
            -- Trigger anti-entropy repair
            let recent = NonEmpty.last bcasts
            castbroad network recent
                { bcastMsgId   = "________"
                , bcastPayload = bcastMsgId recent
                }
            -- Wait done
            drainTaskQueue (netTaskQueue network)

            Map.toList <$> traverse (readIORef . nodeStore) (netNodes network)

    annotateShow (map (length . snd) stores)
    annotateShow stores

    let
        stores' = nonEmpty . map (Map.delete "________" . snd) $ stores
        bcasts' = NonEmpty.toList
                . NonEmpty.map (\bc -> (bcastMsgId bc, bcastPayload bc))
                $ bcasts
     in do
        Just True === fmap allEqual stores'
        fmap NonEmpty.head stores' === Just (Map.fromList bcasts')
  where
    ensureRootHasChain root mid =
        let
            upuntil = NonEmpty.takeWhile ((/= mid) . bcastMsgId) bcasts
         in
            for_ upuntil $ \BroadcastMessage{..} ->
                storeInsert root bcastMsgId bcastPayload

    castbroad network BroadcastMessage {..} =
        case Map.lookup bcastRoot (netNodes network) of
            Nothing -> throwString "castbroad: Broadcast root not found!"
            Just  r -> do
                ensureRootHasChain r bcastMsgId
                void $ storeInsert r bcastMsgId bcastPayload
                runBroadcast network r $
                    broadcast bcastMsgId bcastPayload

-- Node Storage ----------------------------------------------------------------

type Store = IORef (Map MessageId ByteString)

storeInsert :: Node -> MessageId -> ByteString -> IO ApplyResult
storeInsert node mid payload =
    atomicModifyIORef' (nodeStore node) $ \m ->
        let
            re = retransmit m
         in
            case Map.insertLookupWithKey (\_ v _ -> v) mid payload m of
                (Nothing, m') -> (m', Applied re)
                (Just  _, _ ) -> (m , Stale   re)
  where
    retransmit _ | mid == payload = Nothing
    retransmit m =
        case Map.lookup payload m of
            Nothing -> Just payload
            Just  _ -> Nothing

storeLookup :: Node -> MessageId -> IO (Maybe ByteString)
storeLookup (nodeStore -> ref) mid = Map.lookup mid <$> readIORef ref

-- Types -----------------------------------------------------------------------

data Network = Network
    { netNodes     :: Map MockNodeId Node
    , netTaskQueue :: TaskQueue
    , netPRNG      :: IORef SMGen
    , netLinkState :: IORef (InfiniteListOf LinkState)
    }

data Node = Node
    { nodeId    :: MockNodeId
    , nodeEnv   :: Env MockNodeId
    , nodeStore :: Store
    }

type ChainOfEvents = NonEmpty MessageId

data BroadcastMessage = BroadcastMessage
    { bcastRoot    :: MockNodeId
    , bcastMsgId   :: MessageId
    , bcastPayload :: ByteString
    } deriving Show

toBroadcastMessages
    :: NonEmpty MockNodeId
    -> ChainOfEvents
    -> NonEmpty BroadcastMessage
toBroadcastMessages roots chain =
    let
        payloads = NonEmpty.head chain <| chain
        messages = NonEmpty.zip chain payloads
     in
        NonEmpty.zipWith
            (uncurry . BroadcastMessage)
            (NonEmpty.cycle roots)
            messages

--------------------------------------------------------------------------------

genChainOfEvents :: MonadGen m => m ChainOfEvents
genChainOfEvents =
    let
        msgid = Gen.prune $ Gen.utf8 (Range.singleton 8) Gen.alphaNum
        chain = Gen.nonEmpty (Range.constantFrom 1 5 20) msgid
     in
        chain

genRoots :: MonadGen m => ChainOfEvents -> Contacts -> m (NonEmpty MockNodeId)
genRoots chain contacts =
    let
        nodes = map fst contacts
        root  = Gen.element nodes
        len   = NonEmpty.length chain
     in
        Gen.nonEmpty (Range.singleton len) root

genBroadcasts :: MonadGen m => Contacts -> m (NonEmpty BroadcastMessage)
genBroadcasts contacts = do
    chain <- genChainOfEvents
    roots <- genRoots chain contacts
    pure $ toBroadcastMessages roots chain

--------------------------------------------------------------------------------

initNetwork :: SMGen -> Contacts -> InfiniteListOf LinkState -> IO Network
initNetwork rng contacts links = do
    nodes <-
        for contacts $ \(self, peers) -> do
            hdl   <- new self
            store <- newIORef mempty
            pure (self, peers, Node self hdl store)

    tq   <- newTaskQueue
    rng' <- newIORef rng
    lnk  <- newIORef links
    let network = Network
            { netNodes     = Map.fromList $ map (\(self, _, node) -> (self, node)) nodes
            , netTaskQueue = tq
            , netPRNG      = rng'
            , netLinkState = lnk
            }

    for_ nodes $ \(_, peers, node) ->
        runBroadcast network node $ resetPeers (Set.fromList peers)

    pure network

runBroadcast :: Network -> Node -> Plumtree MockNodeId a -> IO a
runBroadcast network node ma = runPlumtree (nodeEnv node) ma >>= eval
  where
    eval = \case
        ApplyMessage mid msg k ->
            storeInsert node mid msg >>= k >>= eval

        LookupMessage mid k ->
            storeLookup node mid >>= k >>= eval

        SendEager to msg k -> do
            onNode network to (receive msg)
            k >>= eval

        SendLazy to round ihaves k -> do
            scheduleTask (netTaskQueue network) $ do
                threadDelay 30000
                onNode network to $
                    receive RPC
                        { rpcSender  = nodeId node
                        , rpcRound   = Just round
                        , rpcPayload = IHave ihaves
                        }
            k >>= eval

        Later _ _ action k -> do
            scheduleTask (netTaskQueue network) $ do
                threadDelay 60000
                runBroadcast network node action
            k >>= eval

        Cancel _ k -> k >>= eval

        Done a -> pure a

onNode :: Network -> MockNodeId -> Plumtree MockNodeId () -> IO ()
onNode network receiverId ma =
    case Map.lookup receiverId (netNodes network) of
        Nothing       -> throwString "onNode: Receiver not found!"
        Just receiver ->
            let
                go         = runBroadcast network receiver ma
                delayed by = scheduleTask (netTaskQueue network) $
                                 threadDelay (by * 1000) *> go
             in do
                link <-
                    atomicModifyIORef' (netLinkState network) $ \l ->
                        case uncons (fromInfiniteListOf l) of
                            Just (x, xs) -> (InfiniteListOf xs, x)
                            Nothing      -> error "Ran out of link states"
                case link of
                    Fast    -> go
                    Slow ms -> delayed ms

                    Flaky l u -> do
                        rng <- atomicModifyIORef' (netPRNG network) split
                        delayed . fst $ randomR (l, u) rng

                    Down -> pure ()
