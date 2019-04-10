{-# LANGUAGE DataKinds                  #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE KindSignatures             #-}
{-# LANGUAGE TemplateHaskell            #-}

module Test.Network.Gossip.Membership.StateMachine (tests) where

import qualified Network.Gossip.HyParView as Impl

import           Control.Monad.IO.Class
import           Data.Generics.Product
import           Data.HashSet (HashSet)
import qualified Data.HashSet as Set
import           GHC.Generics (Generic)
import           Lens.Micro (Lens', set)
import           Lens.Micro.Extras (view)
import           System.Random.SplitMix (seedSMGen')

import           Hedgehog hiding (eval)
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Range as Range
import           Test.Network.Gossip.Gen (MockNodeId, MockPeer(..))
import qualified Test.Network.Gossip.Gen as Gen


tests :: IO Bool
tests = checkParallel $$discover

data Model (v :: * -> *) = Model
    { self  :: MockPeer
    , peers :: Maybe (Var (Impl.Peers MockPeer) v)
    } deriving Generic

active, passive :: Lens' (Impl.Peers MockPeer) (HashSet MockPeer)
active  = field @"active"
passive = field @"passive"

initialState :: MockPeer -> Model v
initialState self = Model self Nothing

-- Join ------------------------------------------------------------------------

newtype Join (v :: * -> *) = Join MockNodeId
    deriving (Eq, Show)

instance HTraversable Join where
    htraverse _ (Join nid) = pure $ Join nid

cmdJoin
    :: MonadGen n
    => (MockNodeId -> m (Impl.Peers MockPeer))
    -> Command n m Model
cmdJoin run =
    let
        gen Model { self } =
            Just . fmap Join
                 . Gen.filter (/= view Impl.peerNodeId self)
                 $ Gen.nodeId maxBound

        exe (Join nid) = run nid
     in
        Command gen exe
            [ Update $ \s _ out ->
                set (field @"peers") (Just out) s

            , Ensure $ \_ after (Join nid) out ->
                let
                    peer   = MockPeer nid
                    peers' = concrete <$> peers after
                 in
                    case peers' of
                        Nothing -> failure
                        Just ps -> do
                            ps === out
                            assert $ Set.member peer $ view active ps
            ]

-- Disconnect ------------------------------------------------------------------

newtype Disconnect (v :: * -> *) = Disconnect MockNodeId
    deriving (Eq, Show)

instance HTraversable Disconnect where
    htraverse _ (Disconnect nid) = pure $ Disconnect nid

cmdDisconnect
    :: MonadGen n
    => (MockNodeId -> m (Impl.Peers MockPeer))
    -> Command n m Model
cmdDisconnect run =
    let
        gen Model { self } =
            Just . fmap Disconnect
                 . Gen.filter (/= view Impl.peerNodeId self)
                 $ Gen.nodeId maxBound

        exe (Disconnect nid) = run nid
     in
        Command gen exe
            [ Update $ \s _ out ->
                set (field @"peers") (Just out) s

            , Ensure $ \_ after (Disconnect nid) out ->
                let
                    peer = MockPeer nid
                    peers' = concrete <$> peers after
                 in
                    case peers' of
                        Nothing -> failure
                        Just ps -> do
                            ps === out
                            assert $       Set.member peer $ view passive ps
                            assert $ not $ Set.member peer $ view active  ps
            ]

--------------------------------------------------------------------------------

prop_singleNode :: Property
prop_singleNode = property $ do
    rng     <- seedSMGen' <$> forAll Gen.splitMixSeed
    self    <- forAll $ Gen.mockPeer Nothing
    nenv    <- liftIO $ Impl.new self Impl.defaultConfig rng
    actions <- forAll $
        Gen.sequential (Range.linear 1 100) (initialState self)
            [ cmdJoin $ runJoin nenv
            , cmdDisconnect $ runDisconnect nenv
            ]
    executeSequential (initialState self) actions
  where
    runJoin nenv nid = liftIO . runSingleNode nenv $ do
        Impl.receive Impl.RPC
            { Impl.rpcSender    = MockPeer nid
            , Impl.rpcRecipient = Impl.envSelf nenv
            , Impl.rpcPayload   = Impl.Join
            }
        Impl.getPeers'

    runDisconnect nenv nid = liftIO . runSingleNode nenv $ do
        Impl.receive Impl.RPC
            { Impl.rpcSender    = MockPeer nid
            , Impl.rpcRecipient = Impl.envSelf nenv
            , Impl.rpcPayload   = Impl.Disconnect
            }
        Impl.getPeers'

runSingleNode :: Impl.Env MockPeer -> Impl.HyParView MockPeer a -> IO a
runSingleNode env ma = Impl.runHyParView env ma >>= eval
  where
    eval = \case
        Impl.ConnectionOpen addr _ k ->
            k (Right (mkConn (MockPeer addr))) >>= eval

        Impl.SendAdHoc    _ k -> k >>= eval
        Impl.NeighborUp   _ k -> k >>= eval
        Impl.NeighborDown _ k -> k >>= eval
        Impl.Done a           -> pure a

    mkConn to' = Impl.Connection
        { Impl.connPeer  = to'
        , Impl.connSend  = const $ pure ()
        , Impl.connClose = pure ()
        }
