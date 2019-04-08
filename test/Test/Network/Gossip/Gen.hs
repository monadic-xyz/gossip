module Test.Network.Gossip.Gen
    ( MockNodeId
    , Contacts
    , SplitMixSeed
    , LinkState (..)
    , InfiniteListOf (..)
    , renderInf

    , NetworkBounds
    , defaultNetworkBounds

    , connectedContacts
    , disconnectedContacts
    , circularContacts

    , linkState

    , splitMixSeed

    , infiniteListOf
    )
where

import qualified Algebra.Graph.Class as Alga
import           Algebra.Graph.Relation.Symmetric
                 ( SymmetricRelation
                 , fromRelation
                 , toRelation
                 )
import qualified Algebra.Graph.ToGraph as Alga
import           Control.Applicative (liftA2)
import           Data.Bifunctor (second)
import qualified Data.Graph as Graph
import           Data.List (uncons, unfoldr)
import           Data.Set (Set)
import qualified Data.Set as Set
import           Data.Word (Word16, Word64)

import           Hedgehog
import qualified Hedgehog.Gen as Gen
import qualified Hedgehog.Gen.QuickCheck as Gen
import qualified Hedgehog.Range as Range
import qualified Test.QuickCheck.Gen as QC
import qualified Test.QuickCheck.Hedgehog as Gen

-- Types -----------------------------------------------------------------------

type MockNodeId   = Word16
type Contacts     = [(MockNodeId, [MockNodeId])]
type SplitMixSeed = (Word64, Word64)
type Milliseconds = Int

data LinkState =
      Fast
    | Slow  Milliseconds
    | Flaky Milliseconds Milliseconds
    | Down
    deriving (Eq, Show)

newtype InfiniteListOf a = InfiniteListOf { fromInfiniteListOf :: [a] }

renderInf :: Show a => Int -> InfiniteListOf a -> String
renderInf n (InfiniteListOf as) = showList (take n as) ""

---- Config --------------------------------------------------------------------

data NetworkBounds = NetworkBounds
    { netMinNodes    :: Int
    , netMaxNodes    :: Int
    , netMaxContacts :: Int
    }

defaultNetworkBounds :: NetworkBounds
defaultNetworkBounds = NetworkBounds
    { netMinNodes    = 5
    , netMaxNodes    = 100
    , netMaxContacts = 5
    }

-- Generators ------------------------------------------------------------------

---- Contacts ------------------------------------------------------------------

connectedContacts :: MonadGen m => NetworkBounds -> m Contacts
connectedContacts bounds = do
    nodes  <- nodeIds bounds
    splits <- Gen.list (Range.singleton (Set.size nodes))
                       (Gen.int (Range.constant 1 (netMaxContacts bounds)))
    graph  <-
        fmap Alga.overlays . traverse subgraph $
            clusters (Set.toList nodes, splits)
    pure $
        Alga.adjacencyList . toRelation $ ensureConnected graph
  where
    -- Split network into randomly-sized chunks.
    clusters :: ([MockNodeId], [Int]) -> [[MockNodeId]]
    clusters = unfoldr $ \case
        ([], _)    -> Nothing
        (ns, [])   -> Just (ns, mempty)
        (ns, s:ss) -> let (h, t) = splitAt s ns in Just (h, (t, ss))

    genTopo :: MonadGen m => m ([MockNodeId] -> SymmetricRelation MockNodeId)
    genTopo = Gen.element
        [ Alga.path
        , Alga.circuit
        , Alga.clique
        , maybe Alga.empty (uncurry Alga.star) . uncons
        ]

    subgraph :: MonadGen m => [MockNodeId] -> m (SymmetricRelation MockNodeId)
    subgraph [node] = pure $ Alga.vertex node
    subgraph nodes  = ($ nodes) <$> genTopo

    -- Ensure the graph is connected: if it has only one component, it is
    -- already connected, otherwise, connect the roots of the forest as a
    -- (undirected) circuit and overlay the result onto the graph.
    ensureConnected :: SymmetricRelation MockNodeId -> SymmetricRelation MockNodeId
    ensureConnected g =
        let rel = toRelation g
         in case Alga.dfsForest rel of
               cs@(_:_:_) -> fromRelation . Alga.overlay rel . Alga.circuit $ map tip cs
               _          -> g

    tip :: Graph.Tree a -> a
    tip (Graph.Node a _) = a

disconnectedContacts :: MonadGen m => NetworkBounds -> m Contacts
disconnectedContacts bounds = map (,[]) . Set.toList <$> nodeIds bounds

circularContacts :: MonadGen m => NetworkBounds -> m Contacts
circularContacts bounds =
    toContacts . zipped . Set.toList <$> nodeIds bounds
  where
    zipped :: [MockNodeId] -> [(MockNodeId, MockNodeId)]
    zipped ns = zip ns $ drop 1 (cycle ns)

    toContacts :: [(MockNodeId, MockNodeId)] -> Contacts
    toContacts = map (second pure)

---- LinkState -----------------------------------------------------------------

linkState :: MonadGen m => Int -> m LinkState
linkState maxLatency = Gen.choice
    [ pure Fast
    , Slow <$> Gen.int (Range.constantFrom 5 1 maxLatency)
    , flaky
    , pure Down
    ]
  where
    flaky = do
        lower <- Gen.int (Range.constantFrom 5 0 maxLatency)
        Flaky lower <$> Gen.int (Range.constant lower maxLatency)

---- SplitMixSeed --------------------------------------------------------------

splitMixSeed :: MonadGen m => m SplitMixSeed
splitMixSeed = liftA2 (,) word64 word64
  where
    word64 = Gen.prune $ Gen.word64 Range.constantBounded

---- Util ----------------------------------------------------------------------

infiniteListOf :: Gen a -> Gen (InfiniteListOf a)
infiniteListOf =
    fmap InfiniteListOf . Gen.quickcheck . QC.infiniteListOf . Gen.hedgehog

-- Internal --------------------------------------------------------------------

nodeIds :: MonadGen m => NetworkBounds -> m (Set MockNodeId)
nodeIds NetworkBounds{..} =
    Gen.set (Range.constantFrom netMinNodes netMinNodes netMaxContacts)
            (nodeId netMaxNodes)

nodeId :: MonadGen m => Int -> m MockNodeId
nodeId maxNodes = Gen.word16 (Range.constant 0 (fromIntegral $ maxNodes - 1))
