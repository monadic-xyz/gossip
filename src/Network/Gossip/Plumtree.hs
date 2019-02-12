{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE RankNTypes        #-}

-- |
-- Copyright   : 2018 Monadic GmbH
-- License     : BSD3
-- Maintainer  : kim@monadic.xyz, team@monadic.xyz
-- Stability   : experimental
-- Portability : non-portable (GHC extensions)
--
-- Implementation of the \"Epidemic Broadcast Trees\" (aka \"Plumtree\")
-- gossip broadcast protocol.
--
-- <http://asc.di.fct.unl.pt/~jleitao/pdf/srds07-leitao.pdf>
module Network.Gossip.Plumtree
    ( MessageId
    , Round

    , RPC (..)
    , Message (..)
    , ApplyResult (..)

    , Env
    , envSelf
    , new

    , PlumtreeC (..)

    , Plumtree
    , runPlumtree

    , eagerPushPeers
    , lazyPushPeers
    , resetPeers

    , broadcast
    , receive
    , neighborUp
    , neighborDown
    )
where

import           Codec.Serialise (Serialise)
import           Control.Applicative (liftA2)
import           Control.Concurrent.STM
import           Control.Monad.Reader
import           Control.Monad.Trans.Cont
import           Data.Bool (bool)
import           Data.ByteString (ByteString)
import           Data.Foldable (foldl', foldlM, for_, traverse_)
import           Data.Function ((&))
import           Data.Hashable (Hashable)
import           Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as Map
import           Data.HashSet (HashSet)
import qualified Data.HashSet as Set
import           Data.Maybe (isNothing)
import           Data.Time.Clock (NominalDiffTime)
import           GHC.Generics (Generic)
import           Lens.Micro (over, set, _1, _2, _3)
import           Prelude hiding (round)

type MessageId = ByteString
type Round     = Word

data RPC n = RPC
    { rpcSender  :: n
    , rpcRound   :: Maybe Round
    , rpcPayload :: Message
    } deriving (Eq, Generic)

instance Serialise n => Serialise (RPC n)

data Message =
      Gossip (HashMap MessageId ByteString)
    | IHave  (HashSet MessageId)
    | Prune
    | Graft  (HashSet MessageId)
    deriving (Eq, Show, Generic)

instance Serialise Message
instance Hashable  Message

-- | Result of 'applyMessage'.
data ApplyResult =
      Applied (Maybe MessageId)
    -- ^ The message was applied successfully, and was not known before. If
    -- 'Just' a 'MessageId' is given, it is indicated that an earlier message by
    -- that id was determined to be missing, and the network is instructed to
    -- attempt to retransmit it.
    | Stale (Maybe MessageId)
    -- ^ The message was either applied before, or a later message rendered it
    -- obsolete. Similar to 'Applied', 'Just' a 'MessageId' indicates that an
    -- earlier message should be retransmitted by the network.
    | Error
    -- ^ An error occurred. Perhaps the message was invalid.
    deriving (Eq, Show)

data Env n = Env
    { envSelf           :: n
    -- ^ Identity of this node.
    , envEagerPushPeers :: TVar (HashSet n)
    -- ^ The peers to eager push to.
    , envLazyPushPeers  :: TVar (HashSet n)
    -- ^ The peers to lazy push to.
    , envMissing        :: TVar (HashMap MessageId (HashSet n))
    -- ^ Received 'IHave's for which we haven't requested the value yet.
    }

new :: (Eq n, Hashable n) => n -> IO (Env n)
new self =
    Env self <$> newTVarIO mempty <*> newTVarIO mempty <*> newTVarIO mempty

-- Continuations ---------------------------------------------------------------

data PlumtreeC n a =
      ApplyMessage  MessageId       ByteString                     (ApplyResult      -> IO (PlumtreeC n a))
    | LookupMessage MessageId                                      (Maybe ByteString -> IO (PlumtreeC n a))
    | SendEager     n               (RPC n)                        (IO (PlumtreeC n a))
    | SendLazy      n               Round      (HashSet MessageId) (IO (PlumtreeC n a))
    | Later         NominalDiffTime MessageId  (Plumtree n ())     (IO (PlumtreeC n a))
    | Cancel        MessageId                                      (IO (PlumtreeC n a))
    | Done          a

sendEager :: n -> RPC n -> Plumtree n ()
sendEager to rpc =
    Plumtree $ ReaderT $ \_ -> ContT $ \k ->
        pure $ SendEager to rpc (k ())

sendLazy :: n -> Round -> HashSet MessageId -> Plumtree n ()
sendLazy to round ihave =
    Plumtree $ ReaderT $ \_ -> ContT $ \k ->
        pure $ SendLazy to round ihave (k ())

later :: NominalDiffTime -> MessageId -> Plumtree n () -> Plumtree n ()
later timeout key action =
    Plumtree $ ReaderT $ \_ -> ContT $ \k ->
        pure $ Later timeout key action (k ())

cancel :: MessageId -> Plumtree n ()
cancel key =
    Plumtree $ ReaderT $ \_ -> ContT $ \k ->
        pure $ Cancel key (k ())

applyMessage :: MessageId -> ByteString -> Plumtree n ApplyResult
applyMessage mid v =
    Plumtree $ ReaderT $ \_ -> ContT $ \k ->
        pure $ ApplyMessage mid v k

lookupMessage :: MessageId -> Plumtree n (Maybe ByteString)
lookupMessage mid =
    Plumtree $ ReaderT $ \_ -> ContT $ \k ->
        pure $ LookupMessage mid k

-- Monad -----------------------------------------------------------------------

newtype Plumtree n a = Plumtree
    { fromPlumtree :: forall x. ReaderT (Env n) (ContT (PlumtreeC n x) IO) a
    } deriving Functor

instance Applicative (Plumtree n) where
    pure x = Plumtree $ pure x
    (<*>)  = ap

instance Monad (Plumtree n) where
    return            = pure
    Plumtree m >>= f = Plumtree $ m >>= fromPlumtree . f
    {-# INLINE (>>=) #-}

instance MonadIO (Plumtree n) where
    liftIO io = Plumtree $ liftIO io

instance MonadReader (Env n) (Plumtree n) where
    ask       = Plumtree $ ReaderT pure
    local f m = Plumtree $ local f (fromPlumtree m)

runPlumtree :: Env n -> Plumtree n a -> IO (PlumtreeC n a)
runPlumtree r (Plumtree ma) = runContT (runReaderT ma r) (pure . Done)

-- API -------------------------------------------------------------------------

eagerPushPeers :: Plumtree n (HashSet n)
eagerPushPeers = asks envEagerPushPeers >>= liftIO . readTVarIO

lazyPushPeers :: Plumtree n (HashSet n)
lazyPushPeers = asks envLazyPushPeers >>= liftIO . readTVarIO

resetPeers
    :: (Eq n, Hashable n)
    => HashSet n
    -> Plumtree n ()
resetPeers peers = do
    Env { envSelf           = self
        , envEagerPushPeers = eagers
        , envLazyPushPeers  = lazies
        } <- ask
    liftIO . atomically $ do
        modifyTVar' eagers $ const (Set.delete self peers)
        modifyTVar' lazies $ const mempty

-- | Broadcast some arbitrary data to the network.
--
-- The caller is responsible for applying the message to the local state, such
-- that subsequent 'lookupMessage' calls would return 'Just' the 'ByteString'
-- passed in here. Correspondingly, subsequent 'applyMessage' calls must return
-- 'Stale' (or 'ApplyError').
broadcast :: MessageId -> ByteString -> Plumtree n ()
broadcast mid msg = push 0 $ Map.singleton mid msg

-- | Receive and handle some 'RPC' from the network.
receive :: (Eq n, Hashable n) => RPC n -> Plumtree n ()
receive RPC { rpcSender, rpcRound, rpcPayload } = case rpcPayload of
    Gossip gossips -> do
        Env { envSelf = self, envMissing = missing } <- ask

        (gossip, grafts, demote) <-
            let
                allMessageIds = Set.fromList $ Map.keys gossips

                -- Request retransmission only if message id is not in received
                -- batch
                addGraft mid
                    | Set.member mid allMessageIds = id
                    | otherwise                    = Set.insert mid

                go !acc (!mid, !payload) = do
                    -- Cancel any timers for this message.
                    liftIO . atomically $
                        modifyTVar' missing $ Map.delete mid
                    cancel mid

                    r <- applyMessage mid payload
                    case r of
                        Applied retransmit ->
                            pure
                                -- Disseminate gossip.
                                . over _1 (Map.insert mid payload)
                                -- Graft missing ancestors
                                . maybe id (over _2 . addGraft) retransmit
                                $ acc

                        Stale retransmit -> do
                            -- If we have no missing ancestors, prune ourselves
                            when (isNothing retransmit) $
                                sendEager rpcSender RPC
                                    { rpcSender  = self
                                    , rpcRound   = Nothing
                                    , rpcPayload = Prune
                                    }
                            pure
                                -- Demote sender
                                . set _3 True
                                -- Graft missing ancestors
                                . maybe id (over _2 . addGraft) retransmit
                                $ acc

                        Error ->
                            -- TODO(kim): log this
                            pure acc
             in
                foldlM go (Map.empty, Set.empty, False) (Map.toList gossips)

        push (maybe 1 (+1) rpcRound) gossip
        rpcSender & bool moveToLazy moveToEager demote
        unless (Set.null grafts) $ do
            -- Fast-path: graft to sender directly
            sendEager rpcSender RPC
                { rpcSender  = self
                , rpcRound   = Just 0
                , rpcPayload = Graft grafts
                }
            -- Fallback to a scheduled graft, which may try other peers
            scheduleGraft rpcSender grafts

    IHave ihaves -> do
        missing <- filterM (fmap isNothing . lookupMessage) $ Set.toList ihaves
        scheduleGraft rpcSender $ Set.fromList missing

    Prune ->
        moveToLazy rpcSender

    Graft mids -> do
        self <- asks envSelf
        msgs <-
            let
                go !acc mid = do
                    msg <- lookupMessage mid
                    case msg of
                        Nothing -> pure acc
                        Just  m -> pure $ Map.insert mid m acc
             in
                foldlM go Map.empty mids
        unless (Map.null msgs) $
            sendEager rpcSender RPC
                { rpcSender  = self
                , rpcRound   = Just 0
                , rpcPayload = Gossip msgs
                }

-- | Peer sampling service callback when a new peer is detected.
--
-- Section 3.6, \"Dynamic Membership\":
--
-- \"When a new member is detected, it is simply added to the set of
-- eagerPushPeers, i.e. it is considered as a candidate to become part of the
-- tree.\".
neighborUp
    :: (Eq n, Hashable n)
    => n
    -> Plumtree n ()
neighborUp n = do
    eagers <- asks envEagerPushPeers
    liftIO . atomically . modifyTVar' eagers $ Set.insert n

-- | Peer sampling service callback when a peer leaves the overlay.
--
-- Section 3.6, \"Dynamic Membership\":
--
-- \"When a neighbor is detected to leave the overlay, it is simple[sic]
-- removed from the membership. Furthermore, the record of 'IHave' messages sent
-- from failed members is deleted from the missing history.\"
neighborDown
    :: (Eq n, Hashable n)
    => n
    -> Plumtree n ()
neighborDown n = do
    Env { envEagerPushPeers = eagers
        , envLazyPushPeers  = lazies
        , envMissing        = missing
        } <- ask

    liftIO . atomically $ do
        modifyTVar' eagers  $ Set.delete n
        modifyTVar' lazies  $ Set.delete n
        modifyTVar' missing $ Map.mapMaybe $ \ihaves ->
            case Set.filter (/= n) ihaves of
                xs | Set.null xs -> Nothing
                   | otherwise   -> Just xs

-- Internal --------------------------------------------------------------------

scheduleGraft :: (Eq n, Hashable n) => n -> HashSet MessageId -> Plumtree n ()
scheduleGraft _   mids | Set.null mids = pure ()
scheduleGraft has mids = do
    missing <- asks envMissing
    liftIO . atomically $
        modifyTVar' missing $ \missing' ->
            foldl' (\m mid -> Map.insertWith (<>) mid (Set.singleton has) m)
                   missing'
                   mids
    for_ mids $ \mid ->
        later timeout1 mid $ go mid (Just timeout2)
  where
    timeout1 = 5 * 1000000
    timeout2 = 1 * 1000000

    go mid next = do
        Env { envSelf = self, envMissing = missing } <- ask

        theyHave <-
            liftIO . atomically $
                Map.lookup mid <$> readTVar missing

        flip (traverse_ . traverse_) theyHave $ \have -> do
            moveToEager have
            sendEager have RPC
                { rpcSender  = self
                , rpcRound   = Nothing
                , rpcPayload = Graft (Set.singleton mid)
                }

        case next of
            Just timeout -> later timeout mid $ go mid Nothing
            Nothing      ->
                liftIO . atomically $
                    modifyTVar' missing (Map.delete mid)

push :: Round -> HashMap MessageId ByteString -> Plumtree n ()
push _     payloads | Map.null payloads = pure ()
push round payloads = do
    Env { envSelf           = self
        , envEagerPushPeers = eagers
        , envLazyPushPeers  = lazies
        } <- ask

    (eagers', lazies') <-
        liftIO . atomically $
            liftA2 (,) (readTVar eagers) (readTVar lazies)

    for_ eagers' $ \to ->
        sendEager to RPC
            { rpcSender  = self
            , rpcRound   = Just round
            , rpcPayload = Gossip payloads
            }

    for_ lazies' $ \to ->
        sendLazy to round $ Set.fromList (Map.keys payloads)

-- Helpers ---------------------------------------------------------------------

moveToLazy :: (Eq n, Hashable n) => n -> Plumtree n ()
moveToLazy peer = do
    hdl <- ask
    liftIO $ updatePeers hdl Set.delete Set.insert peer

moveToEager :: (Eq n, Hashable n) => n -> Plumtree n ()
moveToEager peer = do
    hdl <- ask
    liftIO $ updatePeers hdl Set.insert Set.delete peer

updatePeers
    :: Eq n
    => Env n
    -> (n -> HashSet n -> HashSet n)
    -> (n -> HashSet n -> HashSet n)
    -> n
    -> IO ()
updatePeers Env { envSelf           = self
                , envEagerPushPeers = eagers
                , envLazyPushPeers  = lazies
                }
            updateEager
            updateLazy
            peer
    | self == peer = pure ()
    | otherwise    = atomically $ do
        modifyTVar' eagers $ updateEager peer
        modifyTVar' lazies $ updateLazy  peer
