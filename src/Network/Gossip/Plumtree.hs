{-# LANGUAGE FlexibleContexts       #-}
{-# LANGUAGE FlexibleInstances      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE RankNTypes             #-}

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

    , Meta (..)
    , IHave (..)
    , Gossip (..)
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
    , isAuthorised
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
import           Data.ByteString (ByteString)
import           Data.Foldable (foldl', foldlM, for_)
import           Data.Hashable (Hashable)
import           Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as Map
import           Data.HashSet (HashSet)
import qualified Data.HashSet as Set
import           Data.Maybe (isNothing)
import           Data.Time.Clock (NominalDiffTime)
import           GHC.Generics (Generic)
import           Lens.Micro (Lens', lens, over, set, _1, _2, _3)
import           Lens.Micro.Mtl (view)

type MessageId = ByteString
type Round     = Word

data Meta n = Meta
    { metaMessageId :: MessageId
    , metaRound     :: Round
    , metaSender    :: n
    } deriving (Eq, Generic)

instance Serialise n => Serialise (Meta n)
instance Hashable  n => Hashable  (Meta n)

newtype IHave n = IHave (Meta n)
    deriving (Eq, Generic)

instance Serialise n => Serialise (IHave n)
instance Hashable  n => Hashable  (IHave n)

data Gossip n = Gossip
    { gPayload :: ByteString
    , gMeta    :: Meta n
    } deriving (Eq, Generic)

instance Serialise n => Serialise (Gossip n)
instance Hashable  n => Hashable  (Gossip n)

data Message n =
      GossipM (HashSet (Gossip n))
    | IHaveM  (HashSet (IHave n))
    | Prune   n
    | Graft   (HashSet (Meta n))
    deriving (Eq, Generic)

instance (Eq n, Hashable n, Serialise n) => Serialise (Message n)
instance Hashable n => Hashable (Message n)

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

data Env n = Env
    { envSelf           :: n
    -- ^ Identity of this node.
    , envEagerPushPeers :: TVar (HashSet n)
    -- ^ The peers to eager push to.
    , envLazyPushPeers  :: TVar (HashSet n)
    -- ^ The peers to lazy push to.
    , envMissing        :: TVar (HashMap MessageId (IHave n))
    -- ^ Received 'IHave's for which we haven't requested the value yet.
    }

new :: (Eq n, Hashable n) => n -> IO (Env n)
new self =
    Env self <$> newTVarIO mempty <*> newTVarIO mempty <*> newTVarIO mempty

-- Continuations ---------------------------------------------------------------

data PlumtreeC n a =
      ApplyMessage  MessageId       ByteString                          (ApplyResult      -> IO (PlumtreeC n a))
    | LookupMessage MessageId                                           (Maybe ByteString -> IO (PlumtreeC n a))
    | SendEager     n               (Message n)                         (IO (PlumtreeC n a))
    | SendLazy      n               (HashSet (IHave n))                 (IO (PlumtreeC n a))
    | Later         NominalDiffTime MessageId           (Plumtree n ()) (IO (PlumtreeC n a))
    | Cancel        MessageId                                           (IO (PlumtreeC n a))
    | Done          a

sendEager :: n -> Message n -> Plumtree n ()
sendEager to msg =
    Plumtree $ ReaderT $ \_ -> ContT $ \k ->
        pure $ SendEager to msg (k ())

sendLazy :: n -> HashSet (IHave n) -> Plumtree n ()
sendLazy to ihave =
    Plumtree $ ReaderT $ \_ -> ContT $ \k ->
        pure $ SendLazy to ihave (k ())

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
broadcast
    :: (Eq n, Hashable n)
    => MessageId
    -> ByteString
    -> Plumtree n ()
broadcast mid msg = do
    self <- asks envSelf
    push $ Set.singleton Gossip
        { gPayload = msg
        , gMeta    = Meta
           { metaMessageId = mid
           , metaSender    = self
           , metaRound     = 0
           }
        }

isAuthorised :: Eq n => n -> Message n -> Bool
isAuthorised sender = \case
    IHaveM xs   -> Set.null xs || senderMatches sender xs
    Graft  xs   -> Set.null xs || senderMatches sender xs
    Prune  from -> from == sender
    _           -> True
  where
    senderMatches :: (Foldable t, HasMeta a n, Eq n) => n -> t a -> Bool
    senderMatches s xs = all ((== s) . view (metaL . metaSenderL)) xs

-- | Receive and handle some 'Message' from the network.
receive :: (Eq n, Hashable n) => Message n -> Plumtree n ()
receive (GossipM gs) = do
    Env { envSelf = self, envMissing = missing } <- ask

    (gossip, grafts, demote) <-
        let
            allMessageIds = Set.map (view (metaL . metaMessageIdL)) gs

            -- Request retransmission only if message id is not in received
            -- batch
            addGraft sender mid grafts
                | Set.member mid allMessageIds = grafts
                | otherwise                    =
                    Map.insertWith (<>) sender
                                        (Set.singleton Meta
                                            { metaMessageId = mid
                                            , metaRound     = 0
                                            , metaSender    = self
                                            })
                                        grafts

            go acc g = do
                let sender = view (metaL . metaSenderL)    g
                let mid    = view (metaL . metaMessageIdL) g

                -- Cancel any timers for this message.
                liftIO . atomically $
                    modifyTVar' missing $ Map.delete mid
                cancel mid

                r <- applyMessage mid (gPayload g)
                case r of
                    Applied retransmit ->
                        pure
                            -- Disseminate gossip.
                            . over _1 (Set.insert g)
                            -- Graft missing ancestors
                            . maybe id (over _2 . addGraft sender) retransmit
                            $ acc

                    Stale retransmit -> do
                        -- If we have no missing ancestors, prune ourselves
                        when (isNothing retransmit) $
                            sendEager sender (Prune self)
                        pure
                            -- Demote sender
                            . over _3 (Set.insert sender)
                            -- Graft missing ancestors
                            . maybe id (over _2 . addGraft sender) retransmit
                            $ acc

                    Error ->
                        -- TODO(kim): log this
                        pure acc
         in
            foldlM go (Set.empty, Map.empty, Set.empty) gs

    push $
        flip Set.map gossip $
              set  (metaL . metaSenderL) self
            . over (metaL . metaRoundL)  (+1)

    void . flip Map.traverseWithKey grafts $ \sender gs' -> do
        moveToEager sender
        sendEager sender $ Graft gs'

    for_ demote moveToLazy

receive (IHaveM is) = do
    missing <-
        let
            go !acc ihave@(IHave meta) = do
                msg <- lookupMessage (view metaMessageIdL meta)
                case msg of
                    Just _  -> pure acc
                    Nothing -> pure $ Set.insert ihave acc
         in
            foldlM go Set.empty is

    for_ missing scheduleGraft

receive (Prune sender) =
    moveToLazy sender

receive (Graft gs) = do
    self    <- asks envSelf
    replies <-
        let
            go !acc meta = do
                let sender = view metaSenderL meta
                let mid    = view metaMessageIdL meta
                payload <- lookupMessage mid
                case payload of
                    Nothing -> pure acc
                    Just  p ->
                        let
                            reply = Set.singleton Gossip
                               { gPayload = p
                               , gMeta    = set metaSenderL self meta
                               }
                         in
                            pure $
                                Map.insertWith (<>) sender reply acc
         in
            foldlM go Map.empty gs

    void $ flip Map.traverseWithKey replies $ \sender batch -> do
        moveToEager sender
        sendEager sender $ GossipM batch

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
        modifyTVar' missing $ Map.filter (\(IHave meta) -> metaSender meta /= n)

-- Internal --------------------------------------------------------------------

scheduleGraft :: (Eq n, Hashable n) => IHave n -> Plumtree n ()
scheduleGraft ihave@(IHave Meta { metaMessageId = mid }) = do
    missing <- asks envMissing
    liftIO . atomically $
        modifyTVar' missing $ Map.insert mid ihave
    later timeout1 mid $ go (Just timeout2)
  where
    timeout1 = 5 * 1000000
    timeout2 = 1 * 1000000

    go next = do
        Env { envSelf = self, envMissing = missing } <- ask
        miss <-
            liftIO . atomically $ do
                miss <- Map.lookup mid <$> readTVar missing
                modifyTVar' missing $ Map.delete mid
                pure miss

        let
            add !acc (IHave meta) =
                Map.insertWith
                    (<>)
                    (metaSender meta)
                    (Set.singleton $ set metaSenderL self meta)
                    acc

            graftsBySender = foldl' add Map.empty miss
         in
            void . flip Map.traverseWithKey graftsBySender $ \sender gs -> do
                moveToEager sender
                sendEager sender $ Graft gs

        for_ next $ \timeout ->
            later timeout mid $ go Nothing

push :: (Eq n, Hashable n) => HashSet (Gossip n) -> Plumtree n ()
push gs | Set.null gs = pure ()
        | otherwise   = do
    Env { envSelf           = self
        , envEagerPushPeers = eagers
        , envLazyPushPeers  = lazies
        } <- ask

    (eagers', lazies') <-
        liftIO . atomically $
            let
                senders = Set.map (view (metaL . metaSenderL)) gs
                diff xs = Set.difference xs senders
             in
                liftA2 (,) (diff <$> readTVar eagers) (diff <$> readTVar lazies)

    for_ eagers' $ \to ->
        sendEager to (GossipM gs)

    for_ lazies' $ \to ->
        sendLazy to $ Set.map (IHave . set metaSenderL self . gMeta) gs

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

-- Lenses ----------------------------------------------------------------------

metaMessageIdL :: Lens' (Meta n) MessageId
metaMessageIdL = lens metaMessageId (\s a -> s { metaMessageId = a })

metaRoundL :: Lens' (Meta n) Round
metaRoundL = lens metaRound (\s a -> s { metaRound = a })

metaSenderL :: Lens' (Meta n) n
metaSenderL = lens metaSender (\s a -> s { metaSender = a })

class HasMeta a n | a -> n where
    metaL :: Lens' a (Meta n)

instance HasMeta (Meta n) n where
    metaL = id
    {-# INLINE metaL #-}

instance HasMeta (Gossip n) n where
    metaL = lens gMeta (\s a -> s { gMeta = a })
    {-# INLINE metaL #-}

instance HasMeta (IHave n) n where
    metaL = lens (\(IHave m) -> m) (\_ a -> IHave a)
    {-# INLINE metaL #-}
