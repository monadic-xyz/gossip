{-# LANGUAGE GeneralizedNewtypeDeriving #-}

-- |
-- Copyright   : 2018 Monadic GmbH
-- License     : BSD3
-- Maintainer  : kim@monadic.xyz, team@monadic.xyz
-- Stability   : experimental
-- Portability : non-portable (GHC extensions)
--
module Network.Gossip.Plumtree.Scheduler
    ( Env
    , SchedulerT
    , LazyFlushInterval

    , new

    , withScheduler
    , runSchedulerT

    , sendLazy
    , later
    , cancel
    )
where

import qualified Network.Gossip.Plumtree as P

import           Control.Concurrent (threadDelay)
import           Control.Concurrent.Async (Async, async)
import qualified Control.Concurrent.Async as Async
import           Control.Concurrent.STM (atomically)
import           Control.Exception.Safe
import           Control.Monad (forever)
import           Control.Monad.Reader
import           Data.Foldable (traverse_)
import           Data.Hashable (Hashable)
import           Data.HashSet (HashSet)
import qualified Data.HashSet as Set
import           Data.IORef (IORef, mkWeakIORef, newIORef)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Time.Clock (NominalDiffTime)
import           Data.Unique
import           Data.Void (Void)
import qualified Focus
import qualified ListT
import qualified STMContainers.Map as STMMap

data Env n = Env
    { envFlushInterval :: LazyFlushInterval
    , envLazyQueue     :: STMMap.Map n (HashSet (P.IHave n))
    , envDeferred      :: STMMap.Map P.MessageId (Map Unique (Async ()))
    , _alive           :: IORef ()
    }

type LazyFlushInterval = NominalDiffTime

new :: LazyFlushInterval -> IO (Env n)
new envFlushInterval = do
    envLazyQueue <- STMMap.newIO
    envDeferred  <- STMMap.newIO
    _alive     <- newIORef ()
    let hdl = Env {..}

    void $ mkWeakIORef _alive (destroy hdl)
    pure hdl

destroy :: Env n -> IO ()
destroy Env { envDeferred } = do
    deferreds <-
        atomically
            . ListT.fold
                (\xs -> pure . (xs <>) . map Async.uninterruptibleCancel . Map.elems . snd)
                []
            $ STMMap.stream envDeferred
    sequence_ deferreds

withScheduler :: Env n -> (n -> HashSet (P.IHave n) -> IO ()) -> IO a -> IO a
withScheduler hdl send k =
    bracket (runFlusher hdl send) shutdown (const k)
  where
    -- TODO(kim): allow draining before exit
    shutdown flush =
        Async.uninterruptibleCancel flush `finally` destroy hdl

newtype SchedulerT n m a = SchedulerT (ReaderT (Env n) m a)
    deriving ( Functor
             , Applicative
             , Monad
             , MonadIO
             , MonadReader (Env n)
             )

runSchedulerT :: Env n -> SchedulerT n m a -> m a
runSchedulerT r (SchedulerT ma) = runReaderT ma r

sendLazy
    :: (Eq n, Hashable n, MonadIO m)
    => n
    -> HashSet (P.IHave n)
    -> SchedulerT n m ()
sendLazy to ihaves = do
    queue <- asks envLazyQueue
    liftIO . atomically $ STMMap.focus upsert to queue
  where
    upsert = Focus.alterM $ pure . Just . maybe ihaves (Set.union ihaves)

later :: MonadIO m => NominalDiffTime -> P.MessageId -> IO () -> SchedulerT n m ()
later timeout mid action = do
    defs <- asks envDeferred
    liftIO $ do
        uniq    <- newUnique
        action' <-
            async $ do
                sleep timeout
                action
                    `finally` atomically (STMMap.focus (expunge uniq) mid defs)
        atomically $ STMMap.focus (upsert uniq action') mid defs
  where
    upsert uniq act = Focus.alterM $
        pure . Just . maybe (Map.singleton uniq act) (Map.insert uniq act)

    expunge _    Nothing     = pure ((), Focus.Remove)
    expunge uniq (Just acts) = do
        let acts' = Map.delete uniq acts
        if Map.null acts' then
            pure ((), Focus.Remove)
        else
            pure ((), Focus.Replace acts')

cancel :: MonadIO m => P.MessageId -> SchedulerT n m ()
cancel mid = do
    defs <- asks envDeferred
    liftIO $ do
        actions <-
            atomically $
                STMMap.focus (pure . (,Focus.Remove)) mid defs
        (traverse_ . traverse_) Async.cancel actions

-- Internal --------------------------------------------------------------------

runFlusher :: Env n -> (n -> HashSet (P.IHave n) -> IO ()) -> IO (Async Void)
runFlusher hdl send = async . forever $ do
    sleep $ envFlushInterval hdl
    ihaves <-
        atomically $ do
            ihaves <- ListT.toList $ STMMap.stream (envLazyQueue hdl)
            ihaves `seq` STMMap.deleteAll (envLazyQueue hdl)
            pure ihaves
    traverse_ (uncurry send) ihaves

sleep :: NominalDiffTime -> IO ()
sleep t = threadDelay $ toSeconds t * 1000000
  where
    toSeconds = round @Double . realToFrac
