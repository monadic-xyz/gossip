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
import           Control.Concurrent.STM (STM, atomically)
import           Control.Exception.Safe
import           Control.Monad (forever)
import           Control.Monad.Reader
import           Data.Foldable (for_, traverse_)
import           Data.Hashable (Hashable)
import           Data.HashMap.Strict (HashMap)
import qualified Data.HashMap.Strict as HashMap
import           Data.HashSet (HashSet)
import           Data.IORef (IORef, mkWeakIORef, newIORef)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Time.Clock (NominalDiffTime)
import           Data.Unique
import           Data.Void (Void)
import qualified Focus
import qualified ListT
import qualified StmContainers.Map as STMMap

data Env n = Env
    { envFlushInterval :: LazyFlushInterval
    , envLazyQueue     :: STMMap.Map n (HashMap P.Round (HashSet P.MessageId))
    , envDeferred      :: STMMap.Map P.MessageId (Map Unique (Async ()))
    , _alive           :: IORef ()
    }

type LazyFlushInterval = NominalDiffTime

new :: LazyFlushInterval -> IO (Env n)
new envFlushInterval = do
    envLazyQueue <- STMMap.newIO
    envDeferred  <- STMMap.newIO
    _alive       <- newIORef ()
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
            $ STMMap.listT envDeferred
    sequence_ deferreds

withScheduler
    :: Env n
    -> (n -> P.Round -> HashSet P.MessageId -> IO ())
    -> IO a
    -> IO a
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
    -> P.Round
    -> HashSet P.MessageId
    -> SchedulerT n m ()
sendLazy to r mids = do
    queue <- asks envLazyQueue
    liftIO . atomically $ STMMap.focus upsert to queue
  where
    upsert = Focus.alterM $
        pure . Just . maybe (HashMap.singleton r mids) (HashMap.insertWith (<>) r mids)

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

    expunge :: Unique -> Focus.Focus (Map Unique (Async ())) STM ()
    expunge uniq = do
        el <- Focus.lookup
        case el of
          Nothing -> Focus.delete
          Just acts -> do
              let acts' = Map.delete uniq acts
              if Map.null acts' then
                  Focus.delete
              else
                  Focus.insert acts'

cancel :: MonadIO m => P.MessageId -> SchedulerT n m ()
cancel mid = do
    defs <- asks envDeferred
    liftIO $ do
        actions <-
            atomically $
                STMMap.focus expunge mid defs
        (traverse_ . traverse_) Async.cancel actions
  where
    expunge :: Focus.Focus (Map Unique (Async ())) STM (Maybe (Map Unique (Async ())))
    expunge = Focus.lookup <* Focus.delete

-- Internal --------------------------------------------------------------------

runFlusher
    :: Env n
    -> (n -> P.Round -> HashSet P.MessageId -> IO ())
    -> IO (Async Void)
runFlusher hdl send = async . forever $ do
    sleep $ envFlushInterval hdl
    ihaves <-
        atomically $ do
            ihaves <- ListT.toList $ STMMap.listT (envLazyQueue hdl)
            ihaves `seq` STMMap.reset (envLazyQueue hdl)
            pure ihaves
    for_ ihaves $ \(to, byround) ->
        flip HashMap.traverseWithKey byround $ send to

sleep :: NominalDiffTime -> IO ()
sleep t = threadDelay $ toSeconds t * 1000000
  where
    toSeconds = round @Double . realToFrac
