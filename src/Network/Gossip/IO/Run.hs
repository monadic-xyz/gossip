-- |
-- Copyright   : 2018 Monadic GmbH
-- License     : BSD3
-- Maintainer  : kim@monadic.xyz, team@monadic.xyz
-- Stability   : experimental
-- Portability : non-portable (GHC extensions)
--
module Network.Gossip.IO.Run
    ( ProtocolMessage
    , Env
    , withGossip
    , broadcast
    )
where

import qualified Network.Gossip.HyParView as H
import qualified Network.Gossip.HyParView.Periodic as HP
import           Network.Gossip.IO.Peer
import           Network.Gossip.IO.Protocol (ProtocolMessage(..))
import qualified Network.Gossip.IO.Socket as S
import           Network.Gossip.IO.Trace
import           Network.Gossip.IO.Wire
import qualified Network.Gossip.Plumtree as P
import qualified Network.Gossip.Plumtree.Scheduler as PS

import           Control.Concurrent.Async (async, uninterruptibleCancel)
import           Control.Exception.Safe
                 ( bracket
                 , onException
                 , tryAny
                 , withException
                 )
import           Data.Bifunctor (second)
import           Data.ByteString (ByteString)
import           Data.Foldable (toList)
import           Data.Hashable (Hashable)
import           Prelude hiding (round)
import qualified System.Random.SplitMix as SplitMix

data Env n = Env
    { envPlumtree      :: P.Env  (Peer n)
    , envHyParView     :: H.Env  (Peer n)
    , envScheduler     :: PS.Env (Peer n)
    , envIO            :: S.Env n (ProtocolMessage (Peer n))
    , envApplyMessage  :: P.MessageId -> ByteString -> IO P.ApplyResult
    , envLookupMessage :: P.MessageId -> IO (Maybe ByteString)
    , envTrace         :: Traceable n -> IO ()
    }

withGossip
    :: (Eq n, Hashable n, Traversable t)
    => Peer n
    -- ^ Self
    -> H.Config
    -- ^ "Network.Gossip.HyParView" settings
    -> HP.Config
    -- ^ "Network.Gossip.HyParView.Periodic" settings
    -> PS.LazyFlushInterval
    -- ^ Flush interval for "Network.Gossip.ProtocolMessage.Scheduler"
    -> S.Handshake n (ProtocolMessage (Peer n))
    -- ^ Handshake
    -> (P.MessageId -> ByteString -> IO P.ApplyResult)
    -- ^ Apply message
    -> (P.MessageId -> IO (Maybe ByteString))
    -- ^ Lookup message
    -> (Traceable n -> IO ())
    -- ^ Tracing
    -> t (Peer n)
    -- ^ Intial contacts
    -> (Env n -> IO a)
    -> IO a
withGossip self
           hcfg
           hpcfg
           flushInterval
           handshake
           envApplyMessage
           envLookupMessage
           envTrace
           contacts
           k
    = do
    envPlumtree  <- P.new self
    envHyParView <- H.new self hcfg =<< SplitMix.initSMGen
    envIO        <- S.new handshake
    envScheduler <- PS.new flushInterval
    let env = Env {..}

    PS.withScheduler envScheduler (sendIHaves env) $
        HP.withPeriodic hpcfg (runHyParView env)   . const $
        bracket (listen env) uninterruptibleCancel . const $ do
            envTrace $ TraceBootstrap (Bootstrapping self contacts)
            bootstrap env
            envTrace $ TraceBootstrap (Bootstrapped self)
            k env
  where
    sendIHaves env to round xs =
        protoSend env False to . ProtocolPlumtree $ P.RPC
            { P.rpcSender  = self
            , P.rpcRound   = Just round
            , P.rpcPayload = P.IHave xs
            }

    listen env = async $
        runNetwork env $ S.listen (evalNetwork env) (peerAddr self)

    bootstrap env = do
        peers <-
            runHyParView env $ do
                H.joinAny (toList contacts)
                H.getPeers
        runPlumtree env $ P.resetPeers peers

broadcast :: (Eq n, Hashable n) => Env n -> P.MessageId -> ByteString -> IO ()
broadcast env mid msg = runPlumtree env $ P.broadcast mid msg

evalPlumtree
    :: (Eq n, Hashable n)
    => Env n
    -> P.PlumtreeC (Peer n) a
    -> IO a
evalPlumtree env@Env { envApplyMessage, envLookupMessage } = go
  where
    go = \case
        P.ApplyMessage mid v k ->
            envApplyMessage mid v >>= k >>= go

        P.LookupMessage mid k ->
            envLookupMessage mid >>= k >>= go

        P.SendEager to msg k -> do
            protoSend env False to (ProtocolPlumtree msg)
                `onException` runHyParView env (H.eject to)
            k >>= go

        P.SendLazy to round ihaves k -> do
            runScheduler env $ PS.sendLazy to round ihaves
            k >>= go

        P.Later t mid action k -> do
            runScheduler env $ PS.later t mid (runPlumtree env action)
            k >>= go

        P.Cancel mid k -> do
            runScheduler env $ PS.cancel mid
            k >>= go

        P.Done a -> pure a

evalHyParView
    :: (Eq n, Hashable n)
    => Env n
    -> H.HyParViewC (Peer n) a
    -> IO a
evalHyParView env@Env { envTrace = trace } = go
  where
    go = \case
        H.ConnectionOpen to k -> do
            trace $ TraceConnection (Connecting to)

            conn <-
                    second (const $ mkConn to)
                <$> tryAny (runNetwork env (S.connect (evalNetwork env) to))

            trace . TraceConnection $
                either (ConnectFailed to) (const $ Connected to) conn

            k conn >>= go

        H.SendAdHoc rpc k -> do
            -- FIXME(kim): swallow exceptions?
            protoSend env True (H.rpcRecipient rpc) (ProtocolHyParView rpc)
            k >>= go

        H.NeighborUp n k -> do
            trace $ TraceMembership (Promoted n)
            runPlumtree env $ P.neighborUp n
            k >>= go

        H.NeighborDown n k -> do
            trace $ TraceMembership (Demoted n)
            runPlumtree env $ P.neighborDown n
            k >>= go

        H.Done a -> pure a

    mkConn to = H.Connection
        { connSend  = \rpc ->
            protoSend env False (H.rpcRecipient rpc) (ProtocolHyParView rpc)
        , connClose = do
            runNetwork env $ S.disconnect to
            trace $ TraceConnection (Disconnected to)
        }

evalNetwork
    :: (Eq n, Hashable n)
    => Env n
    -> S.NetworkC n (ProtocolMessage (Peer n)) a
    -> IO a
evalNetwork env@Env { envTrace = trace } = go
  where
    go = \case
        S.PayloadReceived from rpc k -> do
            trace $ TraceWire (ProtocolRecv from rpc)
            if | isAuthorised from rpc ->
                case rpc of
                    ProtocolHyParView p -> runHyParView env $ H.receive p
                    ProtocolPlumtree  p -> runPlumtree  env $ P.receive p
               | otherwise             ->
                case rpc of
                    ProtocolPlumtree{} -> runHyParView env $ H.eject from
                    _                  -> pure ()
            k >>= go

        S.ConnectionLost to e k -> do
            trace $ TraceConnection (ConnectionLost to e)
            runHyParView env $ H.eject to
            k >>= go

        S.ConnectionAccepted from k -> do
            trace $ TraceConnection (ConnectionAccepted from)
            k >>= go

        S.Done a -> pure a

    -- XXX(kim): We cannot use the 'Eq' instance here, since a node's view of
    -- its port and the @accept@ed one differ. Not sure though if we should
    -- consider the address.
    isAuthorised from (ProtocolHyParView rpc) = case H.rpcPayload rpc of
        H.Shuffle{}     -> True
        H.ForwardJoin{} -> True
        _               -> peerNodeId (H.rpcSender rpc) == peerNodeId from
    isAuthorised from (ProtocolPlumtree rpc) =
        peerNodeId (P.rpcSender rpc) == peerNodeId from

--------------------------------------------------------------------------------

protoSend
    :: (Eq n, Hashable n)
    => Env n
    -> Bool
    -> Peer n
    -> ProtocolMessage (Peer n)
    -> IO ()
protoSend env@Env { envTrace = trace } allowAdHoc to proto = do
    trace $ TraceWire (ProtocolSend to proto)
    runNetwork env (S.send allowAdHoc to (WirePayload proto))
        `withException` (trace . TraceWire . ProtocolError to)

--------------------------------------------------------------------------------

runHyParView :: (Eq n, Hashable n) => Env n -> H.HyParView (Peer n) a -> IO a
runHyParView env@Env { envHyParView } ma =
    H.runHyParView envHyParView ma >>= evalHyParView env

runPlumtree :: (Eq n, Hashable n) => Env n -> P.Plumtree (Peer n) a -> IO a
runPlumtree env@Env { envPlumtree } ma =
    P.runPlumtree envPlumtree ma >>= evalPlumtree env

runNetwork
    :: (Eq n, Hashable n)
    => Env n
    -> S.Network n (ProtocolMessage (Peer n)) a
    -> IO a
runNetwork env@Env { envIO } ma =
    S.runNetwork envIO ma >>= evalNetwork env

runScheduler :: Env n -> PS.SchedulerT (Peer n) IO a -> IO a
runScheduler Env { envScheduler } ma =
    PS.runSchedulerT envScheduler ma
