{-# LANGUAGE CPP               #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RankNTypes        #-}

-- |
-- Copyright   : 2018 Monadic GmbH
-- License     : BSD3
-- Maintainer  : kim@monadic.xyz, team@monadic.xyz
-- Stability   : experimental
-- Portability : non-portable (GHC extensions)
--
module Network.Gossip.IO.Socket
    ( Connection (..)

    , HandshakeRole (..)
    , Handshake

    , Env
    , new

    , NetworkC (..)
    , Network
    , runNetwork

    , listen
    , send
    , connect
    , disconnect
    )
where

import           Network.Gossip.IO.Peer (Peer(..))
import           Network.Gossip.IO.Wire

import           Control.Concurrent (forkFinally)
import           Control.Concurrent.STM (STM, atomically)
import           Control.Exception.Safe
import           Control.Monad.Reader
import           Control.Monad.Trans.Cont
import           Data.Coerce (coerce)
import           Data.Conduit (ConduitT, runConduit, (.|))
import qualified Data.Conduit.Combinators as Conduit
import           Data.Foldable (for_)
import           Data.Hashable (Hashable)
import           Data.Text (Text)
import           Data.Traversable (for)
import           Data.Void
import qualified Focus
#if !MIN_VERSION_network(3,0,0)
import           GHC.Stack (HasCallStack)
#endif
import           Network.Socket (SockAddr, Socket, SocketType(Stream))
import qualified Network.Socket as Network
import qualified StmContainers.Map as STMMap


data Error
    = GoawayReceived (Maybe Text)
    | NoSuchPeer     SockAddr     -- FIXME(kim): should be 'Peer n', but that imposes 'Show n, Typeable n'
    deriving Show

instance Exception Error

data Connection n p = Connection
    { connPeer  :: Peer n
    , connSend  :: p -> IO ()
    , connRecv  :: ConduitT () p IO ()
    , connClose :: IO ()
    }

type OpenConnections n p = STMMap.Map n (Connection n p)

data HandshakeRole = Acceptor | Connector
type Handshake n p =
       HandshakeRole
    -> Socket
    -> SockAddr
    -> Maybe n
    -> IO (Connection n (WireMessage p))

data Env n p = Env
    { envConns     :: OpenConnections n (WireMessage p)
    , envHandshake :: Handshake n p
    }

new :: Handshake n p -> IO (Env n p)
new envHandshake = do
    envConns <- STMMap.newIO
    pure Env {..}

-- Continuations ---------------------------------------------------------------

data NetworkC n p a
    = PayloadReceived    (Peer n) p             (IO (NetworkC n p a))
    | ConnectionLost     (Peer n) SomeException (IO (NetworkC n p a))
    | ConnectionAccepted SockAddr               (IO (NetworkC n p a))
    | Done a

payloadReceived :: Peer n -> p -> Network n p ()
payloadReceived from payload =
    Network $ ReaderT $ \_ -> ContT $ \k ->
        pure $ PayloadReceived from payload (k ())

connectionLost :: Peer n -> SomeException -> Network n p ()
connectionLost to e =
    Network $ ReaderT $ \_ -> ContT $ \k ->
        pure $ ConnectionLost to e (k ())

connectionAccepted :: SockAddr -> Network n p ()
connectionAccepted from =
    Network $ ReaderT $ \_ -> ContT $ \k ->
        pure $ ConnectionAccepted from (k ())

-- Monad -----------------------------------------------------------------------

newtype Network n p a = Network
    { fromNetwork ::
        forall x. ReaderT (Env n p) (ContT (NetworkC n p x) IO) a
    } deriving Functor

instance Applicative (Network n p) where
    pure x = Network $ pure x
    (<*>)  = ap

instance Monad (Network n p) where
    return          = pure
    Network m >>= f = Network $ m >>= fromNetwork . f
    {-# INLINE (>>=) #-}

instance MonadIO (Network n p) where
    liftIO io = Network $ liftIO io
    {-# INLINE liftIO #-}

instance MonadReader (Env n p) (Network n p) where
    ask       = Network $ ReaderT pure
    local f m = Network $ local f (fromNetwork m)

    {-# INLINE ask   #-}
    {-# INLINE local #-}

runNetwork :: Env n p -> Network n p a -> IO (NetworkC n p a)
runNetwork r (Network ma) = runContT (runReaderT ma r) (pure . Done)

-- API -------------------------------------------------------------------------

listen
    :: (Eq n, Hashable n)
    => (NetworkC n p () -> IO ())
    -> SockAddr
    -> Network n p Void
listen eval addr = do
    hdl  <- ask
    liftIO $ bracket open Network.close (accept hdl)
  where
    open = do
        sock <- Network.socket (family addr) Stream Network.defaultProtocol
        Network.setSocketOption sock Network.ReuseAddr 1
        Network.bind sock (coerce addr)
#if MIN_VERSION_network(3,1,0)
        Network.withFdSocket sock Network.setCloseOnExecIfNeeded
#elif MIN_VERSION_network(3,0,0)
        Network.setCloseOnExecIfNeeded =<< Network.fdSocket sock
#elif MIN_VERSION_network(2,7,0)
        Network.setCloseOnExecIfNeeded $ Network.fdSocket sock
#endif
        Network.listen sock 10
        pure $! sock

    accept hdl@Env { envHandshake } sock = forever $ do
        (sock', addr') <- Network.accept sock
        forkUltimately_ (Network.close sock') $ do
            runNetwork hdl (connectionAccepted addr') >>= eval
            conn <- envHandshake Acceptor sock' addr' Nothing
            recvAll hdl eval conn

send :: (Eq n, Hashable n) => Bool -> Peer n -> WireMessage p -> Network n p ()
send allowAdHoc Peer { peerNodeId, peerAddr } msg = do
    conns <- asks envConns
    conn  <- liftIO . atomically $ connsGet conns peerNodeId
    case conn of
        Just  c -> liftIO $ connSend c msg
        Nothing | allowAdHoc -> do
            hands <- asks envHandshake
            liftIO . withSocket peerAddr $ \sock -> do
                Network.connect sock (coerce peerAddr)
                c <- hands Connector sock peerAddr (Just peerNodeId)
                connSend c msg `finally` connClose c

                | otherwise -> liftIO . throwIO $ NoSuchPeer peerAddr

connect
    :: (Eq n, Hashable n)
    => (NetworkC n p () -> IO ())
    -> SockAddr
    -> Maybe n
    -> Network n p (Peer n)
connect eval addr mnid = do
    hdl@Env { envConns, envHandshake } <- ask
    liftIO $ do
        known <- fmap join . for mnid $ atomically . connsGet envConns
        case known of
            Just conn -> pure $ connPeer conn
            Nothing   -> do
                sock <- Network.socket (family addr) Stream Network.defaultProtocol
                Network.connect sock (coerce addr)
                conn <- envHandshake Connector sock addr mnid
                forkUltimately_ (connClose conn) $ recvAll hdl eval conn
                pure $ connPeer conn

disconnect :: (Eq n, Hashable n) => Peer n -> Network n p ()
disconnect Peer { peerNodeId } = do
    conns <- asks envConns
    liftIO $ do
        conn <- atomically $ connsDel conns peerNodeId
        for_ conn connClose

--------------------------------------------------------------------------------

recvAll
    :: (Eq n, Hashable n)
    => Env     n p
    -> (NetworkC  n p () -> IO ())
    -> Connection n (WireMessage p)
    -> IO ()
recvAll hdl@Env { envConns } eval conn = do
    ok <- atomically $ connsAdd envConns conn
    if ok then
        withException (runConduit recv) $ \e -> do
            atomically $ connsDel_ envConns conn
            run $ connectionLost (connPeer conn) e
    else
        connSend conn (WireGoaway (Just "Duplicate Node Id"))
            `finally` connClose conn
  where
    recv = connRecv conn .| Conduit.mapM_ dispatch

    dispatch (WirePayload p) = run $ payloadReceived (connPeer conn) p
    dispatch (WireGoaway  e) = throwM $ GoawayReceived e

    run ma = runNetwork hdl ma >>= eval

--------------------------------------------------------------------------------

connsAdd
    :: (Eq n, Hashable n)
    => OpenConnections n p
    -> Connection n p
    -> STM Bool
connsAdd conns conn = do
    have <- STMMap.lookup nid conns
    case have of
        Nothing -> True <$ STMMap.insert conn nid conns
        Just  _ -> pure False
  where
    nid = peerNodeId $ connPeer conn

connsDel_
    :: (Eq n, Hashable n)
    => OpenConnections n p
    -> Connection n p
    -> STM ()
connsDel_ conns conn = STMMap.delete (peerNodeId (connPeer conn)) conns

connsDel
    :: forall n p. (Eq n, Hashable n)
    => OpenConnections n p
    -> n
    -> STM (Maybe (Connection n p))
connsDel conns n = STMMap.focus expunge n conns
  where
    expunge :: Focus.Focus (Connection n p) STM (Maybe (Connection n p))
    expunge = Focus.lookup <* Focus.delete

connsGet
    :: (Eq n, Hashable n)
    => OpenConnections n p
    -> n
    -> STM (Maybe (Connection n p))
connsGet conns n = STMMap.lookup n conns

--------------------------------------------------------------------------------

forkUltimately_ :: IO () -> IO a -> IO ()
forkUltimately_ fin work = void $ forkFinally work (const fin)

--------------------------------------------------------------------------------

family :: SockAddr -> Network.Family
family Network.SockAddrInet{}  = Network.AF_INET
family Network.SockAddrInet6{} = Network.AF_INET6
family Network.SockAddrUnix{}  = Network.AF_UNIX
#if !MIN_VERSION_network(3,0,0)
--family (SockAddr Network.SockAddrCan{} ) = Sock.AF_CAN
family _                       = canNotSupported
#endif

withSocket :: SockAddr -> (Socket -> IO a) -> IO a
withSocket addr =
    bracket (Network.socket (family addr) Stream Network.defaultProtocol)
            Network.close

#if !MIN_VERSION_network(3,0,0)
canNotSupported :: HasCallStack => a
canNotSupported = error "CAN addresses not supported"
#endif
