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
    , NetworkC (..)
    , Network
    , runNetwork
    , new
    , listen
    , send
    , connect
    , disconnect
    )
where

import           Network.Gossip.IO.Peer
import           Network.Gossip.IO.Wire

import           Control.Concurrent (forkFinally)
import           Control.Concurrent.STM (STM, atomically)
import           Control.Exception.Safe
import           Control.Monad.Reader
import           Control.Monad.Trans.Cont
import           Data.Conduit (ConduitT, runConduit, (.|))
import qualified Data.Conduit.Combinators as Conduit
import           Data.Foldable (for_)
import           Data.Hashable (Hashable)
import           Data.Maybe (isJust)
import           Data.Text (Text)
import           Data.Void
import qualified Focus
import           GHC.Stack (HasCallStack)
import           Network.Socket
                 ( AddrInfo(..)
                 , AddrInfoFlag(..)
                 , HostName
                 , PortNumber
                 , SockAddr
                 , Socket
                 , SocketType(Stream)
                 )
import qualified Network.Socket as Sock
import qualified STMContainers.Map as STMMap

newtype Error = GoawayReceived (Maybe Text) deriving Show
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

data NetworkC n p a =
      PayloadReceived (Peer n) p (IO (NetworkC n p a))
    | ConnectionLost  (Peer n)   (IO (NetworkC n p a))
    | Done            a

payloadReceived :: Peer n -> p -> Network n p ()
payloadReceived from payload =
    Network $ ReaderT $ \_ -> ContT $ \k ->
        pure $ PayloadReceived from payload (k ())

connectionLost :: Peer n -> Network n p ()
connectionLost to =
    Network $ ReaderT $ \_ -> ContT $ \k ->
        pure $ ConnectionLost to (k ())

-- Monad -----------------------------------------------------------------------

newtype Network n p a = Network
    { fromNetwork ::
        forall x. ReaderT (Env n p) (ContT (NetworkC n p x) IO) a
    } deriving Functor

instance Applicative (Network n p) where
    pure x = Network $ pure x
    (<*>)  = ap

instance Monad (Network n p) where
    return            = pure
    Network m >>= f = Network $ m >>= fromNetwork . f
    {-# INLINE (>>=) #-}

instance MonadIO (Network n p) where
    liftIO io = Network $ liftIO io

instance MonadReader (Env n p) (Network n p) where
    ask       = Network $ ReaderT pure
    local f m = Network $ local f (fromNetwork m)

runNetwork :: Env n p -> Network n p a -> IO (NetworkC n p a)
runNetwork r (Network ma) = runContT (runReaderT ma r) (pure . Done)

-- API -------------------------------------------------------------------------

listen
    :: (Eq n, Hashable n)
    => (NetworkC n p () -> IO ())
    -> HostName
    -> PortNumber
    -> Network n p Void
listen eval host port = do
    hdl  <- ask
    liftIO $ do
        addr <- resolve host port
        bracket (open addr) Sock.close (accept hdl)
  where
    resolve h p = do
        let hints = Sock.defaultHints
                        { addrFlags      = [AI_PASSIVE, AI_NUMERICSERV]
                        , addrSocketType = Stream
                        }
        addr:_ <- Sock.getAddrInfo (Just hints) (Just h) (Just (show p))
        pure addr

    open addr = do
        sock <- Sock.socket (Sock.addrFamily addr)
                            (Sock.addrSocketType addr)
                            (Sock.addrProtocol addr)
        Sock.setSocketOption sock Sock.ReuseAddr 1
        Sock.bind sock (Sock.addrAddress addr)
#if MIN_VERSION_network(2,7,0)
        Sock.setCloseOnExecIfNeeded $ Sock.fdSocket sock
#endif
        Sock.listen sock 10
        pure $! sock

    accept hdl@Env { envHandshake } sock = forever $ do
        (sock', addr) <- Sock.accept sock
        forkUltimately_ (Sock.close sock') $ do
            conn <- envHandshake Acceptor sock' addr Nothing
            recvAll hdl eval conn

send :: (Eq n, Hashable n) => Peer n -> WireMessage p -> Network n p ()
send Peer { peerNodeId, peerAddr } msg = do
    conns <- asks envConns
    conn  <- liftIO . atomically $ connsGet conns peerNodeId
    case conn of
        Just  c -> liftIO $ connSend c msg
        Nothing -> do
            hands <- asks envHandshake
            liftIO . withSocket peerAddr $ \sock -> do
                Sock.connect sock peerAddr
                c <- hands Connector sock peerAddr (Just peerNodeId)
                connSend c msg `finally` connClose c

connect
    :: (Eq n, Hashable n)
    => (NetworkC n p () -> IO ())
    -> Peer n
    -> Network n p ()
connect eval Peer { peerNodeId, peerAddr } = do
    hdl@Env { envConns, envHandshake } <- ask
    known <- liftIO . atomically $ connsHas envConns peerNodeId
    unless known $
        liftIO . withSocket peerAddr $ \sock -> do
            Sock.connect sock peerAddr
            conn <- envHandshake Connector sock peerAddr (Just peerNodeId)
            forkUltimately_ (connClose conn) $ recvAll hdl eval conn

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
        onException (runConduit recv) $ do
            atomically $ connsDel_ envConns conn
            run $ connectionLost (connPeer conn)
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
    :: (Eq n, Hashable n)
    => OpenConnections n p
    -> n
    -> STM (Maybe (Connection n p))
connsDel conns n = STMMap.focus (pure . (,Focus.Remove)) n conns

connsGet
    :: (Eq n, Hashable n)
    => OpenConnections n p
    -> n
    -> STM (Maybe (Connection n p))
connsGet conns n = STMMap.lookup n conns

connsHas
    :: (Eq n, Hashable n)
    => OpenConnections n p
    -> n
    -> STM Bool
connsHas conns n = STMMap.focus (pure . (,Focus.Keep) . isJust) n conns

--------------------------------------------------------------------------------

forkUltimately_ :: IO () -> IO a -> IO ()
forkUltimately_ fin work = void $ forkFinally work (const fin)

--------------------------------------------------------------------------------

family :: SockAddr -> Sock.Family
family Sock.SockAddrInet{}  = Sock.AF_INET
family Sock.SockAddrInet6{} = Sock.AF_INET6
family Sock.SockAddrUnix{}  = Sock.AF_UNIX
--family Sock.SockAddrCan{}   = Sock.AF_CAN
family _                    = canNotSupported

withSocket :: SockAddr -> (Socket -> IO a) -> IO a
withSocket addr =
    bracket (Sock.socket (family addr) Stream Sock.defaultProtocol) Sock.close

canNotSupported :: HasCallStack => a
canNotSupported = error "CAN addresses not supported"
