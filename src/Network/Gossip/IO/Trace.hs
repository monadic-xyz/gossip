{-# LANGUAGE GADTs #-}

-- |
-- Copyright   : 2019 Monadic GmbH
-- License     : BSD3
-- Maintainer  : kim@monadic.xyz, team@monadic.xyz
-- Stability   : experimental
-- Portability : non-portable (GHC extensions)
--
module Network.Gossip.IO.Trace
    ( Traceable(..)
    , BootstrapEvent(..)
    , ConnectionEvent(..)
    , MembershipEvent(..)
    , WireEvent(..)
    )
where

import           Network.Gossip.IO.Peer (Peer)
import           Network.Gossip.IO.Protocol (ProtocolMessage(..))

import           Control.Exception (SomeException)
import           Network.Socket (SockAddr)


data Traceable n
    = TraceBootstrap  (BootstrapEvent  n)
    | TraceConnection (ConnectionEvent n)
    | TraceMembership (MembershipEvent n)
    | TraceWire       (WireEvent       n)

data BootstrapEvent n where
    Bootstrapping :: Traversable t
                  => Peer n
                  -> t (Maybe n, SockAddr)
                  -> BootstrapEvent n
    Bootstrapped  :: Peer n -> BootstrapEvent n

data ConnectionEvent n
    = Connecting         SockAddr (Maybe n)
    -- ^ Connecting to peer at 'SockAddr', optionally identified by 'n'
    | Connected          (Peer n)
    -- ^ Outgoing connection to 'Peer' established
    | ConnectFailed      SockAddr (Maybe n) SomeException
    -- ^ Outgoing connection attempt to peer failed with 'SomeException'
    | ConnectionLost     (Peer n) SomeException
    -- ^ Connection reset by 'Peer'
    | ConnectionAccepted SockAddr
    -- ^ Incoming connection from 'SockAddr'
    | Disconnected       (Peer n)
    -- ^ Connection to 'Peer' reset by us

data MembershipEvent n
    = Promoted (Peer n)
    | Demoted  (Peer n)

data WireEvent n
    = ProtocolRecv  ~(Peer n) ~(ProtocolMessage (Peer n))
    | ProtocolSend  ~(Peer n) ~(ProtocolMessage (Peer n))
    | ProtocolError (Peer n)  SomeException
