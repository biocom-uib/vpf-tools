{-# language BlockArguments #-}
{-# language DeriveAnyClass #-}
{-# language DeriveFoldable #-}
{-# language DeriveFunctor #-}
{-# language DeriveTraversable #-}
{-# language DeriveGeneric #-}
{-# language GeneralizedNewtypeDeriving #-}
{-# language StrictData #-}
module Control.Distributed.MPI.Streaming where

import GHC.Generics (Generic)

import Control.Concurrent (yield)
import qualified Control.Monad.Catch as MC
import Control.Monad.IO.Class (MonadIO(..))

import qualified Control.Distributed.MPI.Store  as MPI

import Data.Store (Store)

import Streaming (Stream, Of)
import Streaming.Prelude qualified as S


data StreamItem a = StreamItem a | StreamEnd
  deriving (Eq, Ord, Show, Generic, Foldable, Traversable, Functor)

instance Store a => Store (StreamItem a)


waitYielding :: MPI.Request a -> IO (MPI.Status, a)
waitYielding req = loop
  where
    loop = do
        res <- MPI.test req
        case res of
          Just a  -> return a
          Nothing -> yield >> loop


waitYielding_ :: MPI.Request a -> IO a
waitYielding_ req = loop
  where
    loop = do
        res <- MPI.test_ req
        case res of
          Just a  -> return a
          Nothing -> yield >> loop


sendYielding_ :: Store a => a -> MPI.Rank -> MPI.Tag -> MPI.Comm -> IO ()
sendYielding_ a rank tag comm =
    MC.mask_ $
        MPI.isend a rank tag comm >>= waitYielding_


recvYielding :: Store b => MPI.Rank -> MPI.Tag -> MPI.Comm -> IO (MPI.Status, b)
recvYielding rank tag comm =
    MC.mask_ $
        MPI.irecv rank tag comm >>= waitYielding


recvYielding_ :: Store b => MPI.Rank -> MPI.Tag -> MPI.Comm -> IO b
recvYielding_ rank tag comm =
    MC.mask_ $
        MPI.irecv rank tag comm >>= waitYielding_


sendrecvYielding ::
    (Store a, Store b)
    => a
    -> MPI.Rank
    -> MPI.Tag
    -> MPI.Rank
    -> MPI.Tag
    -> MPI.Comm
    -> IO (MPI.Status, b)
sendrecvYielding a rank tag rank' tag' comm =
    MC.mask_ do
        () <- MPI.isend a rank tag comm >>= waitYielding_
        MPI.irecv rank' tag' comm >>= waitYielding


sendrecvYielding_ ::
    (Store a, Store b)
    => a
    -> MPI.Rank
    -> MPI.Tag
    -> MPI.Rank
    -> MPI.Tag
    -> MPI.Comm
    -> IO b
sendrecvYielding_ a rank tag rank' tag' comm =
    MC.mask_ do
        () <- MPI.isend a rank tag comm >>= waitYielding_
        MPI.irecv rank' tag' comm >>= waitYielding_


messagesFrom :: forall a m.
    (MonadIO m, Store a)
    => MPI.Rank
    -> MPI.Tag
    -> MPI.Comm
    -> Stream (Of (MPI.Status, a)) m ()
messagesFrom rank tag comm = loop
  where
    loop = do
        (!status, !msg) <- liftIO $ recvYielding rank tag comm

        case msg of
          StreamItem a -> S.yield (status, a) >> loop
          StreamEnd    -> return ()


genericSender :: forall a m r.
    ( MonadIO m
    , Store a
    )
    => MPI.Comm
    -> Stream (Of (MPI.Status, a)) m r
    -> m r
genericSender comm =
    S.mapM_ \(s, a) ->
        liftIO $ sendYielding_ a (MPI.msgRank s) (MPI.msgTag s) comm
