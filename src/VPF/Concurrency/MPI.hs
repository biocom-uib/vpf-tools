{-# language BlockArguments #-}
{-# language DeriveAnyClass #-}
{-# language DeriveFoldable #-}
{-# language DeriveFunctor #-}
{-# language DeriveTraversable #-}
{-# language DeriveGeneric #-}
{-# language GeneralizedNewtypeDeriving #-}
{-# language StrictData #-}
module VPF.Concurrency.MPI where

import GHC.Generics (Generic)

import Control.Concurrent (yield)
import Control.Monad ((>=>))
import qualified Control.Monad.Catch as MC
import Control.Monad.IO.Class (MonadIO(..))

import qualified Control.Distributed.MPI.Store  as MPI

import Data.Function (fix)
import Data.Profunctor
import Data.Store (Store)

import Pipes (Producer, Pipe, Consumer, (>->))
import qualified Pipes         as P
import qualified Pipes.Prelude as P
import qualified Pipes.Safe    as PS


data JobTagIn a = Store a => JobTagIn MPI.Tag
data JobTagOut a = Store a => JobTagOut MPI.Tag
data JobTags a b = JobTags (JobTagIn a) (JobTagOut b)


data StreamItem a = StreamItem a | StreamEnd
  deriving (Eq, Ord, Show, Generic, Foldable, Traversable, Functor)

data Worker a b = Worker (a -> IO (StreamItem b)) (IO ())

instance Store a => Store (StreamItem a)


instance Profunctor Worker where
    dimap fxa fbc (Worker fab finalizer) = Worker (fmap (fmap fbc) . fab . fxa) finalizer


mapWorker :: (b -> c) -> Worker a b -> Worker a c
mapWorker fbc (Worker fab finalizer) = Worker ((fmap.fmap.fmap) fbc fab) finalizer

mapWorkerIO :: (b -> IO c) -> Worker a b -> Worker a c
mapWorkerIO fbc (Worker fab finalizer) = Worker (fab >=> traverse fbc) finalizer


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
sendYielding_ a rank tag comm = MC.mask_ $
    MPI.isend a rank tag comm >>= waitYielding_


recvYielding :: Store b => MPI.Rank -> MPI.Tag -> MPI.Comm -> IO (MPI.Status, b)
recvYielding rank tag comm = MC.mask_ $
    MPI.irecv rank tag comm >>= waitYielding


recvYielding_ :: Store b => MPI.Rank -> MPI.Tag -> MPI.Comm -> IO b
recvYielding_ rank tag comm = MC.mask_ $
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
sendrecvYielding a rank tag rank' tag' comm = MC.mask_ $ do
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
sendrecvYielding_ a rank tag rank' tag' comm = MC.mask_ $ do
    () <- MPI.isend a rank tag comm >>= waitYielding_
    MPI.irecv rank' tag' comm >>= waitYielding_


messagesFrom :: forall a m.
    (MonadIO m, Store a)
    => MPI.Rank
    -> MPI.Tag
    -> MPI.Comm
    -> Producer (MPI.Status, a) m ()
messagesFrom rank tag comm = fix $ \loop -> do
    (status, msg) <- liftIO $ recvYielding rank tag comm

    case msg of
      StreamItem a -> P.yield (status, a) >> loop
      StreamEnd    -> return ()


messagesFrom_ ::
    (MonadIO m, Store a)
    => MPI.Rank
    -> MPI.Tag
    -> MPI.Comm
    -> Producer a m ()
messagesFrom_ rank tag comm = fix $ \loop -> do
    msg <- liftIO $ recvYielding_ rank tag comm

    case msg of
      StreamItem a -> P.yield a >> loop
      StreamEnd    -> return ()


genericSender :: forall a m r. MonadIO m => Store a => MPI.Comm -> Consumer (MPI.Status, a) m r
genericSender comm = do
    P.mapM_ (\(s, a) -> liftIO $ sendYielding_ a (MPI.msgRank s) (MPI.msgTag s) comm)


messagesTo :: forall m a r. (PS.MonadSafe m, Store a)
           => MPI.Rank
           -> MPI.Tag
           -> MPI.Comm
           -> Consumer a m r
messagesTo rank tag comm = do
    P.mapM_ (\a -> liftIO $ sendYielding_ (StreamItem a) rank tag comm)
      `PS.finally` sendStreamEnd
  where
    sendStreamEnd :: PS.Base m ()
    sendStreamEnd =
        liftIO $ sendYielding_ (StreamEnd @a) rank tag comm


makeProcessWorker ::
    (PS.MonadSafe m)
    => MPI.Rank
    -> JobTags a b
    -> MPI.Comm
    -> (MPI.Status -> a -> m b)
    -> m ()
makeProcessWorker master (JobTags (JobTagIn tagIn) (JobTagOut tagOut)) comm f =
    P.runEffect $
        messagesFrom master tagIn comm
          >-> P.mapM (uncurry f)
          >-> messagesTo master tagOut comm


mpiWorker :: forall a b.  MPI.Rank -> JobTags a b -> MPI.Comm -> Worker a b
mpiWorker rank (JobTags (JobTagIn tagIn) (JobTagOut tagOut)) comm =
    Worker (\a -> fmap snd $ sendrecvYielding (StreamItem a) rank tagIn rank tagOut comm)
           (MC.mask_ $ MPI.send (StreamEnd @a) rank tagIn comm)


mpiWorkers :: Functor f => f (MPI.Rank) -> JobTags a b -> MPI.Comm -> f (Worker a b)
mpiWorkers ranks tags comm = fmap (\rank -> mpiWorker rank tags comm) ranks


workerToPipe :: forall a b m. PS.MonadSafe m => Worker a b -> Pipe a b m ()
workerToPipe (Worker worker finalizer) =
    transform `PS.finally` liftIO finalizer
  where
    transform :: Pipe a b m ()
    transform = P.mapM (liftIO . worker) >-> takeWhileItems

    takeWhileItems :: Pipe (StreamItem b) b m ()
    takeWhileItems = do
      item <- P.await
      case item of
        StreamItem b -> P.yield b >> takeWhileItems
        StreamEnd    -> return ()
