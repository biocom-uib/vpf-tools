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
import qualified Control.Concurrent.Async.Lifted.Safe as Async

import qualified Control.Concurrent.STM.TBQueue as STM
import qualified Control.Concurrent.STM.TMVar   as STM
import qualified Control.Monad.STM              as STM

import Control.Monad ((>=>))
import qualified Control.Monad.Catch as MC
import Control.Monad.IO.Class (MonadIO(..))

import qualified Control.Distributed.MPI.Store  as MPI

import Data.Function (fix)
import Data.Functor (void)
import Data.Store (Store)
import Numeric.Natural (Natural)

import Pipes (Producer, Pipe, Consumer, (>->))
import qualified Pipes         as P
import qualified Pipes.Prelude as P
import qualified Pipes.Safe    as PS


data JobTagIn e a = Store a => JobTagIn e
data JobTagOut e a = Store a => JobTagOut e
data JobTags e e' a b = JobTags (JobTagIn e a) (JobTagOut e' b)


data StreamItem a = StreamItem a | StreamEnd
  deriving (Eq, Ord, Show, Generic, Foldable, Traversable, Functor)

data Worker a b = Worker (a -> IO (StreamItem b)) (IO ())

instance Store a => Store (StreamItem a)


mapWorker :: (b -> c) -> Worker a b -> Worker a c
mapWorker fbc (Worker fab finalizer) = Worker ((fmap.fmap.fmap) fbc fab) finalizer

mapWorkerIO :: (b -> IO c) -> Worker a b -> Worker a c
mapWorkerIO fbc (Worker fab finalizer) = Worker (fab >=> traverse fbc) finalizer


messagesFrom :: forall tag m a. (Enum tag, PS.MonadSafe m, Store a)
             => MPI.Rank                      -- ^ where to listen for values
             -> tag                           -- ^ tag
             -> MPI.Comm                      -- ^ communicator
             -> Producer a m ()               -- ^ produces values of producer tag
messagesFrom rank tag comm = fix $ \loop -> do
    msg <- liftIO $ MC.mask_ $ MPI.recv_ rank tag' comm

    case msg of
      StreamItem a -> do
        P.yield a
        loop
      StreamEnd    -> return ()
  where
    tag' :: MPI.Tag
    tag' = MPI.toTag tag


messagesTo :: forall tag m a r. (Enum tag, PS.MonadSafe m, Store a)
           => MPI.Rank
           -> tag
           -> MPI.Comm
           -> Consumer a m r
messagesTo rank tag comm = do
    P.mapM_ (\s -> liftIO $ MC.mask_ $ MPI.send (StreamItem s) rank tag' comm)
      `PS.finally` sendStreamEnd
  where
    tag' :: MPI.Tag
    tag' = MPI.toTag tag

    sendStreamEnd :: PS.Base m ()
    sendStreamEnd =
        liftIO $ MC.mask_ $ MPI.send (StreamEnd @a) rank tag' comm


makeProcessWorker :: (PS.MonadSafe m, Enum tag, Enum tag')
                  => MPI.Rank
                  -> JobTags tag tag' a b
                  -> MPI.Comm
                  -> (a -> m b)
                  -> m ()
makeProcessWorker master (JobTags (JobTagIn tagIn) (JobTagOut tagOut)) comm f =
    P.runEffect $
        messagesFrom master tagIn comm
          >-> P.mapM f
          >-> messagesTo master tagOut comm


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

recvYielding_ :: Store b => MPI.Rank -> MPI.Tag -> MPI.Comm -> IO b
recvYielding_ rank tag comm = MC.mask_ $
    MPI.irecv rank tag comm >>= waitYielding_

sendrecvYielding_ :: (Store a, Store b)
                => a -> MPI.Rank -> MPI.Tag -> MPI.Rank -> MPI.Tag -> MPI.Comm -> IO b
sendrecvYielding_ a rank tag rank' tag' comm = MC.mask_ $ do
    MPI.isend a rank tag comm >>= waitYielding_
    MPI.irecv rank' tag' comm >>= waitYielding_


mpiWorker :: forall a b tag tag'. (Enum tag, Enum tag')
          => MPI.Rank
          -> JobTags tag tag' a b
          -> MPI.Comm
          -> Worker a b
mpiWorker rank (JobTags (JobTagIn tagIn) (JobTagOut tagOut)) comm =
    Worker (\a -> sendrecvYielding_ (StreamItem a) rank tagIn' rank tagOut' comm)
           (MC.mask_ $ MPI.send (StreamEnd @a) rank tagIn' comm)
  where
    tagIn', tagOut' :: MPI.Tag
    tagIn'  = MPI.toTag tagIn
    tagOut' = MPI.toTag tagOut


mpiWorkers :: (Enum tag, Enum tag', Functor f)
           => f (MPI.Rank)
           -> JobTags tag tag' a b
           -> MPI.Comm
           -> f (Worker a b)
mpiWorkers ranks tags comm = fmap (\rank -> mpiWorker rank tags comm) ranks


workerToPipe :: forall a b m r. (PS.MonadSafe m) => Worker a b -> Pipe a b m ()
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
