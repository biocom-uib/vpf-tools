{-# language DeriveGeneric #-}
{-# language GeneralizedNewtypeDeriving #-}
{-# language StrictData #-}
module VPF.Concurrency.MPI where

import GHC.Generics (Generic)
import qualified GHC.StaticPtr as SP

import qualified Control.Concurrent as Thread
import Control.Concurrent.Async (Async)
import qualified Control.Concurrent.Async       as Async
import qualified Control.Concurrent.STM.TBQueue as TBQ
import qualified Control.Concurrent.STM.TMVar   as TMVar
import qualified Control.Distributed.MPI.Store  as MPI
import qualified Control.Monad.Catch as MC
import Control.Exception (SomeException)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.STM (atomically)
import Control.Monad.Trans.Class (lift)
import qualified Control.Monad.STM as STM

import Data.Function (fix)
import Data.Monoid (Alt(..))
import Data.List (genericLength)
import Data.Store (Store)
import qualified Data.Store as Store
import qualified Data.Set as Set
import Numeric.Natural (Natural)

import Pipes (Producer, Pipe, Consumer, (>->))
import qualified Pipes         as P
import qualified Pipes.Prelude as P
import qualified Pipes.Safe    as PS


newtype JobTagIn e a = JobTagIn e
  deriving (Eq, Ord, Show, Bounded, Enum)

newtype JobTagOut e a = JobTagOut  e
  deriving (Eq, Ord, Show, Bounded, Enum)

data JobTags e e' a b = JobTags (JobTagIn e a) (JobTagOut e' a)


data StreamItem a = StreamItem a | StreamEnd
  deriving (Eq, Ord, Show, Generic)

instance Store a => Store (StreamItem a)


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


makeProcessWorker :: (PS.MonadSafe m, Enum tag, Enum tag', Store a, Store b)
                  => MPI.Rank
                  -> JobTags tag tag' a b
                  -> MPI.Comm
                  -> (a -> m b)
                  -> m ()
makeProcessWorker master (JobTags tagIn tagOut) comm f =
    P.runEffect $
        messagesFrom master tagIn comm
          >-> P.mapM f
          >-> messagesTo master tagOut comm


joinMapM :: forall m a b r. PS.MonadSafe m
         => [StreamItem a -> IO (StreamItem b)] -- ^ worker action (StreamItem) /finalizer (StreamEnd)
         -> Natural                             -- ^ job queue size
         -> Natural                             -- ^ result queue size
         -> Producer a m r                      -- ^ inputs
         -> Producer b m r                      -- ^ outputs
joinMapM workers qsizeIn qsizeOut producer = do
  done <- liftIO $ TMVar.newEmptyTMVarIO

  inputs <- liftIO $ TBQ.newTBQueueIO qsizeIn
  outputs <- liftIO $ TBQ.newTBQueueIO qsizeOut

  PS.bracket (setupWorkers inputs outputs done) cancelWorkers $ \workerThreads -> do
      r <- awaitYielding inputs outputs
      liftIO $ atomically $ TMVar.putTMVar done ()

      consumeOutputs outputs workerThreads
      return r
  where
    awaitYielding :: TBQ.TBQueue a -> TBQ.TBQueue b -> Producer b m r
    awaitYielding inputs outputs =
        P.for producer $ \a -> do
            newResults <- liftIO $ atomically $ TBQ.flushTBQueue outputs
            P.each newResults
            liftIO $ atomically $ TBQ.writeTBQueue inputs a

    consumeOutputs :: TBQ.TBQueue b -> [Async.Async ()] -> Producer b m ()
    consumeOutputs outputs workerThreads = do
      let consumeAndWait :: Async.Async () -> Producer b m ()
          consumeAndWait workerThread = fix $ \loop -> do
            status <- liftIO $ Async.poll workerThread

            case status of
              Just (Left e)   -> MC.throwM e
              Just (Right ()) -> return ()
              Nothing -> do
                bs <- liftIO $ atomically $ TBQ.flushTBQueue outputs
                P.each bs
                loop

      mapM_ consumeAndWait workerThreads

    setupWorkers :: TBQ.TBQueue a -> TBQ.TBQueue b -> TMVar.TMVar () -> PS.Base m [Async.Async ()]
    setupWorkers inputs outputs done =
        liftIO $ mapM (liftIO . Async.async . workerLoop) workers
      where
        workerLoop :: (StreamItem a -> IO (StreamItem b)) -> IO ()
        workerLoop worker = fix $ \loop -> do
          newJob <- atomically $
              STM.orElse (StreamItem <$> TBQ.readTBQueue inputs)
                         (StreamEnd  <$  TMVar.readTMVar done)

          case newJob of
            StreamEnd -> return ()
            StreamItem job -> do
              result <- worker newJob
              case result of
                StreamEnd -> return ()
                StreamItem b -> do
                  atomically $ TBQ.writeTBQueue outputs b
                  loop

    cancelWorkers :: [Async.Async ()] -> PS.Base m ()
    cancelWorkers = liftIO . mapM_ Async.cancel


mpiWorker :: (Enum tag, Enum tag', Store a, Store b)
          => MPI.Rank -> JobTags tag tag' a b -> MPI.Comm -> StreamItem a -> IO (StreamItem b)
mpiWorker rank (JobTags tagIn tagOut) comm a =
    MC.mask_ $ MPI.sendrecv_ a rank tagIn' rank tagOut' comm
  where
    tagIn', tagOut' :: MPI.Tag
    tagIn'  = MPI.toTag tagIn
    tagOut' = MPI.toTag tagOut


mpiWorkers :: (Enum tag, Enum tag', Store a, Store b)
           => [MPI.Rank] -> JobTags tag tag' a b -> MPI.Comm -> [StreamItem a -> IO (StreamItem b)]
mpiWorkers ranks tags comm = map (\rank -> mpiWorker rank tags comm) ranks


delegate :: forall m a b tag tag' r. (PS.MonadSafe m, Enum tag, Enum tag', Store a, Store b)
         => [MPI.Rank]
         -> JobTags tag tag' a b
         -> MPI.Comm
         -> Natural
         -> Producer a m r
         -> Producer b m r
delegate []    _    _    _         = error "delegate called with no workers"
delegate ranks tags comm queueSize = do
  joinMapM (mpiWorkers ranks tags comm)
           queueSize
           (genericLength ranks)
