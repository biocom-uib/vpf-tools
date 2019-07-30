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

import qualified Control.Concurrent.Async.Lifted.Safe as Async

import qualified Control.Concurrent.STM.TBQueue as STM
import qualified Control.Concurrent.STM.TMVar   as STM
import qualified Control.Monad.STM              as STM

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


newtype JobTagIn e a = JobTagIn e
  deriving (Eq, Ord, Show, Bounded, Enum)

newtype JobTagOut e a = JobTagOut  e
  deriving (Eq, Ord, Show, Bounded, Enum)

data JobTags e e' a b = JobTags (JobTagIn e a) (JobTagOut e' a)


data StreamItem a = StreamItem a | StreamEnd
  deriving (Eq, Ord, Show, Generic, Foldable, Traversable, Functor)

data Worker a b = Worker (a -> IO (StreamItem b)) (IO ())

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


-- joinMapM :: forall m a b r. (PS.MonadSafe m, Show a)
--          => [Worker a b]                        -- ^ worker action (StreamItem) /finalizer (StreamEnd)
--          -> Natural                             -- ^ job queue size
--          -> Natural                             -- ^ result queue size
--          -> Producer a m r                      -- ^ inputs
--          -> Producer b m r                      -- ^ outputs
-- joinMapM workers qsizeIn qsizeOut producer = do
--   done <- liftIO $ STM.newEmptyTMVarIO
--
--   inputs <- liftIO $ STM.newTBQueueIO qsizeIn
--   outputs <- liftIO $ STM.newTBQueueIO qsizeOut
--
--   PS.bracket (setupWorkers inputs outputs done) cancelWorkers $ \workerThreads -> do
--       r <- awaitYielding inputs outputs
--       liftIO $ STM.atomically $ STM.putTMVar done ()
--
--       consumeOutputs outputs workerThreads
--       return r
--   where
--     awaitYielding :: STM.TBQueue a -> STM.TBQueue b -> Producer b m r
--     awaitYielding inputs outputs =
--         P.for producer $ \a -> do
--             newResults <- liftIO $ STM.atomically $ STM.flushTBQueue outputs
--             P.each newResults
--             liftIO $ STM.atomically $ STM.writeTBQueue inputs a
--
--     consumeOutputs :: STM.TBQueue b -> [Async.Async ()] -> Producer b m ()
--     consumeOutputs outputs workerThreads = do
--       let consumeAndWait :: Async.Async () -> Producer b m ()
--           consumeAndWait workerThread = fix $ \loop -> do
--             status <- liftIO $ Async.poll workerThread
--
--             case status of
--               Just (Left e)   -> MC.throwM e
--               Just (Right ()) -> return ()
--               Nothing -> do
--                 bs <- liftIO $ STM.atomically $ STM.flushTBQueue outputs
--                 P.each bs
--                 loop
--
--       mapM_ consumeAndWait workerThreads
--
--     setupWorkers :: STM.TBQueue a -> STM.TBQueue b -> STM.TMVar () -> PS.Base m [Async.Async ()]
--     setupWorkers inputs outputs done = do
--         liftIO $ mapM (liftIO . Async.async . workerLoop) workers
--       where
--         workerLoop :: Worker a b -> IO ()
--         workerLoop (Worker worker finalizer) = flip MC.finally finalizer $ fix $ \loop -> do
--           newJob <- STM.atomically $
--               STM.orElse (StreamItem <$> STM.readTBQueue inputs)
--                          (StreamEnd  <$  STM.readTMVar done)
--
--           case newJob of
--             StreamEnd -> return ()
--             StreamItem job -> do
--               result <- worker job
--               case result of
--                 StreamEnd -> return ()
--                 StreamItem b -> do
--                   STM.atomically $ STM.writeTBQueue outputs b
--                   loop
--
--     cancelWorkers :: [Async.Async ()] -> PS.Base m ()
--     cancelWorkers = liftIO . mapM_ Async.cancel


mpiWorker :: forall a b tag tag'. (Enum tag, Enum tag', Store a, Store b)
          => MPI.Rank -> JobTags tag tag' a b -> MPI.Comm -> Worker a b
mpiWorker rank (JobTags tagIn tagOut) comm =
    Worker (\a -> MC.mask_ $ MPI.sendrecv_ (StreamItem a) rank tagIn' rank tagOut' comm)
           (MC.mask_ $ MPI.send (StreamEnd @a) rank tagIn' comm)
  where
    tagIn', tagOut' :: MPI.Tag
    tagIn'  = MPI.toTag tagIn
    tagOut' = MPI.toTag tagOut


mpiWorkers :: (Enum tag, Enum tag', Functor f, Store a, Store b)
           => f (MPI.Rank) -> JobTags tag tag' a b -> MPI.Comm -> f (Worker a b)
mpiWorkers ranks tags comm = fmap (\rank -> mpiWorker rank tags comm) ranks


workerToPipeMaybe :: forall a b m r. (PS.MonadSafe m)
                  => Worker a b
                  -> Pipe a b m (Maybe r)
workerToPipeMaybe (Worker worker finalizer) =
    transform `PS.finally` liftIO finalizer
  where
    transform :: Pipe a b m (Maybe r)
    transform = P.mapM (liftIO . worker) >-> takeWhileItems

    takeWhileItems :: Pipe (StreamItem b) b m (Maybe r)
    takeWhileItems = do
      item <- P.await
      case item of
        StreamItem b -> P.yield b >> takeWhileItems
        StreamEnd    -> return Nothing

workerToPipe_ :: forall a b m r. (PS.MonadSafe m)
              => Worker a b
              -> Pipe a b m ()
workerToPipe_ = void . workerToPipeMaybe
