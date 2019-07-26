{-# language BlockArguments #-}
module VPF.Concurrency.Pipes.Stealing where

import qualified Control.Concurrent.Async       as Async
import qualified Control.Concurrent.MVar        as MVar
import qualified Control.Concurrent.STM.TBQueue as STM
import qualified Control.Concurrent.STM.TMVar   as STM
import qualified Control.Concurrent.STM.TVar    as STM
import qualified Control.Monad.STM              as STM

import Control.Lens (alaf)
import Control.Monad ((<=<), join)
import qualified Control.Monad.Catch as MC
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Morph (hoist)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Control (MonadBaseControl, StM, liftBaseWith, restoreM)

import Data.Function (fix)
import qualified Data.List.NonEmpty as NE
import Data.Monoid (Ap(..))
import Data.Semigroup (First(..))
import Data.Semigroup.Foldable (foldMap1)
import Data.Traversable (forM)
import Numeric.Natural (Natural)

import Pipes (Producer, (>->))
import qualified Pipes            as P
import qualified Pipes.Prelude    as P
import qualified Pipes.Safe       as PS


restoreProducerWith :: (Monad m, MonadBaseControl IO n)
                    => (forall x. m x -> n x)
                    -> Producer (StM n a) m r
                    -> Producer a n r
restoreProducerWith f producer = hoist f producer >-> P.mapM restoreM


workStealing :: forall m n s a b r. (MonadBaseControl IO m, PS.MonadSafe m, MonadBaseControl IO n, PS.MonadSafe n, Semigroup s)
             => Natural
             -> Natural
             -> Natural
             -> (Producer a m r -> Producer b n s)
             -> Producer a m r
             -> IO (Producer b n s)
workStealing _ _ threads _ _ | threads <= 0 = error "workStealing: threads must be > 0"
workStealing splitQueueSize joinQueueSize threads f aproducer = do
    aproducer' <- stealingBufferedProducer splitQueueSize aproducer

    let bproducers' = NE.fromList $ replicate (fromIntegral threads)
                                              (f aproducer')

    joinProducersAsync joinQueueSize bproducers'


stealingBufferedProducer :: (MonadBaseControl IO m, PS.MonadSafe m)
                         => Natural
                         -> Producer a m r
                         -> IO (Producer a m r)
stealingBufferedProducer queueSize producer = do
    producer' <- joinProducersAsync queueSize (fmap First producer NE.:| [])
    return (fmap getFirst producer')


joinProducersAsync :: forall a m r. (MonadBaseControl IO m, PS.MonadSafe m, Semigroup r)
                   => Natural
                   -> NE.NonEmpty (Producer a m r)
                   -> IO (Producer a m r)
joinProducersAsync queueSize producers = do
    threadsVar <- MVar.newMVar []
    values <- STM.newTBQueueIO queueSize
    ndone <- STM.newTVarIO 0

    return $
      PS.mask $ \restore -> do
        threads <- lift $ startFeed threadsVar values ndone

        PS.finally (restore $ do
                      readValues values ndone
                      lift $ waitRestoreAll threads)
                   (liftIO $ mapM_ Async.cancel threads)
  where
    incDone :: MonadIO n => STM.TVar Int -> n ()
    incDone ndone = liftIO $ STM.atomically $ STM.modifyTVar' ndone (1+)

    testTVar :: STM.TVar x -> (x -> Bool) -> STM.STM ()
    testTVar var pre = STM.check . pre =<< STM.readTVar var

    nproducers :: Int
    nproducers = length producers

    startFeed :: MVar.MVar [Async.Async (StM m r)]
              -> STM.TBQueue a
              -> STM.TVar Int
              -> m (NE.NonEmpty (Async.Async (StM m r)))
    startFeed threadsVar values ndone =
        liftBaseWith $ \runInBase ->
          MVar.modifyMVar threadsVar $ \threads ->
            case threads of
              (t:ts) -> return (threads, t NE.:| ts)
              []    -> do
                  threads' <- forM producers $ \producer -> Async.async $ runInBase $
                                feed values producer
                                  `PS.finally` incDone ndone
                  return (NE.toList threads', threads')

    feed :: STM.TBQueue a -> Producer a m r -> m r
    feed values producer =
        P.runEffect $
          producer >-> P.mapM_ (liftIO . STM.atomically . STM.writeTBQueue values)

    readValues :: STM.TBQueue a -> STM.TVar Int -> Producer a m ()
    readValues values ndone = fix $ \loop -> do
      ma <- liftIO $ STM.atomically $
                STM.orElse (Just    <$> STM.readTBQueue values)
                           (Nothing <$  testTVar ndone (>= nproducers))
      case ma of
        Nothing -> return ()
        Just a  -> P.yield a >> loop

    waitRestoreAll :: NE.NonEmpty (Async.Async (StM m r)) -> m r
    waitRestoreAll =
        alaf Ap foldMap1 (restoreM <=< liftIO . Async.wait)
