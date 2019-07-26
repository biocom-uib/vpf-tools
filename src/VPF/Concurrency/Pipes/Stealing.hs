{-# language BlockArguments #-}
module VPF.Concurrency.Pipes.Stealing where

import qualified Control.Concurrent.Async       as Async
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



workStealingWith :: forall m n s a b r. (MonadBaseControl IO m, PS.MonadSafe m, MonadBaseControl IO n, Semigroup s)
                 => Natural
                 -> Natural
                 -> Natural
                 -> (forall x. m x -> n x)
                 -> (Producer a m r -> Producer b n s)
                 -> Producer a m r
                 -> n (Producer b n s)
workStealingWith _ _ threads _ _ _ | threads <= 0 = error "workStealing: threads must be > 0"
workStealingWith splitQueueSize joinQueueSize threads liftM f aproducer =
    (>>= restoreM @IO @n) $
      liftM $ liftBaseWith $ \runN ->
        runN $ liftM $ liftBaseWith $ \runM -> do
            stm_aproducer' <- runM $ stealingBufferedProducer splitQueueSize aproducer
            let aproducer' = join $ lift $ restoreM @IO @m stm_aproducer'

            let bproducers' = NE.fromList $ replicate (fromIntegral threads)
                                                      (f aproducer')

            runN $ joinProducersAsync joinQueueSize bproducers'


stealingBufferedProducer :: (MonadBaseControl IO m, PS.MonadSafe m)
                         => Natural
                         -> Producer a m r
                         -> m (Producer a m r)
stealingBufferedProducer queueSize producer = do
    producer' <- joinProducersAsync queueSize (fmap First producer NE.:| [])
    return (fmap getFirst producer')


joinProducersAsync :: forall a m r. (MonadBaseControl IO m, PS.MonadSafe m, Semigroup r)
                   => Natural
                   -> NE.NonEmpty (Producer a m r)
                   -> m (Producer a m r)
joinProducersAsync queueSize producers = do
    begin <- liftIO $ STM.newEmptyTMVarIO
    ndone <- liftIO $ STM.newTVarIO 0
    values <- liftIO $ STM.newTBQueueIO queueSize

    MC.mask_ $ do
      threads <- liftBaseWith $ \runInBase ->
                   forM producers $ \producer -> Async.async $ runInBase $
                       feed begin values producer
                         `PS.finally` incDone ndone
      return $ do
          signal begin

          PS.onException do readValues values ndone
                            lift $ waitRestoreAll threads
                         do liftIO $ mapM_ Async.cancel threads
  where
    signal :: MonadIO n => STM.TMVar () -> n ()
    signal var = liftIO $ STM.atomically $ STM.putTMVar var ()

    incDone :: MonadIO n => STM.TVar Int -> n ()
    incDone ndone = liftIO $ STM.atomically $ STM.modifyTVar' ndone (1+)

    testTVar :: STM.TVar x -> (x -> Bool) -> STM.STM ()
    testTVar var pre = STM.check . pre =<< STM.readTVar var

    nproducers :: Int
    nproducers = length producers

    feed :: STM.TMVar () -> STM.TBQueue a -> Producer a m r -> m r
    feed begin values producer =  do
      () <- liftIO $ STM.atomically $ STM.readTMVar begin
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
