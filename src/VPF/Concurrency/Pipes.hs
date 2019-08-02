{-# language QuantifiedConstraints #-}
{-# language BlockArguments #-}
module VPF.Concurrency.Pipes where

import qualified Control.Concurrent.Async.Lifted as Async

import qualified Control.Concurrent.MVar        as MVar
import qualified Control.Concurrent.STM.TBQueue as STM
import qualified Control.Concurrent.STM.TMVar   as STM
import qualified Control.Concurrent.STM.TVar    as STM
import qualified Control.Monad.STM              as STM

import Control.Lens (alaf, (%~))
import Control.Monad ((<=<))
import qualified Control.Monad.Catch as MC
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Morph (hoist)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Base (MonadBase, liftBase)
import Control.Monad.Trans.Control (MonadBaseControl, StM, liftBaseWith, restoreM)

import Data.Function (fix)
import qualified Data.List.NonEmpty as NE
import Data.Monoid (Ap(..))
import Data.Semigroup (First(..))
import Data.Semigroup.Foldable (foldMap1)
import Data.Semigroup.Traversable (Traversable1)
import Data.Traversable (forM)
import Numeric.Natural (Natural)

import Pipes (Producer, Pipe, (>->))
import qualified Pipes            as P
import qualified Pipes.Prelude    as P
import qualified Pipes.Group      as PG
import qualified Pipes.Safe       as PS

import VPF.Concurrency.Async (MonadAsync, AsyncProducer(..), premapProducer)


bufferedChunks :: Monad m => Int -> Producer a m r -> Producer [a] m r
bufferedChunks chunkSize =
    PG.chunksOf chunkSize . PG.individually %~ \chunk -> do
      (items, f) <- P.lift $ P.toListM' chunk
      P.yield items
      return f


restoreProducerWith :: (Monad m, MonadBaseControl IO n)
                    => (forall x. m x -> n x)
                    -> Producer (StM n a) m r
                    -> Producer a n r
restoreProducerWith f producer = hoist f producer >-> P.mapM restoreM


restoreProducer :: MonadBaseControl b m => Pipe (StM m a) a m r
restoreProducer = P.mapM restoreM


-- convert a Producer to an asynchronous producer via work stealing
-- an AsyncProducer is less flexible than a producer, but the API is safer
stealingAsyncProducer :: (MonadAsync m, PS.MonadSafe m)
                      => Natural
                      -> Producer a m r
                      -> IO (AsyncProducer a m r m r)
stealingAsyncProducer queueSize producer = do
    producer' <- stealingBufferedProducer queueSize producer

    return $ AsyncProducer (\c -> P.runEffect (producer' >-> c))

stealingAsyncProducer_ :: (MonadAsync m, PS.MonadSafe m, Monoid r)
                       => Natural
                       -> Producer a m r
                       -> IO (AsyncProducer a m () m r)
stealingAsyncProducer_ queueSize = fmap (premapProducer (mempty <$)) . stealingAsyncProducer queueSize


stealingBufferedProducer :: (MonadAsync m, PS.MonadSafe m)
                         => Natural
                         -> Producer a m r
                         -> IO (Producer a m r)
stealingBufferedProducer queueSize producer =
    synchronize queueSize $
        AsyncProducer (\c -> P.runEffect $ (producer >-> c))


synchronize :: forall a m r n s.
            (MonadIO m, MonadAsync n, PS.MonadSafe n)
            => Natural
            -> AsyncProducer a m r n s
            -> IO (Producer a n s)
synchronize queueSize aproducer = do
    threadVar <- MVar.newMVar Nothing
    values <- STM.newTBQueueIO queueSize
    done <- STM.newEmptyTMVarIO

    return $
        PS.mask $ \restore -> do
          thread <- lift $ startFeed threadVar values done

          PS.onException
              (restore $ do
                readValues values done
                lift $ Async.wait thread)
              (liftIO $ Async.cancel thread)
  where
    signalDone :: STM.TMVar () -> n ()
    signalDone done = liftIO $ STM.atomically $ STM.putTMVar done ()

    startFeed :: MVar.MVar (Maybe (Async.Async (StM n s)))
              -> STM.TBQueue a
              -> STM.TMVar ()
              -> n (Async.Async (StM n s))
    startFeed threadVar values done =
        MC.bracketOnError (liftIO $ MVar.takeMVar threadVar)
                          (liftIO . MVar.putMVar threadVar) $ \thread ->
          case thread of
            Just t -> do
              liftIO $ MVar.putMVar threadVar thread
              return t

            Nothing -> do
              thread' <- Async.async $ feed values `MC.finally` signalDone done
              liftIO $ MVar.putMVar threadVar (Just thread')
              return thread'

    feed :: STM.TBQueue a -> n s
    feed values =
        feedFrom aproducer $ P.mapM_ (liftIO . STM.atomically . STM.writeTBQueue values)

    readValues :: STM.TBQueue a -> STM.TMVar () -> Producer a n ()
    readValues values done = fix $ \loop -> do
      na <- liftIO $ STM.atomically $
                STM.orElse (Just    <$> STM.readTBQueue values)
                           (Nothing <$  STM.readTMVar done)

      case na of
        Nothing -> return ()
        Just a  -> P.yield a >> loop
