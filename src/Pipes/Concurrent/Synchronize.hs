module Pipes.Concurrent.Synchronize where

import qualified Control.Concurrent.Async.Lifted as Async

import qualified Control.Concurrent.MVar        as MVar
import qualified Control.Concurrent.STM.TBQueue as STM
import qualified Control.Concurrent.STM.TMVar   as STM
import qualified Control.Monad.STM              as STM

import Control.Lens ((%~))
import qualified Control.Monad.Catch as MC
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Control (StM)

import Data.Function (fix)
import Numeric.Natural (Natural)

import Pipes (Consumer, Producer)
import qualified Pipes.Concurrent.Async as PA

import qualified Pipes            as P
import qualified Pipes.Prelude    as P
import qualified Pipes.Group      as PG
import qualified Pipes.Safe       as PS


bufferedChunks :: Monad m => Natural -> Producer a m r -> Producer [a] m r
bufferedChunks chunkSize =
    PG.chunksOf (fromIntegral chunkSize) . PG.individually %~ \chunk -> do
      (items, f) <- P.lift $ P.toListM' chunk
      P.yield items
      return f


-- convert a Producer to an asynchronous producer via work stealing
-- an AsyncProducer is less flexible than a producer, but the API is safer
stealingAsyncProducer ::
    (PA.MonadAsync m, PS.MonadSafe m)
    => Natural
    -> Producer a m r
    -> IO (PA.AsyncProducer' a m r)
stealingAsyncProducer queueSize producer = do
    producer' <- stealingBufferedProducer queueSize producer

    return $ PA.duplicatingAsyncProducer producer'


stealingBufferedProducer ::
    (PA.MonadAsync m, PS.MonadSafe m)
    => Natural
    -> Producer a m r
    -> IO (Producer a m r)
stealingBufferedProducer queueSize =
    synchronize queueSize . PA.duplicatingAsyncProducer


synchronize :: forall a m r n s.
    (PA.MonadAsync n, PS.MonadSafe n, MonadIO m)
    => Natural
    -> PA.AsyncProducer a m r n s
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

    consumerQueue :: STM.TBQueue a -> Consumer a m u
    consumerQueue values = P.mapM_ (liftIO . STM.atomically . STM.writeTBQueue values)

    feed :: STM.TBQueue a -> n s
    feed values = PA.runAsyncEffect_ $ aproducer PA.>|-> consumerQueue values

    readValues :: STM.TBQueue a -> STM.TMVar () -> Producer a n ()
    readValues values done = fix $ \loop -> do
      na <- liftIO $ STM.atomically $
                STM.orElse (Just    <$> STM.readTBQueue values)
                           (Nothing <$  STM.readTMVar done)

      case na of
        Nothing -> return ()
        Just a  -> P.yield a >> loop
