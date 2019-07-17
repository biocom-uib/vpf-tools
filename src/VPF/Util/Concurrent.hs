module VPF.Util.Concurrent
  ( AsyncProducer
  , asyncProducer
  , asyncConsumer
  , chunked
  , parMap
  , consumeWith
  ) where

import Control.Concurrent.Async (Async, Concurrently(..), async, wait)
import Control.Lens ((^.), to)
import Control.Monad (forM, join)
import Control.Monad.Trans.Control (MonadBaseControl(..), liftBaseDiscard)
import Control.Monad.IO.Class (MonadIO(..))

import Pipes (Producer, Consumer, Effect, (>->))
import qualified Pipes            as P
import qualified Pipes.Concurrent as PC
import qualified Pipes.Group      as PG
import qualified Pipes.Prelude    as P
import qualified Pipes.Safe       as PS

import System.Mem (performGC)



liftWorkers :: forall m. MonadBaseControl IO m => Int -> m () -> m ()
liftWorkers nworkers m =
    restoreN $
        liftBaseWith $ \runInBase ->
            runConcurrently $ sequenceA (replicate nworkers (Concurrently (runInBase m)))
  where
    restore1 :: StM m () -> m ()
    restore1 = restoreM

    restoreN :: m [StM m ()] -> m ()
    restoreN ms = ms >>= mapM_ restore1


liftWithBuffer :: forall m a l r. MonadBaseControl IO m
               => PC.Buffer a
               -> (PC.Output a -> m l)
               -> (PC.Input a -> m r)
               -> m (l, r)
liftWithBuffer buf fo fi =
    restore2 $ liftBaseWith (\runInBase ->
        PC.withBuffer buf (runInBase . fo) (runInBase . fi))
  where
    restore2 :: m (StM m l, StM m r) -> m (l, r)
    restore2 mlr = do
        (sl, sr) <- mlr
        l <- restoreM sl
        r <- restoreM sr
        return (l, r)


type AsyncProducer a m n r = Consumer a m r -> n r
type AsyncConsumer a m n s = Producer a m () -> n s

asyncProducer :: Monad m => Producer a m r -> AsyncProducer a m m r
asyncProducer producer = P.runEffect . (producer >->)

asyncConsumer :: Monad m => Consumer a m () -> AsyncConsumer a m m ()
asyncConsumer consumer = P.runEffect . (>-> consumer)


chunked :: Monad m => Int -> Producer a m () -> Producer (Producer a m ()) m ()
chunked chunkSize producer =
    PG.folds (\p a -> p >> P.yield a) (return ()) id free
  where
    free = producer ^. PG.chunksOf chunkSize


parMap :: forall m n a r.
       (MonadIO m, MonadIO n, MonadBaseControl IO n)
       => Int
       -> (forall x. m x -> n x)
       -> (a -> n r)
       -> AsyncProducer a m m ()
       -> AsyncProducer r n n ()
parMap maxWorkers liftM transform producer consumer = do
    liftWithBuffer (PC.bounded (2*maxWorkers))
      (\output -> liftM $ writeInputs output)
      (\input -> workers input)

    return ()
  where
    workers :: PC.Input a -> n ()
    workers input =
        liftWorkers maxWorkers $
          P.runEffect $
              PC.fromInput input >-> P.mapM transform >-> consumer

    writeInput :: PC.Output a -> a -> m ()
    writeInput output a = liftIO $ do
        PC.atomically (PC.send output a)
        return ()

    writeInputs :: PC.Output a -> m ()
    writeInputs output =
        producer $ P.mapM_ (writeInput output)


consumeWith :: (MonadIO m, MonadIO n, MonadBaseControl IO n)
            => Int
            -> AsyncConsumer r m n s
            -> AsyncProducer r n n ()
            -> n s
consumeWith bufSize reducer source =
    fmap snd $
      liftWithBuffer (PC.bounded bufSize)
        (source . PC.toOutput)
        (reducer . PC.fromInput)
