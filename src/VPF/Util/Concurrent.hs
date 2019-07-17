module VPF.Util.Concurrent
  ( mapReduceChunks
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


mapReduceChunks :: forall m n a r s.
                (MonadIO n, PS.MonadSafe m, MonadBaseControl IO n)
                => Int
                -> Int
                -> (forall x. m x -> n x)
                -> Producer a m ()
                -> (Producer a m () -> n r)
                -> (Producer r m () -> n s)
                -> n s
mapReduceChunks chunkSize maxWorkers liftM producer transform reduce = do
    (_, s) <- liftWithBuffer (PC.bounded (2*maxWorkers))
                (\output' ->
                    liftWithBuffer (PC.bounded (2*maxWorkers))
                        (\output -> writeChunks output)
                        (\input -> workers input output'))
                (\input' ->
                    reduce (PC.fromInput input'))
    return s
  where
    chunks :: PG.FreeT (Producer a m) m ()
    chunks = producer ^. PG.chunksOf chunkSize

    producers :: Producer (Producer a m ()) m ()
    producers = PG.folds (\p a -> p >> P.yield a) (return ()) id chunks

    writeChunk :: PC.Output (Producer a m ()) -> Producer a m () -> m ()
    writeChunk o b = liftIO $ do
        PC.atomically (PC.send o b)
        return ()

    writeChunks :: PC.Output (Producer a m ()) -> n ()
    writeChunks o = do
        liftM $ P.runEffect $ producers >-> P.mapM_ (writeChunk o)

    workers :: PC.Input (Producer a m ()) -> PC.Output r -> n ()
    workers input output' =
        liftWorkers maxWorkers $
            P.runEffect $ do
                PC.fromInput input
                  >-> P.mapM transform
                  >-> PC.toOutput output'
