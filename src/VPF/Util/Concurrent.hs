module VPF.Util.Concurrent
  ( concurrentlyChunked
  ) where

import Control.Concurrent.Async (Async, async, wait)
import Control.Lens ((^.), to)
import Control.Monad (forM, forever)
import Control.Monad.Trans.Control (MonadBaseControl(..), liftBaseDiscard)
import Control.Monad.IO.Class (MonadIO(..))

import Pipes (Producer, Consumer, Effect, (>->))
import qualified Pipes            as P
import qualified Pipes.Concurrent as PC
import qualified Pipes.Group      as PG
import qualified Pipes.Prelude    as P
import qualified Pipes.Safe       as PS

import System.Mem (performGC)



liftAsync :: MonadBaseControl IO m
          => m a
          -> m (Async (StM m a))
liftAsync m = liftBaseWith (\runInBase -> async (runInBase m))


concurrentlyChunked :: forall m n a r.
                    (MonadIO n, PS.MonadSafe m, MonadBaseControl IO n)
                    => Int
                    -> Int
                    -> (forall x. m x -> n x)
                    -> Producer a m ()
                    -> (Producer a m () -> n r)
                    -> n (Producer r m ())
concurrentlyChunked chunkSize maxWorkers liftM producer worker = do
    (as, input') <- do
        (output, input) <- liftIO $ PC.spawn (PC.bounded (2*maxWorkers))
        (output', input') <- liftIO $ PC.spawn (PC.bounded (2*maxWorkers))

        as <- forM [1..maxWorkers] $ \_ ->
                  liftAsync $ do
                      P.runEffect $ do
                          PC.fromInput input
                            >-> P.mapM worker
                            >-> PC.toOutput output'

                          liftIO $ performGC

        liftM $ P.runEffect $ chunkWriter output
        liftIO performGC

        return (as, input')

    return (PC.fromInput input' `PS.finally` liftIO (mapM_ wait as))
  where
    chunks :: PG.FreeT (Producer a m) m ()
    chunks = producer ^. PG.chunksOf chunkSize

    producers :: Producer (Producer a m ()) m ()
    producers = PG.folds (\p a -> p >> P.yield a) (return ()) id chunks

    writeChunk :: PC.Output (Producer a m ()) -> Producer a m () -> m ()
    writeChunk o b = liftIO $ do
        PC.atomically (PC.send o b)
        return ()

    chunkWriter :: PC.Output (Producer a m ()) -> Effect m ()
    chunkWriter o = producers >-> P.mapM_ (writeChunk o)
