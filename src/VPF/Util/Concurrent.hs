{-# language DeriveFunctor #-}
module VPF.Util.Concurrent
  ( AsyncProducer(..)
  , AsyncConsumer(..)
  , AsyncEffect
  , toAsyncProducer
  , toAsyncConsumer
  , asyncProducer
  , asyncConsumer
  , toListM
  , bufferedChunks
  , (>|>)
  , (<|<)
  , parFoldM
  , runAsyncEffect
  , parMapM
  , parMapM_
  , parForM
  , parForM_
  ) where

import Data.Coerce (coerce)
import Data.Functor (($>))
import Data.Maybe (fromMaybe, catMaybes)

import Control.Concurrent.Async (replicateConcurrently)
import Control.Lens ((%~))
import Control.Monad (forM, join, void)
import Control.Monad.Morph (MFunctor(..))
import Control.Monad.IO.Class (MonadIO(..))
import Control.Monad.Trans.Control (MonadBaseControl(..))

import Pipes (Producer, Consumer, Effect, (>->))
import qualified Pipes            as P
import qualified Pipes.Concurrent as PC
import qualified Pipes.Group      as PG
import qualified Pipes.Prelude    as P
import qualified Pipes.Safe       as PS

import System.Mem (performGC)



liftReplicateConcurrently :: forall m a. MonadBaseControl IO m => Int -> m a -> m [a]
liftReplicateConcurrently n m =
    restoreN $
        liftBaseWith $ \runInBase ->
            replicateConcurrently n (runInBase m)
  where
    restore1 :: StM m a -> m a
    restore1 = restoreM

    restoreN :: m [StM m a] -> m [a]
    restoreN ms = ms >>= mapM restore1


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


newtype AsyncProducer a m r n s = AsyncProducer { feedFrom :: Consumer a m r -> n s }
  deriving Functor

newtype AsyncConsumer a m r n s = AsyncConsumer { feedTo   :: Producer a m r -> n s }
  deriving Functor

instance MFunctor (AsyncProducer a m r) where hoist f p = coerce (f . feedFrom p)
instance MFunctor (AsyncConsumer a m r) where hoist f c = coerce (f . feedTo c)

data AsyncEffect n t where
    AsyncEffect :: (MonadIO m, MonadIO m')
                => AsyncProducer a m () n r
                -> AsyncConsumer a m' () n s
                -> AsyncEffect n (r, s)

toAsyncProducer :: (Monad m, Monoid r) => Producer a m r -> AsyncProducer a m r0 m r
toAsyncProducer producer = AsyncProducer $
    P.runEffect . (producer >->) . ($> mempty)

toAsyncConsumer :: (Monad m, Monoid r) => Consumer a m r -> AsyncConsumer a m r0 m r
toAsyncConsumer consumer = AsyncConsumer $
    P.runEffect . (>-> consumer) . ($> mempty)

-- ensure that the Producer is actually run
asyncConsumer :: forall n a s. Monad n
              => (forall m r. Monad m => Producer a m r -> m (s, r))
              -> AsyncConsumer a n () n s
asyncConsumer f = coerce (fmap fst . f @n)

-- ensure that the Consumer is actually run
asyncProducer :: forall n a s. Monad n
              => (forall m r. Monad m => Consumer a m r -> m (s, r))
              -> AsyncProducer a n () n s
asyncProducer f = coerce (fmap fst . f @n)


toListM :: Monad m => AsyncConsumer a m () m [a]
toListM = asyncConsumer P.toListM'


bufferedChunks :: Monad m => Int -> Producer a m r -> Producer [a] m r
bufferedChunks chunkSize =
    PG.chunksOf chunkSize . PG.individually %~ \chunk -> do
      (items, f) <- P.lift $ P.toListM' chunk
      P.yield items
      return f


(>|>) :: (MonadIO m, MonadIO m')
      => AsyncProducer a m () n r
      -> AsyncConsumer a m' () n s
      -> AsyncEffect n (r, s)
(>|>) = AsyncEffect

(<|<) :: (MonadIO m, MonadIO m')
      => AsyncConsumer a m' () n s
      -> AsyncProducer a m () n r
      -> AsyncEffect n (r, s)
(<|<) = flip (>|>)


parFoldM :: (MonadIO m, MonadIO m')
         => AsyncConsumer a m' () n s
         -> AsyncProducer a m () n r
         -> AsyncEffect n (r, s)
parFoldM = (<|<)


runAsyncEffect :: MonadBaseControl IO n => Int -> AsyncEffect n r -> n r
runAsyncEffect bufSize (AsyncEffect aproducer aconsumer) =
    liftWithBuffer (PC.bounded bufSize)
      (feedFrom aproducer . PC.toOutput)
      (feedTo aconsumer . PC.fromInput)


parMapM :: forall m n a r r' s.
        (Monoid s, MonadIO m, MonadIO n, MonadBaseControl IO n)
        => Int
        -> (a -> n r)
        -> AsyncProducer a m () n r'
        -> AsyncProducer r n s  n (r', s)
parMapM maxWorkers f aproducer =
    AsyncProducer $ \consumer ->
        runAsyncEffect (2*maxWorkers) $
            aproducer >|> workers consumer
  where
    workers :: Consumer r n s -> AsyncConsumer a n () n s
    workers consumer = AsyncConsumer $ \input ->
        fmap (mconcat . catMaybes) $
          liftReplicateConcurrently maxWorkers $
            P.runEffect $
                (input $> Nothing)
                  >-> P.mapM f
                  >-> fmap Just consumer


parMapM_ :: forall m n a r r'.
         (MonadIO m, MonadIO n, MonadBaseControl IO n)
         => Int
         -> (a -> n r)
         -> AsyncProducer a m () n r'
         -> AsyncProducer r n () n r'
parMapM_ maxWorkers f = fmap fst . parMapM maxWorkers f


parForM :: forall m n a r r' s.
        (Monoid s, MonadIO m, MonadIO n, MonadBaseControl IO n)
        => Int
        -> AsyncProducer a m () n r'
        -> (a -> n r)
        -> AsyncProducer r n s  n (r', s)
parForM maxWorkers = flip (parMapM maxWorkers)


parForM_ :: forall m n a r r'.
         (MonadIO m, MonadIO n, MonadBaseControl IO n)
         => Int
         -> AsyncProducer a m () n r'
         -> (a -> n r)
         -> AsyncProducer r n () n r'
parForM_ maxWorkers = flip (parMapM_ maxWorkers)

