{-# language InstanceSigs #-}
{-# language DeriveFunctor #-}
module VPF.Concurrency.Pipes
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
  -- , parMapM
  -- , parMapM_
  -- , parForM
  -- , parForM_
  ) where

import Data.Coerce (coerce)
import Data.Functor (($>))
import Data.Maybe (catMaybes)

import Control.Applicative (liftA2)
import Control.Concurrent.Async (concurrently, replicateConcurrently)
import Control.Lens ((%~))
import Control.Monad.Morph (MFunctor(..))
import Control.Monad.IO.Class (MonadIO(..))
import Control.Monad.Trans.Control (MonadBaseControl(..))

import Pipes (Producer, Consumer, (>->))
import qualified Pipes            as P
import qualified Pipes.Concurrent as PC
import qualified Pipes.Group      as PG
import qualified Pipes.Prelude    as P



restore2 :: forall b m a a'. MonadBaseControl b m => m (StM m a, StM m a') -> m (a, a')
restore2 maa' = do
    (sa, sa') <- maa'
    a  <- restoreM sa
    a' <- restoreM sa'
    return (a, a')


restoreN :: forall b m a. MonadBaseControl b m => m [StM m a] -> m [a]
restoreN ms = ms >>= mapM restore1
  where
    restore1 :: StM m a -> m a
    restore1 = restoreM


liftConcurrently2 :: forall m a b c. MonadBaseControl IO m => (a -> b -> c) -> m a -> m b -> m c
liftConcurrently2 f ma mb =
    fmap (uncurry f) $
        restore2 $ liftBaseWith $ \runInBase ->
            concurrently (runInBase ma) (runInBase mb)

liftReplicateConcurrently :: forall m a. MonadBaseControl IO m => Int -> m a -> m [a]
liftReplicateConcurrently n m =
    restoreN $ liftBaseWith $ \runInBase ->
        replicateConcurrently n (runInBase m)


liftWithBuffer :: forall m a l r. MonadBaseControl IO m
               => PC.Buffer a
               -> (PC.Output a -> m l)
               -> (PC.Input a -> m r)
               -> m (l, r)
liftWithBuffer buf fo fi =
    restore2 $ liftBaseWith $ \runInBase ->
        PC.withBuffer buf (runInBase . fo) (runInBase . fi)


newtype AsyncProducer a m n s = AsyncProducer { feedFrom :: forall r. Consumer a m r -> n s }
  deriving Functor

instance MonadBaseControl IO m => Applicative (AsyncProducer a m m) where
    pure :: b -> AsyncProducer a m m b
    pure b = AsyncProducer $ \consumer -> do
        P.runEffect $ return b >-> (b <$ consumer)

    AsyncProducer pfs <*> AsyncProducer ps = AsyncProducer $ \consumer ->
        liftConcurrently2 ($) (pfs consumer) (ps consumer)


newtype AsyncConsumer a m r n s = AsyncConsumer { feedTo   :: Producer a m r -> n s }
  deriving Functor

instance MFunctor (AsyncProducer a m)   where hoist f (AsyncProducer p) = AsyncProducer (f . p)
instance MFunctor (AsyncConsumer a m r) where hoist f (AsyncConsumer c) = AsyncConsumer (f . c)

data AsyncEffect n t where
    AsyncEffect :: (MonadIO m, MonadIO m')
                => AsyncProducer a m n r
                -> AsyncConsumer a m' () n s
                -> AsyncEffect n (r, s)

toAsyncProducer :: (Monad m, Monoid r) => Producer a m r -> AsyncProducer a m m r
toAsyncProducer producer = AsyncProducer $
    P.runEffect . (producer >->) . ($> mempty)

toAsyncConsumer :: (Monad m, Monoid r) => Consumer a m r -> AsyncConsumer a m r0 m r
toAsyncConsumer consumer = AsyncConsumer $
    P.runEffect . (>-> consumer) . ($> mempty)

-- ensure that the Producer is actually run
asyncConsumer :: forall n a s. Monad n
              => (forall m r. Monad m => Producer a m r -> m (s, r))
              -> AsyncConsumer a n () n s
asyncConsumer f = AsyncConsumer (fmap fst . f @n)

-- ensure that the Consumer is actually run
asyncProducer :: forall n a s. Monad n
              => (forall m r. Monad m => Consumer a m r -> m (s, r))
              -> AsyncProducer a n n s
asyncProducer f = AsyncProducer (fmap fst . f @n)


toListM :: Monad m => AsyncConsumer a m () m [a]
toListM = asyncConsumer P.toListM'


bufferedChunks :: Monad m => Int -> Producer a m r -> Producer [a] m r
bufferedChunks chunkSize =
    PG.chunksOf chunkSize . PG.individually %~ \chunk -> do
      (items, f) <- P.lift $ P.toListM' chunk
      P.yield items
      return f


(>|>) :: (MonadIO m, MonadIO m')
      => AsyncProducer a m n r
      -> AsyncConsumer a m' () n s
      -> AsyncEffect n (r, s)
(>|>) = AsyncEffect

(<|<) :: (MonadIO m, MonadIO m')
      => AsyncConsumer a m' () n s
      -> AsyncProducer a m n r
      -> AsyncEffect n (r, s)
(<|<) = flip (>|>)


parFoldM :: (MonadIO m, MonadIO m')
         => AsyncConsumer a m' () n s
         -> AsyncProducer a m n r
         -> AsyncEffect n (r, s)
parFoldM = (<|<)


runAsyncEffect :: MonadBaseControl IO n => Int -> AsyncEffect n r -> n r
runAsyncEffect bufSize (AsyncEffect aproducer aconsumer) = do
    liftWithBuffer (PC.bounded bufSize)
      (feedFrom aproducer . PC.toOutput)
      (feedTo aconsumer . PC.fromInput)



parMapM maxWorkers f aproducer = traverse aproducer (replicate maxWorkers f)

-- parMapM :: forall m n a b r s.
--         (Monoid s, MonadIO m, MonadIO n, MonadBaseControl IO n)
--         => Int
--         -> (a -> n b)
--         -> AsyncProducer a m () n r
--         -> AsyncProducer b n s  n (r, s)
-- parMapM maxWorkers f aproducer =
--     AsyncProducer $ \consumer ->
--         runAsyncEffect (2*maxWorkers) $
--             aproducer >|> workers consumer
--   where
--     workers :: Consumer b n s -> AsyncConsumer a n () n s
--     workers consumer = AsyncConsumer $ \input ->
--         fmap (mconcat . catMaybes) $
--           liftReplicateConcurrently maxWorkers $
--             P.runEffect $
--                 (input $> Nothing)
--                   >-> P.mapM f
--                   >-> fmap Just consumer
--
--
-- parMapM_ :: forall m n a r r'.
--          (MonadIO m, MonadIO n, MonadBaseControl IO n)
--          => Int
--          -> (a -> n r)
--          -> AsyncProducer a m () n r'
--          -> AsyncProducer r n () n r'
-- parMapM_ maxWorkers f = fmap fst . parMapM maxWorkers f
--
--
-- parForM :: forall m n a r r' s.
--         (Monoid s, MonadIO m, MonadIO n, MonadBaseControl IO n)
--         => Int
--         -> AsyncProducer a m () n r'
--         -> (a -> n r)
--         -> AsyncProducer r n s  n (r', s)
-- parForM maxWorkers = flip (parMapM maxWorkers)
--
--
-- parForM_ :: forall m n a r r'.
--          (MonadIO m, MonadIO n, MonadBaseControl IO n)
--          => Int
--          -> AsyncProducer a m () n r'
--          -> (a -> n r)
--          -> AsyncProducer r n () n r'
-- parForM_ maxWorkers = flip (parMapM_ maxWorkers)
--
