{-# language DeriveFunctor #-}
{-# language UndecidableInstances #-}
module VPF.Concurrency.Async
  ( replicate1
  , MonadAsync
  , AsyncProducer(..)
  , AsyncProducer'
  , AsyncConsumer(..)
  , AsyncConsumer'
  , AsyncEffect
  , premapProducer
  , mapProducer
  , premapConsumer
  , mapConsumer
  , duplicatingAsyncProducer
  , asyncProducer
  , asyncConsumer
  , toListM
  , toListM'
  , asyncFold
  , asyncFoldM
  , asyncFoldM'
  , (>||>), (<||<)
  , (>|->), (<-|<)
  , (>-|>), (<|-<)
  , runAsyncEffect
  , asyncMapM
  , asyncFoldMapM
  ) where

import GHC.Stack (HasCallStack)

-- import Data.Constraint ((\\))
-- import Data.Constraint.Forall (Forall, inst)
import Data.Functor.Apply (Apply(..))
import qualified Data.List.NonEmpty as NE
import Data.Semigroup.Foldable (Foldable1, foldMap1)
import Data.Semigroup.Traversable (Traversable1, traverse1)
import Numeric.Natural (Natural)

import qualified Control.Concurrent.Async.Lifted as Async
import qualified Control.Foldl as L
import Control.Monad (void, (<=<), (<=<))
import Control.Monad.Morph (MFunctor(..))
import Control.Monad.IO.Class (MonadIO(..))
import Control.Monad.Trans.Control (MonadBaseControl(..))

import Pipes (Producer, Pipe, Consumer, (>->))
import qualified Pipes            as P
import qualified Pipes.Concurrent as PC
import qualified Pipes.Prelude    as P


type MonadAsync m = (MonadBaseControl IO m) -- , Forall (Async.Pure m))


replicate1 :: HasCallStack => Natural -> a -> NE.NonEmpty a
replicate1 0 _ = error "replicate1: 0"
replicate1 n a = a NE.:| replicate (fromIntegral n - 1) a


newtype AsyncProducer a m r n s = AsyncProducer { feedFrom :: Consumer a m r -> n s }
  deriving Functor

type AsyncProducer' a m r = AsyncProducer a m r m r

instance MonadAsync n => Apply (AsyncProducer a m r n) where
    AsyncProducer pfs <.> AsyncProducer ps = AsyncProducer $ \consumer -> do
        (!fs, !s) <- Async.concurrently (pfs consumer) (ps consumer)
        return $! fs s

instance (m ~ n, MonadAsync m) => Applicative (AsyncProducer a m r n) where
    pure a = AsyncProducer (\c -> P.runEffect $ return a >-> (a <$ c))
    (<*>) = (<.>)

instance (MonadAsync n, Semigroup s) => Semigroup (AsyncProducer a m r n s) where
    p1 <> p2 = liftF2 (<>) p1 p2

instance (m ~ n, MonadAsync n, Monoid s) => Monoid (AsyncProducer a m r n s) where
    mempty = pure mempty

instance MFunctor (AsyncProducer a m r) where
    hoist f (AsyncProducer p) = AsyncProducer (f . p)

premapProducer :: (Consumer a' m' r' -> Consumer a m r)
               -> AsyncProducer a  m  r  n s
               -> AsyncProducer a' m' r' n s
premapProducer f (AsyncProducer p) = AsyncProducer (p . f)

mapProducer :: Monad n => (s -> n t) -> AsyncProducer a m r n s -> AsyncProducer a m r n t
mapProducer f (AsyncProducer p) = AsyncProducer (f <=< p)


newtype AsyncConsumer a m r n s = AsyncConsumer { feedTo :: Producer a m r -> n s }
  deriving Functor

type AsyncConsumer' a m r = AsyncConsumer a m r m r

instance MonadAsync n => Apply (AsyncConsumer a m r n) where
    AsyncConsumer cfs <.> AsyncConsumer cs = AsyncConsumer $ \producer -> do
        (!fs, !s) <- Async.concurrently (cfs producer) (cs producer)
        return $! fs s

instance (m ~ n, MonadAsync n) => Applicative (AsyncConsumer a m r n) where
    pure a = AsyncConsumer (\p -> P.runEffect $ (a <$ p) >-> return a)
    (<*>) = (<.>)

instance (MonadAsync n, Semigroup s) => Semigroup (AsyncConsumer a m r n s) where
    c1 <> c2 = liftF2 (<>) c1 c2

instance (m ~ n, MonadAsync n, Monoid s) => Monoid (AsyncConsumer a m r n s) where
    mempty = pure mempty

instance MFunctor (AsyncConsumer a m r) where
    hoist f (AsyncConsumer c) = AsyncConsumer (f . c)

premapConsumer :: (Producer a' m' r' -> Producer a m r)
               -> AsyncConsumer a  m  r  n s
               -> AsyncConsumer a' m' r' n s
premapConsumer f (AsyncConsumer c) = AsyncConsumer (c . f)

mapConsumer :: Monad n => (s -> n t) -> AsyncConsumer a m r n s -> AsyncConsumer a m r n t
mapConsumer f (AsyncConsumer c) = AsyncConsumer (f <=< c)


data AsyncEffect a m m' n rs where
   AsyncEffect :: AsyncProducer a m  () n r
               -> AsyncConsumer a m' () n s
               -> AsyncEffect a m m' n (r, s)

instance (MonadAsync n, Semigroup r, Semigroup s) => Semigroup (AsyncEffect a m m' n (r, s)) where
    AsyncEffect p1 c1 <> AsyncEffect p2 c2 = AsyncEffect (p1 <> p2) (c1 <> c2)

instance (m ~ n, m' ~ n, MonadAsync n, Monoid r, Monoid s) => Monoid (AsyncEffect a m m' n (r, s)) where
    mempty = AsyncEffect (pure mempty) (pure mempty)

instance MFunctor (AsyncEffect a m m') where
    hoist f (AsyncEffect p c) = AsyncEffect (hoist f p) (hoist f c)


-- The producer is executed twice
duplicatingAsyncProducer :: (Monad m, Monoid r)
                         => Producer a m r
                         -> AsyncProducer a m () m r
duplicatingAsyncProducer p = AsyncProducer (\c -> P.runEffect $ p >-> (mempty <$ c))

-- Ensure that the Producer is actually run
asyncConsumer :: forall n a r s. Monad n
              => (forall m r'. Monad m => Producer a m r' -> m (s, r'))
              -> AsyncConsumer a n r n (s, r)
asyncConsumer f = AsyncConsumer (f @n)

-- Ensure that the Consumer is actually run
asyncProducer :: forall n a r s. Monad n
              => (forall m r'. Monad m => Consumer a m r' -> m (s, r'))
              -> AsyncProducer a n r n (s, r)
asyncProducer f = AsyncProducer (f @n)

toListM :: Monad m => AsyncConsumer a m r m [a]
toListM = fmap fst toListM'

toListM' :: Monad m => AsyncConsumer a m r m ([a], r)
toListM' = AsyncConsumer P.toListM'

asyncFold :: Monad m => L.Fold a b -> AsyncConsumer a m r m b
asyncFold fold = AsyncConsumer (L.purely P.fold fold . void)

asyncFoldM :: Monad m => L.FoldM m a b -> AsyncConsumer a m r m b
asyncFoldM fold = AsyncConsumer (L.impurely P.foldM fold . void)

asyncFoldM' :: Monad m => L.FoldM m a b -> AsyncConsumer a m r m (b, r)
asyncFoldM' fold = AsyncConsumer (L.impurely P.foldM' fold)


(>||>) :: AsyncProducer a m () n r
       -> AsyncConsumer a m' () n s
       -> AsyncEffect a m m' n (r, s)
(>||>) = AsyncEffect

(>|->) :: Functor m => AsyncProducer a m r n s -> Pipe a b m r -> AsyncProducer b m r n s
ap >|-> pipe = premapProducer (pipe >->) ap

(>-|>) :: Functor m => Pipe a b m r -> AsyncConsumer b m r n s -> AsyncConsumer a m r n s
pipe >-|> ac = premapConsumer (>-> pipe) ac

infixr 7 >||>, >|->, >-|>

(<||<) :: AsyncConsumer a m' () n s -> AsyncProducer a m () n r -> AsyncEffect a m m' n (r, s)
(<||<) = flip (>||>)

(<-|<) :: Functor m => Pipe a b m r -> AsyncProducer a m r n s -> AsyncProducer b m r n s
(<-|<) = flip (>|->)

(<|-<) :: Functor m => AsyncConsumer b m r n s -> Pipe a b m r -> AsyncConsumer a m r n s
(<|-<) = flip (>-|>)

infixl 7 <||<, <-|<, <|-<


runAsyncEffect :: forall a m m' n r s.
               (MonadIO m, MonadIO m', MonadAsync n)
               => Natural
               -> AsyncEffect a m m' n (r, s) -> n (r, s)
runAsyncEffect bufSize (AsyncEffect aproducer aconsumer) = do
    (str, sts) <- liftBaseWith $ \runInBase ->
        PC.withBuffer (PC.bounded (fromIntegral bufSize))
            (runInBase . feedFrom aproducer . PC.toOutput)
            (runInBase . feedTo aconsumer . PC.fromInput)
          -- \\ inst @(Async.Pure n) @r
          -- \\ inst @(Async.Pure n) @s

    r <- restoreM str
    s <- restoreM sts
    return (r, s)


asyncMapM :: forall t a b m n r s.
          (Traversable1 t, Monad m, MonadAsync n)
          => t (a -> m b)
          -> AsyncProducer a m r n s
          -> AsyncProducer b m r n (t s)
asyncMapM fs ap =
    traverse1 (\f -> premapProducer (P.mapM f >->) ap) fs


asyncFoldMapM :: forall t a b m n r s.
              (Foldable1 t, Monad m, MonadAsync n, Semigroup s)
              => t (a -> m b)
              -> AsyncProducer a m r n s
              -> AsyncProducer b m r n s
asyncFoldMapM fs ap =
    foldMap1 (\f -> ap >|-> P.mapM f) fs
