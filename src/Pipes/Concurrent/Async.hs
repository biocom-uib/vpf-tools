{-# language DeriveFunctor #-}
{-# language ImplicitParams #-}
{-# language UndecidableInstances #-}
module Pipes.Concurrent.Async
  ( replicate1
  , MonadAsync
  , AsyncProxy(..)
  , AsyncProducer
  , AsyncProducer'
  , AsyncConsumer
  , AsyncConsumer'
  , cmapOutput
  , cmapInput
  , hoistPipe
  , mapPipeM
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
  , runAsyncEffect_
  , runAsyncEffect
  , asyncMapOutputM
  , asyncFoldMapOutputM
  , asyncMapInputM
  , asyncFoldMapInputM
  ) where

import GHC.Stack (HasCallStack)

import Data.Functor.Apply (Apply(..))
import qualified Data.List.NonEmpty as NE
import Data.Semigroup.Foldable (Foldable1, foldMap1)
import Data.Semigroup.Traversable (Traversable1, traverse1)
import Numeric.Natural (Natural)

import qualified Control.Concurrent.Async.Lifted as Async
import qualified Control.Foldl as L
import Control.Monad (void)
import Control.Monad.Morph (MFunctor(..))
import Control.Monad.IO.Class (MonadIO(..))
import Control.Monad.Trans.Control (MonadBaseControl(..))

import Pipes (Consumer, Effect, Producer, Pipe, (>->))
import qualified Pipes            as P
import qualified Pipes.Core       as P
import qualified Pipes.Concurrent as PC
import qualified Pipes.Prelude    as P


type MonadAsync m = (MonadBaseControl IO m) -- , Forall (Async.Pure m))


replicate1 :: HasCallStack => Natural -> a -> NE.NonEmpty a
replicate1 0 _ = error "replicate1: 0"
replicate1 n a = a NE.:| replicate (fromIntegral n - 1) a


newtype AsyncProxy p c n s =
    AsyncProxy { runAsyncProxy :: p -> c -> n s }
    deriving Functor

type AsyncProducer a m r n s = AsyncProxy (Effect m ()) (Consumer a m r) n s

type AsyncProducer' a m r = AsyncProducer a m r m r


type AsyncConsumer a m r n s = AsyncProxy (Producer a m r) (Effect m ()) n s

type AsyncConsumer' a m r = AsyncConsumer a m r m r


instance MonadAsync n => Apply (AsyncProxy p c n) where
    AsyncProxy pfs <.> AsyncProxy ps = AsyncProxy $ \p c -> do
        (!fs, !s) <- Async.concurrently (pfs p c) (ps p c)
        return $! fs s

-- instance (mc ~ n, a ~ b, mp ~ mc, rp ~ rc, MonadAsync n) => Applicative (AsyncProxy a b mp mc rp rc n) where
--     pure a = AsyncPipe (\p c -> a <$ P.runEffect (p >-> c))
--     (<*>) = (<.>)

instance (MonadAsync n, Semigroup s) => Semigroup (AsyncProxy p c n s) where
    p1 <> p2 = liftF2 (<>) p1 p2

-- instance (mc ~ n, a ~ b, mp ~ mc, rp ~ rc, MonadAsync n, Monoid s) => Monoid (AsyncPipe a b mp mc rp rc n s) where
--     mempty = pure mempty

instance MFunctor (AsyncProxy p c) where
    hoist g (AsyncProxy f) = AsyncProxy (\p c -> g (f p c))


cmapOutput ::
    (Consumer a' m' r' -> Consumer a m r)
    -> AsyncProxy p (Consumer a  m  r)  n s
    -> AsyncProxy p (Consumer a' m' r') n s
cmapOutput g (AsyncProxy f) = AsyncProxy (\p c -> f p (g c))


(>|->) :: Functor m => AsyncProxy p (Consumer a m r) n s -> Pipe a b m r -> AsyncProxy p (Consumer b m r) n s
ap >|-> pipe = cmapOutput (pipe >->) ap

(<-|<) :: Functor m => Pipe a b m r -> AsyncProxy p (Consumer a m r) n s -> AsyncProxy p (Consumer b m r) n s
(<-|<) = flip (>|->)


cmapInput ::
    (p' -> p) -- (P.Proxy req a' m' r' -> Producer a mp rp)
    -> AsyncProxy p  c n s
    -> AsyncProxy p' c n s
cmapInput g (AsyncProxy f) = AsyncProxy (\p c -> f (g p) c)

(>-|>) :: Functor m
       => Pipe a b m r
       -> AsyncProxy (Producer b m r) c n s
       -> AsyncProxy (Producer a m r) c n s
pipe >-|> ac = cmapInput (>-> pipe) ac

(<|-<) :: Functor m => AsyncProxy (Producer b m r) c n s -> Pipe a b m r -> AsyncProxy (Producer a m r) c n s
(<|-<) = flip (>-|>)


infixr 6 >-|>, <-|<
infixl 6 <|-<, >|->


hoistPipe ::
    (MFunctor t, MFunctor t', Monad m')
    => (forall x. m' x -> m x)
    -> AsyncProxy (t m r) (t' m r') n s
    -> AsyncProxy (t m' r) (t' m' r') n s
hoistPipe g (AsyncProxy f) = AsyncProxy (\p c -> f (hoist g p) (hoist g c))


mapPipeM :: Monad n => (s -> n t) -> AsyncProxy p c n s -> AsyncProxy p c n t
mapPipeM g (AsyncProxy f) = AsyncProxy (\p c -> g =<< f p c)


-- The producer is executed from start in every thread
duplicatingAsyncProducer :: Monad m => Producer a m r -> AsyncProducer' a m r
duplicatingAsyncProducer producer = AsyncProxy $ \p c ->
    P.runEffect $ do { () <- P.lift (P.runEffect p); producer } >-> c


-- Ensure that the Consumer is actually run
asyncProducer ::
    Monad m
    => (forall m' r'. Monad m' => Consumer a m' r' -> m' (s, r'))
    -> AsyncProducer a m r m (s, r)
asyncProducer f = AsyncProxy $ \p c -> do
    () <- P.runEffect p
    f c

-- Ensure that the Producer is actually run
asyncConsumer ::
    Monad m
    => (forall m' r'. Monad m' => Producer a m' r' -> m' (s, r'))
    -> AsyncConsumer a m r m (s, r)
asyncConsumer f = AsyncProxy $ \p c -> do
    sr <- f p
    () <- P.runEffect c
    return sr

toListM :: Monad m => AsyncConsumer a m r m [a]
toListM = fmap fst toListM'


toListM' :: Monad m => AsyncConsumer a m r m ([a], r)
toListM' = asyncConsumer P.toListM'


asyncFold :: Monad m => L.Fold a b -> AsyncConsumer a m r m b
asyncFold fold = AsyncProxy $ \p c -> do
    b <- L.purely P.fold fold (void p)
    () <- P.runEffect c
    return b


asyncFoldM :: Monad m => L.FoldM m a b -> AsyncConsumer a m r m b
asyncFoldM fold = AsyncProxy $ \p c -> do
    b <- L.impurely P.foldM fold (void p)
    () <- P.runEffect c
    return b


asyncFoldM' :: Monad m => L.FoldM m a b -> AsyncConsumer a m r m (b, r)
asyncFoldM' fold = AsyncProxy $ \p c -> do
    b <- L.impurely P.foldM' fold p
    () <- P.runEffect c
    return b


(>||>) ::
    (?bufSize :: Natural, MonadIO m1, MonadIO m2, MonadAsync n)
    => AsyncProxy p                  (Consumer b m1 ()) n r
    -> AsyncProxy (Producer b m2 ()) c                  n s
    -> AsyncProxy p                  c                  n (r, s)
ap1 >||> ap2 = AsyncProxy $ \p c -> do
    (str, sts) <- liftBaseWith $ \runInBase ->
        PC.withBuffer (PC.bounded (fromIntegral ?bufSize))
            (\output -> runInBase $ runAsyncProxy ap1 p (PC.toOutput output))
            (\input  -> runInBase $ runAsyncProxy ap2 (PC.fromInput input) c)

    r <- restoreM str
    s <- restoreM sts
    return (r, s)


(<||<) ::
    (?bufSize :: Natural, MonadIO m1, MonadIO m2, MonadAsync n)
    => AsyncProxy (Producer b m1 ()) c                  n s
    -> AsyncProxy p                  (Consumer b m2 ()) n r
    -> AsyncProxy p                  c                  n (r, s)
(<||<) = flip (>||>)


infixr 5 <||<
infixl 5 >||>


runAsyncEffect_ ::
    (Functor m, Functor m')
    => AsyncProxy (Effect m ()) (Consumer P.X m' r) n a
    -> n a
runAsyncEffect_ e = runAsyncProxy e (return ()) (P.map P.closed)


runAsyncEffect ::
    (Functor m, Functor m')
    => Natural
    -> ((?bufSize :: Natural) => AsyncProxy (Effect m ()) (Effect m' ()) n a)
    -> n a
runAsyncEffect bufSize e =
    let ?bufSize = bufSize
    in  runAsyncProxy e (return ()) (return ())


asyncMapOutputM ::
    (Traversable1 t, MonadAsync n, Monad m)
    => t (a -> m b)
    -> AsyncProxy p (Consumer a m r) n s
    -> AsyncProxy p (Consumer b m r) n (t s)
asyncMapOutputM fs ap =
    traverse1 (\f -> ap >|-> P.mapM f) fs


asyncMapInputM ::
    (Traversable1 t, MonadAsync n, Monad m)
    => t (b -> m a)
    -> AsyncProxy (Producer a m r) c n s
    -> AsyncProxy (Producer b m r) c n (t s)
asyncMapInputM fs ap =
    traverse1 (\f -> P.mapM f >-|> ap) fs


asyncFoldMapOutputM ::
    (Foldable1 t, MonadAsync n, Monad m, Semigroup s)
    => t (a -> m b)
    -> AsyncProxy p (Consumer a m r) n s
    -> AsyncProxy p (Consumer b m r) n s
asyncFoldMapOutputM fs ap =
    foldMap1 (\f -> ap >|-> P.mapM f) fs


asyncFoldMapInputM ::
    (Foldable1 t, MonadAsync n, Monad m, Semigroup s)
    => t (b -> m a)
    -> AsyncProxy (Producer a m r) p n s
    -> AsyncProxy (Producer b m r) p n s
asyncFoldMapInputM fs ap =
    foldMap1 (\f -> P.mapM f >-|> ap) fs
