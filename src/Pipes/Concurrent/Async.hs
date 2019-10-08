{-# language DeriveFunctor #-}
{-# language ImplicitParams #-}
{-# language UndecidableInstances #-}
module Pipes.Concurrent.Async
  ( replicate1
  , MonadAsync
  , AsyncPipe(..)
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

import Pipes (Producer, Pipe, Consumer, (>->))
import qualified Pipes            as P
import qualified Pipes.Core       as P
import qualified Pipes.Concurrent as PC
import qualified Pipes.Prelude    as P


type MonadAsync m = (MonadBaseControl IO m) -- , Forall (Async.Pure m))


replicate1 :: HasCallStack => Natural -> a -> NE.NonEmpty a
replicate1 0 _ = error "replicate1: 0"
replicate1 n a = a NE.:| replicate (fromIntegral n - 1) a


newtype AsyncPipe a b mp mc rp rc n s =
    AsyncPipe { pipeThrough :: Producer a mp rp -> Consumer b mc rc -> n s }
    deriving Functor

type AsyncProducer a m r n s = AsyncPipe P.X a m m () r n s

type AsyncProducer' a m r = AsyncProducer a m r m r


type AsyncConsumer a m r n s = AsyncPipe a P.X m m r () n s

type AsyncConsumer' a m r = AsyncConsumer a m r m r


instance MonadAsync n => Apply (AsyncPipe a b mp mc rp rc n) where
    AsyncPipe pfs <.> AsyncPipe ps = AsyncPipe $ \p c -> do
        (!fs, !s) <- Async.concurrently (pfs p c) (ps p c)
        return $! fs s

instance (mc ~ n, a ~ b, mp ~ mc, rp ~ rc, MonadAsync n) => Applicative (AsyncPipe a b mp mc rp rc n) where
    pure a = AsyncPipe (\p c -> a <$ P.runEffect (p >-> c))
    (<*>) = (<.>)

instance (MonadAsync n, Semigroup s) => Semigroup (AsyncPipe a b mp mc rp rc n s) where
    p1 <> p2 = liftF2 (<>) p1 p2

instance (mc ~ n, a ~ b, mp ~ mc, rp ~ rc, MonadAsync n, Monoid s) => Monoid (AsyncPipe a b mp mc rp rc n s) where
    mempty = pure mempty

instance MFunctor (AsyncPipe a b mp mc rp rc) where
    hoist g (AsyncPipe f) = AsyncPipe (\p c -> g (f p c))


cmapOutput ::
    (Consumer b' mc' rc' -> Consumer b mc rc)
    -> AsyncPipe a b  mp mc  rp rc n s
    -> AsyncPipe a b' mp mc' rp rc' n s
cmapOutput g (AsyncPipe f) = AsyncPipe (\p c -> f p (g c))


(>|->) :: Functor mc => AsyncPipe a b mp mc rp rc n s -> Pipe b c mc rc -> AsyncPipe a c mp mc rp rc n s
ap >|-> pipe = cmapOutput (pipe >->) ap

(<-|<) :: Functor mc => Pipe b c mc rc -> AsyncPipe a b mp mc rp rc n s -> AsyncPipe a c mp mc rp rc n s
(<-|<) = flip (>|->)


cmapInput ::
    (Producer a' mp' rp' -> Producer a mp rp)
    -> AsyncPipe a  b mp  mc rp  rc n s
    -> AsyncPipe a' b mp' mc rp' rc n s
cmapInput g (AsyncPipe f) = AsyncPipe (\p c -> f (g p) c)

(>-|>) :: Functor mp => Pipe a b mp rp -> AsyncPipe b c mp mc rp rc n s -> AsyncPipe a c mp mc rp rc n s
pipe >-|> ac = cmapInput (>-> pipe) ac

(<|-<) :: Functor mp => AsyncPipe b c mp mc rp rc n s -> Pipe a b mp rp -> AsyncPipe a c mp mc rp rc n s
(<|-<) = flip (>-|>)


hoistPipe :: Monad m' => (forall x. m' x -> m x) -> AsyncPipe a b m m rp rc n s -> AsyncPipe a b m' m' rp rc n s
hoistPipe g (AsyncPipe f) = AsyncPipe (\p c -> f (hoist g p) (hoist g c))


mapPipeM :: Monad n => (s -> n t) -> AsyncPipe a b mp mc rp rc n s -> AsyncPipe a b mp mc rp rc n t
mapPipeM g (AsyncPipe f) = AsyncPipe (\p c -> g =<< f p c)



-- The producer is executed from start in every thread
duplicatingAsyncProducer :: Monad m => Producer a m r -> AsyncProducer' a m r
duplicatingAsyncProducer producer = AsyncPipe $ \p c ->
    P.runEffect $ do { () <- P.lift (P.runEffect p); producer } >-> c


-- Ensure that the Consumer is actually run
asyncProducer ::
    Monad m
    => (forall m' r'. Monad m' => Consumer a m' r' -> m' (s, r'))
    -> AsyncProducer a m r m (s, r)
asyncProducer f = AsyncPipe $ \p c -> do
    () <- P.runEffect p
    f c


-- Ensure that the Producer is actually run
asyncConsumer ::
    Monad m
    => (forall m' r'. Monad m' => Producer a m' r' -> m' (s, r'))
    -> AsyncConsumer a m r m (s, r)
asyncConsumer f = AsyncPipe $ \p c -> do
    sr <- f p
    () <- P.runEffect (return () >-> c)
    return sr


toListM :: Monad m => AsyncConsumer a m r m [a]
toListM = fmap fst toListM'


toListM' :: Monad m => AsyncConsumer a m r m ([a], r)
toListM' = asyncConsumer P.toListM'


asyncFold :: Monad m => L.Fold a b -> AsyncConsumer a m r m b
asyncFold fold = AsyncPipe $ \p c -> do
    b <- L.purely P.fold fold (void p)
    () <- P.runEffect (return () >-> c)
    return b


asyncFoldM :: Monad m => L.FoldM m a b -> AsyncConsumer a m r m b
asyncFoldM fold = AsyncPipe $ \p c -> do
    b <- L.impurely P.foldM fold (void p)
    () <- P.runEffect (return () >-> c)
    return b


asyncFoldM' :: Monad m => L.FoldM m a b -> AsyncConsumer a m r m (b, r)
asyncFoldM' fold = AsyncPipe $ \p c -> do
    b <- L.impurely P.foldM' fold p
    () <- P.runEffect (return () >-> c)
    return b



infixr 7 >||>, >|->, >-|>
infixl 7 <||<, <-|<, <|-<


(>||>) ::
    (?bufSize :: Natural, MonadIO m, MonadIO m', MonadAsync n)
    => AsyncPipe a b mp m  rp () n r
    -> AsyncPipe b c m' mc () rc n s
    -> AsyncPipe a c mp mc rp rc n (r, s)
ap1 >||> ap2 = AsyncPipe $ \p c -> do
    (str, sts) <- liftBaseWith $ \runInBase ->
        PC.withBuffer (PC.bounded (fromIntegral ?bufSize))
            (\output -> runInBase $ pipeThrough ap1 p (PC.toOutput output))
            (\input  -> runInBase $ pipeThrough ap2 (PC.fromInput input) c)

    r <- restoreM str
    s <- restoreM sts
    return (r, s)


(<||<) ::
    (?bufSize :: Natural, MonadIO m2, MonadIO m3, MonadAsync n)
    => AsyncPipe b c m3 m4 () rc n s
    -> AsyncPipe a b m1 m2 rp () n r
    -> AsyncPipe a c m1 m4 rp rc n (r, s)
(<||<) = flip (>||>)


runAsyncEffect_ ::
    (Functor mp, Functor mc)
    => AsyncPipe P.X P.X mp mc () r n a
    -> n a
runAsyncEffect_ e = pipeThrough e (pure ()) (P.map P.closed)


runAsyncEffect ::
    (Functor mp, Functor mc)
    => Natural
    -> ((?bufSize :: Natural) => AsyncPipe P.X P.X mp mc () r n a)
    -> n a
runAsyncEffect bufSize e =
    let ?bufSize = bufSize
    in  runAsyncEffect_ e


asyncMapOutputM ::
    (Traversable1 t, MonadAsync n, Monad mc)
    => t (b -> mc c)
    -> AsyncPipe a b mp mc rp rc n s
    -> AsyncPipe a c mp mc rp rc n (t s)
asyncMapOutputM fs ap =
    traverse1 (\f -> ap >|-> P.mapM f) fs


asyncMapInputM ::
    (Traversable1 t, MonadAsync n, Monad mp)
    => t (a -> mp b)
    -> AsyncPipe b c mp mc rp rc n s
    -> AsyncPipe a c mp mc rp rc n (t s)
asyncMapInputM fs ap =
    traverse1 (\f -> P.mapM f >-|> ap) fs


asyncFoldMapOutputM ::
    (Foldable1 t, Monad mc, MonadAsync n, Semigroup s)
    => t (b -> mc c)
    -> AsyncPipe a b mp mc rp rc n s
    -> AsyncPipe a c mp mc rp rc n s
asyncFoldMapOutputM fs ap =
    foldMap1 (\f -> ap >|-> P.mapM f) fs


asyncFoldMapInputM ::
    (Foldable1 t, Monad mp, MonadAsync n, Semigroup s)
    => t (a -> mp b)
    -> AsyncPipe b c mp mc rp rc n s
    -> AsyncPipe a c mp mc rp rc n s
asyncFoldMapInputM fs ap =
    foldMap1 (\f -> P.mapM f >-|> ap) fs
