{-# language AllowAmbiguousTypes #-}
{-# language Strict #-}
module Control.Effect.Distributed
  ( Distributed(..)
  , getNumWorkers
  , getNumWorkers_
  , withWorkers
  , withWorkers_
  , runInWorker
  , runInWorker_
  ) where

import Control.Distributed.SClosure

import Control.Algebra
import Control.Effect.Sum.Extra

import Numeric.Natural (Natural)

import Data.Functor.Apply (Apply)
import Data.Semigroup.Foldable (fold1)
import Data.Semigroup.Traversable (sequence1)
import Data.List.NonEmpty (NonEmpty)


data Distributed n w m k
    = GetNumWorkers (Natural -> m k)
    | forall a. WithWorkers (w -> m a) (NonEmpty a -> m k)
    | forall a. RunInWorker w (SDict (Serializable a)) (SClosure (n a)) (a -> m k)


instance Functor m => Functor (Distributed n w m) where
    fmap f (GetNumWorkers k)           = GetNumWorkers (fmap f . k)
    fmap f (WithWorkers block k)       = WithWorkers block (fmap f . k)
    fmap f (RunInWorker w sdict clo k) = RunInWorker w sdict clo (fmap f . k)


instance Apply f => Threads f (Distributed n w) where
    thread state handler (GetNumWorkers k) =
        GetNumWorkers (handler . (<$ state) . k)

    thread state handler (WithWorkers block k) =
        WithWorkers (handler . (<$ state) . block)  (handler . fmap k . sequence1)

    thread state handler (RunInWorker w sdict clo k) =
        RunInWorker w sdict clo (handler . (<$ state) . k)


getNumWorkers :: forall n w sig m. Has (Distributed n w) sig m => m Natural
getNumWorkers = send (GetNumWorkers @n @w return)


getNumWorkers_ :: forall n w sig m. HasAny Distributed (Distributed n w) sig m => m Natural
getNumWorkers_ = send (GetNumWorkers @n @w return)


withWorkers :: forall n w sig m a. (Has (Distributed n w) sig m, Semigroup a) => (w -> m a) -> m a
withWorkers block = send (WithWorkers block (return . fold1) :: Distributed n w m a)


withWorkers_ :: forall n w sig m a.
    ( HasAny Distributed (Distributed n w) sig m
    , Semigroup a
    )
    => (w -> m a)
    -> m a
withWorkers_ block = send (WithWorkers block (return . fold1) :: Distributed n w m a)


runInWorker ::
    Has (Distributed n w) sig m
    => w
    -> SDict (Serializable a)
    -> SClosure (n a)
    -> m a
runInWorker w sdict clo = send (RunInWorker w sdict clo return)


runInWorker_ ::
    HasAny Distributed (Distributed n w) sig m
    => w
    -> SDict (Serializable a)
    -> SClosure (n a)
    -> m a
runInWorker_ w sdict clo = send (RunInWorker w sdict clo return)
