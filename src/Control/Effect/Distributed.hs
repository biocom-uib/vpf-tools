{-# language AllowAmbiguousTypes #-}
module Control.Effect.Distributed
  ( Distributed(..)
  , getNumWorkers
  , withWorkers
  , runInWorker
  ) where

import Control.Distributed.SClosure

import Control.Carrier

import Numeric.Natural (Natural)

import Data.Functor.Apply (Apply)
import Data.Semigroup.Traversable (sequence1)
import Data.List.NonEmpty (NonEmpty)


data Distributed n w m k
    = GetNumWorkers (Natural -> m k)
    | forall a. WithWorkers (w n -> m a) (NonEmpty a -> m k)
    | forall a. RunInWorker (w n) (SDict (Serializable a)) (SClosure (n a)) (a -> m k)


instance Functor m => Functor (Distributed n w m) where
    fmap f (GetNumWorkers k)           = GetNumWorkers (fmap f . k)
    fmap f (WithWorkers block k)       = WithWorkers block (fmap f . k)
    fmap f (RunInWorker w sdict clo k) = RunInWorker w sdict clo (fmap f . k)


instance HFunctor (Distributed n w) where
    hmap f (GetNumWorkers k)           = GetNumWorkers (f . k)
    hmap f (WithWorkers block k)       = WithWorkers (f . block) (f . k)
    hmap f (RunInWorker w sdict clo k) = RunInWorker w sdict clo (f . k)


instance Apply f => Handles f (Distributed n w) where
    handle state handler (GetNumWorkers k) =
        GetNumWorkers (handler . (<$ state) . k)

    handle state handler (WithWorkers block k) =
        WithWorkers (handler . (<$ state) . block)  (handler . fmap k . sequence1)

    handle state handler (RunInWorker w sdict clo k) =
        RunInWorker w sdict clo (handler . (<$ state) . k)


getNumWorkers :: forall n w sig m. Has (Distributed n w) sig m => m Natural
getNumWorkers = send (GetNumWorkers @n @w return)


withWorkers :: Has (Distributed n w) sig m => (w n -> m a) -> m (NonEmpty a)
withWorkers block = send (WithWorkers block return)


runInWorker ::
    Has (Distributed n w) sig m
    => w n
    -> SDict (Serializable a)
    -> SClosure (n a)
    -> m a
runInWorker w sdict clo = send (RunInWorker w sdict clo return)
