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

import Data.Kind


type Distributed :: (Type -> Type) -> Type -> (Type -> Type) -> Type -> Type

data Distributed n w m a where
    GetNumWorkers :: forall n w m. Distributed n w m Natural
    WithWorkers :: forall n w m a. Semigroup a => (w -> m a) -> Distributed n w m a
    RunInWorker :: forall n w m a. w -> SDict (Serializable a) -> SClosure (n a) -> Distributed n w m a


getNumWorkers :: forall n w m. Has (Distributed n w) m => m Natural
getNumWorkers = send (GetNumWorkers @n @w)


getNumWorkers_ :: forall n w m. HasAny Distributed (Distributed n w) m => m Natural
getNumWorkers_ = send (GetNumWorkers @n @w)


withWorkers :: forall n w m a. (Has (Distributed n w) m, Semigroup a) => (w -> m a) -> m a
withWorkers block = send (WithWorkers @n @w block)


withWorkers_ :: forall n w m a.
    ( HasAny Distributed (Distributed n w) m
    , Semigroup a
    )
    => (w -> m a)
    -> m a
withWorkers_ block = send (WithWorkers @n @w block)


runInWorker ::
    Has (Distributed n w) m
    => w
    -> SDict (Serializable a)
    -> SClosure (n a)
    -> m a
runInWorker w sdict clo = send (RunInWorker w sdict clo)


runInWorker_ ::
    HasAny Distributed (Distributed n w) m
    => w
    -> SDict (Serializable a)
    -> SClosure (n a)
    -> m a
runInWorker_ w sdict clo = send (RunInWorker w sdict clo)
