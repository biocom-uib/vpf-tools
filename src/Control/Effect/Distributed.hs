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
import Data.Kind


type Distributed :: (Type -> Type) -> Type -> (Type -> Type) -> Type -> Type

data Distributed n w m a where
    GetNumWorkers :: forall n w m. Distributed n w m Natural
    WithWorkers :: forall n w m a. (w -> m a) -> Distributed n w m (NonEmpty a)
    RunInWorker :: forall n w m a. w -> SDict (Serializable a) -> SClosure (n a) -> Distributed n w m a

-- instance Functor m => Functor (Distributed n w m) where
--     fmap f (GetNumWorkers k)           = GetNumWorkers (fmap f . k)
--     fmap f (WithWorkers block k)       = WithWorkers block (fmap f . k)
--     fmap f (RunInWorker w sdict clo k) = RunInWorker w sdict clo (fmap f . k)
--
--
-- instance Apply f => Threads f (Distributed n w) where
--     thread state handler (GetNumWorkers k) =
--         GetNumWorkers (handler . (<$ state) . k)
--
--     thread state handler (WithWorkers block k) =
--         WithWorkers (handler . (<$ state) . block)  (handler . fmap k . sequence1)
--
--     thread state handler (RunInWorker w sdict clo k) =
--         RunInWorker w sdict clo (handler . (<$ state) . k)


getNumWorkers :: forall n w m. Has (Distributed n w) m => m Natural
getNumWorkers = send (GetNumWorkers @n @w)


getNumWorkers_ :: forall n w m. HasAny Distributed (Distributed n w) m => m Natural
getNumWorkers_ = send (GetNumWorkers @n @w)


withWorkers :: forall n w m a. (Has (Distributed n w) m, Semigroup a) => (w -> m a) -> m (NonEmpty a)
withWorkers block = send (WithWorkers @n @w block)


withWorkers_ :: forall n w m a.
    ( HasAny Distributed (Distributed n w) m
    , Semigroup a
    )
    => (w -> m a)
    -> m (NonEmpty a)
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
