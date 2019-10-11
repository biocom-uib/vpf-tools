{-# language StaticPointers #-}
{-# language TemplateHaskell #-}
{-# language UndecidableInstances #-}
module Control.Effect.Distributed where

import Control.Distributed.StoreClosure
import Control.Carrier
import Control.Effect.MTL.TH

import Data.Functor.Apply (Apply)
import Data.Semigroup.Traversable (sequence1)
import Data.List.NonEmpty (NonEmpty)


newtype ScopeT s m a = ScopeT { unScopeT :: m a }


runScopeT :: (forall (s :: *). ScopeT s m a) -> m a
runScopeT sm =
    case sm @() of
      ScopeT m -> m


deriveMonadTrans ''ScopeT


instance Carrier sig m => Carrier sig (ScopeT s m) where
    eff = eff . handleCoercible


newtype Scoped s a = Scoped a


scope :: a -> Scoped s a
scope = Scoped


getScoped :: Monad m => Scoped s a -> ScopeT s m a
getScoped (Scoped a) = return a


data ReplicateM m k = forall a. ReplicateM (m a) (NonEmpty a -> m k)

instance HFunctor ReplicateM where
    hmap f (ReplicateM m k) = ReplicateM (f m) (f . k)

instance Apply f => Effect f ReplicateM where
    handle state handler (ReplicateM m k) = ReplicateM (handler (m <$ state)) (handler . fmap k . sequence1)


data Distributed n w m k
    = forall a. WithWorkers (forall (s :: *). Scoped s (w n) -> ScopeT s m a) (NonEmpty a -> m k)
    | forall a. HasInstance (Serializable a) => RunInWorker (w n) (Closure (n a)) (a -> m k)


instance Functor m => Functor (Distributed n w m) where
    fmap f (WithWorkers block k) = WithWorkers block (fmap f . k)
    fmap f (RunInWorker w clo k) = RunInWorker w clo (fmap f . k)


instance HFunctor (Distributed n w) where
    hmap f (WithWorkers block k) = WithWorkers (ScopeT . f . unScopeT . block) (f . k)
    hmap f (RunInWorker w clo k) = RunInWorker w clo (f . k)


instance Apply f => Effect f (Distributed n w) where
    handle state handler (WithWorkers block k) =
        WithWorkers (ScopeT . handler . (<$ state) . unScopeT . block)  (handler . fmap k . sequence1)

    handle state handler (RunInWorker w clo k) =
        RunInWorker w clo (handler . (<$ state) . k)


withWorkers ::
    ( Carrier sig m
    , Has (Distributed n w) sig m
    , HasInstance (Serializable a)
    )
    => (forall (s :: *). Scoped s (w n) -> ScopeT s m a)
    -> m (NonEmpty a)
withWorkers block = send (WithWorkers block return)


runInWorker ::
    ( Carrier sig m
    , Has (Distributed n w) sig m
    , HasInstance (Serializable a)
    )
    => w n
    -> Closure (n a)
    -> m a
runInWorker w clo = send (RunInWorker w clo return)


-- distribute ::
--     ( Carrier sig m
--     , Member (Distributed n) sig
--     , HasInstance (Serializable a)
--     )
--     => Closure (n a)
--     -> m (NonEmpty a)
-- distribute clo = send (Distribute clo return)
--
--
-- distributeBase :: forall sig m base a.
--     ( Carrier sig m
--     , Member (Distributed m) sig
--     , MonadBaseControl base m
--     , Typeable a
--     , Typeable base
--     , Typeable m
--     , Typeable (StM m a)
--     , HasInstance (Serializable (StM m a))
--     , HasInstance (MonadBaseControl base m)
--     )
--     => Closure (m a)
--     -> m (NonEmpty a)
-- distributeBase clo = do
--     stas <- distribute baseClo
--     mapM restoreM stas
--   where
--     baseClo :: Closure (m (StM m a))
--     baseClo =
--         liftC2 (static (\Dict m -> liftBaseWith ($ m)))
--             (staticInstance @(MonadBaseControl base m))
--             clo


newtype SingleProcessT n m a = SingleProcessT { runSingleProcessT :: m a }


deriveMonadTrans ''SingleProcessT


data LocalWorker n = LocalWorker


interpretSingleProcessT :: (Monad m, n ~ m) => Distributed n LocalWorker (SingleProcessT n m) a -> SingleProcessT n m a
interpretSingleProcessT (WithWorkers block k)           = k . pure =<< runScopeT (block (Scoped LocalWorker))
interpretSingleProcessT (RunInWorker LocalWorker clo k) = k =<< SingleProcessT (evalClosure clo)


deriveCarrier 'interpretSingleProcessT
