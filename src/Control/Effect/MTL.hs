{-# language UndecidableInstances #-}
{-# language QuantifiedConstraints #-}
{-# language DerivingVia #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Control.Effect.MTL
  ( relayCarrierIso
  , SubEffects
  , relayCarrierUnwrap
  , relayCarrierControl
  , relayCarrierControlYo
  ) where

import Data.Coerce
import Data.Reflection (give, Given(given))
import Data.Tuple (swap)

import Control.Effect.Carrier
import Control.Effect.Error
import Control.Effect.Lift
import Control.Effect.Reader
import Control.Effect.State

import Data.Functor.Yoneda

import Control.Monad (join)
import qualified Control.Monad.Trans.Control  as MTC
import qualified Control.Monad.Trans.Identity as MT
import qualified Control.Monad.Trans.Except   as MT
import qualified Control.Monad.Trans.Reader   as MT
import qualified Control.Monad.Trans.State    as MT


-- relay the effect to an equivalent carrier

relayCarrierIso :: forall t' t sig alg' m a.
    ( Effect sig
    , Carrier (alg' :+: sig)  (t' m)
    , Functor (t m)
    )
    => (forall x. t m x -> t' m x)
    -> (forall x. t' m x -> t m x)
    -> sig (t m) a
    -> t m a
relayCarrierIso tt' t't = t't . eff . R . hmap tt'


class SubEffects sub sup where injR :: sub m a -> sup m a

instance SubEffects sub sub where
    injR = id
instance {-# overlappable #-} SubEffects sub (sub' :+: sub)where
    injR = R
instance {-# overlappable #-} SubEffects sub sup => SubEffects sub (sub' :+: sup) where
    injR = R . injR


-- relay the effect to the inner type of a newtype

relayCarrierUnwrap :: forall m' sig' m sig a.
    ( Coercible m m'
    , Coercible (m' a) (m a)
    , Effect sig
    , Carrier sig' m'
    , SubEffects sig sig'
    , Functor m
    )
    => (forall x. m' x -> m x)
    -> sig m a
    -> m a
relayCarrierUnwrap _ = coerce @(m' a) @(m a) . eff . injR @sig @sig' . handleCoercible


newtype StT t a = StT { unStT :: MTC.StT t a }

newtype StFunctor t = StFunctor (forall x y. (x -> y) -> MTC.StT t x -> MTC.StT t y)

instance Given (StFunctor t) => Functor (StT t) where
    fmap f (StT st) =
        case given @(StFunctor t) of
          StFunctor fmap' -> StT (fmap' f st)


-- use StT as the state functor with a reflected instance

relayCarrierControl :: forall sig t m a.
    ( MTC.MonadTransControl t
    , Carrier sig m
    , Effect sig
    , Monad m
    , Monad (t m)
    )
    => (forall x y. (x -> y) -> MTC.StT t x -> MTC.StT t y)
    -> sig (t m) a
    -> t m a
relayCarrierControl fmap' sig = give (StFunctor @t fmap') $ do
    state <- captureStT

    sta <- MTC.liftWith $ \runT -> do
        let runStT :: forall x. t m x -> m (StT t x)
            runStT = fmap StT . runT

            handler :: forall x. StT t (t m x) -> m (StT t x)
            handler = runStT . join . restoreStT

            handle' :: sig (t m) a -> sig m (StT t a)
            handle' = handle state handler

        eff (handle' sig)

    restoreStT sta
  where
    captureStT :: t m (StT t ())
    captureStT = fmap StT MTC.captureT

    restoreStT :: forall x. StT t x -> t m x
    restoreStT = MTC.restoreT . return . unStT



-- same trick using Yoneda instead of reflection

newtype YoStT t a = YoStT { unYoStT :: Yoneda (StT t) a }
  deriving Functor via Yoneda (StT t)


relayCarrierControlYo :: forall sig t m a.
    ( MTC.MonadTransControl t
    , Carrier sig m
    , Effect sig
    , Monad m
    , Monad (t m)
    )
    => (forall x y. (x -> y) -> MTC.StT t x -> MTC.StT t y)
    -> sig (t m) a
    -> t m a
relayCarrierControlYo fmap' sig = do
    state <- captureYoT

    yosta <- MTC.liftWith $ \runT -> do
        let runTYo :: forall x. t m x -> m (YoStT t x)
            runTYo = fmap liftYo' . runT

            handler :: forall x. YoStT t (t m x) -> m (YoStT t x)
            handler = runTYo . join . restoreYoT

            handle' :: sig (t m) a -> sig m (YoStT t a)
            handle' = handle state handler

        eff (handle' sig)

    restoreYoT yosta
  where
    restoreYoT :: forall x. YoStT t x -> t m x
    restoreYoT = MTC.restoreT . return . lowerYo'

    captureYoT :: t m (YoStT t ())
    captureYoT = fmap liftYo' MTC.captureT

    liftYo' :: forall x. MTC.StT t x -> YoStT t x
    liftYo' stx = YoStT (Yoneda (\f -> StT (fmap' f stx)))

    lowerYo' :: forall x. YoStT t x -> MTC.StT t x
    lowerYo' = unStT . lowerYoneda . unYoStT


instance (Monad m, Carrier sig m, Effect sig) => Carrier (Error e :+: sig) (MT.ExceptT e m) where
    eff (L (Throw e))     = MT.throwE e
    eff (L (Catch m h k)) = MT.catchE m h >>= k
    eff (R other)         = relayCarrierIso (ErrorC . MT.runExceptT) (MT.ExceptT . runErrorC) other


instance Monad m => Carrier (Lift m) (MT.IdentityT m) where
    eff (Lift m) = MT.IdentityT (m >>= MT.runIdentityT)


instance (Monad m, Carrier sig m, Effect sig) => Carrier (Reader r :+: sig) (MT.ReaderT r m) where
    eff (L (Ask k))       = MT.ask >>= k
    eff (L (Local g m k)) = MT.local g m >>= k
    eff (R other)         = relayCarrierIso (ReaderC . MT.runReaderT) (MT.ReaderT . runReaderC) other


instance (Monad m, Carrier sig m, Effect sig) => Carrier (State s :+: sig) (MT.StateT s m) where
    eff (L (Get k))   = MT.get >>= k
    eff (L (Put s k)) = MT.put s >> k
    eff (R other)     = relayCarrierIso (StateC . (fmap swap .) . MT.runStateT) (MT.StateT . (fmap swap .) . runStateC) other
