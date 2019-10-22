{-# language DerivingVia #-}
{-# language QuantifiedConstraints #-}
{-# language Strict #-}
{-# language UndecidableInstances #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Control.Algebra.Helpers
  ( relayAlgebraIso
  , relayAlgebraUnwrap
  , relayAlgebraControl
  , relayAlgebraControlYo
  ) where

import Control.Algebra
import Control.Effect.Sum.Extra

import Control.Monad (join)
import qualified Control.Monad.Trans.Control as MTC

import Data.Coerce
import Data.Functor.Yoneda
import Data.Reflection (give, Given(given))


-- relay the effect to an equivalent carrier

relayAlgebraIso :: forall t' t sig alg' m a.
    ( Algebra (alg' :+: sig)  (t' m)
    , Functor (t m)
    , HFunctor sig
    )
    => (forall x. t m x -> t' m x)
    -> (forall x. t' m x -> t m x)
    -> sig (t m) a
    -> t m a
relayAlgebraIso tt' t't = t't . eff . R . hmap tt'


-- relay the effect to the inner type of a newtype

relayAlgebraUnwrap :: forall m' sig' m sig a.
    ( Coercible m m'
    , Coercible (m' a) (m a)
    , Algebra sig' m'
    , Subsumes sig sig'
    , HFunctor sig
    , Functor m
    )
    => (forall x. m' x -> m x)
    -> sig m a
    -> m a
relayAlgebraUnwrap _ = coerce @(m' a) @(m a) . eff . injR @sig @sig' . handleCoercible


newtype StT t a = StT { unStT :: MTC.StT t a }

newtype StFunctor t = StFunctor (forall x y. (x -> y) -> MTC.StT t x -> MTC.StT t y)

instance Given (StFunctor t) => Functor (StT t) where
    fmap f (StT st) =
        case given @(StFunctor t) of
          StFunctor fmap' -> StT (fmap' f st)


-- use StT as the state functor with a reflected instance

relayAlgebraControl :: forall sig t m a.
    ( MTC.MonadTransControl t
    , Algebra sig m
    , forall f. Functor f => Handles f sig
    , Monad m
    , Monad (t m)
    )
    => (forall x y. (x -> y) -> MTC.StT t x -> MTC.StT t y)
    -> sig (t m) a
    -> t m a
relayAlgebraControl fmap' sig = give (StFunctor @t fmap') $ do
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


relayAlgebraControlYo :: forall sig t m a.
    ( MTC.MonadTransControl t
    , Algebra sig m
    , Handles (YoStT t) sig
    , Monad m
    , Monad (t m)
    )
    => (forall x y. (x -> y) -> MTC.StT t x -> MTC.StT t y)
    -> sig (t m) a
    -> t m a
relayAlgebraControlYo fmap' sig = do
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
