{-# language GeneralizedNewtypeDeriving #-}
{-# language QuantifiedConstraints #-}
{-# language Strict #-}
{-# language UndecidableInstances #-}
{-# OPTIONS_GHC -Wno-orphans #-}
module Control.Algebra.Helpers
    ( relayAlgebraIso
    , algUnwrapL
    , relayAlgebraUnwrap
    , StT
    , algControlL
    , relayAlgebraControl
    , YoStT
    , algControlYoL
    , relayAlgebraControlYo
    ) where

import Control.Algebra

import Control.Monad (join)
import Control.Monad.Trans.Control qualified as MTC

import Data.Coerce
import Data.Functor.Compose
import Data.Functor.Yoneda
import Data.Reflection (give, Given(given))


-- relay the effect to an equivalent carrier

relayAlgebraIso ::
    ( Sig m ~ Sig m'
    , Algebra ctx m'
    )
    => (forall x. m' x -> m x)
    -> (forall x. m x -> m' x)
    -> Handler ctx n m
    -> Sig m n a
    -> ctx ()
    -> m (ctx a)
relayAlgebraIso m'm mm' hdl sig ctx =
    m'm $ alg (mm' . hdl) sig ctx


-- relay the effect to the inner type of a newtype

algUnwrapL :: forall m m' n ctx sig1 sig2 a.
    ( Algebra ctx m'
    , Coercible m m'
    , Sig m ~ (sig1 :+: sig2)
    )
    => (forall n0 x. sig2 n0 x -> Sig m' n0 x)
    -> (forall x. m' x -> m x)
    -> (Handler ctx n m -> sig1 n a -> ctx () -> m (ctx a))
    -> Handler ctx n m
    -> Sig m n a
    -> ctx ()
    -> m (ctx a)
algUnwrapL inj m'm algL hdl sig ctx =
    case sig of
        L sigL -> algL hdl sigL ctx
        R sigR -> relayAlgebraUnwrap inj m'm hdl sigR ctx


relayAlgebraUnwrap :: forall m' m n sig ctx a.
    ( Algebra ctx m'
    , Coercible m m'
    )
    => (forall n0 x. sig n0 x -> Sig m' n0 x)
    -> (forall x. m' x -> m x)
    -> Handler ctx n m
    -> sig n a
    -> ctx ()
    -> m (ctx a)
relayAlgebraUnwrap inj m'm hdl sig ctx =
    m'm $ alg (mm' . hdl) (inj sig) ctx
  where
    mm' :: forall x. m x -> m' x
    mm' = coerce


newtype StT t a = StT { unStT :: MTC.StT t a }

newtype StFunctor t = StFunctor (forall x y. (x -> y) -> MTC.StT t x -> MTC.StT t y)

instance Given (StFunctor t) => Functor (StT t) where
    fmap f (StT st) =
        case given @(StFunctor t) of
          StFunctor fmap' -> StT (fmap' f st)


-- use StT as the state functor with a reflected instance

algControlL ::
    ( MTC.MonadTransControl t
    , Algebra1 Functor m
    , Functor ctx
    , Monad (t m)
    , Sig (t m) ~ (sig1 :+: Sig m)
    )
    => (forall x y. (x -> y) -> MTC.StT t x -> MTC.StT t y)
    -> (Handler ctx n (t m) -> sig1 n a -> ctx () -> t m (ctx a))
    -> Handler ctx n (t m)
    -> Sig (t m) n a
    -> ctx ()
    -> t m (ctx a)
algControlL fmap' algL hdl sig ctx =
    case sig of
        L sigL -> algL hdl sigL ctx
        R sigR -> relayAlgebraControl fmap' hdl sigR ctx


relayAlgebraControl :: forall t m n ctx a.
    ( MTC.MonadTransControl t
    , Algebra1 Functor m
    , Functor ctx
    , Monad (t m)
    )
    => (forall x y. (x -> y) -> MTC.StT t x -> MTC.StT t y)
    -> Handler ctx n (t m)
    -> Sig m n a
    -> ctx ()
    -> t m (ctx a)
relayAlgebraControl fmap' hdl sig ctx = give (StFunctor @t fmap') do
    state <- captureStT

    sta <- MTC.liftWith $ \runT -> do
        let runStT :: forall x. t m x -> m (StT t x)
            runStT = fmap StT . runT

            hdlT :: Handler (StT t) (t m) m
            hdlT = runStT . join . restoreStT

        thread (hdlT ~<~ hdl) sig (ctx <$ state)

    restoreStT sta
  where
    captureStT :: t m (StT t ())
    captureStT = fmap StT MTC.captureT

    restoreStT :: forall x. StT t x -> t m x
    restoreStT = MTC.restoreT . return . unStT


-- same trick using Yoneda instead of reflection

newtype YoStT t a = YoStT { unYoStT :: Yoneda (StT t) a }
  deriving newtype Functor


algControlYoL ::
    ( MTC.MonadTransControl t
    , Algebra (Compose (YoStT t) ctx) m
    , Monad m
    , Monad (t m)
    , Sig (t m) ~ (sig1 :+: Sig m)
    )
    => (forall x y. (x -> y) -> MTC.StT t x -> MTC.StT t y)
    -> (Handler ctx n (t m) -> sig1 n a -> ctx () -> t m (ctx a))
    -> Handler ctx n (t m)
    -> Sig (t m) n a
    -> ctx ()
    -> t m (ctx a)
algControlYoL fmap' algL hdl sig ctx =
    case sig of
        L sigL -> algL hdl sigL ctx
        R sigR -> relayAlgebraControlYo fmap' hdl sigR ctx


relayAlgebraControlYo :: forall t m n ctx a.
    ( MTC.MonadTransControl t
    , Algebra (Compose (YoStT t) ctx) m
    , Monad m
    , Monad (t m)
    )
    => (forall x y. (x -> y) -> MTC.StT t x -> MTC.StT t y)
    -> Handler ctx n (t m)
    -> Sig m n a
    -> ctx ()
    -> t m (ctx a)
relayAlgebraControlYo fmap' hdl sig ctx = do
    state <- captureYoT

    yosta <- MTC.liftWith $ \runT -> do
        let runTYo :: forall x. t m x -> m (YoStT t x)
            runTYo = fmap liftYo' . runT

            hdlY :: Handler (YoStT t) (t m) m
            hdlY = runTYo . join . restoreYoT

        thread (hdlY ~<~ hdl) sig (ctx <$ state)

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
