{-# language DeriveAnyClass #-}
{-# language DeriveGeneric #-}
{-# language EmptyCase #-}
{-# language QuantifiedConstraints #-}
{-# language Strict #-}
{-# language TemplateHaskell #-}
{-# language UndecidableInstances #-}
module Control.Carrier.Error.Excepts
  ( module Control.Effect.Error
  , Errors(..)
  , MemberError
  , ExceptsT(..)
  , runExceptsT
  , runPureExceptsT
  , runLastExceptT
  , handleErrorCase
  , ThrowErrors, throwErrors
  ) where

import GHC.Generics (Generic)
import Data.Functor.Identity
import Data.Kind (Type)
import Data.Store (Store)

import Control.Algebra
import Control.Algebra.Helpers (relayAlgebraUnwrap)
import Control.Carrier.MTL.TH (deriveMonadTrans)

import Control.Effect.Error

import Control.Monad.Trans        as MT
import Control.Monad.Trans.Except as MT


data family Errors (es :: [Type])

data instance Errors '[]
    deriving Generic

deriving instance Store (Errors '[])

data instance Errors (e ': es) = Error e | InjError (Errors es)
    deriving Generic

deriving instance (Store e, Store (Errors es)) => Store (Errors (e ': es))


class MemberError e es where
    injE :: e -> Errors es
    prjE :: Errors es -> Maybe e

instance {-# overlappable #-} MemberError e (e ': es) where
    injE = Error

    prjE (Error e) = Just e
    prjE _         = Nothing

instance {-# incoherent #-} MemberError e es => MemberError e (e' ': es) where
    injE = InjError . injE

    prjE (Error _)     = Nothing
    prjE (InjError es) = prjE es


newtype ExceptsT es m a = ExceptsT { unExceptsT :: MT.ExceptT (Errors es) m a }

deriveMonadTrans ''ExceptsT


runExceptsT :: ExceptsT es m a -> m (Either (Errors es) a)
runExceptsT (ExceptsT m) = MT.runExceptT m


runPureExceptsT :: Functor m => ExceptsT '[] m a -> m a
runPureExceptsT = fmap noLeft . runExceptsT
  where
    noLeft :: Either (Errors '[]) a -> a
    noLeft (Left e)  = case e of
    noLeft (Right a) = a


runLastExceptT :: Functor m => ExceptsT '[e] m a -> m (Either e a)
runLastExceptT (ExceptsT m) = MT.runExceptT (MT.withExceptT single m)
  where
    single :: Errors '[e] -> e
    single (Error e)    = e


class Algebra sig m => ThrowErrors es sig m | m -> sig where
    throwErrors :: Errors es -> m a


instance Algebra sig m => ThrowErrors '[] sig m where
    throwErrors e = case e of

instance (Has (Throw e) sig m, ThrowErrors es sig m) => ThrowErrors (e ': es) sig m where
    throwErrors (Error e)    = throwError e
    throwErrors (InjError e) = throwErrors e


handleErrorCase :: forall e es m a. Monad m => (e -> ExceptsT es m a) -> ExceptsT (e ': es) m a -> ExceptsT es m a
handleErrorCase h m = ExceptsT (MT.catchE (unExceptsT m) (\es -> handleE es h))
  where
    handleE :: Errors (e ': es) -> (e -> ExceptsT es m a) -> MT.ExceptT (Errors es) m a
    handleE (Error e)     h = unExceptsT (h e)
    handleE (InjError es) _ = MT.throwE es


interpretExceptsT :: (Monad m, MemberError e es) => Error e (ExceptsT es m) a -> ExceptsT es m a
interpretExceptsT (L (Throw e))          = ExceptsT (MT.throwE (injE e))
interpretExceptsT (R (Catch mb emb bmk)) =
    ExceptsT $ MT.catchE (unExceptsT mb) (\es -> handleE es emb) >>= unExceptsT . bmk
  where
    handleE es h =
        case prjE es of
          Just e  -> unExceptsT (h e)
          Nothing -> MT.throwE es



class ErrorsAlgebra sig es where
    algErrs :: Monad m => sig (ExceptsT es m) a -> ExceptsT es m a


instance MemberError e es => ErrorsAlgebra (Error e) es where
    algErrs = interpretExceptsT
    {-# inline algErrs #-}


instance (ErrorsAlgebra sig es, MemberError e es) => ErrorsAlgebra (Error e :+: sig) es where
    algErrs (L e)     = interpretExceptsT e
    algErrs (R other) = algErrs other
    {-# inline algErrs #-}


instance Algebra sig m => Algebra sig (ExceptsT '[] m) where
    alg = MT.lift . handleIdentity runPureExceptsT
    {-# inline alg #-}


instance
    ( Algebra sig m
    , Threads (Either (Errors '[e])) sig
    ) =>
    Algebra
        (Error e :+: sig)
        (ExceptsT '[e] m)
        where

    alg (L e)     = algErrs e
    alg (R other) = relayAlgebraUnwrap ExceptsT other
    {-# inline alg #-}


instance
    ( Algebra sig m
    , Algebra (errsig :+: sig) (ExceptsT (e2 ': es) m) -- only required for the functional dependency
    , ErrorsAlgebra errsig (e1 ': e2 ': es)
    , Threads Identity errsig
    , Threads (Either (Errors (e1 ': e2 ': es))) sig
    )
    => Algebra
        ((Error e1 :+: errsig) :+: sig)
        (ExceptsT (e1 ': e2 ': es) m)
        where

    alg (L e)     = algErrs e
    alg (R other) = relayAlgebraUnwrap ExceptsT other
    {-# inline alg #-}
