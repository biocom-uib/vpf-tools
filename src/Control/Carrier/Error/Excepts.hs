{-# language DeriveAnyClass #-}
{-# language DeriveGeneric #-}
{-# language EmptyCase #-}
{-# language QuantifiedConstraints #-}
{-# language Strict #-}
{-# language TemplateHaskell #-}
{-# language UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}
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
import Data.Kind (Constraint, Type)
import Data.Store (Store)

import Control.Algebra
import Control.Algebra.Helpers (relayAlgebraUnwrap)
import Control.Carrier.MTL.TH (deriveMonadTrans)

import Control.Effect.Error
import Control.Effect.Sum.Extra

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


class Algebra Identity m => ThrowErrors es m where
    throwErrors :: Errors es -> m a


instance Algebra Identity m => ThrowErrors '[] m where
    throwErrors e = case e of

instance (Has (Throw e) m, ThrowErrors es m) => ThrowErrors (e ': es) m where
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
interpretExceptsT (R (Catch mb emb)) =
    ExceptsT $ MT.catchE (unExceptsT mb) (\es -> handleE es emb)
  where
    handleE es h =
        case prjE es of
          Just e  -> unExceptsT (h e)
          Nothing -> MT.throwE es



type family ErrorsSig es where
    ErrorsSig (e ': '[]) = Error e
    ErrorsSig (e1 ': e2 ': es) = Error e1 :+: ErrorsSig (e2 ': es)


type ErrorsAlgebra :: ((Type -> Type) -> Type -> Type) -> [Type] -> Constraint

class ErrorsAlgebra sig es where
    algErrs :: Monad m => sig (ExceptsT es m) a -> ExceptsT es m a


instance MemberError e es => ErrorsAlgebra (Error e) es where
    algErrs = interpretExceptsT
    {-# inline algErrs #-}


instance (ErrorsAlgebra sig es, MemberError e es) => ErrorsAlgebra (Error e :+: sig) es where
    algErrs (L e)     = interpretExceptsT e
    algErrs (R other) = algErrs other
    {-# inline algErrs #-}

instance Algebra ctx m => Algebra ctx (ExceptsT '[] m) where
    type Sig (ExceptsT '[] m) = Sig m

    alg hdl sig ctx = MT.lift $ alg (runPureExceptsT . hdl) sig ctx
    {-# inline alg #-}


instance (Algebra ctx m, ErrorsAlgebra (ErrorsSig (e ': es)) es)
    => Algebra ctx (ExceptsT (e ': es) m) where

    type Sig (ExceptsT (e ': es) m) = ErrorsSig (e ': es) :+: Sig m

    alg hdl (L e)     ctx = _ $ algErrs @_ @(e ': es) @m e
    alg hdl (R other) ctx = relayAlgebraUnwrap ExceptsT hdl other ctx
    {-# inline alg #-}


--instance
--    ( Algebra ctx m
--    --, Algebra (errsig :+: sig) (ExceptsT (e2 ': es) m) -- only required for the functional dependency
--    , ErrorsAlgebra errsig (e1 ': e2 ': es)
--    --, Threads Identity errsig
--    --, Threads (Either (Errors (e1 ': e2 ': es))) sig
--    )
--    => Algebra
--        ctx
--        (ExceptsT (e1 ': e2 ': es) m)
--        where
--    --((Error e1 :+: errsig) :+: sig)
--    type Sig (ExceptsT (e1 ': e2 ': es) m) =
--        (Error e1 :+: SumFst (Sig (ExceptsT (e2 ': es) m)))
--        :+:
--        Sig m
--
--    alg hdl (L e)     ctx = algErrs e
--    alg hdl (R other) ctx = relayAlgebraUnwrap ExceptsT other
--    {-# inline alg #-}
