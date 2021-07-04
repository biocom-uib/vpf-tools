{-# language DeriveAnyClass #-}
{-# language DeriveGeneric #-}
{-# language EmptyCase #-}
{-# language QuantifiedConstraints #-}
{-# language Strict #-}
{-# language TemplateHaskell #-}
{-# language UndecidableInstances #-}
{-# LANGUAGE UndecidableSuperClasses #-}
{-# LANGUAGE InstanceSigs #-}
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
import Data.Kind (Constraint, Type)
import Data.Store (Store)

import Control.Algebra
import Control.Algebra.Helpers
import Control.Carrier.MTL.TH (deriveMonadTrans)

import Control.Effect.Error

import Control.Monad.Trans        as MT
import Control.Monad.Trans.Except as MT
import Control.Effect.Sum.Extra (injR)


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


handleErrorCase :: forall e es m a.
    Monad m
    => (e -> ExceptsT es m a)
    -> ExceptsT (e ': es) m a
    -> ExceptsT es m a
handleErrorCase h m =
    ExceptsT (MT.catchE (unExceptsT m) (\es -> handleE es h))
  where
    handleE :: Errors (e ': es) -> (e -> ExceptsT es m a) -> MT.ExceptT (Errors es) m a
    handleE (Error e)     h = unExceptsT (h e)
    handleE (InjError es) _ = MT.throwE es


class ThrowErrors es m where
    throwErrors :: Errors es -> m a

instance ThrowErrors '[] m where
    throwErrors e = case e of

instance (Has (Throw e) m, ThrowErrors es m) => ThrowErrors (e ': es) m where
    throwErrors (Error e)    = throwError e
    throwErrors (InjError e) = throwErrors e


type family ErrorsSig es where
    ErrorsSig (e ': '[]) = Error e
    ErrorsSig (e1 ': e2 ': es) = Error e1 :+: ErrorsSig (e2 ': es)



algErr :: forall e es m n ctx a.
    (Functor ctx, Monad m, MemberError e es)
    => Handler ctx n (ExceptsT es m)
    -> Error e n a
    -> ctx ()
    -> ExceptsT es m (ctx a)
algErr hdl sig ctx =
    case sig of
        L (Throw e)      -> throwEs e
        R (Catch na ena) -> catchEs na ena
  where
    throwEs :: e -> ExceptsT es m (ctx a)
    throwEs = ExceptsT . MT.throwE . injE

    hdl_ :: forall a. n a -> ExceptsT es m (ctx a)
    hdl_ = hdl . (<$ ctx)

    catchEs :: n a -> (e -> n a) -> ExceptsT es m (ctx a)
    catchEs na ena =
        ExceptsT $
            MT.catchE (unExceptsT $ hdl_ na) \es ->
                unExceptsT $ handleEs es ena

    handleEs :: Errors es -> (e -> n a) -> ExceptsT es m (ctx a)
    handleEs es h =
        case prjE es of
          Just e  -> hdl_ (h e)
          Nothing -> ExceptsT $ MT.throwE es



type ErrorsAlgebra :: ((Type -> Type) -> Type -> Type) -> [Type] -> Constraint

class ErrorsAlgebra sig es where
    algErrs :: (Functor ctx, Monad m) => Handler ctx n (ExceptsT es m) -> sig n a -> ctx () -> ExceptsT es m (ctx a)


instance MemberError e es => ErrorsAlgebra (Error e) es where
    algErrs = algErr
    {-# inline algErrs #-}


instance (ErrorsAlgebra sig es, MemberError e es) => ErrorsAlgebra (Error e :+: sig) es where
    algErrs hdl (L e)     ctx = algErr hdl e ctx
    algErrs hdl (R other) ctx = algErrs hdl other ctx
    {-# inline algErrs #-}


instance Algebra ctx m => Algebra ctx (ExceptsT '[] m) where
    type Sig (ExceptsT '[] m) = Sig m

    alg = relayAlgebraIso MT.lift runPureExceptsT
    {-# inline alg #-}


instance
    ( Algebra1 Functor m
    , Functor ctx
    , ErrorsAlgebra (ErrorsSig (e ': es)) (e ': es)
    )
    => Algebra ctx (ExceptsT (e ': es) m) where

    type Sig (ExceptsT (e ': es) m) = ErrorsSig (e ': es) :+: Sig m

    alg = algUnwrapL injR ExceptsT algErrs
    {-# inline alg #-}
