{-# language DeriveAnyClass #-}
{-# language DeriveGeneric #-}
{-# language EmptyCase #-}
{-# language QuantifiedConstraints #-}
{-# language StrictData #-}
{-# language TemplateHaskell #-}
{-# language UndecidableInstances #-}
module Control.Carrier.Error.Excepts
  ( module Control.Effect.Error
  , Errors(..)
  , KnownList
  , MemberError
  , ExceptsSig
  , ExceptsT(..)
  , runExceptsT
  , runLastExceptT
  , handleErrorCase
  ) where

import GHC.Generics (Generic)
import Data.Kind (Type)
import Data.Store (Store)

import Control.Carrier
import Control.Carrier.MTL (relayCarrierUnwrap)
import Control.Carrier.MTL.TH (deriveMonadTrans)

import Control.Effect.Error

import Control.Monad.Trans.Except as MT


data family Errors (es :: [Type])

data instance Errors '[]
    deriving Generic

deriving instance Store (Errors '[])

data instance Errors (e ': es) = Error e | InjError (Errors es)
    deriving Generic

deriving instance (Store e, Store (Errors es)) => Store (Errors (e ': es))


data SList as where
    SNil  :: SList '[]
    SCons :: SList as -> SList (a ': as)


class KnownList as where
    singList :: SList as

instance KnownList '[] where
    singList = SNil

instance KnownList as => KnownList (a ': as) where
    singList = SCons singList


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


type family ExceptsSig es sig where
    ExceptsSig '[]       sig = sig
    ExceptsSig (e ': es) sig = Error e :+: ExceptsSig es sig


newtype ExceptsT es m a = ExceptsT { unExceptsT :: MT.ExceptT (Errors es) m a }

deriveMonadTrans ''ExceptsT


runExceptsT :: ExceptsT es m a -> m (Either (Errors es) a)
runExceptsT (ExceptsT m) = MT.runExceptT m


runLastExceptT :: Functor m => ExceptsT '[e] m a -> m (Either e a)
runLastExceptT (ExceptsT m) = MT.runExceptT (MT.withExceptT single m)
  where
    single :: Errors '[e] -> e
    single (Error e)    = e
    single (InjError e) = case e of


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


instance (KnownList es, HFunctor sig', Handles (Either (Errors es)) sig, Carrier sig m, sig' ~ ExceptsSig es sig)
    => Carrier sig' (ExceptsT es m) where

    eff = go (singList @es)
      where
        go ::
            ( forall e. MemberError e es' => MemberError e es
            , Carrier sig m
            )
            => SList es'
            -> ExceptsSig es' sig (ExceptsT es m) a
            -> ExceptsT es m a
        go SNil         sig = relayCarrierUnwrap ExceptsT sig
        go (SCons sing) sig =
            case sig of
              L e     -> interpretExceptsT e
              R other -> go sing other
    {-# inline eff #-}
