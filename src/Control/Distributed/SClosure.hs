{-# options_ghc -ddump-simpl -dsuppress-idinfo
-dsuppress-coercions -dsuppress-type-applications
-dsuppress-uniques -dsuppress-module-prefixes #-}

{-# language DeriveGeneric #-}
{-# language QuantifiedConstraints #-}
{-# language StaticPointers #-}
{-# language Strict #-}
{-# language UndecidableInstances #-}
{-# language UndecidableSuperClasses #-}
module Control.Distributed.SClosure
  ( Serializable
  , SClosure
  , Dict(..)
  , SDict
  , Implies(..)
  , Typeable
  , spureWith
  , spure
  , smap
  , (<:*>)
  , slift2
  , seval
  , withSDict
  , weakenSDict
  , SInstance(..)
  , reflectSDict
  , SDynClosure
  , makeSDynClosure
  , fromSDynClosure
  , withSDynClosure
  ) where

import GHC.StaticPtr

import Data.Constraint
import Data.Kind
import Data.Reflection
import Data.Store
import Data.Store.Internal
import Data.Word
import Type.Reflection

import System.IO.Unsafe (unsafePerformIO)
import Unsafe.Coerce (unsafeCoerce)


type Serializable a = (Store a, Typeable a)


data SClosure a where
    Static :: StaticPtr a -> SClosure a
    Pure   :: SDict (Serializable a) -> a -> SClosure a
    Apply  :: SClosure (a -> b) -> SClosure a -> SClosure b


instance IsStatic SClosure where
    fromStaticPtr = Static



data SClosureShow a =
    Static_
    | Pure_ (TypeRep a)
    | forall x. Apply_ (SClosureShow (x -> a)) (SClosureShow x)

deriving instance Show (SClosureShow a)


sshow :: SClosure a -> SClosureShow a
sshow (Static _)     = Static_
sshow (Pure sdict _) = withSDict sdict (Pure_ typeRep)
sshow (Apply f a)    = Apply_ (sshow f) (sshow a)


instance Show (SClosure a) where
    show = show . sshow


type SDict c = SClosure (Dict c)


spureWith :: SDict (Serializable a) -> a -> SClosure a
spureWith = Pure


spure :: SInstance (Serializable a) => a -> SClosure a
spure = spureWith sinst


smap :: SClosure (a -> b) -> SClosure a -> SClosure b
smap = Apply


(<:*>) :: SClosure (a -> b) -> SClosure a -> SClosure b
(<:*>) = smap


infixl 4 <:*>


slift2 :: SClosure (a -> b -> c) -> SClosure a -> SClosure b -> SClosure c
slift2 f a b = smap (smap f a) b


seval :: SClosure a -> a
seval (Pure _ a)     = a
seval (Static ptr)   = deRefStaticPtr ptr
seval (Apply fn val) = seval fn (seval val)


instance Store (SClosure a) where
    size = VarSize $ \case
        Static ptr ->
            getSize @Word8 0 + getSize (staticKey ptr)

        Pure sdict a ->
            getSize @Word8 1 + getSize sdict + withSDict sdict (getSize a)

        Apply fn val ->
            getSize @Word8 2 + getSize fn + getSize val

    poke = \case
        Static ptr -> do
            poke @Word8 0
            poke (staticKey ptr)

        Pure sdict a -> do
            poke @Word8 1
            poke sdict
            withSDict sdict (poke a)

        Apply fn val -> do
            poke @Word8 2
            poke fn
            poke val

    peek = do
        tag <- peek @Word8

        case tag of
          0 -> do
              key <- peek

              case unsafePerformIO (unsafeLookupStaticPtr key) of
                Just ptr -> return (Static ptr)
                Nothing  -> fail $ "invalid StaticKey: " ++ show key

          1 -> do
              sdict <- peek
              Pure sdict <$> withSDict sdict peek

          2 -> Apply <$> peek <*> peek
          _ -> fail $ "invalid closure tag: " ++ show tag



class SInstance c where
    sinst :: SDict c

instance {-# incoherent #-} Given (SDict c) => SInstance c where
    sinst = given @(SDict c)

instance (Typeable a, SInstance (Typeable a)) => SInstance (Typeable (SClosure a)) where
    sinst =
        let sdict = sinst @(Typeable a)
        in withSDict sdict $ smap (static (\Dict -> Dict)) sdict

instance Typeable a => SInstance (Store (SClosure a)) where
    sinst = static Dict

instance (Typeable c1, Typeable c2, SInstance c1, SInstance c2) => SInstance (c1, c2) where
    sinst = slift2 (static (\Dict Dict -> Dict)) (sinst @c1) (sinst @c2)


data Implies c1 c2 = (c1 => c2) => Impl


withSDict :: SDict c -> (c => a) -> a
withSDict sdict a = case seval sdict of Dict -> a


weakenSDict :: forall c2 c1.  (Typeable c1, Typeable c2) => SClosure (Implies c1 c2) -> SDict c1 -> SDict c2
weakenSDict = slift2 (static (\Impl Dict -> Dict))


reflectSDict :: SDict c -> (SInstance c => a) -> a
reflectSDict = give



data SDynClosure (f :: Type -> Type) = forall a.
    Typeable a => SDynClosure (SClosure (TypeRep a)) (SClosure (f a))


data SDynClosureShow f = forall a. SDynClosure_ (SClosureShow (f a)) (TypeRep (f a))

deriving instance Show (SDynClosureShow f)


instance Typeable f => Show (SDynClosure f) where
    show (SDynClosure _ clo) = show $ SDynClosure_ (sshow clo) (App typeRep typeRep)


sreflectTypeable :: forall (a :: Type). Typeable a => SClosure (TypeRep a -> Dict (Typeable a))
sreflectTypeable = static (\rep -> withTypeable rep Dict)
{-# noinline sreflectTypeable #-}


sSomeTypeRep :: forall (a :: Type). Typeable a => SClosure (TypeRep a -> SomeTypeRep)
sSomeTypeRep = static (SomeTypeRep @Type @a)
{-# noinline sSomeTypeRep #-}


sUnsafeUnSomeTypeRep :: forall (a :: Type). Typeable a => SClosure (SomeTypeRep -> TypeRep a)
sUnsafeUnSomeTypeRep = static (\(SomeTypeRep rep) -> unsafeCoerce rep :: TypeRep a)
{-# noinline sUnsafeUnSomeTypeRep #-}


instance Store (SDynClosure f) where
    size = VarSize $ \case
        SDynClosure tyRepClo a ->
            getSize (sSomeTypeRep <:*> tyRepClo) + getSize a

    poke (SDynClosure tyRepClo a) = do
        poke (sSomeTypeRep <:*> tyRepClo)
        poke a

    peek = do
        sTyRepClo <- peek

        case seval sTyRepClo of
          SomeTypeRep (rep :: TypeRep a)
            | Just HRefl <- eqTypeRep (typeRepKind rep) (typeRep @Type) ->
                withTypeable rep $ do
                    let tyRepClo = sUnsafeUnSomeTypeRep @a <:*> sTyRepClo
                    SDynClosure tyRepClo <$> peek

            | otherwise -> fail $ "invalid typeRep: " ++ show rep


makeSDynClosure :: forall a f. (Typeable a, SInstance (Typeable a)) => SClosure (f a) -> SDynClosure f
makeSDynClosure =
    let sdict = sinst @(Typeable a)
    in  withSDict sdict $
          let tyRepClo = smap (static (\Dict -> typeRep @a)) sdict
          in  SDynClosure tyRepClo


fromSDynClosure :: forall a f. Typeable a => SDynClosure f -> Maybe (SClosure (f a))
fromSDynClosure (SDynClosure tyRepClo clo) =
    case eqTypeRep (typeRep @a) (seval tyRepClo) of
      Just HRefl -> Just clo
      Nothing    -> Nothing


withSDynClosure :: SDynClosure f -> (forall (a :: Type). SInstance (Typeable a) => f a -> r) -> r
withSDynClosure (SDynClosure tyRepClo clo) g =
    reflectSDict (sreflectTypeable <:*> tyRepClo) $
        g (seval clo)
