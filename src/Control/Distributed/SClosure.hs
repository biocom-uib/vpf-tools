{-# language DeriveGeneric #-}
{-# language QuantifiedConstraints #-}
{-# language StaticPointers #-}
{-# language StrictData #-}
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
  , liftS2
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


liftS2 :: SClosure (a -> b -> c) -> SClosure a -> SClosure b -> SClosure c
liftS2 f a b = smap (smap f a) b


seval :: SClosure a -> a
seval (Pure _ a)     = a
seval (Static ptr)   = deRefStaticPtr ptr
seval (Apply fn val) = seval fn (seval val)


unsafePeekStaticPtr :: Peek (StaticPtr a)
unsafePeekStaticPtr = do
    key <- peek

    case unsafePerformIO (unsafeLookupStaticPtr key) of
      Just ptr -> return ptr
      Nothing  -> fail $ "invalid StaticKey: " ++ show key


instance Store (SClosure a) where
    size = VarSize $ \case
        Static ptr ->
            getSize @Word8 0 + getSize (staticKey ptr)

        Pure inst a ->
            getSize @Word8 1 + getSize inst + withSDict inst (getSize a)

        Apply fn val ->
            getSize @Word8 2 + getSize fn + getSize val

    poke = \case
        Static ptr -> do
            poke @Word8 0
            poke (staticKey ptr)

        Pure inst a -> do
            poke @Word8 1
            poke inst
            withSDict inst (poke a)

        Apply fn val -> do
            poke @Word8 2
            poke fn
            poke val

    peek = do
        tag <- peek @Word8

        case tag of
          0 -> Static <$> unsafePeekStaticPtr
          1 -> do
              inst <- peek
              Pure inst <$> withSDict inst peek
          2 -> Apply <$> peek <*> peek
          _ -> fail $ "invalid closure tag: " ++ show tag



class SInstance c where
    sinst :: SDict c

instance {-# incoherent #-} Given (SDict c) => SInstance c where
    sinst = given @(SDict c)

instance (Typeable a, SInstance (Typeable a)) => SInstance (Typeable (SClosure a)) where
    sinst =
        let inst = sinst @(Typeable a)
        in withSDict inst $ smap (static (\Dict -> Dict)) inst

instance Typeable a => SInstance (Store (SClosure a)) where
    sinst = static Dict

instance (Typeable c1, Typeable c2, SInstance c1, SInstance c2) => SInstance (c1, c2) where
    sinst = liftS2 (static (\Dict Dict -> Dict)) (sinst @c1) (sinst @c2)


data Implies c1 c2 = (c1 => c2) => Impl


withSDict :: SDict c -> (c => a) -> a
withSDict inst a = case seval inst of Dict -> a


weakenSDict :: forall c2 c1.  (Typeable c1, Typeable c2) => SClosure (Implies c1 c2) -> SDict c1 -> SDict c2
weakenSDict = liftS2 (static (\Impl Dict -> Dict))


reflectSDict :: SDict c -> (SInstance c => a) -> a
reflectSDict inst = give inst



data SDynClosure (f :: Type -> Type) = forall a.
    Typeable a => SDynClosure (SClosure (TypeRep a)) (SClosure (f a))


getSomeTyRepClo :: forall (a :: Type). Typeable a => SClosure (TypeRep a) -> SClosure SomeTypeRep
getSomeTyRepClo = smap (static SomeTypeRep)


reflectTypeable :: TypeRep a -> Dict (Typeable a)
reflectTypeable rep = withTypeable rep Dict


unsafeCoerceTypeRepClo :: forall (a :: Type). Typeable a => SClosure SomeTypeRep -> SClosure (TypeRep a)
unsafeCoerceTypeRepClo =
    smap $ static (\(SomeTypeRep rep') -> unsafeCoerce rep')


instance Store (SDynClosure f) where
    size = VarSize $ \case
        SDynClosure tyRepClo a ->
            getSize (getSomeTyRepClo tyRepClo) + getSize a

    poke (SDynClosure tyRepClo a) = do
        poke (getSomeTyRepClo tyRepClo)
        poke a

    peek = do
        sTyRepClo <- peek

        case seval sTyRepClo of
          SomeTypeRep (rep :: TypeRep a)
            | Just HRefl <- eqTypeRep (typeRepKind rep) (typeRep @Type) ->
                withTypeable rep $
                    let tyRepClo = unsafeCoerceTypeRepClo @a sTyRepClo
                    in  SDynClosure tyRepClo <$> peek

            | otherwise -> fail $ "invalid typeRep: " ++ show rep


makeSDynClosure :: forall a f. (Typeable a, SInstance (Typeable a)) => SClosure (f a) -> SDynClosure f
makeSDynClosure =
    let inst = sinst @(Typeable a)
    in  withSDict inst $
          let tyRepClo = smap (static (\Dict -> typeRep @a)) inst
          in  SDynClosure tyRepClo


fromSDynClosure :: forall a f. Typeable a => SDynClosure f -> Maybe (SClosure (f a))
fromSDynClosure (SDynClosure tyRepClo clo) =
    case eqTypeRep (typeRep @a) (seval tyRepClo) of
      Just HRefl -> Just clo
      Nothing    -> Nothing


withSDynClosure :: SDynClosure f -> (forall a. SInstance (Typeable a) => f a -> r) -> r
withSDynClosure (SDynClosure tyRepClo clo) g =
    reflectSDict (smap (static reflectTypeable) tyRepClo) $
        g (seval clo)
