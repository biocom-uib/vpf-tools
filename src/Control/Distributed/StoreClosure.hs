{-# language DeriveGeneric #-}
{-# language QuantifiedConstraints #-}
{-# language StaticPointers #-}
{-# language StrictData #-}
{-# language UndecidableInstances #-}
{-# language UndecidableSuperClasses #-}
module Control.Distributed.StoreClosure where

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


type Instance c = Closure (Dict c)

type Serializable a = (Store a, Typeable a)
type Serializer a = Instance (Serializable a)


data Closure a where
    Static :: StaticPtr a -> Closure a
    Pure   :: Serializer a -> a -> Closure a
    Apply  :: Closure (a -> b) -> Closure a -> Closure b


instance IsStatic Closure where
    fromStaticPtr = Static


class HasInstance c where
    staticInstance :: Instance c


instance {-# incoherent #-} Given (Instance c) => HasInstance c where
    staticInstance = given @(Instance c)


instance (Typeable a, HasInstance (Typeable a)) => HasInstance (Typeable (Closure a)) where
    staticInstance =
        let inst = staticInstance @(Typeable a)
        in withInstance inst $ mapClosure (static (\Dict -> Dict)) inst


instance Typeable a => HasInstance (Store (Closure a)) where
    staticInstance = static Dict


instance (Typeable c1, Typeable c2, HasInstance c1, HasInstance c2) => HasInstance (c1, c2) where
    staticInstance = liftC2 (static (\Dict Dict -> Dict)) (staticInstance @c1) (staticInstance @c2)


withInstance :: Instance c -> (c => a) -> a
withInstance inst a = case evalClosure inst of Dict -> a


reflectInstance :: Instance c -> (HasInstance c => a) -> a
reflectInstance inst = give inst


data Implies c1 c2 = (c1 => c2) => Impl


weakenInstance :: forall c2 c1.
    (Typeable c1, Typeable c2)
    => Closure (Implies c1 c2)
    -> Instance c1 -> Instance c2
weakenInstance = liftC2 (static (\Impl Dict -> Dict))



unsafePeekStaticPtr :: Peek (StaticPtr a)
unsafePeekStaticPtr = do
    key <- peek
    let Just ptr = unsafePerformIO $ unsafeLookupStaticPtr key
    return ptr


instance Store (Closure a) where
    size = VarSize $ \case
        Static ptr ->
            getSize @Word8 0 + getSize (staticKey ptr)

        Pure inst a ->
            getSize @Word8 1 + getSize inst + withInstance inst (getSize a)

        Apply fn val ->
            getSize @Word8 2 + getSize fn + getSize val

    poke = \case
        Static ptr -> do
            poke @Word8 0
            poke (staticKey ptr)

        Pure inst a -> do
            poke @Word8 1
            poke inst
            withInstance inst (poke a)

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
              Pure inst <$> withInstance inst peek
          2 -> Apply <$> peek <*> peek
          _ -> fail $ "invalid closure tag: " ++ show tag



pureClosure :: HasInstance (Serializable a) => a -> Closure a
pureClosure = Pure staticInstance


mapClosure :: Closure (a -> b) -> Closure a -> Closure b
mapClosure = Apply


liftC2 :: Closure (a -> b -> c) -> Closure a -> Closure b -> Closure c
liftC2 f a b = mapClosure (mapClosure f a) b


mapSerializable :: HasInstance (Serializable a) => Closure (a -> b) -> a -> Closure b
mapSerializable clo = Apply clo . pureClosure


evalClosure :: Closure a -> a
evalClosure (Pure _ a)     = a
evalClosure (Static ptr)   = deRefStaticPtr ptr
evalClosure (Apply fn val) = evalClosure fn (evalClosure val)


reduceClosure :: HasInstance (Serializable a) => Closure a -> Closure a
reduceClosure c =
    case c of
      Pure _ _  -> c
      Static _  -> c
      Apply _ _ -> pureClosure (evalClosure c)


data DynClosure (f :: Type -> Type) = forall a.
    Typeable a => DynClosure (Closure (TypeRep a)) (Closure (f a))


getSomeTyRepClo :: forall (a :: Type). Typeable a => Closure (TypeRep a) -> Closure SomeTypeRep
getSomeTyRepClo = mapClosure (static SomeTypeRep)


reflectTypeable :: TypeRep a -> Dict (Typeable a)
reflectTypeable rep = withTypeable rep Dict


unsafeCoerceTypeRepClo :: forall (a :: Type). Typeable a => Closure SomeTypeRep -> Closure (TypeRep a)
unsafeCoerceTypeRepClo =
    mapClosure $ static (\(SomeTypeRep rep') -> unsafeCoerce rep')


instance Store (DynClosure f) where
    size = VarSize $ \case
        DynClosure tyRepClo a ->
            getSize (getSomeTyRepClo tyRepClo) + getSize a

    poke (DynClosure tyRepClo a) = do
        poke (getSomeTyRepClo tyRepClo)
        poke a

    peek = do
        sTyRepClo <- peek

        case evalClosure sTyRepClo of
          SomeTypeRep (rep :: TypeRep a)
            | Just HRefl <- eqTypeRep (typeRepKind rep) (typeRep @Type) ->
                withTypeable rep $
                    let tyRepClo = unsafeCoerceTypeRepClo @a sTyRepClo
                    in  DynClosure tyRepClo <$> peek

            | otherwise -> fail $ "invalid typeRep: " ++ show rep


buildSomeTypeRepClo :: forall (a :: Type). Instance (Typeable a) -> Closure SomeTypeRep
buildSomeTypeRepClo inst =
    withInstance inst $
        static (\Dict -> SomeTypeRep (typeRep @a)) `mapClosure` inst


makeDynClosure :: forall a f. HasInstance (Typeable a) => Closure (f a) -> DynClosure f
makeDynClosure =
    let inst = staticInstance @(Typeable a)
    in  withInstance inst $
          let tyRepClo = mapClosure (static (\Dict -> typeRep @a)) inst
          in  DynClosure tyRepClo


fromDynClosure :: forall a f. Typeable a => DynClosure f -> Maybe (Closure (f a))
fromDynClosure (DynClosure tyRepClo clo) =
    case eqTypeRep (typeRep @a) (evalClosure tyRepClo) of
      Just HRefl -> Just clo
      Nothing    -> Nothing


withDynClosure :: DynClosure f -> (forall a. HasInstance (Typeable a) => f a -> r) -> r
withDynClosure (DynClosure tyRepClo clo) g =
    reflectInstance (mapClosure (static reflectTypeable) tyRepClo) $
        g (evalClosure clo)
