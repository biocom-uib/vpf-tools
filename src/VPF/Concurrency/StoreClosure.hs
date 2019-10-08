{-# language DeriveGeneric #-}
{-# language StaticPointers #-}
{-# language StrictData #-}
module VPF.Concurrency.StoreClosure where

import GHC.StaticPtr

import Data.Constraint
import Data.Functor.Identity
import Data.Kind
import Data.Store
import Data.Store.Internal
import Data.Word
import Type.Reflection

import System.IO.Unsafe (unsafePerformIO)


type StaticInstance c = StaticPtr (Dict c)
type Serializable a = (Store a, Typeable a)
type Serializer a = StaticInstance (Serializable a)

withStaticInstance :: StaticInstance c -> (c => a) -> a
withStaticInstance inst a = case deRefStaticPtr inst of Dict -> a


data Closure m a where
    Static :: StaticPtr a -> Closure m a

    Pure :: Serializer a -> a -> Closure m a

    Apply :: Closure m (a -> b)
          -> Closure m a
          -> Closure m b

    Bind :: Closure m (a -> m b)
         -> Closure m a
         -> Closure m b


instance IsStatic (Closure m) where
    fromStaticPtr = Static


class HasStaticInstance c where
    staticInstance :: StaticInstance c


pureClosureWith :: Serializer a -> a -> Closure m a
pureClosureWith = Pure

pureClosure :: HasStaticInstance (Serializable a) => a -> Closure m a
pureClosure = pureClosureWith staticInstance

mapClosure :: Closure m (a -> b) -> Closure m a -> Closure m b
mapClosure = Apply

bindClosure :: Closure m (a -> m b) -> Closure m a -> Closure m b
bindClosure = Bind


evalClosure :: Monad m => Closure m a -> m a
evalClosure (Pure _ a)       = pure a
evalClosure (Static ptr)     = pure (deRefStaticPtr ptr)
evalClosure (Apply fn val) = evalClosure fn <*> evalClosure val
evalClosure (Bind fn val)  = evalClosure fn >>= (=<< evalClosure val)


simplifyClosureWith :: Monad m => Serializer a -> Closure m a -> m (Closure m a)
simplifyClosureWith _    c@(Pure _ _)  = pure c
simplifyClosureWith _    c@(Static _)  = pure c
simplifyClosureWith inst c@(Apply _ _) = pureClosureWith inst <$> evalClosure c
simplifyClosureWith inst c@(Bind _ _)  = pureClosureWith inst <$> evalClosure c


simplifyClosure :: (HasStaticInstance (Serializable a), Monad m) => Closure m a -> m (Closure m a)
simplifyClosure = simplifyClosureWith staticInstance


unsafePeekStaticPtr :: Peek (StaticPtr a)
unsafePeekStaticPtr = do
    key <- peek
    let Just ptr = unsafePerformIO $ unsafeLookupStaticPtr key
    return ptr


instance Store (Closure m a) where
    size = VarSize $ \case
        Static ptr ->
            getSize @Word8 0 + getSize (staticKey ptr)

        Pure inst a -> withStaticInstance inst $
            getSize @Word8 1 + getSize (staticKey inst) + getSize a

        Apply fn val ->
            getSize @Word8 2 + getSize fn + getSize val

        Bind fn val ->
            getSize @Word8 3 + getSize fn + getSize val

    poke = \case
        Static ptr -> do
            poke @Word8 0
            poke (staticKey ptr)

        Pure inst a -> do
            poke @Word8 1
            poke (staticKey inst)
            withStaticInstance inst $ poke a

        Apply fn val -> do
            poke @Word8 2
            poke fn
            poke val

        Bind fn val -> do
            poke @Word8 3
            poke fn
            poke val

    peek = do
        tag <- peek @Word8

        case tag of
          0 -> Static <$> unsafePeekStaticPtr
          1 -> do
              inst <- unsafePeekStaticPtr
              withStaticInstance @(Serializable a) inst $
                  Pure inst <$> peek

          2 -> Apply <$> peek <*> peek
          3 -> Bind  <$> peek <*> peek
          _ -> fail $ "invalid closure tag: " ++ show tag


data DynClosure m = forall a.
    DynClosure (Closure Identity SomeTypeRep) (Serializer a) (Closure m a)


instance Store (DynClosure m) where
    size = VarSize $ \case
        DynClosure sTyRepClo inst a ->
            getSize sTyRepClo + getSize (staticKey inst) + getSize a

    poke (DynClosure sTyRepClo inst a) = do
        poke sTyRepClo
        poke (staticKey inst)
        poke a

    peek = do
        sTyRepClo <- peek

        case runIdentity $ evalClosure sTyRepClo of
          SomeTypeRep (rep :: TypeRep a)
            | Just HRefl <- eqTypeRep (typeRepKind rep) (typeRep @Type) ->
                DynClosure @m @a sTyRepClo <$> unsafePeekStaticPtr <*> peek

            | otherwise -> fail $ "invalid typeRep: " ++ show rep


buildSomeTypeRepClo :: forall a m. Serializer a -> Closure m SomeTypeRep
buildSomeTypeRepClo inst =
    case deRefStaticPtr inst of
      Dict -> mapClosure (static dictSomeTyRep) (Static inst)
  where
    dictSomeTyRep :: forall b. Dict (Serializable b) -> SomeTypeRep
    dictSomeTyRep Dict = SomeTypeRep (typeRep @b)


makeDynClosure :: Serializer a -> Closure m a -> DynClosure m
makeDynClosure inst = DynClosure (buildSomeTypeRepClo inst) inst


fromDynClosure :: forall a m. Typeable a => DynClosure m -> Maybe (Closure m a)
fromDynClosure (DynClosure _ inst (clo :: Closure m b)) =
    case deRefStaticPtr inst of
      Dict ->
          case eqTypeRep (typeRep @a) (typeRep @b) of
            Just HRefl -> Just clo
            Nothing    -> Nothing


simplifyDynClosure :: Monad m => DynClosure m -> m (DynClosure m)
simplifyDynClosure (DynClosure sTyRepClo inst a) =
    DynClosure sTyRepClo inst <$> simplifyClosureWith inst a
