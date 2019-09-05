{-# language AllowAmbiguousTypes #-}
{-# language InstanceSigs #-}
{-# language UndecidableInstances #-}
module VPF.Util.Vinyl
  ( ElFieldStore(..)
  , VinylStore(..)
  , FieldSpec
  , NameSpec
  , InferFieldKind
  , MonoFieldSpec
  , IncSeq
  , DecSeq
  , Monotone
  , RecSubseq(..)
  , RSubseq
  , rsubseq_
  , rsubseq
  , RecQuotient
  , RQuotient
  , rquotient
  , rquotientSplit
  , rquotientSplit'
  , over_rsubset
  , rsubset_
  , rsubset'
  , rreordered
  , rrename
  , renameField
  , renameFieldTo
  , renamedField
  ) where

import GHC.TypeLits (KnownSymbol, Symbol)

import Control.Lens (Iso, Iso', from, iso, withIso, Lens, lens, view, Setter, over, set, sets)

import Data.Bifunctor (first, second)
import Data.Kind (Type)
import Data.Functor.Contravariant (contramap)
import Data.Store (Store, Size(..))
import qualified Data.Store as Store
import qualified Data.Type.Equality as E

import Data.Vinyl (ElField(..), Rec(..), (<+>))
import Data.Vinyl.Derived (KnownField, FieldType)
import Data.Vinyl.Functor (Compose(..))
import Data.Vinyl.Lens (RecElem, RecElemFCtx,
                        rlens', type (<:), type (:~:), rcast)
import Data.Vinyl.TypeLevel

import Frames.Melt (RDeleteAll)


newtype ElFieldStore rs = ElFieldStore (ElField rs)
newtype VinylStore record f rs = VinylStore (record f rs)

instance (KnownField rs, Store (Snd rs)) => Store (ElFieldStore rs) where
    size = contramap (\(ElFieldStore (Field a)) -> a)  Store.size
    peek = fmap (ElFieldStore . Field) Store.peek
    poke (ElFieldStore (Field a)) = Store.poke a

instance Store (VinylStore Rec f '[]) where
    size = ConstSize 0
    peek = return (VinylStore RNil)
    poke _ = return ()


instance (Store (f r), Store (VinylStore Rec f rs)) => Store (VinylStore Rec f (r ': rs)) where
    size =
        case (Store.size, Store.size) of
          (VarSize f, VarSize g)     -> VarSize (\(VinylStore (r :& rs)) -> f r + g (VinylStore rs))
          (VarSize f, ConstSize m)   -> VarSize (\(VinylStore (r :& _))  -> f r + m)
          (ConstSize n, VarSize g)   -> VarSize (\(VinylStore (_ :& rs)) -> n + g (VinylStore rs))
          (ConstSize n, ConstSize m) -> ConstSize (n + m)

    peek = do
      r <- Store.peek
      VinylStore rs <- Store.peek
      return (VinylStore (r :& rs))

    poke (VinylStore (r :& rs)) = do
      Store.poke r
      Store.poke (VinylStore rs)



type family ProjField (ki :: Type) (kr :: Type) (rs :: [(Symbol, Type)]) (i :: ki) :: kr where
  ProjField (Symbol, Type) (Symbol, Type)  rs  '(s, a)   = '(s, a)
  ProjField Symbol         (Symbol, Type)  rs  s         = '(s, FieldType s rs)
  ProjField (Symbol, Type) Symbol          rs  '(s, a)   = s
  ProjField Symbol         Symbol          rs  s         = s
  ProjField (Symbol, Type) [kr]            rs  '(s, a)   = ProjField [(Symbol, Type)] [kr] rs '[ '(s, a)]
  ProjField Symbol         [kr]            rs  s         = ProjField [Symbol] [kr] rs '[s]
  ProjField [k]            [k]             rs  r         = r
  ProjField [ki]           [kr]            rs  '[]       = '[]
  ProjField [ki]           [kr]            rs  (i ': is) = ProjField ki kr rs i ': ProjField [ki] [kr] rs is

type FieldSpec rs (i :: ki) (r :: kr) = ProjField ki kr rs i ~ r


type family InferFieldKind (ki :: Type) = (kr :: Type) where
  InferFieldKind [ki] = [InferFieldKind ki]
  InferFieldKind ki   = (Symbol, Type)

type MonoFieldSpec rs (i :: ki) (r :: kr) = (InferFieldKind ki ~ kr, ProjField ki kr rs i ~ r)


type family ProjName (ki :: Type) (i :: ki) :: Symbol
type instance ProjName Symbol         s       = s
type instance ProjName (Symbol, Type) '(s, a) = s

type NameSpec (i :: ki) s = ProjName ki i ~ s


type family IfEmpty (xs :: [k1]) (a :: k) (b :: k) :: k where
  IfEmpty '[]       a b = a
  IfEmpty (x ': xs) a b = b


over_rsubset :: forall ss ss' rs f.
             ( ss <: rs
             , RDeleteAll ss rs <: rs
             , ss ++ RDeleteAll ss rs :~: rs
             )
             => (Rec f ss -> Rec f ss')
             -> Rec f rs
             -> Rec f (ss' ++ RDeleteAll ss rs)
over_rsubset f rec =
    f (rcast @ss rec) <+> rcast @(RDeleteAll ss rs) rec
{-# inline over_rsubset #-}


rsubset_ :: forall (ss :: [(Symbol, Type)]) ss' rs f.
         ( ss <: rs
         , RDeleteAll ss rs <: rs
         , ss ++ RDeleteAll ss rs :~: rs
         )
         => Setter (Rec f rs) (Rec f (ss' ++ RDeleteAll ss rs))
                   (Rec f ss) (Rec f ss')
rsubset_ = sets over_rsubset
{-# inline rsubset_ #-}


rsubset' :: forall is ss ss' rs f.
         ( FieldSpec rs is ss
         , ss <: rs
         , RDeleteAll ss rs <: rs
         , ss ++ RDeleteAll ss rs :~: rs
         )
         => Setter (Rec f rs) (Rec f (ss' ++ RDeleteAll ss rs))
                   (Rec f ss) (Rec f ss')
rsubset' = rsubset_
{-# inline rsubset' #-}


type family IncSeq is = dec_is | dec_is -> is where
  IncSeq '[] = '[]
  IncSeq (i ': is) = S i ': IncSeq is

type DecSeq is dec_is = IncSeq dec_is ~ is


-- strictly monotone
class Monotone (is :: [Nat])

instance Monotone '[]
instance (DecSeq is' dec_is', Monotone dec_is') => Monotone (Z ': is')
instance (DecSeq is' dec_is', Monotone (dec_i ': dec_is')) => Monotone (S dec_i ': is')



class (Monotone is, is ~ RImage ss rs) => RecSubseq rec f ss ss' rs rs' (is :: [Nat])
    | is rs -> ss
    , is ss rs' -> rs
    , is ss' rs -> rs'
    where
  rsubseqC :: Lens (rec f rs) (rec f rs') (rec f ss) (rec f ss')

  rsubseqSplitC :: ss' E.:~: '[] -> Iso' (rec f rs) (rec f ss, rec f rs')


-- RecSubseq implementation helpers

type family DiscriminateEmpty xs a b where
  DiscriminateEmpty '[]       a b = a
  DiscriminateEmpty (x ': xs) a b = b

impossibleList :: (x ': xs) E.:~: '[] -> a
impossibleList eq = E.castWith (transport eq) ()
  where
    transport :: ys E.:~: zs
              -> DiscriminateEmpty ys a b E.:~: DiscriminateEmpty zs a b
    transport E.Refl = E.Refl

rsubseqAppend :: (rs' ~ (ss' ++ rs)) => Lens (Rec f rs) (Rec f rs') (Rec f '[]) (Rec f ss')
rsubseqAppend = lens (const RNil) (\rs ss' -> ss' <+> rs)
{-# inline rsubseqAppend #-}
{-# RULES "rsubseqC/append" rsubseqC = rsubseqAppend #-}

keep :: Functor g => (s -> (a, s')) -> g s -> Compose g ((,) a) s'
keep split = Compose . fmap split

restore :: Functor g => (a -> s' -> s) -> Compose g ((,) a) s' -> g s
restore join = fmap (uncurry join) . getCompose


-- insert ss' at head

instance (rs ~ rs') => RecSubseq Rec f '[] '[] rs rs' '[] where
  rsubseqC f rs = fmap (\RNil -> rs) (f RNil)

  rsubseqSplitC _ = iso (\rs -> (RNil, rs)) (\(RNil, rs) -> rs)
  {-# inline rsubseqC #-}


instance
    ( RecSubseq Rec f '[] ss' rs rs' '[]
    , s' ~ r'
    )
    => RecSubseq Rec f '[] (s' ': ss') rs (r' ': rs') '[] where
  rsubseqC =
      lens (const RNil)
           (\rs (s' :& ss') -> s' :& set (rsubseqC @Rec @f @'[]) ss' rs)
  {-# inline rsubseqC #-}

  rsubseqSplitC = impossibleList


-- delete ss

-- NOTE:
--  view rsubseqC :: rs -> ss
--  ker (view rsubseqC) ~ rs'

--  set rsubseqC RNil :: rs -> rs'
--  ker (set rsubseqC RNil) ~ ss

-- (ss, rs') -> rs

instance
    ( RecSubseq Rec f ss '[] rs rs' dec_is'
    , DecSeq is' dec_is'
    , RImage ss (r ': rs) ~ is'
    , s ~ r
    )
    => RecSubseq Rec f (s ': ss) '[] (r ': rs) rs' (Z ': is') where
  rsubseqC f (r :& rs) = rsubseqC (f . (r :&)) rs
  {-# inline rsubseqC #-}

  rsubseqSplitC eq =
      withIso (rsubseqSplitC @Rec @f @ss eq) $ \split join ->
          iso (\(s :& rs)      -> first (s :&) (split rs))
              (\(s :& ss, rs') -> s :& join (ss, rs'))
  {-# inline rsubseqSplitC #-}


-- replace nonempty ss with nonempty ss'

instance
    ( RecSubseq Rec f ss ss' rs rs' dec_is'
    , DecSeq is' dec_is'
    , RImage ss (r ': rs) ~ is'
    , s ~ r
    , s' ~ r'
    )
    => RecSubseq Rec f (s ': ss) (s' ': ss') (r ': rs) (r' ': rs') (Z ': is') where
  rsubseqC f (r :& rs) =
      restore (:&) $ rsubseqC (keep runcons . f . (r :&)) rs
    where
      runcons :: Rec f (s' ': ss') -> (f s', Rec f ss')
      runcons (s' :& ss') = (s', ss')
  {-# inline rsubseqC #-}

  rsubseqSplitC = impossibleList


instance
    ( RecSubseq Rec f ss ss' rs rs' (i ': dec_is')
    , DecSeq is' dec_is'
    , RImage ss (r ': rs) ~ IncSeq (RImage ss rs)
    , r ~ r'
    )
    => RecSubseq Rec f ss ss' (r ': rs) (r' ': rs') (S i ': is') where
  rsubseqC f (r :& rs) =
      (r :&) <$> rsubseqC f rs
  {-# inline rsubseqC #-}

  -- rsubseqSplitC :: (Profunctor p, Functor g)
  --               => p (Rec f ss, Rec f (r' ': rs')) (g (Rec f ss, Rec f (r' ': rs')))
  --               -> p (Rec f rs) (g (Rec f rs))

  --     iso (\(r :& rs)      -> case view go rs of (ss, rs') -> (ss, r :& rs'))
  --         (\(ss, r :& rs') -> r :& view (from go) (ss, rs'))

  rsubseqSplitC eq =
      withIso (rsubseqSplitC @Rec @f @ss eq) $ \split join ->
          iso (\(r :& rs)      -> second (r :&) (split rs))
              (\(ss, r :& rs') -> r :& join (ss, rs'))



type RSubseq ss ss' rs rs' = RecSubseq Rec ElField ss ss' rs rs' (RImage ss rs)


rsubseq_ :: forall ss ss' rs rs' rec f.
         ( RecSubseq rec f ss ss' rs rs' (RImage ss rs)
         )
         => Lens (rec f rs) (rec f rs') (rec f ss) (rec f ss')
rsubseq_ = rsubseqC
{-# inline rsubseq_ #-}


rsubseq :: forall i ss ss' rs rs' rec f.
        ( FieldSpec rs i ss
        , RecSubseq rec f ss ss' rs rs' (RImage ss rs)
        )
        => Lens (rec f rs) (rec f rs') (rec f ss) (rec f ss')
rsubseq = rsubseq_
{-# inline rsubseq #-}


type RecQuotient rec f sub rs q = RecSubseq rec f sub '[] rs q (RImage sub rs)
type RQuotient sub rs q = RecQuotient Rec ElField sub rs q


rquotient :: forall i ss rs q f.
          ( FieldSpec rs i ss
          , RecQuotient Rec f ss rs q
          )
          => Rec f rs
          -> Rec f q
rquotient = set (rsubseq @ss) RNil
{-# inline rquotient #-}


rquotientSplit :: forall i ss rs q rec f.
               ( FieldSpec rs i ss
               , RecQuotient rec f ss rs q
               )
               => Iso' (rec f rs) (rec f ss, rec f q)
rquotientSplit = rsubseqSplitC E.Refl
{-# inline rquotientSplit #-}


rquotientSplit' :: forall i ss ss' rs rs' q q' rec f.
               ( FieldSpec rs i ss
               , RecQuotient rec f ss  rs  q
               , RecQuotient rec f ss' rs' q'
               )
               => Iso (rec f rs) (rec f rs') (rec f ss, rec f q) (rec f ss', rec f q')
rquotientSplit' = iso split unsplit'
  where
    split = view (rquotientSplit @ss)
    unsplit' = view (from (rquotientSplit @ss'))
{-# inline rquotientSplit' #-}


rreordered :: forall rs' rs f. (rs :~: rs') => Iso' (Rec f rs) (Rec f rs')
rreordered = iso rcast rcast
{-# inline rreordered #-}


rrename :: forall i i' s s' a r r' rs rs' record.
        ( FieldSpec rs i r
        , NameSpec i' s'
        , r ~ '(s, a)
        , r' ~ '(s', a)
        , RecElem record r r' rs rs' (RIndex r rs)
        , RecElemFCtx record ElField
        , KnownSymbol s'
        )
        => record ElField rs -> record ElField rs'
rrename = over (rlens' @r @r') renameField
{-# inline rrename #-}


renameField :: forall s1 s2 a. KnownSymbol s2 => ElField '(s1, a) -> ElField '(s2, a)
renameField (Field a) = Field a
{-# inline renameField #-}


renameFieldTo :: forall s2 s1 a. KnownSymbol s2 => ElField '(s1, a) -> ElField '(s2, a)
renameFieldTo = renameField
{-# inline renameFieldTo #-}


renamedField :: forall s1 s2 a b. (KnownSymbol s1, KnownSymbol s2)
             => Iso (ElField '(s1, a)) (ElField '(s1, b)) (ElField '(s2, a)) (ElField '(s2, b))
renamedField = iso renameField renameField
{-# inline renamedField #-}
