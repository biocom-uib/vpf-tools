{-# language AllowAmbiguousTypes #-}
{-# language ImplicitParams #-}
{-# language Strict #-}
module VPF.Frames.Dplyr.Row
  (
  -- Generic operations on different record representations
    elfield
  , rsingleField
  , field
  , get
  , val
  , give

  -- ss is subsequence of rs if RImage ss rs is strictly monotone
  , rsubseq_
  , rsubseq

  -- special case: if RSubseq ss '[] rs rs', we can split rs into ss and rs'
  , rquotient
  , rquotientSplit
  , rquotientSplit'

  -- type-changing rsubset-related lenses
  , over_rsubset
  , rsubset_
  , rsubset
  , rreorder

  -- zipping/unzipping
  , ZipNamesTup
  , unzipped
  , unzipAs
  , zipped
  , zipAs

  -- field renaming
  , rrename
  , renameField
  , renameFieldTo
  , renamedField
  ) where

import GHC.TypeLits (KnownSymbol, Symbol)

import Control.Lens (Iso, Iso', Lens)
import qualified Control.Lens as L

import Data.Kind (Type)
import qualified Data.Type.Equality as E

import Data.Vinyl (ElField(..))
import Data.Vinyl.Derived (rfield)
import Data.Vinyl.Lens (rlens', rcast)

import qualified Data.Vinyl.FromTuple as VX
import qualified Data.Vinyl.XRec      as VX

import VPF.Frames.Types
import VPF.Frames.VinylExts

import Frames.Melt (RDeleteAll)



-- field access

elfield :: KnownSymbol s => Iso (ElField '(s, a)) (ElField '(s, b)) a b
elfield = L.iso (\(Field a) -> a) Field
{-# inline elfield #-}


rsingleField :: (KnownSymbol s, RSingleton rec)
             => Iso (rec ElField '[ '(s, a)]) (rec ElField '[ '(s, b)]) a b
rsingleField = rsingleton . elfield
{-# inline rsingleField #-}


field :: forall i s a b col col' cols cols' rec.
      ( FieldSpec cols i col
      , col ~ '(s ,a)
      , col' ~ '(s, b)
      , ReplField rec col col' cols cols'
      )
      => Lens (Fields rec cols) (Fields rec cols') a b
field = rlens' @col . rfield
{-# inline field #-}


get :: forall i s a col cols rec.
    ( FieldSpec cols i col
    , GetField rec col cols
    , col ~ '(s, a)
    )
    => Fields rec cols
    -> a
get = L.view (field @col)
{-# inline get #-}


val :: forall i s a col cols rec.
    ( FieldSpec cols i col
    , GetField rec col cols
    , col ~ '(s, a)
    , ?row :: Fields rec cols
    )
    => a
val = get @col (?row)
{-# inline val #-}


give :: ((?row :: Fields rec cols) => a) -> Fields rec cols -> a
give a row =
    let ?row = row
    in  a
{-# inline give #-}


-- rsubseq lenses

rsubseq_ :: forall ss ss' rs rs' rec f.
         ( RecSubseq rec ss ss' rs rs' (RImage ss rs)
         )
         => Lens (rec f rs) (rec f rs') (rec f ss) (rec f ss')
rsubseq_ = rsubseqC
{-# inline rsubseq_ #-}


rsubseq :: forall i ss' ss rs rs' rec f.
        ( FieldSpec rs i ss
        , RecSubseq rec ss ss' rs rs' (RImage ss rs)
        )
        => Lens (rec f rs) (rec f rs') (rec f ss) (rec f ss')
rsubseq = rsubseq_
{-# inline rsubseq #-}


-- special case for quotients and splitting records

rquotient :: forall i ss rs q rec f.
          ( FieldSpec rs i ss
          , RMonoid rec
          , RecQuotient rec ss rs q
          )
          => rec f rs
          -> rec f q
rquotient = L.set (rsubseq @ss) rempty
{-# inline rquotient #-}


rquotientSplit :: forall i ss rs q rec f.
               ( FieldSpec rs i ss
               , RecQuotient rec ss rs q
               )
               => Iso' (rec f rs) (rec f ss, rec f q)
rquotientSplit = rsubseqSplitC E.Refl
{-# inline rquotientSplit #-}


rquotientSplit' :: forall i ss' ss rs rs' q q' rec f.
               ( FieldSpec rs i ss
               , RecQuotient rec ss  rs  q
               , RecQuotient rec ss' rs' q'
               )
               => Iso (rec f rs) (rec f rs') (rec f ss, rec f q) (rec f ss', rec f q')
rquotientSplit' = L.iso split unsplit'
  where
    split = L.view (rquotientSplit @ss)
    unsplit' = L.review (rquotientSplit @ss')
{-# inline rquotientSplit' #-}


-- type-changing rsubset-related lenses

over_rsubset :: forall ss ss' rs rec.
             ( FieldSubset rec ss rs
             , FieldSubset rec (RDeleteAll ss rs) rs
             , EquivFields rec (ss ++ RDeleteAll ss rs) rs
             , RMonoid rec
             )
             => (Fields rec ss -> Fields rec ss')
             -> Fields rec rs
             -> Fields rec (ss' ++ RDeleteAll ss rs)
over_rsubset f rec =
    f (rcast @ss rec) `rappend` rcast @(RDeleteAll ss rs) rec
{-# inline over_rsubset #-}


rsubset_ :: forall ss ss' rs rec.
         ( FieldSubset rec ss rs
         , FieldSubset rec (RDeleteAll ss rs) rs
         , FieldSubset rec (ss ++ RDeleteAll ss rs) rs
         , RMonoid rec
         )
         => Lens (Fields rec rs) (Fields rec (ss' ++ RDeleteAll ss rs))
                 (Fields rec ss) (Fields rec ss')
rsubset_ = L.lens (rcast @ss) (\rec ss -> ss `rappend` rcast @(RDeleteAll ss rs) rec)
{-# inline rsubset_ #-}


rsubset :: forall is ss ss' rs rec.
        ( FieldSpec rs is ss
        , FieldSubset rec ss rs
        , FieldSubset rec (RDeleteAll ss rs) rs
        , FieldSubset rec (ss ++ RDeleteAll ss rs) rs
        , RMonoid rec
        )
        => Lens (Fields rec rs) (Fields rec (ss' ++ RDeleteAll ss rs))
                (Fields rec ss) (Fields rec ss')
rsubset = rsubset_
{-# inline rsubset #-}


rreorder :: forall rs' rs rec. EquivFields rec rs rs' => Fields rec rs -> Fields rec rs'
rreorder = rcast
{-# inline rreorder #-}


-- zipping/unzipping

type family ZipNamesTup (ss :: [Symbol]) (t :: Type) :: [(Symbol, Type)] where
  ZipNamesTup '[]                         ()                = '[]
  ZipNamesTup '[sa,sb]                    (a,b)             = '[ '(sa,a), '(sb,b)]
  ZipNamesTup '[sa,sb,sc]                 (a,b,c)           = '[ '(sa,a), '(sb,b), '(sc,c)]
  ZipNamesTup '[sa,sb,sc,sd]              (a,b,c,d)         = '[ '(sa,a), '(sb,b), '(sc,c), '(sd,d)]
  ZipNamesTup '[sa,sb,sc,sd,se]           (a,b,c,d,e)       = '[ '(sa,a), '(sb,b), '(sc,c), '(sd,d), '(se,e)]
  ZipNamesTup '[sa,sb,sc,sd,se,sf]        (a,b,c,d,e,f)     = '[ '(sa,a), '(sb,b), '(sc,c), '(sd,d), '(se,e), '(sf,f)]
  ZipNamesTup '[sa,sb,sc,sd,se,sf,sg]     (a,b,c,d,e,f,g)   = '[ '(sa,a), '(sb,b), '(sc,c), '(sd,d), '(se,e), '(sf,f), '(sg,g)]
  ZipNamesTup '[sa,sb,sc,sd,se,sf,sg,sh]  (a,b,c,d,e,f,g,h) = '[ '(sa,a), '(sb,b), '(sc,c), '(sd,d), '(se,e), '(sf,f), '(sg,g), '(sh,h)]


unzipped :: forall is' s a r ss' tss' tss'_tup.
          ( NameSpec is' ss'
          , r ~ '(s, a)
          , tss' ~ ZipNamesTup ss' tss'_tup
          , tss'_tup ~ VX.ListToHKDTuple ElField tss'
          , VX.IsoXRec ElField tss'
          , VX.TupleXRec ElField tss'
          )
          => Iso (Rec ElField '[r]) (Rec ElField tss')
                 a                  tss'_tup
unzipped = L.iso (\(Field a :& RNil) -> a) VX.xrec
{-# inline unzipped #-}


unzipAs :: forall is' s a r ss' tss' tss'_tup.
        ( NameSpec is' ss'
        , r ~ '(s, a)
        , tss' ~ ZipNamesTup ss' tss'_tup
        , tss'_tup ~ VX.ListToHKDTuple ElField tss'
        , VX.IsoXRec ElField tss'
        , VX.TupleXRec ElField tss'
        , a ~ tss'_tup
        )
        => Rec ElField '[r]
        -> Rec ElField tss'
unzipAs = L.over (unzipped @is') id
{-# inline unzipAs #-}


zipped :: forall i' s' b r' ss ss_tup.
        ( NameSpec i' s'
        , KnownSymbol s'
        , r' ~ '(s', b)
        , ss_tup ~ VX.ListToHKDTuple ElField ss
        , VX.IsoXRec ElField ss
        , VX.TupleXRec ElField ss
        )
        => Iso  (Rec ElField ss) (Rec ElField '[r'])
                ss_tup           b
zipped = L.iso VX.ruple (\a -> Field a :& RNil)
{-# inline zipped #-}


zipAs :: forall i' s' b r' ss ss_tup.
      ( NameSpec i' s'
      , KnownSymbol s'
      , r' ~ '(s', b)
      , ss_tup ~ VX.ListToHKDTuple ElField ss
      , VX.IsoXRec ElField ss
      , VX.TupleXRec ElField ss
      , b ~ ss_tup
      )
      => Rec ElField ss
      -> Rec ElField '[r']
zipAs = L.over (zipped @i') id
{-# inline zipAs #-}


-- field renaming

rrename :: forall i i' s s' a r r' rs rs' rec.
        ( FieldSpec rs i r
        , NameSpec i' s'
        , r ~ '(s, a)
        , r' ~ '(s', a)
        , ReplField rec r r' rs rs'
        , KnownSymbol s'
        )
        => Fields rec rs
        -> Fields rec rs'
rrename = L.over (rlens' @r @r') renameField
{-# inline rrename #-}


renameField :: forall s1 s2 a. KnownSymbol s2 => ElField '(s1, a) -> ElField '(s2, a)
renameField (Field a) = Field a
{-# inline renameField #-}


renameFieldTo :: forall s2 s1 a. KnownSymbol s2 => ElField '(s1, a) -> ElField '(s2, a)
renameFieldTo = renameField
{-# inline renameFieldTo #-}


renamedField :: forall s1 s2 a b. (KnownSymbol s1, KnownSymbol s2)
             => Iso (ElField '(s1, a)) (ElField '(s1, b)) (ElField '(s2, a)) (ElField '(s2, b))
renamedField = L.iso renameField renameField
{-# inline renamedField #-}
