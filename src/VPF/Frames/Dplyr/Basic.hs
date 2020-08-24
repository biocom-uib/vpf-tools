{-# language AllowAmbiguousTypes #-}
{-# language Strict #-}
{-# language TypeSynonymInstances #-}
{-# language UndecidableInstances #-}
module VPF.Frames.Dplyr.Basic where

import GHC.TypeLits (KnownSymbol)

import Prelude hiding (drop, filter, zipWith)

import qualified Control.Lens as L
import Control.Lens.Type

import qualified Data.Vector         as Vec
import qualified Data.Vector.Generic as GVec

import qualified Data.Vinyl as V

import VPF.Frames.Dplyr.Ops
import VPF.Frames.Dplyr.Row
import VPF.Frames.InCore (rowsVec, toRowsVec)
import VPF.Frames.Types
import VPF.Frames.VinylExts


cat :: a -> a
cat = id
{-# inline cat #-}


rowwise :: (row -> row') -> Frame row -> Frame row'
rowwise = fmap
{-# inline rowwise #-}


rows :: Setter (Frame row) (Frame row') row row'
rows = L.mapped
{-# inline rows #-}


foldedRows :: Fold (Frame row) row
foldedRows = L.folded
{-# inline foldedRows #-}


-- generalized 'rows', but slower
traverseRows :: Traversal (Frame row) (Frame row') row row'
traverseRows = rowsVec . traverse
{-# inline traverseRows #-}


(!@) :: GVec.Vector v Int => Frame row -> v Int -> Frame row
df !@ idx = Frame
    { frameLength = GVec.length idx
    , frameRow = \i -> frameRow df (idx GVec.! i)
    }
{-# inline (!@) #-}


col :: forall i s a b col col' cols cols' rec.
    ( FieldSpec cols i col
    , col ~ '(s, a)
    , col' ~ '(s, b)
    , ReplField rec col col' cols cols'
    )
    => Setter (FrameFields rec cols) (FrameFields rec cols') a b
col = rows . field @col
{-# inline col #-}


cols :: forall i subs subs' cols cols' rec.
     ( FieldSpec cols i subs
     , ReplFields rec subs subs' cols cols'
     )
     => Setter (FrameFields rec cols) (FrameFields rec cols')
               (Fields rec subs)      (Fields rec subs')
cols = rows . rsubseq @subs
{-# inline cols #-}


only :: forall i s a b rec. (NameSpec i s, KnownSymbol s, RSingleton rec)
     => Iso (Fields rec '[ '(s, a)]) (Fields rec '[ '(s, b)]) a b
only = rsingleton . elfield
{-# inline only #-}


singleField :: forall i s a rec. (NameSpec i s, KnownSymbol s, RSingleton rec)
            => a
            -> Fields rec '[ '(s, a)]
singleField = L.review (only @i)
{-# inline singleField #-}


singleRow :: row -> Frame row
singleRow row = Frame { frameLength = 1, frameRow = const row }
{-# inline singleRow #-}


frameProductWith :: (row1 -> row2 -> row3) -> Frame row1 -> Frame row2 -> Frame row3
frameProductWith f df1 df2 = Frame
    { frameLength = n1 * n2
    , frameRow = \i -> f (frameRow df1 (i `div` n2)) (frameRow df2 (i `mod` n2))
    }
  where
    !n1 = frameLength df1
    !n2 = frameLength df2
{-# inline frameProductWith #-}


firstN :: Int -> Frame row -> Frame row
firstN n df = df { frameLength = min (frameLength df) n }
{-# inline firstN #-}


drop :: forall i ss rs q rec.
     ( FieldSpec rs i ss
     , RecQuotient rec ss rs q
     , RMonoid rec
     )
     => FrameFields rec rs
     -> FrameFields rec q
drop = fmap (rquotient @ss)
{-# inline drop #-}


select :: forall is ss rs rec.
       ( FieldSpec rs is ss
       , FieldSubset rec ss rs
       )
       => FrameFields rec rs
       -> FrameFields rec ss
select = fmap V.rcast
{-# inline select #-}


select_ :: forall ss rs rec. FieldSubset rec ss rs
        => FrameFields rec rs
        -> FrameFields rec ss
select_ = fmap V.rcast
{-# inline select_ #-}


mutate :: RMonoid rec
       => (Fields rec cols -> Fields rec new)
       -> FrameFields rec cols
       -> FrameFields rec (new ++ cols)
mutate f = fmap (\row -> rappend (f row) row)
{-# inline mutate #-}


mutate1 :: forall i s a cols rec.
        ( NameSpec i s
        , KnownSymbol s
        , RMonoid rec
        , RSingleton rec
        )
        => (Fields rec cols -> a)
        -> FrameFields rec cols
        -> FrameFields rec ('(s, a) ': cols)
mutate1 f = fmap (\row -> rcons (V.Field (f row)) row)
{-# inline mutate1 #-}


replace :: forall i s a b col col' cols cols' rec.
        ( FieldSpec cols i col
        , col ~ '(s, a)
        , col' ~ '(s, b)
        , ReplField rec col col' cols cols'
        )
       => (Fields rec cols -> b)
       -> FrameFields rec cols
       -> FrameFields rec cols'
replace f = rows %~ \row -> L.set (field @col) (f row) row
{-# inline replace #-}


filter :: (row -> Bool) -> Frame row -> Frame row
filter f = rowsVec %~ Vec.filter f
{-# inline filter #-}


dropNothings :: forall i s a col col' cols cols' rec.
             ( FieldSpec cols i col
             , KnownSymbol s
             , col ~ '(s, Maybe a)
             , col' ~ '(s, a)
             , GetField rec col cols
             , ReplField rec col col' cols cols'
             )
             => FrameFields rec cols
             -> FrameFields rec cols'
dropNothings = rowsVec %~ Vec.mapMaybe (L.sequenceAOf (field @col))
{-# inline dropNothings #-}


unnest :: forall i s a col col' cols cols' rec.
       ( FieldSpec cols i col
       , col ~ '(s, [a])
       , col' ~ '(s, a)
       , ReplField rec col col' cols cols'
       )
       => FrameFields rec cols
       -> FrameFields rec cols'
unnest =
    rowsVec %~ Vec.concatMap (Vec.fromList . L.sequenceAOf (field @col))
{-# inline unnest #-}


unnestFrame :: forall i s col col's cols cols' rec.
            ( FieldSpec cols i col
            , col ~ '(s, FrameFields rec col's)
            , GetField rec col cols
            , ReplFields rec '[col] col's cols cols'
            , RSingleton rec
            )
            => FrameFields rec cols
            -> FrameFields rec cols'
unnestFrame =
    rowsVec %~ Vec.concatMap (toRowsVec . factorFrameOut)
  where
    factorFrameOut :: Fields rec cols -> FrameFields rec cols'
    factorFrameOut = L.traverseOf (rsubseq @col) (V.getField . L.view rsingleton)
{-# inline unnestFrame #-}


rename :: forall i i' s s' a col col' cols cols' rec.
        ( FieldSpec cols i col
        , NameSpec i' s'
        , col ~ '(s, a)
        , col' ~ '(s', a)
        , KnownSymbol s'
        , ReplField rec col col' cols cols'
        )
        => FrameFields rec cols
        -> FrameFields rec cols'
rename = fmap (rrename @i @i')
{-# inline rename #-}


reorder :: EquivFields rec rs rs' => FrameFields rec rs -> FrameFields rec rs'
reorder = fmap V.rcast
{-# inline reorder #-}

