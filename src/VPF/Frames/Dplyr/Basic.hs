{-# language AllowAmbiguousTypes #-}
{-# language UndecidableInstances #-}
module VPF.Frames.Dplyr.Basic where

import GHC.TypeLits (KnownSymbol)

import Prelude hiding (filter)

import Control.Lens (Setter, Traversal, mapped, (%~), set, sequenceOf)
import Data.Maybe (fromJust, isJust)
import Data.Proxy (Proxy(..))

import qualified Data.Vector         as Vec
import qualified Data.Vector.Generic as GVec

import qualified Data.Vinyl           as V
import qualified Data.Vinyl.TypeLevel as V

import Frames (Frame(..), FrameRec, Record, rgetField)

import VPF.Frames.InCore (rowsVec)
import VPF.Util.Vinyl (FieldSpec, NameSpec, RSubseq, rsubseq, rrename)


cat :: a -> a
cat = id
{-# inline cat #-}


tApply :: (Proxy i -> r) -> r
tApply f = f Proxy
{-# inline tApply #-}


rowwise :: (row -> row') -> Frame row -> Frame row'
rowwise = fmap
{-# inline rowwise #-}


rows :: Setter (Frame row) (Frame row') row row'
rows = mapped
{-# inline rows #-}


traverseRows :: Traversal (Frame row) (Frame row') row row'
traverseRows = rowsVec . traverse
{-# inline traverseRows #-}


(!@) :: GVec.Vector v Int => Frame row -> v Int -> Frame row
df !@ idx = Frame
    { frameLength = GVec.length idx
    , frameRow = \i -> frameRow df (idx GVec.! i)
    }
{-# inline (!@) #-}


get :: forall i s a col cols.
    ( FieldSpec cols i col
    , V.RElem col cols (V.RIndex col cols)
    , col ~ '(s, a)
    )
    => Record cols
    -> a
get = rgetField @col
{-# inline get #-}


col :: forall i s a b col col' cols cols'.
    ( FieldSpec cols i col
    , col ~ '(s, a)
    , col' ~ '(s, b)
    , V.RecElem V.Rec col col' cols cols' (V.RIndex col cols)
    )
    => Setter (FrameRec cols) (FrameRec cols') a b
col = rows . V.rlens' @col . V.rfield
{-# inline col #-}


field :: forall i s a. (NameSpec i s, KnownSymbol s) => a -> V.ElField '(s, a)
field = V.Field
{-# inline field #-}


singleField :: forall i s a. (NameSpec i s, KnownSymbol s) => a -> Record '[ '(s, a)]
singleField a = field @i a V.:& V.RNil
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
    n1 = frameLength df1
    n2 = frameLength df2
{-# inline frameProductWith #-}


filter :: (Record cols -> Bool) -> FrameRec cols -> FrameRec cols
filter f = rowsVec %~ Vec.filter f
{-# inline filter #-}


dropNothings :: forall i s a col col' cols cols'.
             ( FieldSpec cols i col
             , KnownSymbol s
             , col ~ '(s, Maybe a)
             , col' ~ '(s, a)
             , V.RElem col cols (V.RIndex col cols)
             , V.RecElem V.Rec col col' cols cols' (V.RIndex col cols)
             )
             => FrameRec cols
             -> FrameRec cols'
dropNothings = replace @col (fromJust . rgetField @col) . filter (isJust . rgetField @col)
{-# inline dropNothings #-}


firstN :: Int -> Frame row -> Frame row
firstN n df = df { frameLength = min (frameLength df) n }
{-# inline firstN #-}


select :: forall is ss rs.
       ( FieldSpec rs is ss
       , ss V.<: rs
       )
       => FrameRec rs
       -> FrameRec ss
select = fmap V.rcast
{-# inline select #-}


select_ :: forall ss rs. (ss V.<: rs) => FrameRec rs -> FrameRec ss
select_ = fmap V.rcast
{-# inline select_ #-}


mutate :: (Record cols -> Record new) -> FrameRec cols -> FrameRec (new V.++ cols)
mutate f = fmap (\row -> f row V.<+> row)
{-# inline mutate #-}


mutate1 :: forall i s a cols.
        ( NameSpec i s
        , KnownSymbol s
        )
        => (Record cols -> a)
        -> FrameRec cols
        -> FrameRec ('(s, a) ': cols)
mutate1 f = fmap (\row -> V.Field (f row) V.:& row)
{-# inline mutate1 #-}


replace :: forall i s a b col col' cols cols'.
        ( FieldSpec cols i col
        , col ~ '(s, a)
        , col' ~ '(s, b)
        , V.RecElem V.Rec col col' cols cols' (V.RIndex col cols)
        )
       => (Record cols -> b)
       -> FrameRec cols
       -> FrameRec cols'
replace f = fmap (\row -> set (V.rlens' @col . V.rfield) (f row) row)
{-# inline replace #-}


unnest :: forall i s a col col' cols cols'.
       ( FieldSpec cols i col
       , col ~ '(s, [a])
       , col' ~ '(s, a)
       , V.RecElem V.Rec col col' cols cols' (V.RIndex col cols)
       )
       => FrameRec cols
       -> FrameRec cols'
unnest =
    rowsVec %~ Vec.concatMap (Vec.fromList . sequenceOf (V.rlens' @col . V.rfield))
{-# inline unnest #-}


unzipWith :: forall i s a col cols col's cols'.
          ( FieldSpec cols i col
          , col ~ '(s, a)
          , RSubseq '[col] col's cols cols'
          )
          => (a -> Record col's)
          -> FrameRec cols
          -> FrameRec cols'
unzipWith f = rows . rsubseq @col %~ f . get @col
{-# inline unzipWith #-}


rename :: forall i i' s s' a col col' cols cols'.
        ( FieldSpec cols i col
        , NameSpec i' s'
        , col ~ '(s, a)
        , col' ~ '(s', a)
        , KnownSymbol s'
        , V.RecElem V.Rec col col' cols cols' (V.RIndex col cols)
        )
        => FrameRec cols
        -> FrameRec cols'
rename = fmap (rrename @i @i')
{-# inline rename #-}


reorder :: (rs' V.:~: rs) => FrameRec rs -> FrameRec rs'
reorder = fmap V.rcast
{-# inline reorder #-}

