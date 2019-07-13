{-# language AllowAmbiguousTypes #-}
module VPF.Util.Dplyr where

import Prelude hiding (filter)

import GHC.TypeLits (KnownSymbol)

import qualified Control.Foldl as L

import qualified Data.Foldable as F
import Data.Function (on)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Traversable (Traversable)

import qualified Data.Vector                 as Vec
import qualified Data.Vector.Generic         as GVec
import qualified Data.Vector.Unboxed         as UVec
import qualified Data.Vector.Algorithms.Tim  as Tim

import qualified Data.Vinyl           as V
import qualified Data.Vinyl.Derived   as V
import qualified Data.Vinyl.Lens      as V
import qualified Data.Vinyl.TypeLevel as V

import Frames (Frame(..), FrameRec, Record, filterFrame, rgetField)
import Frames.InCore (RecVec)

import VPF.Util.Vinyl (rrename)


cat :: a -> a
cat = id
{-# inline cat #-}


(|.) :: (a -> b) -> (b -> c) -> a -> c
f |. g = g . f
{-# inline (|.) #-}

infixl 3 |.


(!@) :: GVec.Vector v Int => Frame row -> v Int -> Frame row
df !@ idx = Frame
    { frameLength = GVec.length idx
    , frameRow = \i -> frameRow df (idx GVec.! i)
    }
{-# inline (!@) #-}


singleRow :: row -> Frame row
singleRow row = Frame { frameLength = 1, frameRow = const row }
{-# inline singleRow #-}


copyAoS :: Frame rows -> Frame rows
copyAoS df = df { frameRow = (rowsVec Vec.!) }
  where
    rowsVec = Vec.generate (frameLength df) (frameRow df)
{-# inline copyAoS #-}


toFrameN :: Foldable f => Int -> f (Record cols) -> FrameRec cols
toFrameN n fr =
    Frame { frameLength = n, frameRow = (v Vec.!) }
  where
    v = Vec.fromListN n (F.toList fr)


desc :: (a -> a -> Ordering) -> a -> a -> Ordering
desc cmp = \a a' -> revOrd (cmp a a')
  where
    revOrd EQ = EQ
    revOrd GT = LT
    revOrd LT = GT
{-# inline desc #-}


cmpF :: forall ss rs. (Ord (Record ss), ss V.<: rs)
     => Record rs -> Record rs -> Ordering
cmpF = compare `on` V.rcast @ss
{-# inline cmpF #-}


cmpF1 :: forall r rs. (V.KnownField r, Ord (V.Snd r), V.RElem r rs (V.RIndex r rs))
      => Record rs -> Record rs -> Ordering
cmpF1 = compare `on` rgetField @r
{-# inline cmpF1 #-}


mapToFrame :: Map (Record as) (Record bs) -> FrameRec (as V.++ bs)
mapToFrame m = toFrameN (Map.size m) [rec1 V.<+> rec2 | (rec1, rec2) <- Map.toAscList m]
{-# inline mapToFrame #-}


arrangeBy :: (row -> row -> Ordering) -> Frame row -> Frame row
arrangeBy cmp df =
    df !@ reorderedIdxs
  where
    cmp' :: Int -> Int -> Ordering
    cmp' i j = cmp (frameRow df i) (frameRow df j)

    nrows :: Int
    nrows = frameLength df

    reorderedIdxs :: UVec.Vector Int
    reorderedIdxs = UVec.modify (Tim.sortBy cmp') (UVec.generate nrows id)
{-# inline arrangeBy #-}


arrange :: forall ss rs. (Ord (Record ss), ss V.<: rs) => FrameRec rs -> FrameRec rs
arrange = arrangeBy (cmpF @ss)
{-# inline arrange #-}


filter :: RecVec cols => (Record cols -> Bool) -> FrameRec cols -> FrameRec cols
filter = filterFrame
{-# inline filter #-}


firstN :: Int -> Frame row -> Frame row
firstN n df = df { frameLength = min (frameLength df) n }
{-# inline firstN #-}


topBy :: (row -> row -> Ordering) -> Int -> Frame row -> Frame row
topBy cmp n = arrangeBy (desc cmp)
           |. firstN n
{-# inline topBy #-}

top :: forall ss rs. (Ord (Record ss), ss V.<: rs)
    => Int -> FrameRec rs -> FrameRec rs
top = topBy (cmpF @ss)
{-# inline top #-}

top1 :: forall r rs. (V.KnownField r, Ord (V.Snd r), V.RElem r rs (V.RIndex r rs))
     => Int -> FrameRec rs -> FrameRec rs
top1 = topBy (cmpF1 @r)
{-# inline top1 #-}


select :: (ss V.<: rs) => FrameRec rs -> FrameRec ss
select = fmap V.rcast
{-# inline select #-}


mutate :: (Record cols -> Record new) -> FrameRec cols -> FrameRec (new V.++ cols)
mutate f = fmap (\row -> f row V.<+> row)
{-# inline mutate #-}


mutate1 :: forall col cols. (V.KnownField col)
        => (Record cols -> V.Snd col)
        -> FrameRec cols
        -> FrameRec (col ': cols)
mutate1 f = fmap (\row -> V.Field (f row) V.:& row)
{-# inline mutate1 #-}


mutateL :: (KnownSymbol l)
        => V.Label l -> (Record cols -> a) -> FrameRec cols -> FrameRec ('(l, a) ': cols)
mutateL l f = fmap (\row -> V.Field (f row) V.:& row)
{-# inline mutateL #-}


rename :: forall r r' rs rs' record.
        ( V.RecElem record r r' rs rs' (V.RIndex r rs)
        , V.RecElemFCtx record V.ElField
        , V.KnownField r, V.KnownField r'
        , V.Snd r ~ V.Snd r'
        , KnownSymbol (V.Fst r')
        )
        => Frame (record V.ElField rs) -> Frame (record V.ElField rs')
rename = fmap (rrename @r @r')
{-# inline rename #-}


reorder :: (rs' V.:~: rs) => FrameRec rs -> FrameRec rs'
reorder = fmap V.rcast
{-# inline reorder #-}


splitOn :: forall k row row' f. Ord k => (row -> k) -> Frame row -> [Frame row]
splitOn key df =
    let buildSubframes :: Map k [Int] -> [Frame row]
        buildSubframes = map (df !@) . F.toList . fmap UVec.fromList

        grouper :: L.Fold Int [Frame row]
        grouper = buildSubframes <$> L.groupBy (key . frameRow df) L.list
    in
        L.fold grouper df { frameRow = id }
{-# inlinable splitOn #-}


groupingOn :: forall k row row' f. Ord k
           => (row -> k)
           -> (Frame row -> Frame row')
           -> Frame row
           -> Frame row'
groupingOn key f = mconcat . map f . splitOn key
{-# inline groupingOn #-}


grouping :: forall subs cols cols'. (Ord (Record subs), subs V.<: cols)
         => (FrameRec cols -> FrameRec cols')
         -> FrameRec cols
         -> FrameRec cols'
grouping = groupingOn (V.rcast @subs)
{-# inline grouping #-}


grouping1 :: forall col cols cols'.
          (V.KnownField col, Ord (V.Snd col), V.RElem col cols (V.RIndex col cols))
          => (FrameRec cols -> FrameRec cols')
          -> FrameRec cols
          -> FrameRec cols'
grouping1 = groupingOn (rgetField @col)
{-# inline grouping1 #-}


fixing :: forall ss ss' rs. (Ord (Record ss), ss V.<: rs)
       => (FrameRec rs -> FrameRec ss')
       -> FrameRec rs
       -> FrameRec (ss V.++ ss')
fixing f =  grouping @ss applyGroup
  where
    applyGroup :: FrameRec rs -> FrameRec (ss V.++ ss')
    applyGroup group =
      let rec_ss  = V.rcast @ss (frameRow group 0)
      in fmap (rec_ss V.<+>) (f group)
{-# inline fixing #-}


fixing1 :: forall r ss' rs.
        (V.KnownField r, Ord (V.Snd r), V.RElem r rs (V.RIndex r rs))
        => (FrameRec rs -> FrameRec ss')
        -> FrameRec rs
        -> FrameRec (r ': ss')
fixing1 = fixing @'[r]
{-# inline fixing1 #-}


summarizing :: forall ss ss' rs. (Ord (Record ss), ss V.<: rs)
            => (FrameRec rs -> Record ss')
            -> FrameRec rs
            -> FrameRec (ss V.++ ss')
summarizing f = fixing @ss (singleRow . f)
{-# inline summarizing #-}


summarizing1 :: forall r ss' rs.
             (V.KnownField r, Ord (V.Snd r), V.RElem r rs (V.RIndex r rs))
             => (FrameRec rs -> Record ss')
             -> FrameRec rs
             -> FrameRec (r ': ss')
summarizing1 = summarizing @'[r]
{-# inline summarizing1 #-}


summary1 :: V.KnownField s => (FrameRec rs -> V.Snd s) -> FrameRec rs -> Record '[s]
summary1 summarize = \group ->
    V.Field (summarize group) V.:& V.RNil
{-# noinline summary1 #-}
