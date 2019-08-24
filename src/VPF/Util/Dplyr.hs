{-# language AllowAmbiguousTypes #-}
{-# language GeneralizedNewtypeDeriving #-}
module VPF.Util.Dplyr where

import Prelude hiding (filter)

import GHC.Generics (Generic)
import GHC.TypeLits (KnownSymbol)

import qualified Control.Foldl as L

import Data.Coerce (coerce)
import qualified Data.Foldable as F
import Data.Function (on)
import Data.Functor.Contravariant (contramap)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Store (Store, Size(..))
import qualified Data.Store as Store
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

import VPF.Util.Vinyl (rrename, VinylStore(..), ElFieldStore(..))


newtype FrameRowStore row = FrameRowStore (Vec.Vector row)
  deriving (Store)

type FrameRowStoreRec rs = FrameRowStore (VinylStore V.Rec ElFieldStore rs)


fromFrameRowStore :: FrameRowStore row -> Frame row
fromFrameRowStore (FrameRowStore v) = rowVecToFrame v

toFrameRowStore :: Frame row -> FrameRowStore row
toFrameRowStore = FrameRowStore . rowsVec

fromFrameRowStoreRec :: FrameRowStoreRec cols -> FrameRec cols
fromFrameRowStoreRec = coerce . fromFrameRowStore

toFrameRowStoreRec :: FrameRec cols -> FrameRowStoreRec cols
toFrameRowStoreRec = toFrameRowStore . coerce


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


rowsVec :: Frame row -> Vec.Vector row
rowsVec df = Vec.generate (frameLength df) (frameRow df)
{-# inline rowsVec #-}


copyAoS :: Frame rows -> Frame rows
copyAoS df = df { frameRow = (rv Vec.!) }
  where
    rv = rowsVec df


rowVecToFrame :: Vec.Vector row -> Frame row
rowVecToFrame rows =
    Frame { frameLength = length rows, frameRow = (rows Vec.!) }
{-# inline rowVecToFrame #-}


toFrameN :: Foldable f => Int -> f row -> Frame row
toFrameN n fr = rowVecToFrame v
  where
    v = Vec.fromListN n (F.toList fr)
{-# inline toFrameN #-}


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


frameProductWith :: (row1 -> row2 -> row3) -> Frame row1 -> Frame row2 -> Frame row3
frameProductWith f df1 df2 = Frame
    { frameLength = n1 * n2
    , frameRow = \i -> f (frameRow df1 (i `div` n2)) (frameRow df2 (i `mod` n2))
    }
  where
    n1 = frameLength df1
    n2 = frameLength df2
{-# inline frameProductWith #-}


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


splitSortOn :: forall k row. Ord k => (row -> k) -> Frame row -> [Frame row]
splitSortOn key =
    let
      cacheKeys :: Frame row -> Frame (k, row)
      cacheKeys = fmap (\row -> (key row, row))

      arrangedRows :: Frame (k, row) -> Vec.Vector (k, row)
      arrangedRows = rowsVec . arrangeBy (\(k1, _) (k2, _) -> compare k1 k2)

      splitVec :: Vec.Vector (k, row) -> [Vec.Vector (k, row)]
      splitVec v
        | Vec.null v = []
        | otherwise  =
            let
              (currentKey, _) = Vec.head v
              (group, rest) = Vec.span (\(k, _) -> k == currentKey) v
            in
              group : splitVec rest
    in
      map (fmap snd . rowVecToFrame) . splitVec . arrangedRows . cacheKeys
{-# inlinable splitSortOn #-}


splitOn :: forall k row. Ord k => (row -> k) -> Frame row -> [Frame row]
splitOn key df =
    let buildSubframes :: Map k [Int] -> [Frame row]
        buildSubframes = map (df !@) . F.toList . fmap UVec.fromList

        grouper :: L.Fold Int [Frame row]
        grouper = buildSubframes <$> L.groupBy (key . frameRow df) L.list
    in
        L.fold grouper df { frameRow = id }
{-# inlinable splitOn #-}


mapToFrame :: Map (Record as) (Record bs) -> FrameRec (as V.++ bs)
mapToFrame m = toFrameN (Map.size m) [rec1 V.<+> rec2 | (rec1, rec2) <- Map.toAscList m]
{-# inline mapToFrame #-}


reindexBy :: Ord k => (row -> k) -> Frame row -> Map k (Frame row)
reindexBy key df = Map.fromAscList [(key (frameRow grp 0), grp) | grp <- splitSortOn key df]
{-# inline reindexBy #-}


resetIndex :: Map k (Frame row) -> Frame row
resetIndex = mconcat . Map.elems
{-# inline resetIndex #-}


reindex :: forall cols' cols.
        (Ord (Record cols'), cols' V.<: cols)
        => FrameRec cols
        -> Map (Record cols') (FrameRec cols)
reindex = reindexBy (V.rcast @cols')
{-# inline reindex #-}


reindex1 :: forall col cols.
         (V.KnownField col, Ord (V.Snd col), V.RElem col cols (V.RIndex col cols))
         => FrameRec cols
         -> Map (V.Snd col) (FrameRec cols)
reindex1 = reindexBy (rgetField @col)
{-# inline reindex1 #-}


innerJoinWith :: Ord k
              => (row1 -> row2 -> row3)
              -> Map k (Frame row1)
              -> Map k (Frame row2)
              -> Map k (Frame row3)
innerJoinWith f = Map.intersectionWith (frameProductWith f)
{-# inline innerJoinWith #-}


innerJoin :: forall cols1 cols2 k. Ord k
          => Map k (FrameRec cols1)
          -> Map k (FrameRec cols2)
          -> Map k (FrameRec (cols1 V.++ cols2))
innerJoin = innerJoinWith (V.<+>)
{-# inline innerJoin #-}


groupingOn :: forall k row row' f. Ord k
           => (row -> k)
           -> (Frame row -> Frame row')
           -> Frame row
           -> Frame row'
groupingOn key f = mconcat . map f . splitSortOn key
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
fixing f = grouping @ss applyGroup
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
