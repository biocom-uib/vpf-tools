{-# language AllowAmbiguousTypes #-}
{-# language UndecidableInstances #-}
{-# language QuantifiedConstraints #-}
module VPF.Frames.Dplyr.Group where

import GHC.TypeLits (Symbol)

import qualified Control.Foldl as Foldl
import qualified Control.Lens as L
import Control.Lens.Type

import Data.Bifunctor (second)
import Data.Foldable (toList)
import Data.Function (on)
import Data.Kind (Constraint, Type)
import Data.Map.Strict (Map)
import Data.Monoid (Ap(..))
import qualified Data.Vector                 as Vec
import qualified Data.Vector.Unboxed         as UVec
import qualified Data.Vector.Algorithms.Tim  as Tim

import qualified Data.Vinyl           as V
import qualified Data.Vinyl.TypeLevel as V

import Frames (Frame(..), FrameRec, Record)

import VPF.Frames.Dplyr.Basic
import VPF.Frames.InCore (fromRowsVec, toRowsVec)
import VPF.Frames.TaggedField

import VPF.Util.Vinyl (MonoFieldSpec)


class (forall cols. HasKey r cols => (KeyCols r V.<: cols)) => AsKey r where
    type KeyType (r :: kr) :: Type
    type KeyCols (r :: kr) :: [(Symbol, Type)]

    type HasKey  (r :: kr) (cols :: [(Symbol, Type)]) :: Constraint

    getKey :: HasKey r cols => Record cols -> KeyType r
    keyRecord :: Iso' (KeyType r) (Record (KeyCols r))

instance AsKey (subs :: [(Symbol, Type)]) where
    type KeyType subs = Record subs
    type KeyCols subs = subs
    type HasKey subs cols = subs V.<: cols

    getKey = V.rcast
    keyRecord = id

instance V.KnownField col => AsKey (col :: (Symbol, Type)) where
    type KeyType col = Field col
    type KeyCols col = '[col]
    type HasKey  col cols = V.RElem col cols (V.RIndex col cols)

    getKey = rgetTagged @col
    keyRecord = L.iso (singleField @col . untag) (rgetTagged @col)


type GroupingKey r cols = (AsKey r, Ord (KeyType r), HasKey r cols, KeyCols r V.<: cols)


data SortOrder = forall i. Asc i | forall i. Desc i

class SortKey i cols where
    compareRows :: Record cols -> Record cols -> Ordering

instance (MonoFieldSpec cols i r, AsKey r, Ord (KeyType r), HasKey r cols)
    => SortKey (Asc i) cols where
    compareRows = compare `on` getKey @r

instance (MonoFieldSpec cols i r, AsKey r, Ord (KeyType r), HasKey r cols)
    => SortKey (Desc i) cols where
    compareRows = compare `on` getKey @r

instance SortKey ('[] :: [SortOrder]) cols where
    compareRows _ _ = EQ

instance (SortKey i cols, SortKey is cols) => SortKey (i ': is :: [SortOrder]) cols where
    compareRows row1 row2 =
        compareRows @i row1 row2 <> compareRows @is row1 row2


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


arrange :: forall is cols. SortKey is cols => FrameRec cols -> FrameRec cols
arrange = arrangeBy (compareRows @is)
{-# inline arrange #-}


topBy :: (row -> row -> Ordering) -> Int -> Frame row -> Frame row
topBy cmp n = firstN n . arrangeBy cmp
{-# inline topBy #-}


top :: forall is cols. SortKey is cols => Int -> FrameRec cols -> FrameRec cols
top = topBy (compareRows @is)
{-# inline top #-}


splitSortOn :: forall k row row'. Ord k => (row -> (k, row')) -> Frame row -> [(k, Frame row')]
splitSortOn key =
    let
      cacheKeys :: Frame row -> Frame (k, row')
      cacheKeys = fmap key

      arrangedRows :: Frame (k, row') -> Vec.Vector (k, row')
      arrangedRows = toRowsVec . arrangeBy (\(k1, _) (k2, _) -> compare k1 k2)

      splitVec :: Vec.Vector (k, row') -> [(k, Vec.Vector (k, row'))]
      splitVec v
        | Vec.null v = []
        | otherwise  =
            let
              (currentKey, _) = Vec.head v
              (group, rest) = Vec.span (\(k, _) -> k == currentKey) v
            in
              (currentKey, group) : splitVec rest
    in
      map (second $ fmap snd . fromRowsVec) . splitVec . arrangedRows . cacheKeys
{-# inlinable splitSortOn #-}


splitOn :: forall k row. Ord k => (row -> k) -> Frame row -> [Frame row]
splitOn key df =
    let buildSubframes :: Map k [Int] -> [Frame row]
        buildSubframes = map (df !@) . toList . fmap UVec.fromList

        grouper :: Foldl.Fold Int [Frame row]
        grouper = buildSubframes <$> Foldl.groupBy (key . frameRow df) Foldl.list
    in
        Foldl.fold grouper df { frameRow = id }
{-# inlinable splitOn #-}


groupedOn :: forall k row row'. Ord k
          => (row -> k)
          -> IndexedTraversal k
               (Frame row) (Frame row')
               (Frame row) (Frame row')
groupedOn key f =
    getAp . foldMap (Ap . uncurry (L.indexed f)) . splitSortOn (\row -> (key row, row))
{-# inline groupedOn #-}


-- NOTE: not a lawful traversal because it sorts the frame
grouped :: forall i r cols cols'.
        ( MonoFieldSpec cols i r
        , GroupingKey r cols
        )
        => IndexedTraversal (KeyType r)
             (FrameRec cols) (FrameRec cols')
             (FrameRec cols) (FrameRec cols')
grouped = groupedOn (getKey @r)
{-# inline grouped #-}


-- NOTE: not a lawful traversal because it sorts the frame
fixed :: forall i r cols cols'.
       ( MonoFieldSpec cols i r
       , GroupingKey r cols
       )
       => IndexedTraversal (KeyType r)
            (FrameRec cols) (FrameRec (KeyCols r V.++ cols'))
            (FrameRec cols) (FrameRec cols')
fixed f =
    grouped @i $ L.Indexed $ \k ->
        let !recKey = L.view (keyRecord @r) k
            !f'     = ixf k
        in  \group -> fmap (rowwise (recKey V.<+>)) (f' group)
  where
    ixf = L.indexed f
{-# inline fixed #-}


-- NOTE: not a lawful traversal because it sorts the frame
summarizing :: forall i r cols cols'.
            ( MonoFieldSpec cols i r
            , GroupingKey r cols
            )
            => IndexedTraversal (KeyType r)
                 (FrameRec cols) (FrameRec (KeyCols r V.++ cols'))
                 (FrameRec cols) (Record cols')
summarizing = fixed @i . L.rmap (fmap singleRow)
{-# inline summarizing #-}
