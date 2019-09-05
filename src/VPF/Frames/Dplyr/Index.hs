{-# language AllowAmbiguousTypes #-}
module VPF.Frames.Dplyr.Index where

import qualified Control.Lens as L
import Control.Lens.Operators
import Control.Lens.Type

import Data.Coerce (coerce)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map

import qualified Data.Vinyl           as V
import qualified Data.Vinyl.TypeLevel as V

import Frames (Frame(..), FrameRec, Record)

import Unsafe.Coerce (unsafeCoerce)

import VPF.Frames.Dplyr.Basic
import VPF.Frames.Dplyr.Group
import VPF.Frames.TaggedField
import VPF.Util.Vinyl (MonoFieldSpec, RQuotient, rquotient, rquotientSplit)


newtype GroupedFrame k row = GroupedFrame { getGroups :: Map k (Frame row) }
type GroupedFrameRec k cols = GroupedFrame k (Record cols)


groups :: IndexedTraversal k
            (GroupedFrame k row) (GroupedFrame k row')
            (Frame row)          (Frame row')
groups = L.dimap getGroups (fmap GroupedFrame) . L.itraversed
{-# inline groups #-}


reindexBy :: Ord k => (row -> (k, row')) -> Frame row -> GroupedFrame k row'
reindexBy key = GroupedFrame . Map.fromAscList . splitSortOn key
{-# inline reindexBy #-}


reindex :: forall i r cols.
        ( MonoFieldSpec cols i r
        , GroupingKey r cols
        )
        => FrameRec cols
        -> GroupedFrameRec (KeyType r) cols
reindex = reindexBy (\row -> (getKey @r row, row))
{-# inline reindex #-}


reindex' :: forall i r cols quot_cols.
         ( MonoFieldSpec cols i r
         , GroupingKey r cols
         , RQuotient (KeyCols r) cols quot_cols
         )
         => FrameRec cols
         -> GroupedFrameRec (KeyType r) quot_cols
reindex' = reindexBy (\row -> (getKey @r row, rquotient @(KeyCols r) row))
{-# inline reindex' #-}


mapIndexMonotonic :: (k -> k') -> GroupedFrame k row -> GroupedFrame k' row
mapIndexMonotonic f = GroupedFrame . Map.mapKeysMonotonic f . getGroups
{-# inline mapIndexMonotonic #-}


renameIndexTo :: forall s' s a row. GroupedFrame (Tagged s a) row -> GroupedFrame (Tagged s' a) row
renameIndexTo = unsafeCoerce
{-# inline renameIndexTo #-}


dropIndex :: GroupedFrame k row -> Frame row
dropIndex = L.foldOf groups
{-# inline dropIndex #-}


resetIndexWith :: (k -> row -> row') -> GroupedFrame k row -> Frame row'
resetIndexWith f = L.ifoldMapOf groups (fmap . f)
{-# inline resetIndexWith #-}


resetIndex :: GroupedFrameRec (Record as) bs  -> FrameRec (as V.++ bs)
resetIndex = resetIndexWith (V.<+>)
{-# inline resetIndex #-}


resetIndex1 :: V.KnownField col => GroupedFrameRec (Field col) cols  -> FrameRec (col ': cols)
resetIndex1 = resetIndexWith (\k -> let elf = elfield k in (elf V.:&))
{-# inline resetIndex1 #-}


-- NOTE: not a lawful iso because it sorts the frame
reindexed :: forall i r cols k' cols'.
          ( MonoFieldSpec cols i r
          , GroupingKey r cols
          )
          => Iso (FrameRec cols)                    (FrameRec cols')
                 (GroupedFrameRec (KeyType r) cols) (GroupedFrameRec k' cols')
reindexed = L.iso (reindex @i) dropIndex
{-# inline reindexed #-}


-- NOTE: not a lawful iso because it sorts the frame
reindexed' :: forall i r cols quot_cols cols' quot_cols'.
           ( MonoFieldSpec cols i r
           , GroupingKey r cols
           , V.RImage (KeyCols r) cols ~ V.RImage (KeyCols r) cols'
           , RQuotient (KeyCols r) cols  quot_cols
           , RQuotient (KeyCols r) cols' quot_cols'
           )
           => Iso (FrameRec cols)
                  (FrameRec cols')
                  (GroupedFrameRec (KeyType r) quot_cols)
                  (GroupedFrameRec (KeyType r) quot_cols')
reindexed' = L.iso (reindexBy split) (resetIndexWith unsplit')
  where
    split :: Record cols -> (KeyType r, Record quot_cols)
    split row =
      let (!recKey, !row') = row ^. rquotientSplit @(KeyCols r)
          !k               = recKey ^. L.from (keyRecord @r)
      in  (k, row')

    unsplit' :: KeyType r -> Record quot_cols' -> Record cols'
    unsplit' k =
      let !recKey = k ^. keyRecord @r
          !merge  = L.view (L.from (rquotientSplit @(KeyCols r)))
      in  \row' -> merge (recKey, row')
{-# inline reindexed' #-}


innerJoinWith :: Ord k
              => (row1 -> row2 -> row3)
              -> GroupedFrame k row1
              -> GroupedFrame k row2
              -> GroupedFrame k row3
innerJoinWith f = coerce (Map.intersectionWith (frameProductWith f))
{-# inline innerJoinWith #-}


innerJoin :: forall cols1 cols2 k. Ord k
          => GroupedFrameRec k cols1
          -> GroupedFrameRec k cols2
          -> GroupedFrameRec k (cols2 V.++ cols1)
innerJoin = innerJoinWith (\row1 row2 -> row2 V.<+> row1)
    -- flipped for partial application in |. chains
{-# inline innerJoin #-}
