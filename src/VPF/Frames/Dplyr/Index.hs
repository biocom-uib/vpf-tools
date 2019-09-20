{-# language AllowAmbiguousTypes #-}
{-# language DeriveFunctor #-}
{-# language DeriveFoldable #-}
module VPF.Frames.Dplyr.Index where

import qualified Control.Lens as L
import Control.Lens.Operators
import Control.Lens.Type

import Data.Coerce (coerce)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map

import qualified Data.Vinyl.TypeLevel as V

import Unsafe.Coerce (unsafeCoerce)

import VPF.Frames.Dplyr.Basic
import VPF.Frames.Dplyr.Group
import VPF.Frames.Dplyr.Row
import VPF.Frames.TaggedField
import VPF.Frames.Types
import VPF.Frames.VinylExts


groups :: IndexedTraversal k
            (GroupedFrame k row) (GroupedFrame k row')
            (Frame row)          (Frame row')
groups = L.dimap getGroups (fmap GroupedFrame) . L.itraversed
{-# inline groups #-}


reindexBy :: Ord k => (row -> (k, row')) -> Frame row -> GroupedFrame k row'
reindexBy key = GroupedFrame . Map.fromAscList . splitSortOn key
{-# inline reindexBy #-}


reindex :: forall i keys cols rec.
        ( FieldSpec cols i keys
        , GroupingKey rec keys cols
        )
        => FrameFields rec cols
        -> GroupedFrameFields rec (KeyType keys rec) cols
reindex = reindexBy (\row -> (getKey @keys row, row))
{-# inline reindex #-}


setIndex :: forall i keys cols quot_cols rec.
         ( FieldSpec cols i keys
         , GroupingKey rec keys cols
         , RecQuotient rec keys cols quot_cols
         , RMonoid rec
         )
         => FrameFields rec cols
         -> GroupedFrameFields rec (KeyType keys rec) quot_cols
setIndex = reindexBy (\row -> (getKey @keys row, rquotient @keys row))
{-# inline setIndex #-}


mapIndexMonotonic :: (k -> k') -> GroupedFrame k row -> GroupedFrame k' row
mapIndexMonotonic f = GroupedFrame . Map.mapKeysMonotonic f . getGroups
{-# inline mapIndexMonotonic #-}


renameIndexTo :: forall s' s a row. GroupedFrame (Tagged s a) row -> GroupedFrame (Tagged s' a) row
renameIndexTo = coerce renameMap
  where
    renameMap :: Map (Tagged s a) (Frame row) -> Map (Tagged s' a) (Frame row)
    renameMap = unsafeCoerce
{-# inline renameIndexTo #-}


dropIndex :: GroupedFrame k row -> Frame row
dropIndex = L.foldOf groups
{-# inline dropIndex #-}


resetIndexWith :: (k -> row -> row') -> GroupedFrame k row -> Frame row'
resetIndexWith f = L.ifoldMapOf groups (fmap . f)
{-# inline resetIndexWith #-}


resetIndex :: forall keys rec cols.
           (RMonoid rec, RSingleton rec, AsKey keys, KeyRecordCtx keys rec)
           => GroupedFrameFields rec (KeyType keys rec) cols
           -> FrameFields rec (keys ++ cols)
resetIndex = resetIndexWith (\k -> let keyRec = k^.keyRecord @keys in rappend keyRec)
{-# inline resetIndex #-}


-- NOTE: not a lawful iso because it sorts the frame
reindexed_ :: forall i keys cols k' row' rec.
           ( FieldSpec cols i keys
           , GroupingKey rec keys cols
           )
           => Iso (FrameFields rec cols)
                  (Frame row')
                  (GroupedFrameFields rec (KeyType keys rec) cols)
                  (GroupedFrame k' row')
reindexed_ = L.iso (reindex @i) dropIndex
{-# inline reindexed_ #-}


-- NOTE: not a lawful iso because it sorts the frame
reindexed :: forall i keys keys' cols quot_cols cols' quot_cols' rec rec'.
          ( FieldSpec cols i keys
          , GroupingKey rec keys cols
          , AsKey keys'
          , KeyRecordCtx keys' rec'
          , V.RImage keys cols ~ V.RImage keys' cols'
          , RecQuotient rec  keys  cols  quot_cols
          , RecQuotient rec' keys' cols' quot_cols'
          )
          => Iso (FrameFields rec cols)
                 (FrameFields rec' cols')
                 (GroupedFrameFields rec  (KeyType keys rec)   quot_cols)
                 (GroupedFrameFields rec' (KeyType keys' rec') quot_cols')
reindexed = L.iso (reindexBy split) (resetIndexWith unsplit')
  where
    split :: Fields rec cols -> (KeyType keys rec, Fields rec quot_cols)
    split row =
      let (!recKey, !row') = row ^. rquotientSplit @keys
          !k               = keyRecord @keys # recKey
      in  (k, row')

    unsplit' :: KeyType keys' rec' -> Fields rec' quot_cols' -> Fields rec' cols'
    unsplit' k =
      let !recKey = k ^. keyRecord @keys'
          !merge  = L.review (rquotientSplit @keys')
      in  \row' -> merge (recKey, row')
{-# inline reindexed #-}


innerJoinWith :: Ord k
              => (row1 -> row2 -> row3)
              -> GroupedFrame k row1
              -> GroupedFrame k row2
              -> GroupedFrame k row3
innerJoinWith f = coerce (Map.intersectionWith (frameProductWith f))
{-# inline innerJoinWith #-}


innerJoin :: (Ord k, RMonoid rec)
          => GroupedFrameFields rec k cols1
          -> GroupedFrameFields rec k cols2
          -> GroupedFrameFields rec k (cols2 ++ cols1)
innerJoin = innerJoinWith (\row1 row2 -> rappend row2 row1)
    -- flipped for partial application in |. chains
{-# inline innerJoin #-}
