{-# language DeriveGeneric #-}
{-# language GeneralizedNewtypeDeriving #-}
{-# language Strict #-}
{-# language UndecidableInstances #-}
module VPF.Frames.InCore
  (
  -- Rec f serialization
    ElFieldStore(..)
  , VinylStore(..)

  -- AoS
  , toRowsVecN
  , toRowsVec
  , fromRowsVec
  , rowsVec
  , copyAoS

  -- AoS serialization
  , FrameRowStore
  , fromFrameRowStore
  , toFrameRowStore
  , frameRowStore

  , FrameRowStoreRec
  , fromFrameRowStoreRec
  , toFrameRowStoreRec
  , frameRowStoreRec

  -- SoA
  , ColVecs
  , toColsRec
  , colVecsFoldM
  , toColVecsN
  , toColVecs
  , fromColVecs
  , colVecs
  , copySoA

  -- SoA serialization
  , FrameColStoreRec
  , fromFrameColStoreRec
  , toFrameColStoreRec
  , frameColStoreRec
  ) where

import GHC.Generics (Generic)

import qualified Control.Foldl as L
import Control.Lens (Iso, iso)
import Control.Monad.Primitive (PrimMonad)
import Control.Monad.ST (runST)

import Data.Coerce (coerce)
import Data.Foldable (toList)
import Data.Functor.Contravariant (contramap)
import Data.Proxy (Proxy(..))
import Data.Store (Store, Size(..))
import qualified Data.Store as Store
import qualified Data.Vector as Vec

import qualified Data.Vinyl           as V
import qualified Data.Vinyl.TypeLevel as V

import qualified Frames.InCore as IC

import VPF.Frames.Types


-- Store instances for Rec f

newtype ElFieldStore rs = ElFieldStore (V.ElField rs)
newtype VinylStore record f rs = VinylStore (record f rs)


instance (V.KnownField rs, Store (V.Snd rs)) => Store (ElFieldStore rs) where
    size = contramap (\(ElFieldStore (V.Field a)) -> a)  Store.size
    peek = fmap (ElFieldStore . V.Field) Store.peek
    poke (ElFieldStore (V.Field a)) = Store.poke a

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


-- AoS

toRowsVecN :: Foldable f => Int -> f row -> Vec.Vector row
toRowsVecN n = Vec.force . Vec.fromListN n . toList
{-# inline toRowsVecN #-}


toRowsVec :: Frame row -> Vec.Vector row
toRowsVec df = Vec.force $ Vec.generate (frameLength df) (frameRow df)
{-# inline toRowsVec #-}


fromRowsVec :: Vec.Vector row -> Frame row
fromRowsVec rows =
    Frame { frameLength = Vec.length rows, frameRow = (rows Vec.!) }
{-# inline fromRowsVec #-}


rowsVec :: Iso (Frame row) (Frame row') (Vec.Vector row) (Vec.Vector row')
rowsVec = iso toRowsVec fromRowsVec
{-# inline rowsVec #-}


copyAoS :: Frame rows -> Frame rows
copyAoS = fromRowsVec . toRowsVec


-- AoS serialization

newtype FrameRowStore row = FrameRowStore (Vec.Vector row)
    deriving (Store)

type FrameRowStoreRec rs = FrameRowStore (VinylStore V.Rec ElFieldStore rs)


fromFrameRowStore :: FrameRowStore row -> Frame row
fromFrameRowStore (FrameRowStore v) = fromRowsVec v

toFrameRowStore :: Frame row -> FrameRowStore row
toFrameRowStore = FrameRowStore . toRowsVec

frameRowStore :: Iso (Frame row) (Frame row') (FrameRowStore row) (FrameRowStore row')
frameRowStore = iso toFrameRowStore fromFrameRowStore


fromFrameRowStoreRec :: FrameRowStoreRec cols -> FrameRec cols
fromFrameRowStoreRec = coerce . fromFrameRowStore

toFrameRowStoreRec :: FrameRec cols -> FrameRowStoreRec cols
toFrameRowStoreRec = toFrameRowStore . coerce

frameRowStoreRec :: Iso (FrameRec cols) (FrameRec cols') (FrameRowStoreRec cols) (FrameRowStoreRec cols')
frameRowStoreRec = iso toFrameRowStoreRec fromFrameRowStoreRec


-- SoA

data ColVecsM m cols = ColVecsM Int Int (Record (IC.VectorMs m cols))
data ColVecs cols = ColVecs Int (Record (IC.Vectors cols))

instance Semigroup (Record (IC.Vectors cols)) => Semigroup (ColVecs cols) where
    ColVecs n cols <> ColVecs n' cols' = ColVecs (n+n') (cols <> cols')

instance Monoid (Record (IC.Vectors cols)) => Monoid (ColVecs cols) where
    mempty = ColVecs 0 mempty


toColsRec :: ColVecs cols -> Record (IC.Vectors cols)
toColsRec (ColVecs _ cols) = cols
{-# inline toColsRec #-}


colVecsFoldM :: forall cols m. (PrimMonad m, IC.RecVec cols)
             => Int
             -> L.FoldM m (Record cols) (ColVecs cols)
colVecsFoldM initialCapacity =
    L.FoldM feed start freeze
  where
    start :: m (ColVecsM m cols)
    start = do
      mvs <- IC.allocRec (Proxy @cols) initialCapacity
      return (ColVecsM 0 initialCapacity mvs)

    feed :: ColVecsM m cols -> Record cols -> m (ColVecsM m cols)
    feed (ColVecsM i sz mvs) row
      | i == sz = do
          mvs' <- IC.growRec (Proxy @cols) mvs
          feed (ColVecsM i (sz*2) mvs') row
      | otherwise = do
          IC.writeRec (Proxy @cols) i mvs row
          return (ColVecsM (i+1) sz mvs)

    freeze :: ColVecsM m cols -> m (ColVecs cols)
    freeze (ColVecsM n _ mvs) = do
      vs <- IC.freezeRec (Proxy @cols) n mvs
      return (ColVecs n vs)
{-# inline colVecsFoldM #-}


toColVecsN :: IC.RecVec cols => Foldable f => Int -> f (Record cols) -> ColVecs cols
toColVecsN n rows = runST (L.foldM (colVecsFoldM n) rows)
{-# inline toColVecsN #-}


toColVecs :: IC.RecVec cols => FrameRec cols -> ColVecs cols
toColVecs df = toColVecsN (frameLength df) df
{-# inline toColVecs #-}


fromColVecs :: IC.RecVec cols => ColVecs cols -> FrameRec cols
fromColVecs (ColVecs n cols) = IC.toAoS n (IC.produceRec Proxy cols )
{-# inline fromColVecs #-}


colVecs :: (IC.RecVec cols, IC.RecVec cols')
        => Iso (FrameRec cols) (FrameRec cols') (ColVecs cols) (ColVecs cols')
colVecs = iso toColVecs fromColVecs
{-# inline colVecs #-}


copySoA :: IC.RecVec cols => FrameRec cols -> FrameRec cols
copySoA = fromColVecs . toColVecs


-- SoA serialization

data FrameColStoreRec rs = FrameColStore !Int !(VinylStore V.Rec ElFieldStore (IC.Vectors rs))
    deriving (Generic)

instance (IC.RecVec rs, Store (VinylStore V.Rec ElFieldStore (IC.Vectors rs)))
    => Store (FrameColStoreRec rs)


fromFrameColStoreRec :: IC.RecVec rs => FrameColStoreRec rs -> FrameRec rs
fromFrameColStoreRec (FrameColStore n cols) = fromColVecs (ColVecs n (coerce cols))
{-# inline fromFrameColStoreRec #-}


toFrameColStoreRec :: IC.RecVec rs => FrameRec rs -> FrameColStoreRec rs
toFrameColStoreRec df =
    case toColVecs df of
      ColVecs n cols -> FrameColStore n (coerce cols)
{-# inline toFrameColStoreRec #-}


frameColStoreRec :: (IC.RecVec cols, IC.RecVec cols')
                 => Iso (FrameRec cols) (FrameRec cols') (FrameColStoreRec cols) (FrameColStoreRec cols')
frameColStoreRec = iso toFrameColStoreRec fromFrameColStoreRec
{-# inline frameColStoreRec #-}
