{-# language AllowAmbiguousTypes #-}
{-# language UndecidableInstances #-}
module VPF.Util.Vinyl
  ( ElFieldStore(..), VinylStore(..)
  , over_rsubset'
  , rsubset'
  , rreordered
  , rrename
  , rrenamed
  , renameField
  , renameFieldTo
  , renamedField
  ) where

import GHC.TypeLits (KnownSymbol)

import Control.Lens (Iso, Iso', iso, over, Setter, sets)
import Control.Monad (liftM2)

import Data.Functor.Contravariant (contramap)
import Data.Store (Store, Size(..))
import qualified Data.Store as Store

import Data.Vinyl (ElField(..), Rec(..), (<+>))
import Data.Vinyl.Derived (KnownField)
import Data.Vinyl.Lens (RecElem, RecElemFCtx, RecSubsetFCtx,
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


over_rsubset' :: forall ss ss' rs f.
              ( ss <: rs
              , RDeleteAll ss rs <: rs
              , ss ++ RDeleteAll ss rs :~: rs
              )
              => (Rec f ss -> Rec f ss')
              -> Rec f rs
              -> Rec f (ss' ++ RDeleteAll ss rs)
over_rsubset' f rec =
    f (rcast @ss rec) <+> rcast @(RDeleteAll ss rs) rec


rsubset' :: forall ss ss' rs f.
         ( ss <: rs
         , RDeleteAll ss rs <: rs
         , ss ++ RDeleteAll ss rs :~: rs
         )
         => Setter (Rec f rs) (Rec f (ss' ++ RDeleteAll ss rs))
                   (Rec f ss) (Rec f ss')
rsubset' = sets over_rsubset'


rreordered :: forall rs' rs f.  (rs :~: rs') => Iso' (Rec f rs) (Rec f rs')
rreordered = iso rcast rcast


rrename :: forall r r' rs rs' record.
        ( RecElem record r r' rs rs' (RIndex r rs)
        , RecElemFCtx record ElField
        , KnownField r, KnownField r'
        , Snd r ~ Snd r'
        , KnownSymbol (Fst r')
        )
        => record ElField rs -> record ElField rs'
rrename = over (rlens' @r @r') renameField


rrenamed :: forall r r' rs rs' record.
          ( RecElem record r r' rs rs' (RIndex r rs)
          , RecElem record r' r rs' rs (RIndex r rs)
          , RecElemFCtx record ElField
          , KnownField r, KnownField r'
          , KnownSymbol (Fst r), KnownSymbol (Fst r')
          , Snd r ~ Snd r'
          )
          => Iso' (record ElField rs) (record ElField rs')
rrenamed = iso (rrename @r @r') (rrename @r' @r)



renameField :: forall s1 s2 a. KnownSymbol s2 => ElField '(s1, a) -> ElField '(s2, a)
renameField (Field a) = Field a

renameFieldTo :: forall s2 s1 a. KnownSymbol s2 => ElField '(s1, a) -> ElField '(s2, a)
renameFieldTo = renameField


renamedField :: forall s1 s2 a b. (KnownSymbol s1, KnownSymbol s2)
             => Iso (ElField '(s1, a)) (ElField '(s1, b)) (ElField '(s2, a)) (ElField '(s2, b))
renamedField = iso renameField renameField
