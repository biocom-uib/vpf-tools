{-# language AllowAmbiguousTypes #-}
module VPF.Util.Vinyl
  ( over_rsubset'
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

import Data.Vinyl (ElField(..), Rec, (<+>))
import Data.Vinyl.Derived (KnownField)
import Data.Vinyl.Lens (RecElem, RecElemFCtx, RecSubsetFCtx,
                        rlens', type (<:), type (:~:), rcast)
import Data.Vinyl.TypeLevel (Fst, Snd, RIndex, type (++))

import Frames.Melt (RDeleteAll)


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
