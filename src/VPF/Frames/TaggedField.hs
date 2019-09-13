module VPF.Frames.TaggedField
  ( Field
  , untagged
  , rgetTagged
  , Tagged(..)
  , untag
  , retagField
  , retagFieldTo
  ) where

import GHC.TypeLits (KnownSymbol)

import Control.Lens.Type
import qualified Control.Lens as L

import Data.Coerce (coerce)
import Data.Tagged (Tagged(..), untag)
import Data.Vinyl (ElField(..), RecElem, RecElemFCtx, rget)
import Data.Vinyl.Derived (getField)
import Data.Vinyl.TypeLevel (Fst, Snd, RIndex)


type Field tag = Tagged (Fst tag) (Snd tag)


untagged :: KnownSymbol s1 => Iso (Tagged s1 a) (Tagged s2 b) (ElField '(s1, a)) (ElField '(s2, b))
untagged = L.iso (\(Tagged a) -> Field a) (\(Field b) -> Tagged b)


rgetTagged :: forall r s a rs rec.
           ( r ~ '(s, a)
           , RecElem rec r r rs rs (RIndex r rs)
           , RecElemFCtx rec ElField
           )
           => rec ElField rs -> Field r
rgetTagged = coerce . getField . rget @r


retagField :: forall r r' s s' a. (r ~ '(s, a), r' ~ '(s', a))
           => Tagged s a -> Tagged s' a
retagField = coerce


retagFieldTo :: forall r' r s' s a. (r ~ '(s, a), r' ~ '(s', a))
             => Tagged s a -> Tagged s' a
retagFieldTo = coerce
