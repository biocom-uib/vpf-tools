{-# language DeriveFunctor #-}
{-# language DeriveFoldable #-}
{-# language UndecidableInstances #-}
module VPF.Frames.Types
  ( module Frames
  , module Vinyl
  , module TaggedField
  , module ExtraClasses

  -- kinds
  , FieldK
  , FieldsK
  , RecK

  -- type-level polymorphic field indexers
  , FieldSpec
  , FieldsOf
  , FieldOf

  , MonoFieldSpec
  , NameSpec

  -- Fields aliases
  , Fields
  , FrameFields

  -- Grouped frames
  , GroupedFrame(..)
  , GroupedFrameFields
  , GroupedFrameRec

  -- Constraint aliases
  , ReplField
  , GetField
  , FieldSubset
  , EquivFields
  , FieldSubseq
  , ReplFields
  ) where

import GHC.TypeLits (Symbol)

import Data.Kind (Type)
import Data.Map.Strict (Map)

import Data.Vinyl           as Vinyl (Rec(..))
import Data.Vinyl.ARec      as Vinyl (ARec)
import Data.Vinyl.Derived   as Vinyl (type (:::), FieldType)
import Data.Vinyl.TypeLevel as Vinyl (type (++), RIndex, RImage)

import qualified Data.Vinyl as V

import Frames as Frames (Record, Frame(..), FrameRec)

import VPF.Frames.TaggedField as TaggedField (Field)
import VPF.Frames.VinylExts as ExtraClasses (RSingleton, RMonoid,
                                             RecSubseq, RSubseq,
                                             RecQuotient, RQuotient)


-- kinds

type FieldK = (Symbol, Type)
type FieldsK = [FieldK]
type RecK = (FieldK -> Type) -> FieldsK -> Type


-- type-level polymorphic field indexers

type ProjField :: forall (ki :: Type) -> forall (kr :: Type) -> FieldsK -> ki -> kr

type family ProjField ki kr rs i where
    ProjField (Symbol, Type) (Symbol, Type)  rs  '(s, a)   = '(s, a)
    ProjField Symbol         (Symbol, Type)  rs  s         = '(s, FieldType s rs)
    ProjField (Symbol, Type) Symbol          rs  '(s, a)   = s
    ProjField Symbol         Symbol          rs  s         = s
    ProjField (Symbol, Type) [kr]            rs  '(s, a)   = ProjField [(Symbol, Type)] [kr] rs '[ '(s, a)]
    ProjField Symbol         [kr]            rs  s         = ProjField [Symbol] [kr] rs '[s]
    ProjField [k]            [k]             rs  r         = r
    ProjField [ki]           [kr]            rs  '[]       = '[]
    ProjField [ki]           [kr]            rs  (i ': is) = ProjField ki kr rs i ': ProjField [ki] [kr] rs is

type FieldSpec rs (i :: ki) (r :: kr) = ProjField ki kr rs i ~ r
type FieldsOf (rs :: FieldsK) (i :: ki) = ProjField ki FieldsK rs i
type FieldOf (rs :: FieldsK) (i :: ki) = ProjField ki FieldK rs i


type family InferFieldKind (ki :: Type) = (kr :: Type) where
    InferFieldKind [ki] = [InferFieldKind ki]
    InferFieldKind ki   = FieldK

type MonoFieldSpec rs (i :: ki) (r :: kr) = (InferFieldKind ki ~ kr, ProjField ki kr rs i ~ r)


type ProjName :: forall (ki :: Type) -> forall (kr :: Type) -> ki -> kr

type family ProjName (ki :: Type) (kr :: Type) (i :: ki) :: kr where
    ProjName Symbol          Symbol s         = s
    ProjName (Symbol, Type)  Symbol '(s, a)   = s
    ProjName (Symbol, Type) [kr]    i         = ProjName [(Symbol, Type)] [kr] '[i]
    ProjName Symbol         [kr]    i         = ProjName [Symbol] [kr] '[i]
    ProjName [k]            [k]     is        = is
    ProjName [ki]           [kr]    '[]       = '[]
    ProjName [ki]           [kr]    (i ': is) = ProjName ki kr i ': ProjName [ki] [kr] is

type NameSpec (i :: ki) (s :: kr) = ProjName ki kr i ~ s


-- Fields type aliases

type Fields rec cols = rec V.ElField cols
type FrameFields rec cols = Frame (Fields rec cols)


-- Grouped frames

newtype GroupedFrame k row = GroupedFrame { getGroups :: Map k (Frame row) }
  deriving (Functor, Foldable)

type GroupedFrameFields rec k cols = GroupedFrame k (Fields rec cols)
type GroupedFrameRec k cols = GroupedFrame k (Record cols)


-- Constraint aliases

type ReplField rec col col' cols cols' =
    ( V.RecElem rec col col' cols cols' (RIndex col cols)
    , V.RecElemFCtx rec V.ElField
    )

type GetField rec col cols = ReplField rec col col cols cols

type FieldSubset rec subs cols =
    ( V.RecSubset rec subs cols (RImage subs cols)
    , V.RecSubsetFCtx rec V.ElField
    )

type EquivFields rec cols cols' = (FieldSubset rec cols cols', FieldSubset rec cols' cols)

type FieldSubseq rec subs cols =
    RecSubseq rec subs subs cols cols (RImage subs cols)

type ReplFields rec subs subs' cols cols' =
    RecSubseq rec subs subs' cols cols' (RImage subs cols)
