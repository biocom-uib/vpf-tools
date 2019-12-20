{-# language Strict #-}
{-# language UndecidableInstances #-}
module Control.Effect.Sum.Extra
  ( FindMember
  , HasAny
  , Subsumes
  , injR
  ) where

import Control.Algebra
import Control.Effect.Sum

import Data.Type.Equality (type (==))

import Data.Kind (Type)


type SigK = (Type -> Type) -> Type -> Type


type family Desaturate' k l (sig :: l) :: Maybe k where
    Desaturate' k k sig                   = 'Just sig
    Desaturate' k l ((sig1 :: l' -> l) _) = Desaturate' k (l' -> l) sig1
    Desaturate' k l _                     = 'Nothing


type Desaturate (sigF :: k) (sig :: l) = Desaturate' k l sig


type family If (c :: Bool) (a :: k) (b :: k) :: k where
    If 'True  a _ = a
    If 'False _ b = b


type family (a :: Maybe k) <|> (b :: Maybe k) :: Maybe k where
    'Just a  <|> _   = 'Just a
    'Nothing <|> mb = mb


type family Find (sigF :: k) (sigs :: SigK) :: Maybe SigK where
    Find sigF (sig1 :+: sig2) = Find sigF sig1 <|> Find sigF sig2
    Find sigF sig             = If (Desaturate sigF sig == 'Just sigF)
                                  ('Just sig) 'Nothing


class Find sigF sigs ~ 'Just sig => Found sigF sig sigs | sigF sigs -> sig
instance Find sigF sigs ~ 'Just sig => Found sigF sig sigs


class (Member sig sigs, Found sigF sig sigs) => FindMember sigF sig sigs | sigF sigs -> sig
instance (Member sig sigs, Found sigF sig sigs) => FindMember sigF sig sigs


class (Algebra sigs m, FindMember sigF sig sigs) => HasAny sigF sig sigs m | sigF m -> sig sigs
instance (Algebra sigs m, FindMember sigF sig sigs) => HasAny sigF sig sigs m


class Subsumes (sub :: SigK) (sup :: SigK) where
    injR :: sub m a -> sup m a

instance Subsumes sub sub where
    injR = id
instance {-# overlappable #-} Subsumes sub (sub' :+: sub) where
    injR = R
instance {-# overlappable #-} Subsumes sub sup => Subsumes sub (sub' :+: sup) where
    injR = R . injR

