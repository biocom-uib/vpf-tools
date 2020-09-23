{-# language BlockArguments #-}
{-# language OverloadedLabels #-}
{-# language OverloadedStrings #-}
{-# language Strict #-}
{-# language TemplateHaskell #-}
module VPF.Model.Class.Cols where

import qualified Control.Lens as L

import Data.Coerce (coerce)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Text (Text)
import Data.Vector (Vector)
import qualified Data.Vinyl.FromTuple as V

import Frames.InCore (VectorFor)
import Frames.TH (declareColumn)

import VPF.Frames.Dplyr.Ops
import VPF.Frames.TaggedField
import VPF.Frames.Types

import qualified VPF.Frames.Dplyr as F


declareColumn "class_key" ''Text

declareColumn "model_name" ''Text

declareColumn "class_name"    ''Text
declareColumn "class_percent" ''Double
declareColumn "class_cat"     ''Int


data Class = HomogClass (Field ClassName) (Field ClassCat)
           | NonHomogClass (Map (Field ClassName) (Field ClassPercent, Field ClassCat))
  deriving (Eq, Ord, Show)

type instance VectorFor Class = Vector


declareColumn "class_obj" ''Class


type ModelClassCols = '[ClassName, ClassPercent, ClassCat]
type ClassificationCols = ModelName ': ModelClassCols


summarizeToClassObj :: FrameRec ModelClassCols -> Class
summarizeToClassObj = toClassObj . coerce . fmap V.ruple
  where
    toClassObj :: Frame (Field ClassName, Field ClassPercent, Field ClassCat) -> Class
    toClassObj df
      | frameLength df == 1 =
          case frameRow df 0 of
            (name, _, cat) -> HomogClass name cat

      | otherwise =
          let
            keyValuePairs (name, percent, cat) = (name, (percent, cat))
          in
            NonHomogClass (Map.fromList $ L.toListOf (L.folded . L.to keyValuePairs) df)


unnestClassObj :: Class -> FrameRec ModelClassCols
unnestClassObj =
    F.cat
      |. F.singleRow . F.singleField @"class_obj"
      |. F.col @"class_obj" %~ inspectClass
      |. F.unnest @"class_obj"
      |. F.cols @"class_obj" . F.unzipped @'["class_name", "class_percent", "class_cat"] %~
          \(Tagged name, Tagged percent, Tagged cat) -> (name, percent, cat)
  where
    inspectClass :: Class -> [(Field ClassName, Field ClassPercent, Field ClassCat)]
    inspectClass (HomogClass name cat) = [(name, 100, cat)]
    inspectClass (NonHomogClass m)     = [(name, percent, cat) | (name, (percent, cat)) <- Map.toAscList m]
