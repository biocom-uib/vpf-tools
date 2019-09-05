{-# language BlockArguments #-}
{-# language OverloadedLabels #-}
{-# language OverloadedStrings #-}
{-# language TemplateHaskell #-}
module VPF.Model.Class.Cols where

import Data.Map.Strict (Map)
import Data.Text (Text)
import Data.Vector (Vector)

import Frames (Frame(..), Record)
import Frames.InCore (VectorFor)
import Frames.TH (declareColumn)

import VPF.Frames.Dplyr.Ops
import VPF.Frames.TaggedField
import qualified VPF.Frames.Dplyr as F


data Class = HomogClass !Text !Int
           | NonHomogClass !(Map Text Double)
  deriving (Eq, Ord, Show)

type instance VectorFor Class = Vector


declareColumn "model_name" ''Text

declareColumn "class_name"  ''Text
declareColumn "class_score" ''Double

declareColumn "class_obj" ''Class


type RawClassificationCols = '[ModelName, ClassName, ClassScore]
type RawClassification = Record RawClassificationCols

type ClassificationCols = '[ModelName, ClassObj]
type Classification = Record ClassificationCols


fromRawClassification :: F.GroupedFrameRec (Field ModelName) '[ClassName, ClassScore]
                      -> F.GroupedFrameRec (Field ModelName) '[ClassObj]
fromRawClassification = F.groups %~ do
    F.cat
      |. F.grouped @"class_name" %@~ do
          \(Tagged class_name) df ->
            if frameLength df == 1 then
              let
                cat = round $ F.get @"class_score" (frameRow df 0)
              in
                F.singleRow $ F.singleField @"class_obj" (HomogClass class_name cat)
            else
              F.singleRow $ F.singleField @"class_obj" (NonHomogClass mempty)



toRawClassification :: F.GroupedFrameRec (Field ModelName) '[ClassObj]
                    -> F.GroupedFrameRec (Field ModelName) '[ClassName, ClassScore]
toRawClassification = undefined
