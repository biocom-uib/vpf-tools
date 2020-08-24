{-# language DeriveGeneric #-}
{-# language Strict #-}
module VPF.Model.Class
  ( classObjs
  , loadClassification
  , loadClassObjs
  , loadScoreSamples
  , ClassificationFiles(..)
  , traverseClassificationFiles
  , ClassificationParams(..)
  , loadClassificationParams
  ) where

import GHC.Generics (Generic)

import Control.Algebra (Has)
import Control.Effect.Throw (Throw)
import Control.Monad.IO.Class (MonadIO)
import qualified Control.Lens as L
import Control.Lens.Type

import Data.Vector (Vector)

import qualified VPF.Frames.Dplyr as F
import qualified VPF.Frames.InCore as F
import qualified VPF.Frames.TaggedField as F
import VPF.Frames.Dplyr.Ops
import VPF.Frames.Types

import qualified VPF.Model.Cols as M
import VPF.Model.Class.Cols

import VPF.Formats
import qualified VPF.Frames.DSV as DSV


classObjs :: Iso' (GroupedFrameRec (Field ModelName) ModelClassCols)
                  (GroupedFrameRec (Field ModelName) '[ClassObj])
classObjs = L.iso (F.groups %~ F.cat
                     F.singleRow . F.singleField @"class_obj" . summarizeToClassObj)
                  (F.groups %~ do
                     F.col @"class_obj" %~ unnestClassObj |. F.unnestFrame @"class_obj")


loadClassification ::
    ( MonadIO m
    , Has (Throw DSV.ParseError) sig m
    )
    => Path (DSV "\t" ClassificationCols)
    -> m (GroupedFrameRec (Field ModelName) ModelClassCols)
loadClassification fp = F.setIndex @"model_name" <$> DSV.readFrame opts fp
  where
    opts = (DSV.defParserOptions '\t') { DSV.hasHeader = True }


loadClassObjs ::
    ( MonadIO m
    , Has (Throw DSV.ParseError) sig m
    )
    => Path (DSV "\t" ClassificationCols)
    -> m (GroupedFrameRec (Field ModelName) '[ClassObj])
loadClassObjs = fmap (L.view classObjs) . loadClassification


loadScoreSamples ::
    ( MonadIO m
    , Has (Throw DSV.ParseError) sig m
    )
    => Path (DSV "\t" '[M.VirusHitScore])
    -> m (Vector (Field M.VirusHitScore))
loadScoreSamples fp = do
    df <- DSV.readFrame opts fp
    return $ F.toRowsVec (fmap unRec df)
  where
    unRec :: Record '[M.VirusHitScore] -> Field M.VirusHitScore
    unRec = L.review F.untagged . L.view F.rsingleton

    opts = (DSV.defParserOptions '\t') { DSV.hasHeader = False }


data ClassificationFiles = ClassificationFiles
    { modelClassesFile :: Path (DSV "\t" ClassificationCols)
    , scoreSamplesFile :: Path (DSV "\t" '[M.VirusHitScore])
    }
    deriving Generic


traverseClassificationFiles :: Traversal' ClassificationFiles FilePath
traverseClassificationFiles f (ClassificationFiles mc ss) =
    ClassificationFiles <$> L._Wrapped f mc <*> L._Wrapped f ss


data ClassificationParams = ClassificationParams
    { modelClasses :: GroupedFrameRec (Field ModelName) '[ClassName, ClassPercent, ClassCat]
    , scoreSamples :: Vector (Field M.VirusHitScore)
    }


loadClassificationParams ::
    ( MonadIO m
    , Has (Throw DSV.ParseError) sig m
    )
    => ClassificationFiles
    -> m ClassificationParams
loadClassificationParams paths = do
    cls <- loadClassification (modelClassesFile paths)
    scores <- loadScoreSamples (scoreSamplesFile paths)

    return $ ClassificationParams cls scores
