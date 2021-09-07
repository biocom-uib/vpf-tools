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

import VPF.Frames.Dplyr qualified as F
import VPF.Frames.InCore qualified as F
import VPF.Frames.TaggedField qualified as F
import VPF.Frames.Dplyr.Ops
import VPF.Frames.Types

import VPF.Model.Cols qualified as M
import VPF.Model.Class.Cols

import VPF.Formats
import VPF.Frames.DSV qualified as DSV
import VPF.Util.Lift


classObjs :: Iso' (GroupedFrameRec (Field ModelName) ModelClassCols)
                  (GroupedFrameRec (Field ModelName) '[ClassObj])
classObjs = L.iso (F.groups %~ F.cat
                     F.singleRow . F.singleField @"class_obj" . summarizeToClassObj)
                  (F.groups %~ do
                     F.col @"class_obj" %~ unnestClassObj |. F.unnestFrame @"class_obj")


loadClassification ::
    ( MonadIO m
    , Has (Throw DSV.ParseError) m
    )
    => Path (DSV "\t" ClassificationCols)
    -> m (GroupedFrameRec (Field ModelName) ModelClassCols)
loadClassification fp =
    F.setIndex @"model_name" <$> liftEitherIO (DSV.readFrame opts fp)
  where
    opts = (DSV.defParserOptions '\t') { DSV.hasHeader = True }


loadClassObjs ::
    ( MonadIO m
    , Has (Throw DSV.ParseError) m
    )
    => Path (DSV "\t" ClassificationCols)
    -> m (GroupedFrameRec (Field ModelName) '[ClassObj])
loadClassObjs = fmap (L.view classObjs) . loadClassification


loadScoreSamples ::
    ( MonadIO m
    , Has (Throw DSV.ParseError) m
    )
    => Path (DSV "\t" '[M.VirusHitScore])
    -> m (Vector (Field M.VirusHitScore))
loadScoreSamples fp =
    F.toRowsVec . fmap unRec <$> liftEitherIO (DSV.readFrame opts fp)
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
    , Has (Throw DSV.ParseError) m
    )
    => ClassificationFiles
    -> m ClassificationParams
loadClassificationParams paths = do
    cls <- loadClassification (modelClassesFile paths)
    scores <- loadScoreSamples (scoreSamplesFile paths)

    return $ ClassificationParams cls scores
