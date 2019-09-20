module VPF.Model.Class
  ( rawClassification
  , loadRawClassification
  , loadClassification
  , loadScoreSamples
  , Classification
  , ClassificationCols
  , RawClassification
  , RawClassificationCols
  ) where

import Control.Eff
import Control.Eff.Exception (Exc)
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


rawClassification :: Iso' (GroupedFrameRec (Field M.ModelName) '[ClassObj])
                          (GroupedFrameRec (Field M.ModelName) '[ClassName, ClassPercent, ClassCat])
rawClassification = L.iso (F.groups %~ do
                             F.col @"class_obj" %~ unnestClassObj |. F.unnestFrame @"class_obj")
                          (F.groups %~ F.cat
                             F.singleRow . F.singleField @"class_obj" . summarizeToClassObj)


loadRawClassification :: (Lifted IO r, Member (Exc DSV.ParseError) r)
                      => Path (DSV "\t" RawClassificationCols)
                      -> Eff r (GroupedFrameRec (Field M.ModelName) '[ClassName, ClassPercent, ClassCat])
loadRawClassification fp = F.setIndex @"model_name" <$> DSV.readFrame fp opts
  where
    opts = (DSV.defParserOptions '\t') { DSV.hasHeader = True }


loadClassification :: (Lifted IO r, Member (Exc DSV.ParseError) r)
                   => Path (DSV "\t" RawClassificationCols)
                   -> Eff r (GroupedFrameRec (Field M.ModelName) '[ClassObj])
loadClassification = fmap (L.review rawClassification) . loadRawClassification


loadScoreSamples :: (Lifted IO r, Member (Exc DSV.ParseError) r)
                 => Path (DSV "\t" '[M.VirusHitScore])
                 -> Eff r (Vector (Field M.VirusHitScore))
loadScoreSamples fp = do
    df <- DSV.readFrame fp opts
    return $ F.toRowsVec (fmap unRec df)
  where
    unRec :: Record '[M.VirusHitScore] -> Field M.VirusHitScore
    unRec = L.review F.untagged . L.view F.rsingleton

    opts = (DSV.defParserOptions '\t') { DSV.hasHeader = False }
