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

import VPF.Model.Cols (VirusHitScore)

import VPF.Model.Class.Cols (Classification, RawClassification,
                             ClassificationCols, RawClassificationCols,
                             summarizeToClassObj,
                             unnestClassObj)

import VPF.Formats
import qualified VPF.Frames.DSV as DSV


rawClassification :: Iso' (Frame Classification) (Frame RawClassification)
rawClassification = L.iso (F.reindexed @"model_name" . F.groups %~ do
                             F.col @"class_obj" %~ unnestClassObj |. F.unnestFrame @"class_obj")
                          (F.reindexed @"model_name" . F.groups %~ F.cat
                             F.singleRow . F.singleField @"class_obj" . summarizeToClassObj)


loadRawClassification :: (Lifted IO r, Member (Exc DSV.ParseError) r)
                      => Path (DSV "\t" RawClassificationCols)
                      -> Eff r (Frame RawClassification)
loadRawClassification fp = DSV.readFrame fp opts
  where
    opts = (DSV.defParserOptions '\t') { DSV.hasHeader = True }


loadClassification :: (Lifted IO r, Member (Exc DSV.ParseError) r)
                   => Path (DSV "\t" RawClassificationCols)
                   -> Eff r (Frame Classification)
loadClassification = fmap (L.review rawClassification) . loadRawClassification


loadScoreSamples :: (Lifted IO r, Member (Exc DSV.ParseError) r)
                 => Path (DSV "\t" '[VirusHitScore])
                 -> Eff r (Vector (Field VirusHitScore))
loadScoreSamples fp = do
    df <- DSV.readFrame fp opts
    return $ F.toRowsVec (fmap unRec df)
  where
    unRec :: Record '[VirusHitScore] -> Field VirusHitScore
    unRec = L.review F.untagged . L.view F.rsingleton

    opts = (DSV.defParserOptions '\t') { DSV.hasHeader = False }
