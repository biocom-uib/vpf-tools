{-# language ApplicativeDo #-}
{-# language BlockArguments #-}
{-# language OverloadedLabels #-}
{-# language OverloadedStrings #-}
{-# language StrictData #-}
module VPF.Model.VirusClass where

import Control.Eff
import Control.Eff.Reader.Strict (Reader, reader)
import Control.Eff.Exception (Exc)
import Control.Lens (view, toListOf, (%~))
import Control.Monad.Trans.Except (ExceptT, runExceptT)
import qualified Control.Foldl as L

import Data.Kind (Type)
import Data.Text (Text)
import qualified Data.Text as T

import qualified Data.Vinyl           as V
import qualified Data.Vinyl.TypeLevel as V

import Frames (Frame, FrameRec, RecordColumns, frameLength)
import Frames.InCore (RecVec)
import Frames.Joins (innerJoin)

import Pipes (Producer, (>->))
import Pipes.Safe (SafeT)
import qualified Pipes.Prelude as P

import VPF.Eff.Cmd (Cmd)
import VPF.Ext.Prodigal (Prodigal, prodigal)
import VPF.Ext.HMMER.Search (HMMSearch, hmmsearch, ProtSearchHit, ProtSearchHitCols)
import qualified VPF.Ext.HMMER.Search.Cols as HMM
import qualified VPF.Ext.HMMER.TableFormat as Tbl

import VPF.Formats
import VPF.Model.Class (ClassificationCols, RawClassificationCols)
import qualified VPF.Model.Cols as M

import VPF.Util.Dplyr ((|.))
import qualified VPF.Util.Dplyr as D
import VPF.Util.FS (emptyTmpFile)
import qualified VPF.Util.DSV as DSV


data ModelInput (r :: [Type -> Type]) where
    GivenHitsFile :: Path (HMMERTable ProtSearchHitCols)
                  -> ModelInput r

    GivenGenomes :: (Lifted IO r, '[Cmd HMMSearch, Cmd Prodigal] <:: r)
                 => { workDir   :: Path Directory
                    , vpfModels :: Path HMMERModel
                    , genomes   :: Path (FASTA Nucleotide)
                    }
                 -> ModelInput r


data ModelConfig = ModelConfig
    { modelEValueThreshold :: Double
    }


produceHits :: Member (Reader ModelConfig) r
            => ModelInput r
            -> Eff r (Producer ProtSearchHit (SafeT IO) ())
produceHits input = do
    hitsFile <- case input of
        GivenHitsFile hitsFile -> return hitsFile

        GivenGenomes wd vpfModel genomes -> do
            aminoacidFile <- emptyTmpFile @(FASTA Aminoacid) wd "proteins.faa"
            prodigal genomes aminoacidFile Nothing

            hitsFile <- emptyTmpFile @(HMMERTable ProtSearchHitCols) wd "hits.txt"
            hmmsearch vpfModel aminoacidFile hitsFile

            return hitsFile

    return (Tbl.produceRows hitsFile)


runModel :: ( Lifted IO r
            , '[ Reader ModelConfig
               , Exc DSV.RowParseError
               ] <:: r
            )
         => ModelInput r
         -> Eff r (FrameRec '[M.VirusName, M.ModelName, M.NumHits])
runModel modelInput = do
    hitRows <- produceHits modelInput
    thr <- reader modelEValueThreshold

    hitsFrame <- DSV.inCoreAoSExc $
        hitRows
        >-> P.filter (\row -> view HMM.sequenceEValue row <= thr)

    return (countHits hitsFrame)
  where
    countHits :: Frame ProtSearchHit -> FrameRec '[M.VirusName, M.ModelName, M.NumHits]
    countHits = D.cat
        |. D.mutate1 @M.VirusName (virusName . V.rget)
        |. D.fixing1 @M.VirusName do
             D.cat
               |. D.grouping1 @HMM.TargetName do
                    D.top1 @HMM.SequenceEValue 1
               |. D.summarizing1 @HMM.QueryName do
                    D.summary1 @M.NumHits frameLength
        |. D.rename @HMM.QueryName @M.ModelName
        |. D.copyAoS

    virusName :: V.ElField HMM.TargetName -> Text
    virusName (V.Field t) =
        let (name_, protein) = T.breakOnEnd "_" t
        in
          case T.stripSuffix "_" name_ of
            Just name -> name
            Nothing -> error (
                "unrecognized protein naming scheme in hmmsearch result "
                ++ T.unpack t)


type PredictedCols rs = rs V.++ V.RDelete M.ModelName ClassificationCols
type RawPredictedCols rs = rs V.++ V.RDelete M.ModelName RawClassificationCols


predictClassification ::
                      ( V.RElem M.ModelName rs (V.RIndex M.ModelName rs)
                      , rs V.<: PredictedCols rs
                      , RecVec rs
                      , RecVec (PredictedCols rs)
                      )
                      => FrameRec rs -> FrameRec ClassificationCols -> FrameRec (PredictedCols rs)
predictClassification hits cls =
    innerJoin @'[M.ModelName] hits cls
