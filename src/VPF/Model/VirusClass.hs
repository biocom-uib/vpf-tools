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
import Control.Monad.IO.Class (MonadIO(..))
import Control.Monad.Trans.Except (ExceptT, runExceptT)
import qualified Control.Foldl as L

import Data.Kind (Type)
import Data.Functor (void)
import Data.Text (Text)
import qualified Data.Text as T

import qualified Data.Vinyl           as V
import qualified Data.Vinyl.TypeLevel as V

import Frames (Frame, FrameRec, RecordColumns, frameLength)
import Frames.InCore (RecVec)
import Frames.Joins (innerJoin)

import Pipes (Pipe, Producer, (>->))
import Pipes.Safe (SafeT, runSafeT)
import qualified Pipes         as P
import qualified Pipes.Prelude as P

import VPF.Eff.Cmd (Cmd)
import qualified VPF.Ext.Fasta as FA
import VPF.Ext.Prodigal (Prodigal, prodigal)
import VPF.Ext.HMMER.Search (HMMSearch, hmmsearch, ProtSearchHit, ProtSearchHitCols)
import qualified VPF.Ext.HMMER.Search.Cols as HMM
import qualified VPF.Ext.HMMER.TableFormat as Tbl

import VPF.Formats
import VPF.Model.Class (ClassificationCols, RawClassificationCols)
import qualified VPF.Model.Cols as M

import VPF.Util.Concurrent (concurrentlyChunked)
import VPF.Util.Dplyr ((|.))
import qualified VPF.Util.Dplyr as D
import qualified VPF.Util.DSV   as DSV
import qualified VPF.Util.FS    as FS



data ConcurrencyOpts = ConcurrencyOpts
    { fastaChunkSize      :: Int
    , maxSearchingWorkers :: Int
    }


data ModelInput (r :: [Type -> Type]) where
    GivenHitsFile :: Path (HMMERTable ProtSearchHitCols)
                  -> ModelInput r

    GivenGenomes :: (LiftedBase IO r, '[Cmd HMMSearch, Cmd Prodigal] <:: r)
                 => { workDir         :: Path Directory
                    , vpfsFile        :: Path HMMERModel
                    , genomesFile     :: Path (FASTA Nucleotide)
                    , concurrencyOpts :: ConcurrencyOpts
                    }
                 -> ModelInput r


data ModelConfig = ModelConfig
    { modelEValueThreshold :: Double
    }


searchGenomeHits :: (Lifted IO r, Member (Cmd HMMSearch) r, Member (Cmd Prodigal) r)
                 => Path Directory
                 -> Path HMMERModel
                 -> Producer [Text] (SafeT IO) ()
                 -> Eff r (Path (HMMERTable ProtSearchHitCols))
searchGenomeHits wd vpfsFile genomes = do
    genomesFile <- FS.emptyTmpFile @(FASTA Nucleotide) wd "split-genomesFile.faa"

    liftIO $ runSafeT $ P.runEffect $
        genomes
          >-> FA.ungroupFasta
          >-> FS.fileWriter (untag genomesFile)

    aminoacidsFile <- FS.emptyTmpFile @(FASTA Aminoacid) wd "split-proteins.faa"
    prodigal genomesFile aminoacidsFile Nothing

    hitsFile <- FS.emptyTmpFile @(HMMERTable ProtSearchHitCols) wd "split-hits.txt"
    hmmsearch vpfsFile aminoacidsFile hitsFile

    return hitsFile


produceHits :: forall r. Member (Reader ModelConfig) r
            => ModelInput r
            -> Eff r (Producer ProtSearchHit (SafeT IO) ())
produceHits input = do
    case input of
      GivenHitsFile hitsFile ->
          return $ Tbl.produceRows hitsFile

      GivenGenomes wd vpfsFile genomesFile concOpts -> do
          let splitGenomes :: Producer [Text] (SafeT IO) ()
              splitGenomes = void $ FA.fastaGroups $ FS.fileReader (untag genomesFile)

          hitsFiles <- concurrentlyChunked @(SafeT IO) @(Eff r)
              (fastaChunkSize concOpts)
              (maxSearchingWorkers concOpts)
              (liftIO . runSafeT)
              splitGenomes
              (searchGenomeHits wd vpfsFile)

          return $ flatten (hitsFiles >-> P.map Tbl.produceRows)
  where
    flatten :: Functor m => Producer (Producer a m ()) m () -> Producer a m ()
    flatten p = P.for p id



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
        >-> P.map V.rcast

    return (countHits hitsFrame)
  where
    countHits :: FrameRec '[HMM.TargetName, HMM.QueryName, HMM.SequenceScore]
              -> FrameRec '[M.VirusName, M.ModelName, M.NumHits]
    countHits = D.cat
        |. D.mutate1 @M.VirusName (virusName . V.rget)
        |. D.fixing1 @M.VirusName do
             D.cat
               |. D.grouping1 @HMM.TargetName do
                    D.top1 @HMM.SequenceScore 1
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
