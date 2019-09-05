{-# language ApplicativeDo #-}
{-# language BlockArguments #-}
{-# language OverloadedLabels #-}
{-# language OverloadedStrings #-}
{-# language StrictData #-}
module VPF.Model.VirusClass where

import Control.Eff
import Control.Eff.Reader.Strict (Reader, reader)
import Control.Eff.Exception (Exc, liftEither)
import Control.Lens (folded, view, sumOf, (^.), (%~))
import Control.Monad.IO.Class (MonadIO(..))
import Control.Monad.Morph (hoist)
import Control.Monad.Trans.Control (StM)
import qualified Control.Foldl as L

import Data.Function ((&))
import Data.Kind (Type)
import qualified Data.List.NonEmpty as NE
import Data.Monoid (Ap(..))
import Data.Semigroup.Foldable (foldMap1)
import Data.Text (Text)

import qualified Data.Vinyl           as V
import qualified Data.Vinyl.TypeLevel as V

import Frames (FrameRec, Record)

import Pipes (Pipe, Producer, (>->))
import qualified Pipes         as P
import qualified Pipes.Prelude as P
import qualified Pipes.Safe    as P

import VPF.Concurrency.Async ((>||>), (>|->), (>-|>))
import qualified VPF.Concurrency.Async as Conc
import qualified VPF.Concurrency.Pipes as Conc

import VPF.Eff.Cmd (Cmd)
import VPF.Ext.Prodigal (Prodigal, prodigal)
import qualified VPF.Ext.HMMER.Search      as HMM
import qualified VPF.Ext.HMMER.Search.Cols as HMM
import qualified VPF.Ext.HMMER.TableFormat as Tbl

import VPF.Formats
import VPF.Frames.Dplyr.Ops
import VPF.Frames.TaggedField
import qualified VPF.Frames.Dplyr  as F
import qualified VPF.Frames.DSV    as DSV
import qualified VPF.Frames.InCore as F

import VPF.Model.Class (ClassificationCols, RawClassificationCols)
import qualified VPF.Model.Cols       as M
import qualified VPF.Model.Class.Cols as Cls

import qualified VPF.Util.Fasta as FA
import qualified VPF.Util.FS    as FS


data ConcurrencyOpts m = ConcurrencyOpts
    { fastaChunkSize  :: Int
    , pipelineWorkers :: NE.NonEmpty (
                          Pipe [FA.FastaEntry Nucleotide]
                               (StM m (FrameRec '[M.VirusName, M.ModelName, M.ProteinHitScore]))
                               (P.SafeT IO)
                               ())
    }


data ModelInput (r :: [Type -> Type]) where
    -- GivenHitsFile :: Path (HMMERTable HMM.ProtSearchHitCols)
    --               -> ModelInput r

    GivenGenomes ::
                 ( '[ Cmd HMM.HMMSearch
                    , Cmd Prodigal
                    , Exc FA.ParseError
                    ] <:: r
                 )
                 => { genomesFile     :: Path (FASTA Nucleotide)
                    , concurrencyOpts :: ConcurrencyOpts (Eff r)
                    }
                 -> ModelInput r


data ModelConfig = ModelConfig
    { modelEValueThreshold    :: Double
    , modelVirusNameExtractor :: Text -> Text
    }


searchGenomeHits ::
                 ( Lifted IO r
                 , Member (Cmd HMM.HMMSearch) r
                 , Member (Cmd Prodigal) r
                 )
                 => Path Directory
                 -> Path HMMERModel
                 -> Producer (FA.FastaEntry Nucleotide) (P.SafeT IO) ()
                 -> Eff r (Path (FASTA Aminoacid), Path (HMMERTable HMM.ProtSearchHitCols))
searchGenomeHits wd vpfsFile genomes = do
    genomesFile <- FS.emptyTmpFile @(FASTA Nucleotide) wd "split-genomes.fna"

    liftIO $ P.runSafeT $ P.runEffect $ genomes >-> FA.fastaFileWriter genomesFile

    aminoacidsFile <- FS.emptyTmpFile @(FASTA Aminoacid) wd "split-proteins.faa"
    prodigal genomesFile aminoacidsFile Nothing

    hitsFile <- FS.emptyTmpFile @(HMMERTable HMM.ProtSearchHitCols) wd "split-hits.txt"
    HMM.hmmsearch vpfsFile aminoacidsFile hitsFile

    return (aminoacidsFile, hitsFile)


processHMMOut :: forall r.
              ( Lifted IO r
              , '[ Reader ModelConfig
                 , Exc DSV.ParseError
                 , Exc FA.ParseError
                 ] <:: r
              )
              => Path (FASTA Aminoacid)
              -> Path (HMMERTable HMM.ProtSearchHitCols)
              -> Eff r (FrameRec '[M.VirusName, M.ModelName, M.ProteinHitScore])
processHMMOut aminoacidsFile hitsFile = do
    proteinSizes <- loadProteinSizes

    let hitRows = Tbl.produceRows hitsFile

    thr <- reader modelEValueThreshold
    getVirusName <- reader modelVirusNameExtractor

    hitsFrame <- DSV.inCoreAoSExc $
        hitRows
        >-> P.filter (\row -> row^.HMM.sequenceEValue <= thr)
        >-> P.map V.rcast

    return $ hitsFrame
        & F.mutate1 @"virus_name" (getVirusName . view HMM.targetName)
        & aggregateHits proteinSizes
  where
    fastaEntryToRow :: FA.FastaEntry Aminoacid -> Record '[M.ProteinName, M.KBaseSize]
    fastaEntryToRow entry =
        V.fieldRec
          ( #protein_name =: FA.removeNameComments (FA.entryName entry)
          , #k_base_size  =: fromIntegral (FA.entrySeqNumBases entry) / 1000
          )

    loadProteinSizes :: Eff r (F.GroupedFrameRec (Field M.ProteinName) '[M.KBaseSize])
    loadProteinSizes = do
        (colVecs, errs) <- lift $ P.runSafeT $
            L.impurely P.foldM' (F.colVecsFoldM 128) $
                FA.fastaFileReader aminoacidsFile
                  >-> P.mapM (\entry -> return $! fastaEntryToRow entry)

        liftEither errs
        return $! F.fromColVecs colVecs ^. F.reindexed' @"protein_name"


    aggregateHits :: F.GroupedFrameRec (Field M.ProteinName) '[M.KBaseSize]
                  -> FrameRec '[M.VirusName, HMM.TargetName, HMM.QueryName, HMM.SequenceScore]
                  -> FrameRec '[M.VirusName, M.ModelName, M.ProteinHitScore]
    aggregateHits proteinSizes = F.cat
        |. F.fixed @"virus_name" %~ do
             F.cat
               |. F.grouped @"target_name" %~ F.top @(F.Desc "sequence_score") 1
               |. F.reindex' @"target_name"
               |. F.renameIndexTo @"protein_name"
               |. F.innerJoin proteinSizes
               |. F.dropIndex
               |. F.rowwise do \row -> row & HMM.sequenceScore %~ (/ row^.M.kBaseSize)
               |. F.summarizing @"query_name" %~ do
                    F.singleField @"protein_hit_score" . sumOf (folded . HMM.sequenceScore)
        |. F.rename @"query_name" @"model_name"
        |. F.copySoA


predictMembership :: F.GroupedFrameRec (Field M.VirusName)
                                       '[Cls.ClassObj, M.ProteinHitScore]
                  -> F.GroupedFrameRec (Field M.VirusName)
                                       '[Cls.ClassName, M.MembershipRatio, M.VirusHitScore]
predictMembership = F.groups %~ do
    F.cat
      |. F.col @"class_obj" %~ inspectClass
      |. F.unnest @"class_obj"

      |. F.unzipWith @"class_obj" do
          \(name, prop) -> V.fieldRec (#class_name =: name, #class_prop =: prop)

      |. F.summarizing @"class_name" %~ do
          F.cat
            |. F.rowwise do \row -> F.get @"protein_hit_score" row * F.get @"class_prop" row
            |. F.singleField @"protein_hit_score" . sum

      |. \df ->
          let
            totalScore = sumOf (folded . M.proteinHitScore) df
          in
            df & F.cat
              |. F.mutate1 @"virus_hit_score"  (const totalScore)
              |. F.mutate1 @"membership_ratio" (\row -> row^.M.proteinHitScore / totalScore)
              |. F.select_
  where
    inspectClass :: Cls.Class -> [(Text, Double)]
    inspectClass (Cls.HomogClass name cat) = [(name, (5 - fromIntegral cat) / 4)]
    inspectClass (Cls.NonHomogClass _)     = [("patata", 52.43923/100)]


syncPipeline ::
             ( Lifted IO r
             , '[ Cmd HMM.HMMSearch
                , Cmd Prodigal
                , Exc DSV.ParseError
                , Exc FA.ParseError
                , Reader ModelConfig
                ] <:: r
             )
             => Path Directory
             -> Path HMMERModel
             -> Producer (FA.FastaEntry Nucleotide) (P.SafeT IO) ()
             -> Eff r (FrameRec '[M.VirusName, M.ModelName, M.ProteinHitScore])
syncPipeline wd vpfsFile genomes = do
  (aminoacidsFile, hitsFile) <- searchGenomeHits wd vpfsFile genomes
  processHMMOut aminoacidsFile hitsFile


asyncPipeline :: forall r.
              ( LiftedBase IO r
              , '[ Reader ModelConfig
                 , Exc DSV.ParseError
                 , Exc FA.ParseError
                 ] <:: r
              )
              => Producer (FA.FastaEntry Nucleotide) (P.SafeT IO) (Either FA.ParseError ())
              -> ConcurrencyOpts (Eff r)
              -> Eff r (FrameRec '[M.VirusName, M.ModelName, M.ProteinHitScore])
asyncPipeline genomes concOpts = do
  asyncGenomeChunkProducer <- lift $ toAsyncEffProducer chunkedGenomes

  ((), df) <- Conc.runAsyncEffect (nworkers+1) $
      foldMap1 (asyncGenomeChunkProducer >|->) (pipelineWorkers concOpts)
      >||>
      Conc.restoreProducer >-|> Conc.asyncFold L.mconcat

  return df
  where
    toAsyncEffProducer :: Producer a (P.SafeT IO) (Either FA.ParseError ())
                       -> IO (Conc.AsyncProducer a (P.SafeT IO) () (Eff r) ())
    toAsyncEffProducer prod = do
      asyncProd <- fmap getAp <$> Conc.stealingAsyncProducer_ (nworkers+1) (fmap Ap prod)

      let hoistToEff = hoist (lift . P.runSafeT)

      return $ Conc.mapProducer liftEither (hoistToEff asyncProd)

    chunkedGenomes :: Producer [FA.FastaEntry Nucleotide] (P.SafeT IO) (Either FA.ParseError ())
    chunkedGenomes = Conc.bufferedChunks (fastaChunkSize concOpts) genomes

    nworkers :: Num a => a
    nworkers = fromIntegral $ length (pipelineWorkers concOpts)



runModel :: forall r.
         ( LiftedBase IO r
         , '[ Reader ModelConfig
            , Exc DSV.ParseError
            ] <:: r
         )
         => ModelInput r
         -> Eff r (FrameRec '[M.VirusName, M.ModelName, M.ProteinHitScore])
-- runModel (GivenHitsFile hitsFile) = processHMMOut hitsFile
runModel (GivenGenomes genomesFile concOpts) = asyncPipeline genomes concOpts
  where
    genomes = FA.fastaFileReader genomesFile


type ClassifiedCols rs = rs V.++ V.RDelete M.ModelName ClassificationCols
type RawClassifiedCols rs = rs V.++ V.RDelete M.ModelName RawClassificationCols


appendClassification :: forall rs. V.RElem M.ModelName rs (V.RIndex M.ModelName rs)
                     => FrameRec ClassificationCols
                     -> FrameRec rs
                     -> FrameRec (ClassifiedCols rs)
appendClassification cls =
    F.reindexed @M.ModelName %~ F.innerJoin (F.reindex' @M.ModelName cls)
