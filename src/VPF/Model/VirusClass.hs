{-# language ApplicativeDo #-}
{-# language BlockArguments #-}
{-# language OverloadedStrings #-}
{-# language StrictData #-}
module VPF.Model.VirusClass where

import Control.Eff
import Control.Eff.Reader.Strict (Reader, reader)
import Control.Eff.Exception (Exc, liftEither, throwError)
import Control.Lens (view, toListOf, (%~))
import Control.Monad ((<=<))
import Control.Monad.IO.Class (MonadIO(..))
import Control.Monad.Morph (hoist)
import qualified Control.Monad.Trans.Class as MT
import Control.Monad.Trans.Control (StM, liftBaseWith, restoreM, control)
import qualified Control.Foldl as L

import Data.Function ((&))
import Data.Kind (Type)
import qualified Data.List.NonEmpty as NE
import Data.Monoid (Ap(..))
import Data.Semigroup (stimes)
import Data.Semigroup.Foldable (foldMap1)
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
import VPF.Ext.Prodigal (Prodigal, prodigal)
import qualified VPF.Ext.HMMER.Search      as HMM
import qualified VPF.Ext.HMMER.Search.Cols as HMM
import qualified VPF.Ext.HMMER.TableFormat as Tbl

import VPF.Formats
import VPF.Model.Class (ClassificationCols, RawClassificationCols)
import qualified VPF.Model.Cols as M

import VPF.Concurrency.Async ((>||>), (>|->), (>-|>))
import qualified VPF.Concurrency.Async as Conc
import qualified VPF.Concurrency.Pipes as Conc

import VPF.Util.Dplyr ((|.))
import qualified VPF.Util.Dplyr as D
import qualified VPF.Util.DSV   as DSV
import qualified VPF.Util.Fasta as FA
import qualified VPF.Util.FS    as FS


data ConcurrencyOpts m = ConcurrencyOpts
    { fastaChunkSize  :: Int
    , pipelineWorkers :: NE.NonEmpty (
                          Pipe [FA.FastaEntry]
                               (StM m (FrameRec '[M.VirusName, M.ModelName, M.NumHits]))
                               (SafeT IO)
                               ())
    }


data ModelInput (r :: [Type -> Type]) where
    GivenHitsFile :: Path (HMMERTable HMM.ProtSearchHitCols)
                  -> ModelInput r

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
    { modelEValueThreshold :: Double
    }


searchGenomeHits ::
                 ( Lifted IO r
                 , Member (Cmd HMM.HMMSearch) r
                 , Member (Cmd Prodigal) r
                 )
                 => Path Directory
                 -> Path HMMERModel
                 -> Producer FA.FastaEntry (SafeT IO) ()
                 -> Eff r (Path (HMMERTable HMM.ProtSearchHitCols))
searchGenomeHits wd vpfsFile genomes = do
    genomesFile <- FS.emptyTmpFile @(FASTA Nucleotide) wd "split-genomes.fna"

    liftIO $ runSafeT $ P.runEffect $
        genomes
          >-> FA.fastaLines
          >-> FS.fileWriter (untag genomesFile)

    aminoacidsFile <- FS.emptyTmpFile @(FASTA Aminoacid) wd "split-proteins.faa"
    prodigal genomesFile aminoacidsFile Nothing

    hitsFile <- FS.emptyTmpFile @(HMMERTable HMM.ProtSearchHitCols) wd "split-hits.txt"
    HMM.hmmsearch vpfsFile aminoacidsFile hitsFile

    return hitsFile


processHMMOut ::
              ( Lifted IO r
              , '[ Reader ModelConfig
                 , Exc DSV.ParseError
                 ] <:: r
              )
              => Path (HMMERTable HMM.ProtSearchHitCols)
              -> Eff r (FrameRec '[M.VirusName, M.ModelName, M.NumHits])
processHMMOut hitsFile = do
    let hitRows = Tbl.produceRows hitsFile

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


asyncPipeline :: forall r.
              ( LiftedBase IO r
              , '[ Reader ModelConfig
                 , Exc DSV.ParseError
                 , Exc FA.ParseError
                 ] <:: r
              )
              => Producer FA.FastaEntry (SafeT IO) (Either FA.ParseError ())
              -> ConcurrencyOpts (Eff r)
              -> Eff r (FrameRec '[M.VirusName, M.ModelName, M.NumHits])
asyncPipeline genomes concOpts = do
  asyncGenomeChunkProducer <- lift $ toAsyncEffProducer chunkedGenomes

  ((), df) <- Conc.runAsyncEffect (nworkers+1) $
      foldMap1 (asyncGenomeChunkProducer >|->) (pipelineWorkers concOpts)
      >||>
      Conc.restoreProducer >-|> Conc.asyncFold L.mconcat

  return df
  where
    toAsyncEffProducer :: Producer a (SafeT IO) (Either FA.ParseError ())
                       -> IO (Conc.AsyncProducer a (SafeT IO) () (Eff r) ())
    toAsyncEffProducer prod = do
      asyncProd <- fmap getAp <$> Conc.stealingAsyncProducer_ (nworkers+1) (fmap Ap prod)

      let hoistToEff = hoist (lift . runSafeT)

      return $ Conc.mapProducer liftEither (hoistToEff asyncProd)

    chunkedGenomes :: Producer [FA.FastaEntry] (SafeT IO) (Either FA.ParseError ())
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
         -> Eff r (FrameRec '[M.VirusName, M.ModelName, M.NumHits])
runModel (GivenHitsFile hitsFile) = processHMMOut hitsFile
runModel (GivenGenomes genomesFile concOpts) = asyncPipeline genomes concOpts
  where
    genomes = FA.parsedFastaEntries $ FS.fileReader (untag genomesFile)


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
