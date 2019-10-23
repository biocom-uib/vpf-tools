{-# language ApplicativeDo #-}
{-# language BlockArguments #-}
{-# language DeriveGeneric #-}
{-# language DerivingVia #-}
{-# language OverloadedLabels #-}
{-# language OverloadedStrings #-}
{-# language StrictData #-}
module VPF.Model.VirusClass where

import GHC.Generics (Generic)

import Control.Eff
import Control.Eff.Reader.Strict (Reader, reader)
import Control.Eff.Exception (Exc, liftEither, throwError)
import qualified Control.Lens as L
import Control.Monad.IO.Class (MonadIO(..))
import Control.Monad.Trans.Control (MonadBaseControl, StM)
import qualified Control.Foldl as Fold

import qualified Data.Aeson as Aeson
import qualified Data.ByteString.Char8 as BC
import Data.List (isPrefixOf, isSuffixOf)
import qualified Data.List.NonEmpty as NE
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe)
import Data.Semigroup (stimes)
import Data.Semigroup.Foldable (foldMap1)
import Data.Store (Store)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)
import qualified Data.Vector as Vec

import qualified Data.Vinyl as V

import Numeric.Natural (Natural)

import Pipes (Pipe, Producer, (>->))
import qualified Pipes         as P
import qualified Pipes.Prelude as P
import Pipes.Safe (SafeT, runSafeT)

import System.Directory as D
import System.FilePath ((</>), takeFileName)
import System.IO as IO

import VPF.Concurrency.Async ((>||>), (>|->), (>-|>))
import qualified VPF.Concurrency.Async as Conc
import qualified VPF.Concurrency.Pipes as Conc

import VPF.Eff.Cmd (Cmd)
import VPF.Ext.Prodigal (Prodigal, prodigal)
import VPF.Ext.HMMER.Search (HMMSearch, ProtSearchHitCols, hmmsearch)
import qualified VPF.Ext.HMMER.Search.Cols as HMM
import qualified VPF.Ext.HMMER.TableFormat as Tbl

import VPF.Formats
import VPF.Frames.Dplyr.Ops
import VPF.Frames.Types
import qualified VPF.Frames.Dplyr  as F
import qualified VPF.Frames.DSV    as DSV
import qualified VPF.Frames.InCore as F

import qualified VPF.Model.Cols       as M
import qualified VPF.Model.Class      as Cls
import qualified VPF.Model.Class.Cols as Cls

import qualified VPF.Util.Hash  as Hash
import qualified VPF.Util.Fasta as FA
import qualified VPF.Util.FS    as FS


newtype GenomeChunkKey = GenomeChunkKey { getGenomeChunkHash :: String }
    deriving Store via String
    deriving Aeson.FromJSON via String
    deriving Aeson.ToJSON via String



type AggregatedHitsCols = '[M.VirusName, M.ModelName, M.ProteinHitScore]


data ModelConfig = ModelConfig
    { modelEValueThreshold    :: Double
    , modelVirusNameExtractor :: Text -> Text
    }

type PredictedCols = '[M.VirusName, Cls.ClassName, M.MembershipRatio, M.VirusHitScore, M.ConfidenceScore]


createGenomesSubdir :: Path Directory -> IO (Path Directory)
createGenomesSubdir wd = do
    let fp = untag wd </> "genomes"
    D.createDirectoryIfMissing True fp
    return (Tagged fp)


createProteinsSubdir :: Path Directory -> IO (Path Directory)
createProteinsSubdir wd = do
    let fp = untag wd </> "proteins"
    D.createDirectoryIfMissing True fp
    return (Tagged fp)


createHitsSubdir :: Path Directory -> IO (Path Directory)
createHitsSubdir wd = do
    let fp = untag wd </> "search"
    D.createDirectoryIfMissing True fp
    return (Tagged fp)


createProcessedHitsSubdir :: Path Directory -> IO (Path Directory)
createProcessedHitsSubdir wd = do
    let fp = untag wd </> "processed"
    D.createDirectoryIfMissing True fp
    return (Tagged fp)


getGenomeChunkKey :: Path (FASTA Nucleotide) -> IO GenomeChunkKey
getGenomeChunkKey f =
    fmap (GenomeChunkKey . BC.unpack . Hash.digestToHex)
         (Hash.hashFileSHA512t_256 (untag f))


genomesFileFor :: Path Directory -> GenomeChunkKey -> Path (FASTA Nucleotide)
genomesFileFor dir (GenomeChunkKey hash) =
    Tagged (untag dir </> name)
  where
    name = "split-genomes-" ++ hash ++ ".fna"


checkpointFileFor :: Path Directory -> GenomeChunkKey -> Path (JSON Checkpoint)
checkpointFileFor dir (GenomeChunkKey hash) =
    Tagged (untag dir </> name)
  where
    name = "checkpoint-" ++ hash ++ ".json"


parseGenomesFileName :: Path (FASTA Nucleotide) -> Either (Path (FASTA Nucleotide)) GenomeChunkKey
parseGenomesFileName (Tagged fp) =
    if isPrefixOf prefix fileName && isSuffixOf suffix fileName then
        let
          withoutPrefix = drop (length prefix) fileName
          withoutSuffix = take (length withoutPrefix - length suffix) withoutPrefix
        in
          Right (GenomeChunkKey withoutSuffix)
    else
        Left (Tagged fileName)
  where
    fileName = takeFileName fp

    prefix = "split-hits-"
    suffix = ".fna"


proteinFileFor :: Path Directory -> GenomeChunkKey -> Path (FASTA Aminoacid)
proteinFileFor dir (GenomeChunkKey hash) =
    Tagged (untag dir </> name)
  where
    name = "split-proteins-" ++ hash ++ ".faa"


hitsFileFor :: Path Directory -> GenomeChunkKey -> Path (HMMERTable ProtSearchHitCols)
hitsFileFor dir (GenomeChunkKey hash) =
    Tagged (untag dir </> name)
  where
    name = "split-hits-" ++ hash ++ ".hmmout"


processedHitsFileFor :: Path Directory -> GenomeChunkKey -> Path (DSV "\t" AggregatedHitsCols)
processedHitsFileFor dir (GenomeChunkKey hash) =
    Tagged (untag dir </> name)
  where
    name = "processed-hits-" ++ hash ++ ".tsv"


searchGenomeHits ::
                 ( LiftedBase IO r
                 , Member (Cmd HMMSearch) r
                 , Member (Cmd Prodigal) r
                 )
                 => Path Directory
                 -> Path HMMERModel
                 -> Producer (FA.FastaEntry Nucleotide) (SafeT IO) ()
                 -> Eff r (GenomeChunkKey, Path (FASTA Aminoacid), Path (HMMERTable ProtSearchHitCols))
searchGenomeHits wd vpfsFile genomes = do
    genomesDir <- liftIO $ createGenomesSubdir wd

    tmpGenomesFile <- FS.emptyTmpFile genomesDir "split-genomes.fna"
    liftIO $ runSafeT $ P.runEffect $ genomes >-> FA.fastaFileWriter tmpGenomesFile

    key <- liftIO $ getGenomeChunkKey tmpGenomesFile
    let genomesFile = genomesFileFor genomesDir key
    liftIO $ D.renameFile (untag tmpGenomesFile) (untag genomesFile)

    protsDir <- liftIO $ createProteinsSubdir wd
    let protsFile = proteinFileFor protsDir key

    FS.whenNotExists protsFile $ FS.atomicCreateFile protsFile $ \tmpProtsFile ->
        prodigal genomesFile tmpProtsFile Nothing

    hitsDir <- liftIO $ createHitsSubdir wd
    let hitsFile = hitsFileFor hitsDir key

    FS.whenNotExists hitsFile $ FS.atomicCreateFile hitsFile $ \tmpHitsFile -> do
        protsFileIsEmpty <- liftIO $ isEmptyFAA protsFile

        if protsFileIsEmpty then
            liftIO $ createEmptyHitsFile tmpHitsFile
        else
            hmmsearch vpfsFile protsFile tmpHitsFile

    return (key, protsFile, hitsFile)
  where
    isEmptyFAA :: Path (FASTA Aminoacid) -> IO Bool
    isEmptyFAA fp = runSafeT $ do
        firstItem <- P.next (FA.fastaFileReader fp)

        case firstItem of
          Left (Left _e)  -> return False
          Left (Right ()) -> return True
          Right (_, _)    -> return False

    createEmptyHitsFile :: Path (HMMERTable ProtSearchHitCols) -> IO ()
    createEmptyHitsFile fp = IO.withFile (untag fp) IO.WriteMode $ \_ -> return ()


aggregateHits :: forall r.
              ( Lifted IO r
              , '[ Reader ModelConfig
                 , Exc DSV.ParseError
                 , Exc FA.ParseError
                 ] <:: r
              )
              => Path (FASTA Aminoacid)
              -> Path (HMMERTable ProtSearchHitCols)
              -> Eff r (FrameRec AggregatedHitsCols)
aggregateHits aminoacidsFile hitsFile = do
    proteinSizes <- loadProteinSizes

    let hitRows = Tbl.produceRows hitsFile

    thr <- reader modelEValueThreshold
    getVirusName <- reader modelVirusNameExtractor

    hitsFrame <- DSV.inCoreAoSExc $
        hitRows
        >-> P.filter (\row -> row^.HMM.sequenceEValue <= thr)
        >-> P.map V.rcast

    return $ hitsFrame
        & F.mutate1 @"virus_name" (getVirusName . L.view HMM.targetName)
        & aggregate proteinSizes
  where
    fastaEntryToRow :: FA.FastaEntry Aminoacid -> Record '[M.ProteinName, M.KBaseSize]
    fastaEntryToRow entry =
        V.fieldRec
          ( #protein_name =: FA.removeNameComments (FA.entryName entry)
          , #k_base_size  =: fromIntegral (FA.entrySeqNumBases entry) / 1000
          )

    loadProteinSizes :: Eff r (GroupedFrameRec (Field M.ProteinName) '[M.KBaseSize])
    loadProteinSizes = do
        (colVecs, errs) <- lift $ runSafeT $
            Fold.impurely P.foldM' (F.colVecsFoldM 128) $
                FA.fastaFileReader aminoacidsFile
                  >-> P.mapM (\entry -> return $! fastaEntryToRow entry)

        liftEither errs
        return $! F.setIndex @"protein_name" (F.fromColVecs colVecs)

    aggregate :: GroupedFrameRec (Field M.ProteinName) '[M.KBaseSize]
              -> FrameRec '[M.VirusName, HMM.TargetName, HMM.QueryName, HMM.SequenceScore]
              -> FrameRec '[M.VirusName, M.ModelName, M.ProteinHitScore]
    aggregate proteinSizes =
        F.reindexed @"virus_name" . F.groups %~ do
          F.cat
            |. F.reindexed @"target_name" %~ do
                F.cat
                  |. F.renameIndexTo @"protein_name"
                  |. F.groups %~ F.top @(F.Desc "sequence_score") 1
                  |. F.innerJoin proteinSizes

            |. F.summarizing @"query_name" %~ do
                  let normalizedScore = F.give $ F.val @"sequence_score" / F.val @"k_base_size"

                  F.singleField @"protein_hit_score" . sum . F.rowwise normalizedScore

            |. F.rename @"query_name" @"model_name"


processHits :: forall r.
              ( Lifted IO r
              , '[ Reader ModelConfig
                 , Exc DSV.ParseError
                 , Exc FA.ParseError
                 ] <:: r
              )
              => Path Directory
              -> GenomeChunkKey
              -> Path (FASTA Aminoacid)
              -> Path (HMMERTable ProtSearchHitCols)
              -> Eff r (Path (DSV "\t" AggregatedHitsCols))
processHits wd key protsFile hitsFile = do
    aggregatedHits <- aggregateHits protsFile hitsFile

    processedHitsDir <- liftIO $ createProcessedHitsSubdir wd

    let processedHitsFile = processedHitsFileFor processedHitsDir key
        writerOpts = DSV.defWriterOptions '\t'

    liftIO $ runSafeT $ FS.atomicCreateFile processedHitsFile $ \tmpFile ->
        DSV.writeDSV writerOpts (FS.fileWriter (untag tmpFile)) aggregatedHits

    return processedHitsFile


predictMembership :: Cls.ClassificationParams
                  -> GroupedFrameRec (Field M.VirusName) '[M.ModelName, M.ProteinHitScore]
                  -> GroupedFrameRec (Field M.VirusName)
                                     '[Cls.ClassName, M.MembershipRatio, M.VirusHitScore, M.ConfidenceScore]
predictMembership classParams = F.groups %~ do
    F.cat
      |. L.iso (F.setIndex @"model_name") F.dropIndex %~
            F.innerJoin (Cls.modelClasses classParams)

      |. F.summarizing @"class_name" %~ do
            let products = F.give $ F.val @"protein_hit_score" * F.val @"class_percent"/100 * F.val @"protein_hit_score"

            F.singleField @"protein_hit_score" . sum . F.rowwise products

      |. do
          \df -> do
            let totalScore = sum (df & F.rows %~ F.get @"protein_hit_score")
                confidence = percentileRank (Cls.scoreSamples classParams) (Tagged totalScore)

            df & F.cat
              |. F.mutate1 @"virus_hit_score"  (const totalScore)
              |. F.mutate1 @"confidence_score" (const confidence)
              |. F.mutate1 @"membership_ratio" (F.give $ F.val @"protein_hit_score" / totalScore)
              |. F.select_
  where
    percentileRank :: Ord a => Vector a -> a -> Double
    percentileRank v a =
        case Vec.span (< a) v of
          (less, v') ->
              case Vec.span (== a) v' of
                (equal, _) ->
                    let c = fromIntegral $ Vec.length less
                        f = fromIntegral $ Vec.length equal
                        n = fromIntegral $ Vec.length v
                    in  (c + 0.5*f) / n


-- asynchronous versions

pipeAsyncWorkers :: forall n m a b c. (MonadIO n, MonadIO m, MonadBaseControl IO m)
                 => Conc.AsyncProducer a n () m ()
                 -> NE.NonEmpty (Pipe a (StM m b) n ())
                 -> Conc.AsyncConsumer b m () m c
                 -> m c
pipeAsyncWorkers asyncProducer workers asyncConsumer = do
    let nworkers = fromIntegral $ length workers

    ((), rs) <- Conc.runAsyncEffect (nworkers+1) $
        foldMap1 (asyncProducer >|->) workers
        >||>
        Conc.restoreProducer >-|> asyncConsumer

    return rs


type SearchHitsPipeline m =
    Pipe [FA.FastaEntry Nucleotide]
         (StM m [(GenomeChunkKey, Path (FASTA Aminoacid), Path (HMMERTable ProtSearchHitCols))])
         (SafeT IO)
         ()

data SearchHitsConcurrencyOpts m = SearchHitsConcurrencyOpts
    { fastaChunkSize   :: Int
    , searchingWorkers :: NE.NonEmpty (SearchHitsPipeline m)
    }

asyncSearchHits :: forall r.
                ( LiftedBase IO r
                , '[ Exc FA.ParseError
                   ] <:: r
                )
                => Producer (FA.FastaEntry Nucleotide) (SafeT IO) (Either FA.ParseError ())
                -> SearchHitsConcurrencyOpts (Eff r)
                -> Eff r [(GenomeChunkKey, Path (FASTA Aminoacid), Path (HMMERTable ProtSearchHitCols))]
asyncSearchHits genomes concOpts = do
    let nworkers = fromIntegral $ length (searchingWorkers concOpts)

    asyncChunkProducer <- liftIO $ Conc.stealingAsyncProducerA (nworkers+1) $
        Conc.bufferedChunks (fastaChunkSize concOpts) genomes

    let asyncEffProducer :: Conc.AsyncProducer [FA.FastaEntry Nucleotide] (SafeT IO) () (Eff r) ()
        asyncEffProducer = Conc.mapProducerM liftEither $ Conc.runAsyncSafeT asyncChunkProducer

    pipeAsyncWorkers asyncEffProducer
                     (searchingWorkers concOpts)
                     (concatList >-|> Conc.toListM)
  where
    concatList :: Monad m => Pipe [b] b m s
    concatList = P.concat


type ProcessHitsPipeline m =
    Pipe (GenomeChunkKey, Path (FASTA Aminoacid), Path (HMMERTable ProtSearchHitCols))
         (StM m (GenomeChunkKey, Path (DSV "\t" AggregatedHitsCols)))
         (SafeT IO)
         ()

newtype ProcessHitsConcurrencyOpts m = ProcessHitsConcurrencyOpts
    { processingHitsWorkers :: NE.NonEmpty (ProcessHitsPipeline m)
    }

asyncProcessHits :: LiftedBase IO r
                 => [(GenomeChunkKey, Path (FASTA Aminoacid), Path (HMMERTable ProtSearchHitCols))]
                 -> ProcessHitsConcurrencyOpts (Eff r)
                 -> Eff r [(GenomeChunkKey, Path (DSV "\t" AggregatedHitsCols))]
asyncProcessHits hitsFiles concOpts = do
    let nworkers = fromIntegral $ length (processingHitsWorkers concOpts)

    asyncHitFileProducer <- liftIO $ Conc.stealingAsyncProducer_ (nworkers+1) (P.each hitsFiles)

    pipeAsyncWorkers (Conc.runAsyncSafeT asyncHitFileProducer)
                     (processingHitsWorkers concOpts)
                     Conc.toListM


newtype PredictMembershipConcurrencyOpts m = PredictMembershipConcurrencyOpts
    { predictingNumWorkers :: Natural
    }

asyncPredictMemberships :: forall r. (LiftedBase IO r, Member (Exc DSV.ParseError) r)
                        => Map (Field Cls.ClassKey) Cls.ClassificationFiles
                        -> [Path (DSV "\t" AggregatedHitsCols)]
                        -> Path Directory
                        -> PredictMembershipConcurrencyOpts (Eff r)
                        -> Eff r [Path (DSV "\t" PredictedCols)]
asyncPredictMemberships classFiles aggHitsFiles outputDir concOpts = do
    let nworkers :: Num a => a
        nworkers = fromIntegral $ predictingNumWorkers concOpts

    liftIO $ D.createDirectoryIfMissing True (untag outputDir)

    ((), aggHitss) <- Conc.runAsyncEffect (nworkers+1) $
        Conc.duplicatingAsyncProducer (P.each aggHitsFiles)
        >||>
        P.mapM (\aggHitFile -> DSV.readFrame aggHitFile (DSV.defParserOptions '\t'))
        >-|>
        P.map (F.setIndex @"virus_name")
        >-|>
        stimes (nworkers @Natural) Conc.toListM

    ((), paths) <- Conc.runAsyncEffect (nworkers+1) $
        Conc.duplicatingAsyncProducer (P.each (Map.toAscList classFiles))
        >||>
        P.mapM (L._2 %%~ Cls.loadClassificationParams)
        >-|>
        P.map (L._2 %~ predictAll aggHitss)
        >-|>
        P.mapM (\(classKey, prediction) -> writeOutput classKey prediction)
        >-|>
        stimes (nworkers @Natural) Conc.toListM

    return paths
  where
    predictAll :: Monad m
               => [GroupedFrameRec (Field M.VirusName) '[M.ModelName, M.ProteinHitScore]]
               -> Cls.ClassificationParams
               -> Producer (Record PredictedCols) m ()
    predictAll aggHitss classParams =
        P.each aggHitss
        >-> P.map do
              predictMembership classParams
                |. F.groups %~ F.arrange @(F.Desc "membership_ratio")
                |. F.resetIndex
        >-> P.concat

    writeOutput :: Field ("class_key" ::: Text)
                -> Producer (Record PredictedCols) (SafeT IO) ()
                -> Eff r (Path (DSV "\t" PredictedCols))
    writeOutput classKey rows = do
        let path = untag outputDir </> T.unpack (untag classKey) ++ ".tsv"

        liftIO $ runSafeT $ P.runEffect $
            rows
            >-> DSV.pipeDSVLines (DSV.defWriterOptions '\t')
            >-> FS.fileWriter path

        return (Tagged path)


-- stopping/resuming from checkpoints

data Checkpoint
    = ContinueSearchingHits
    | ContinueFromHitsFiles [GenomeChunkKey]
    | ContinueFromProcessedHits [GenomeChunkKey]
    deriving Generic

instance Aeson.FromJSON Checkpoint
instance Aeson.ToJSON Checkpoint


newtype CheckpointLoadError = CheckpointJSONLoadError String
    deriving Store via CheckpointLoadError


data ClassificationStep
    = SearchHitsStep {
        runSearchHitsStep :: forall r.
            ( LiftedBase IO r
            , '[ Exc FA.ParseError
               ] <:: r
            )
            => Path (FASTA Nucleotide)
            -> SearchHitsConcurrencyOpts (Eff r)
            -> Eff r Checkpoint
    }
    | ProcessHitsStep {
        runProcessHitsStep :: forall r.
            ( LiftedBase IO r
            , '[ Reader ModelConfig
               , Exc DSV.ParseError
               , Exc FA.ParseError
               ] <:: r
            )
            => ProcessHitsConcurrencyOpts (Eff r)
            -> Eff r Checkpoint
    }
    | PredictMembershipStep {
        runPredictMembershipStep :: forall r.
            ( LiftedBase IO r
            , '[ Exc DSV.ParseError
               ] <:: r
            )
            => Map (Field Cls.ClassKey) Cls.ClassificationFiles
            -> Path Directory
            -> PredictMembershipConcurrencyOpts (Eff r)
            -> Eff r [Path (DSV "\t" PredictedCols)]
    }


tryLoadCheckpoint :: (Lifted IO r, Member (Exc CheckpointLoadError) r)
                  => Path (JSON Checkpoint)
                  -> Eff r (Maybe Checkpoint)
tryLoadCheckpoint checkpointFile = do
    exists <- liftIO $ D.doesFileExist (untag checkpointFile)

    if exists then do
        r <- liftIO $ Aeson.eitherDecodeFileStrict' (untag checkpointFile)

        case r of
          Left e           -> throwError (CheckpointJSONLoadError e)
          Right checkpoint -> return (Just checkpoint)
    else
        return Nothing


saveCheckpoint :: Path (JSON Checkpoint) -> Checkpoint -> IO ()
saveCheckpoint checkpointFile checkpoint =
    FS.atomicCreateFile checkpointFile $ \tmp ->
        Aeson.encodeFile (untag tmp) checkpoint


checkpointStep :: Path Directory -> Checkpoint -> ClassificationStep
checkpointStep wd checkpoint =
    case checkpoint of
      ContinueSearchingHits ->
          SearchHitsStep $ \genomesFile concOpts -> do
              r <- asyncSearchHits (FA.fastaFileReader genomesFile) concOpts
              return $ ContinueFromHitsFiles (map (L.view L._1) r)

      ContinueFromHitsFiles keys ->
          ProcessHitsStep $ \concOpts -> do
              protsSubdir <- liftIO $ createProteinsSubdir wd
              hitsSubdir <- liftIO $ createHitsSubdir wd

              let hitsFiles =
                    [ (key, proteinFileFor protsSubdir key, hitsFileFor hitsSubdir key)
                      | key <- keys
                    ]

              r <- asyncProcessHits hitsFiles concOpts
              return (ContinueFromProcessedHits (map fst r))

      ContinueFromProcessedHits keys ->
          PredictMembershipStep $ \classFiles outputDir concOpts -> do
              aggHitsSubdir <- liftIO $ createProcessedHitsSubdir wd

              let aggHitsFiles = map (processedHitsFileFor aggHitsSubdir) keys

              asyncPredictMemberships classFiles aggHitsFiles outputDir concOpts


-- run the whole process

runClassification :: forall r a. (Lifted IO r, Member (Exc CheckpointLoadError) r)
                  => Path Directory
                  -> Path (FASTA Nucleotide)
                  -> ((Checkpoint -> Eff r a) -> ClassificationStep -> Eff r a)
                  -> Eff r a
runClassification wd fullGenomesFile runSteps = do
    key <- liftIO $ getGenomeChunkKey fullGenomesFile

    let checkpointFile = checkpointFileFor wd key

    maybeCheckpoint <- tryLoadCheckpoint checkpointFile

    let checkpoint = fromMaybe ContinueSearchingHits maybeCheckpoint

    runSteps (resume checkpointFile) (checkpointStep wd checkpoint)
  where
    resume :: Path (JSON Checkpoint) -> Checkpoint -> Eff r a
    resume checkpointFile checkpoint = do
        liftIO $ saveCheckpoint checkpointFile checkpoint
        runSteps (resume checkpointFile) (checkpointStep wd checkpoint)
