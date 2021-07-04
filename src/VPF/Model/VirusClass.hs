{-# options_ghc -Wno-partial-type-signatures #-}
{-# language ApplicativeDo #-}
{-# language BlockArguments #-}
{-# language DeriveGeneric #-}
{-# language DerivingVia #-}
{-# language PartialTypeSignatures #-}
{-# language OverloadedLabels #-}
{-# language StaticPointers #-}
{-# language Strict #-}
module VPF.Model.VirusClass where

import GHC.Generics (Generic)

import Control.Algebra
import Control.Carrier.Error.Excepts (ExceptsT, runExceptsT, Errors, throwErrors)
import Control.Distributed.SClosure
import Control.Effect.Reader
import Control.Effect.Distributed
import Control.Effect.Sum.Extra (HasAny)
import Control.Effect.Throw
import qualified Control.Foldl as Fold
import qualified Control.Lens as L
import Control.Monad
import Control.Monad.IO.Class (MonadIO(..))
import Control.Monad.Trans.Control (MonadBaseControl)
import Control.Monad.Trans.Reader (runReaderT)

import qualified Data.Aeson as Aeson
import Data.Bifunctor (first)
import qualified Data.ByteString.Char8 as BC
import Data.List (genericLength, isPrefixOf, isSuffixOf)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe)
import Data.Monoid (Ap(..))
import Data.Proxy (Proxy(..))
import Data.Semigroup (stimes)
import Data.Store (Store)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as T
import Data.Vector (Vector)
import qualified Data.Vector as Vec

import qualified Data.Vinyl as V

import Pipes (Producer, Pipe, (>->))
import qualified Pipes         as P
import qualified Pipes.Lift    as P
import qualified Pipes.Prelude as P
import Pipes.Safe (SafeT, runSafeT)
import qualified Pipes.Safe as PS

import Pipes.Concurrent.Async ((>||>), (>-|>))
import qualified Pipes.Concurrent.Async       as PA
import qualified Pipes.Concurrent.Synchronize as PA

import System.Directory as D
import System.FilePath ((</>), takeFileName)
import System.IO as IO

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

import qualified VPF.Util.Hash     as Hash
import qualified VPF.Util.Fasta    as FA
import qualified VPF.Util.FS       as FS
import qualified VPF.Util.Progress as Progress


newtype GenomeChunkKey = GenomeChunkKey { getGenomeChunkHash :: String }
    deriving Store via String
    deriving Aeson.FromJSON via String
    deriving Aeson.ToJSON via String


newtype WorkDir = WorkDir (Path Directory)
    deriving Store via (Path Directory)


type AggregatedHitsCols = '[M.VirusName, M.ModelName, M.ProteinHitScore]


data ModelConfig = ModelConfig
    { modelEValueThreshold    :: Double
    , modelVirusNameExtractor :: Text -> Text
    }

type PredictedCols = '[M.VirusName, Cls.ClassName, M.MembershipRatio, M.VirusHitScore, M.ConfidenceScore]


createGenomesSubdir :: (MonadIO m, Has (Reader WorkDir) m) => m (Path Directory)
createGenomesSubdir = do
    WorkDir wd <- ask
    let fp = untag wd </> "genomes"
    liftIO $ D.createDirectoryIfMissing True fp
    return (Tagged fp)


createProteinsSubdir :: (MonadIO m, Has (Reader WorkDir) m) => m (Path Directory)
createProteinsSubdir = do
    WorkDir wd <- ask
    let fp = untag wd </> "proteins"
    liftIO $ D.createDirectoryIfMissing True fp
    return (Tagged fp)


createHitsSubdir :: (MonadIO m, Has (Reader WorkDir) m) => m (Path Directory)
createHitsSubdir = do
    WorkDir wd <- ask
    let fp = untag wd </> "search"
    liftIO $ D.createDirectoryIfMissing True fp
    return (Tagged fp)


createProcessedHitsSubdir :: (MonadIO m, Has (Reader WorkDir) m) => m (Path Directory)
createProcessedHitsSubdir = do
    WorkDir wd <- ask
    let fp = untag wd </> "processed"
    liftIO $ D.createDirectoryIfMissing True fp
    return (Tagged fp)


getGenomeChunkKey :: Path (FASTA Nucleotide) -> IO GenomeChunkKey
getGenomeChunkKey f =
    fmap (GenomeChunkKey . BC.unpack . Hash.digestToHex)
         (Hash.hashFileSHA512t_256 (untag f))


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


writeGenomesFile ::
    ( MonadIO m
    , Has (Reader WorkDir) m
    )
    => Producer (FA.FastaEntry Nucleotide) (SafeT IO) ()
    -> m (GenomeChunkKey, Path (FASTA Nucleotide))
writeGenomesFile genomes = do
    genomesDir <- createGenomesSubdir

    tmpGenomesFile <- FS.emptyTmpFile genomesDir "split-genomes.fna"
    liftIO $ runSafeT $ P.runEffect $ genomes >-> FA.fastaFileWriter tmpGenomesFile

    key <- liftIO $ getGenomeChunkKey tmpGenomesFile
    let genomesFile = genomesFileFor genomesDir key
    liftIO $ D.renameFile (untag tmpGenomesFile) (untag genomesFile)

    return (key, genomesFile)


genomesFileFor :: Path Directory -> GenomeChunkKey -> Path (FASTA Nucleotide)
genomesFileFor dir (GenomeChunkKey hash) =
    Tagged (untag dir </> name)
  where
    name = "split-genomes-" ++ hash ++ ".fna"


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
    ( MonadBaseControl IO m
    , MonadIO m
    , Has HMMSearch m
    , Has Prodigal m
    , Has (Reader WorkDir) m
    )
    => GenomeChunkKey
    -> Path (FASTA Nucleotide)
    -> Path HMMERModel
    -> m (Path (FASTA Aminoacid), Path (HMMERTable ProtSearchHitCols))
searchGenomeHits key genomesFile vpfsFile = do
    protsDir <- createProteinsSubdir
    let protsFile = proteinFileFor protsDir key

    FS.whenNotExists protsFile $ FS.atomicCreateFile protsFile $ \tmpProtsFile ->
        prodigal genomesFile tmpProtsFile Nothing

    hitsDir <- createHitsSubdir
    let hitsFile = hitsFileFor hitsDir key

    FS.whenNotExists hitsFile $ FS.atomicCreateFile hitsFile $ \tmpHitsFile -> do
        protsFileIsEmpty <- liftIO $ isEmptyFAA protsFile

        if protsFileIsEmpty then
            liftIO $ createEmptyHitsFile tmpHitsFile
        else
            hmmsearch vpfsFile protsFile tmpHitsFile

    return (protsFile, hitsFile)
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


aggregateHits :: forall m.
    ( Has (Reader ModelConfig) m
    , Has (Throw DSV.ParseError) m
    , Has (Throw FA.ParseError) m
    , MonadIO m
    )
    => Path (FASTA Aminoacid)
    -> Path (HMMERTable ProtSearchHitCols)
    -> m (FrameRec AggregatedHitsCols)
aggregateHits aminoacidsFile hitsFile = do
    proteinSizes <- loadProteinSizes

    let hitRows = Tbl.produceRows hitsFile

    thr <- asks modelEValueThreshold
    getVirusName <- asks modelVirusNameExtractor

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

    loadProteinSizes :: m (GroupedFrameRec (Field M.ProteinName) '[M.KBaseSize])
    loadProteinSizes = do
        (colVecs, errs) <- liftIO $ runSafeT $
            Fold.impurely P.foldM' (F.colVecsFoldM 128) $
                FA.fastaFileReader aminoacidsFile
                  >-> P.map fastaEntryToRow

        either throwError return errs
        return $ F.setIndex @"protein_name" (F.fromColVecs colVecs)

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
                  aggNormScore <- L.sumOf (F.foldedRows . L.to normalizedScore)

                  return (F.singleField @"protein_hit_score" aggNormScore)

            |. F.rename @"query_name" @"model_name"
            |. F.copySoA


processHits ::
    ( MonadIO m
    , Has (Reader ModelConfig) m
    , Has (Reader WorkDir) m
    , Has (Throw DSV.ParseError) m
    , Has (Throw FA.ParseError) m
    )
    => GenomeChunkKey
    -> Path (FASTA Aminoacid)
    -> Path (HMMERTable ProtSearchHitCols)
    -> m (Path (DSV "\t" AggregatedHitsCols))
processHits key protsFile hitsFile = do
    aggregatedHits <- aggregateHits protsFile hitsFile

    processedHitsDir <- createProcessedHitsSubdir

    let processedHitsFile = processedHitsFileFor processedHitsDir key
        writerOpts = DSV.defWriterOptions '\t'

    liftIO $ runSafeT $ FS.atomicCreateFile processedHitsFile $ \tmpFile ->
        DSV.writeDSV writerOpts (FS.fileWriter (untag tmpFile)) aggregatedHits

    return processedHitsFile


predictMembership ::
    Cls.ClassificationParams
    -> GroupedFrameRec (Field M.VirusName) '[M.ModelName, M.ProteinHitScore]
    -> GroupedFrameRec (Field M.VirusName)
                       '[Cls.ClassName, M.MembershipRatio, M.VirusHitScore, M.ConfidenceScore]
predictMembership classParams = F.groups %~ do
    F.cat
      |. L.iso (F.setIndex @"model_name") F.dropIndex %~
            F.innerJoin (Cls.modelClasses classParams)

      |. F.summarizing @"class_name" %~ do
            let products = F.give $
                  F.val @"protein_hit_score"
                  * F.val @"class_percent"/100
                  * catWeight (F.val @"class_cat")

            classScore <- L.sumOf (F.foldedRows . L.to products)
            return (F.singleField @"class_score" classScore)

      |. F.arrange @(F.Desc "class_score")

      |. F.copySoA

      |. id %~ do
            totalScore <- L.sumOf (F.foldedRows . F.field @"class_score")
            let confidence = percentileRank (Cls.scoreSamples classParams) (Tagged totalScore)

            F.cat
              |. F.mutate1 @"virus_hit_score"  (const totalScore)
              |. F.mutate1 @"confidence_score" (const confidence)
              |. F.mutate1 @"membership_ratio" (F.give $ F.val @"class_score" / totalScore)
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

    catWeight :: Int -> Double
    catWeight cat = (5 - fromIntegral cat) / 4


-- asynchronous versions

data SearchHitsConcurrencyOpts = SearchHitsConcurrencyOpts
    { fastaChunkSize      :: Int
    , numSearchingWorkers :: Int
    }
    deriving Generic

instance Store SearchHitsConcurrencyOpts


asyncSearchHits :: forall m.
    ( MonadBaseControl IO m
    , MonadIO m
    , Has HMMSearch m
    , Has Prodigal m
    , Has (Reader WorkDir) m
    )
    => SearchHitsConcurrencyOpts
    -> Path HMMERModel
    -> [(GenomeChunkKey, Path (FASTA Nucleotide))]
    -> m [(GenomeChunkKey, Path (FASTA Aminoacid), Path (HMMERTable ProtSearchHitCols))]
asyncSearchHits concOpts vpfsFile genomesFiles = do
    let nworkers = numSearchingWorkers concOpts

    genomesFilesProducer <- liftIO $ PA.stealingEach genomesFiles

    PA.feedAsyncConsumer genomesFilesProducer $
        P.mapM (\(key, genomesFile) -> do
            (protsFile, hitsFile) <- searchGenomeHits key genomesFile vpfsFile
            return (key, protsFile, hitsFile))
        >-|>
        stimes nworkers PA.toListM


distribSearchHits :: forall m n w.
    ( MonadBaseControl IO m
    , MonadIO m
    , Has (Reader WorkDir) m
    , Has (Throw FA.ParseError) m
    , HasAny Distributed (Distributed n w) m
    , Typeable n
    , Typeable (Sig n)
    )
    => SDict
        ( MonadBaseControl IO n
        , MonadIO n
        , Has HMMSearch n
        , Has Prodigal n
        , Has (Reader WorkDir) n
        )
    -> SearchHitsConcurrencyOpts
    -> Path HMMERModel
    -> Producer (FA.FastaEntry Nucleotide) (SafeT IO) (Either FA.ParseError ())
    -> m [(GenomeChunkKey, Path (FASTA Aminoacid), Path (HMMERTable ProtSearchHitCols))]
distribSearchHits sdict concOpts vpfsFile genomes = do
    nslaves <- getNumWorkers_

    wd <- ask @WorkDir

    let genomesChunkWriter :: Pipe [FA.FastaEntry Nucleotide] (Integer, (GenomeChunkKey, Path _)) (SafeT IO) r
        genomesChunkWriter = P.mapM $ \chunk -> do
            kp <- liftIO $ runReaderT (writeGenomesFile (P.each chunk)) wd
            return (genericLength chunk, kp)

        chunkGenomeCount :: Pipe [(Integer, a)] (Integer, [a]) (SafeT IO) r
        chunkGenomeCount = P.map (first sum . unzip)

    genomesFilesProducer :: PA.AsyncProducer (Integer, [(_, _)]) (SafeT IO) () m () <- liftIO $
        genomes
          & PA.bufferedChunks (fromIntegral $ fastaChunkSize concOpts)
          & (>-> genomesChunkWriter)
          & PA.bufferedChunks (fromIntegral $ numSearchingWorkers concOpts)
          & (>-> chunkGenomeCount)
          & PA.stealingAsyncProducer (nslaves+1)
          & fmap \ap -> ap
          & PA.defaultOutput (Right ())
          & PA.hoist (liftIO . runSafeT)
          & PA.mapResultM (either throwError return)

    let asyncSearchHits' kps =
          static (\Dict -> asyncSearchHits @n)
            <:*> sdict
            <:*> spureWith (static Dict) concOpts
            <:*> spureWith (static Dict) vpfsFile
            <:*> spureWith (static Dict) kps

    progress <- liftIO $ Progress.init 0 (\nseqs -> "processsed " ++ show nseqs ++ " sequences")
    let updateProgress n = liftIO $ Progress.update progress (+ n)

    rs <- withWorkers_ $ \w -> do
        ((), r) <- PA.runAsyncEffect (nslaves+1) $
            genomesFilesProducer
            >||>
            P.mapM (\(n, kps) -> runInWorker_ w (static Dict) (asyncSearchHits' kps) <* updateProgress n)
            >-|>
            P.concat
            >-|>
            PA.toListM

        return r

    liftIO $ Progress.finish progress Nothing

    return rs


newtype ProcessHitsConcurrencyOpts = ProcessHitsConcurrencyOpts
    { numProcessingHitsWorkers :: Int
    }


asyncProcessHits :: forall m.
    ( MonadIO m
    , MonadBaseControl IO m
    , Has (Reader ModelConfig) m
    , Has (Reader WorkDir) m
    , Has (Throw DSV.ParseError) m
    , Has (Throw FA.ParseError) m
    )
    => ProcessHitsConcurrencyOpts
    -> [(GenomeChunkKey, Path (FASTA Aminoacid), Path (HMMERTable ProtSearchHitCols))]
    -> m [(GenomeChunkKey, Path (DSV "\t" AggregatedHitsCols))]
asyncProcessHits concOpts hitsFiles = do
    let nworkers = numProcessingHitsWorkers concOpts

    hitsFilesProducer <- liftIO $ PA.stealingEach hitsFiles

    PA.feedAsyncConsumer hitsFilesProducer $
        P.mapM (\(key, protsFile, hitsFile) -> (,) key <$> processHits key protsFile hitsFile)
        >-|>
        stimes nworkers PA.toListM


newtype PredictMembershipConcurrencyOpts = PredictMembershipConcurrencyOpts
    { numPredictingWorkers :: Int
    }


asyncPredictMemberships :: forall m.
    ( MonadBaseControl IO m
    , MonadIO m
    , Has (Throw DSV.ParseError) m
    )
    => PredictMembershipConcurrencyOpts
    -> Map (Field Cls.ClassKey) Cls.ClassificationFiles
    -> [Path (DSV "\t" AggregatedHitsCols)]
    -> Path Directory
    -> m [Path (DSV "\t" PredictedCols)]
asyncPredictMemberships concOpts classFiles aggHitsFiles outputDir = do
    let nworkers = fromIntegral $ numPredictingWorkers concOpts

    liftIO $ D.createDirectoryIfMissing True (untag outputDir)

    liftIO $ IO.hPutStrLn IO.stderr $ "loading VPF classifications"

    classes <- mapM (L._2 %%~ Cls.loadClassificationParams) (Map.toAscList classFiles)

    (Ap errors, paths) <- liftIO $ runSafeT $ do
        let sep = T.singleton '\t'
            parserOpts = DSV.defParserOptions '\t'

        classesWithHandles <-
            forM classes \(classKey, classParams) -> do
                let path = untag outputDir </> T.unpack (untag classKey) ++ ".tsv"

                h <- PS.mask_ $ do
                    hdl <- liftIO $ IO.openFile path IO.WriteMode
                    PS.register (IO.hClose hdl)
                    return hdl

                liftIO $ T.hPutStrLn h (DSV.headerToDSV @PredictedCols Proxy sep)

                return (Tagged path, (classParams, h))

        let (paths, classHandles) = unzip classesWithHandles

        liftIO $ IO.hPutStrLn IO.stderr $ "found " ++ show (length aggHitsFiles) ++ " aggregated hit files"

        Progress.tracking 0 (\nchunks -> "predicted " ++ show nchunks ++ " units") $ \progress -> do
            hitChunks <- liftIO $ PA.stealingEach aggHitsFiles

            let toBeWritten :: PA.AsyncProducer' (IO.Handle, Vec.Vector Text) IO (Ap (Either _) ())
                toBeWritten = PA.duplicatingAsyncProducer $
                    (pure () <$ hitChunks)
                    >->
                    runApExceptsP (P.mapM (DSV.readFrame parserOpts))
                    >->
                    scheduleForWriting classHandles sep

            liftIO $ PA.runAsyncEffect nworkers $
                stimes nworkers (PA.defaultOutput (pure ()) toBeWritten)
                >||>
                -- we don't want concurrent writes
                (paths <$ writeOutput progress)

    case errors of
      Left errs -> throwErrors @'[DSV.ParseError] errs
      Right ()  -> return paths
  where
    runApExceptsP ::
        Monad n
        => P.Proxy a' a b' b (ExceptsT es n) r
        -> P.Proxy a' a b' b n (Ap (Either (Errors es)) r)
    runApExceptsP = fmap Ap . runExceptsT . P.distribute

    scheduleForWriting ::
        [(Cls.ClassificationParams, IO.Handle)]
        -> Text
        -> Pipe (FrameRec AggregatedHitsCols) (IO.Handle, Vec.Vector Text) IO r
    scheduleForWriting classHandles sep =
        P.for P.cat \aggHits -> do
            let indexed = F.setIndex @"virus_name" aggHits

            forM_ classHandles \(classParams, h) -> do
                let predictedDSV = predictMembership classParams indexed
                      & F.resetIndex
                      & F.rows %~ DSV.rowToDSV sep
                      & F.toRowsVec

                P.yield $! (h, predictedDSV)

    writeOutput :: Progress.State Int -> PA.AsyncConsumer' (IO.Handle, Vec.Vector Text) IO ()
    writeOutput progress =
        PA.duplicatingAsyncConsumer $
            P.mapM_ \(h, rows) -> do
              mapM_ (T.hPutStrLn h) rows
              Progress.update progress (+1)


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
        runSearchHitsStep :: forall m n w.
            ( MonadBaseControl IO m
            , MonadIO m
            , Has (Reader WorkDir) m
            , Has (Throw FA.ParseError) m
            , HasAny Distributed (Distributed n w) m
            , Typeable (Sig n)
            , Typeable n
            )
            => SDict
                ( MonadBaseControl IO n
                , MonadIO n
                , Has HMMSearch n
                , Has Prodigal n
                , Has (Reader WorkDir) n
                )
            -> SearchHitsConcurrencyOpts
            -> Path HMMERModel
            -> Path (FASTA Nucleotide)
            -> m Checkpoint
    }
    | ProcessHitsStep {
        runProcessHitsStep :: forall m.
            ( MonadBaseControl IO m
            , MonadIO m
            , Has (Reader ModelConfig) m
            , Has (Reader WorkDir) m
            , Has (Throw DSV.ParseError) m
            , Has (Throw FA.ParseError) m
            )
            => ProcessHitsConcurrencyOpts
            -> m Checkpoint
    }
    | PredictMembershipStep {
        runPredictMembershipStep :: forall m.
            ( MonadBaseControl IO m
            , MonadIO m
            , Has (Reader WorkDir) m
            , Has (Throw DSV.ParseError) m
            )
            => PredictMembershipConcurrencyOpts
            -> Map (Field Cls.ClassKey) Cls.ClassificationFiles
            -> Path Directory
            -> m [Path (DSV "\t" PredictedCols)]
    }


tryLoadCheckpoint ::
    ( MonadIO m
    , Has (Throw CheckpointLoadError) m
    )
    => Path (JSON Checkpoint)
    -> m (Maybe Checkpoint)
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


checkpointStep :: Checkpoint -> ClassificationStep
checkpointStep checkpoint =
    case checkpoint of
      ContinueSearchingHits ->
          SearchHitsStep $ \sdict concOpts vpfsFile genomesFile -> do
              r <- distribSearchHits sdict concOpts vpfsFile (FA.fastaFileReader genomesFile)
              return $ ContinueFromHitsFiles (map (L.view L._1) r)

      ContinueFromHitsFiles keys ->
          ProcessHitsStep $ \concOpts -> do
              protsSubdir <- createProteinsSubdir
              hitsSubdir <- createHitsSubdir

              let hitsFiles =
                    [ (key, proteinFileFor protsSubdir key, hitsFileFor hitsSubdir key)
                      | key <- keys
                    ]

              r <- asyncProcessHits concOpts hitsFiles
              return (ContinueFromProcessedHits (map fst r))

      ContinueFromProcessedHits keys ->
          PredictMembershipStep $ \concOpts classFiles outputDir -> do
              aggHitsSubdir <- createProcessedHitsSubdir

              let aggHitsFiles = map (processedHitsFileFor aggHitsSubdir) keys

              asyncPredictMemberships concOpts classFiles aggHitsFiles outputDir


-- run the whole process


checkpointFileFor :: WorkDir -> GenomeChunkKey -> Path (JSON Checkpoint)
checkpointFileFor (WorkDir dir) (GenomeChunkKey hash) =
    Tagged (untag dir </> name)
  where
    name = "checkpoint-" ++ hash ++ ".json"


runClassification :: forall m a.
    ( MonadIO m
    , Has (Reader WorkDir) m
    , Has (Throw CheckpointLoadError) m
    )
    => Path (FASTA Nucleotide)
    -> ((Checkpoint -> m a) -> ClassificationStep -> m a)
    -> m a
runClassification fullGenomesFile runSteps = do
    key <- liftIO $ getGenomeChunkKey fullGenomesFile

    wd <- ask

    let checkpointFile = checkpointFileFor wd key

    maybeCheckpoint <- tryLoadCheckpoint checkpointFile

    let checkpoint = fromMaybe ContinueSearchingHits maybeCheckpoint

    runSteps (resume checkpointFile) (checkpointStep checkpoint)
  where
    resume :: Path (JSON Checkpoint) -> Checkpoint -> m a
    resume checkpointFile checkpoint = do
        liftIO $ saveCheckpoint checkpointFile checkpoint
        runSteps (resume checkpointFile) (checkpointStep checkpoint)
