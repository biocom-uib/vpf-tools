{-# LANGUAGE PartialTypeSignatures #-}
{-# language BlockArguments #-}
{-# language MonoLocalBinds #-}
{-# language OverloadedStrings #-}
module Main where

import Control.Applicative ((<**>))
import qualified Control.Distributed.MPI.Store as MPI
import Control.Eff (Eff, Member, Lift, Lifted, LiftedBase, lift, runLift)
import Control.Eff.Reader.Strict (Reader, runReader)
import Control.Eff.Exception (Exc, runError, Fail, die, runFail)
import qualified Control.Lens as L
import qualified Control.Monad.Catch as MC
import Control.Monad.IO.Class (MonadIO(liftIO))
import Control.Monad.Trans.Control (RunInBase, StM, liftBaseWith, restoreM)

import qualified Data.ByteString as BS
import Data.Foldable (toList)
import qualified Data.Map as Map
import Data.Maybe (fromMaybe, listToMaybe)
import qualified Data.List.NonEmpty as NE
import Data.Text (Text)
import qualified Data.Text                  as T
import qualified Data.Text.Encoding         as T
import qualified Text.Regex.Base            as PCRE
import qualified Text.Regex.PCRE.ByteString as PCRE

import qualified System.Directory as D
import System.Exit (exitFailure)
import qualified System.IO as IO

import qualified Pipes         as P
import qualified Pipes.Prelude as P
import qualified Pipes.Safe    as PS

import qualified Options.Applicative as OptP

import qualified VPF.Concurrency.Async as Conc
import qualified VPF.Concurrency.MPI   as Conc

import VPF.Eff.Cmd (Cmd)
import VPF.Ext.HMMER.Search (HMMSearch, HMMSearchError, hmmsearchConfig, execHMMSearch)
import VPF.Ext.Prodigal     (Prodigal, ProdigalError, prodigalConfig, execProdigal)

import VPF.Formats
import VPF.Frames.Dplyr.Ops
import qualified VPF.Frames.Dplyr  as F
import qualified VPF.Frames.DSV    as DSV
import qualified VPF.Frames.InCore as F

import qualified VPF.Model.Class      as Cls
import qualified VPF.Model.Cols       as M
import qualified VPF.Model.VirusClass as VC

import qualified VPF.Util.Fasta as FA
import qualified VPF.Util.FS    as FS

import qualified Opts


type HitCols = '[M.VirusName, M.ModelName, M.ProteinHitScore]
type OutputCols = VC.ClassifiedCols HitCols
type RawOutputCols = VC.RawClassifiedCols HitCols

type Config = Opts.Config

type ClassifyM = Eff '[
    Exc FA.ParseError,
    Cmd HMMSearch,
    Exc HMMSearchError,
    Cmd Prodigal,
    Exc ProdigalError,
    Reader VC.ModelConfig,
    Exc DSV.ParseError,
    Fail,
    Lift IO
    ]


tagsClassify :: Conc.JobTags Int Int
                [FA.FastaEntry Nucleotide]
                (StM ClassifyM (F.FrameRowStoreRec HitCols))
tagsClassify = Conc.JobTags (Conc.JobTagIn 0) (Conc.JobTagOut 0)


putErrLn :: MonadIO m => String -> m ()
putErrLn = liftIO . IO.hPutStrLn IO.stderr

mpiAbortWith :: Int -> String -> IO a
mpiAbortWith ec msg = do
  IO.hFlush IO.stdout
  IO.hPutStrLn IO.stderr msg
  IO.hFlush IO.stderr
  MPI.abort MPI.commWorld ec
  exitFailure


main :: IO ()
main = MPI.mainMPI $ do
    let comm = MPI.commWorld
    rank <- MPI.commRank comm
    size <- MPI.commSize comm

    let slaves = [succ MPI.rootRank .. pred size]
    parser <- Opts.configParserIO slaves
    cfg <- OptP.execParser (opts parser)


    case MPI.fromRank rank of
      0 -> do
          let run = runLift . runFail . handleDSVParseErrors

          result <- run (masterClassify cfg slaves) `MC.catch` \(e :: MC.SomeException) -> do
                      putErrLn "exception was caught in master task"
                      mpiAbortWith 2 (show e)

          case result of
            Just _  -> return ()
            Nothing -> mpiAbortWith 1 "errors were produced in master task, aborting"

      r -> do
          let run = runLift . runFail

          result <- run (slaveClassify cfg r) `MC.catch` \(e :: MC.SomeException) -> do
                      putErrLn "exception was caught in slave task"
                      mpiAbortWith 4 (show e)

          case result of
            Just _  -> return ()
            Nothing -> mpiAbortWith 3 "errors were produced in slave task, aborting"
  where
    opts parser = OptP.info (parser <**> OptP.helper) $
        OptP.fullDesc
        <> OptP.progDesc "Classify virus sequences using an existing VPF classification"
        <> OptP.header "vpf-class: VPF-based virus sequence classifier"



masterClassify :: Config
               -> [MPI.Rank]
               -> Eff '[Exc DSV.ParseError, Fail, Lift IO] ()
masterClassify cfg slaves = do
  modelCfg <- newModelCfg cfg

  hitCounts <- runReader modelCfg $
      case Opts.inputFiles cfg of
        -- Opts.GivenHitsFile hitsFile ->
        --     VC.runModel (VC.GivenHitsFile hitsFile)

        Opts.GivenSequences vpfsFile genomesFile ->
            withCfgWorkDir cfg $ \workDir ->
            withProdigalCfg cfg $
            withHMMSearchCfg cfg $
            handleFastaParseErrors $ do
              concOpts <- newConcurrencyOpts cfg (Just slaves) workDir vpfsFile

              VC.runModel (VC.GivenGenomes genomesFile concOpts)

  let classFiles = Map.intersectionWith (,) (Opts.vpfClassFiles cfg)
                                            (Opts.scoreSampleFiles cfg)

  L.iforMOf_ L.itraversed classFiles $ \classKey (classFile, samplesFile) -> do
      cls <- Cls.loadClassification classFile
      scoreSamples <- Cls.loadScoreSamples samplesFile

      let prediction = hitCounts
            & VC.appendClassification cls
            & F.select @["virus_name", "class_obj", "protein_hit_score"]
            & F.reindexed @"virus_name" %~ VC.predictMembership scoreSamples
            & F.arrange @'[F.Asc "virus_name", F.Desc "membership_ratio"]

      let outputPath = Opts.outputPrefix cfg ++ "." ++ T.unpack classKey ++ ".tsv"
          tsvOpts = DSV.defWriterOptions '\t'

      lift $ PS.runSafeT $
          DSV.writeDSV tsvOpts (FS.fileWriter outputPath) prediction


slaveClassify :: Config -> MPI.Rank -> Eff '[Fail, Lift IO] ()
slaveClassify cfg _ = do
  modelCfg <- newModelCfg cfg

  handleDSVParseErrors $ runReader modelCfg $
      case Opts.inputFiles cfg of
        -- Opts.GivenHitsFile hitsFile -> return ()

        Opts.GivenSequences vpfsFile _ ->
            withCfgWorkDir cfg $ \workDir ->
            withProdigalCfg cfg $
            withHMMSearchCfg cfg $
            handleFastaParseErrors $
              worker workDir vpfsFile
  where
    worker :: Path Directory -> Path HMMERModel -> ClassifyM ()
    worker workDir vpfsFile = do
      concOpts <- newConcurrencyOpts cfg Nothing workDir vpfsFile

      liftBaseWith $ \runInIO ->
          PS.runSafeT $
            Conc.makeProcessWorker MPI.rootRank tagsClassify MPI.commWorld $ \chunk ->
                liftIO $ runInIO $
                  fmap F.toFrameRowStoreRec $
                    VC.asyncPipeline (fmap Right $ P.each chunk) concOpts


newModelCfg :: (Lifted IO r, Member Fail r) => Config -> Eff r VC.ModelConfig
newModelCfg cfg = do
  virusNameExtractor <- compileRegex (Opts.virusNameRegex cfg)

  return VC.ModelConfig
        { VC.modelEValueThreshold    = Opts.evalueThreshold cfg
        , VC.modelVirusNameExtractor = \protName ->
            fromMaybe (error $ "Could not extract virus name from protein: " ++ show protName)
                      (virusNameExtractor protName)
        }


compileRegex :: (Lifted IO r, Member Fail r) => Text -> Eff r (Text -> Maybe Text)
compileRegex src = do
    erx <- lift $ PCRE.compile (PCRE.compUTF8 + PCRE.compAnchored)
                               (PCRE.execAnchored + PCRE.execNoUTF8Check)
                               (T.encodeUtf8 src)
    case erx of
      Left (_, err) -> do
          putErrLn $ "Could not compile the regex " ++ show src ++ ": " ++ err
          die

      Right rx -> return $ \text -> do
          let btext = T.encodeUtf8 text
          arr <- PCRE.matchOnce rx btext
          (off, len) <- listToMaybe (toList arr)

          return (T.decodeUtf8 (BS.drop off (BS.take len btext)))


newConcurrencyOpts :: Config
                   -> Maybe [MPI.Rank]
                   -> Path Directory
                   -> Path HMMERModel
                   -> ClassifyM (VC.ConcurrencyOpts ClassifyM)
newConcurrencyOpts cfg slaves workDir vpfsFile =
    liftBaseWith $ \runInIO -> return $
        case slaves of
          Nothing  -> multithreadedMode runInIO
          Just []  -> multithreadedMode runInIO
          Just slv -> mpiMode runInIO (NE.fromList slv)
  where
    nworkers :: Int
    nworkers = max 1 $ Opts.numWorkers (Opts.concurrencyOpts cfg)

    mpiMode :: RunInBase ClassifyM IO -> NE.NonEmpty MPI.Rank -> VC.ConcurrencyOpts ClassifyM
    mpiMode runInIO slv = VC.ConcurrencyOpts
        { VC.fastaChunkSize  = Opts.fastaChunkSize (Opts.concurrencyOpts cfg) * nworkers
        , VC.pipelineWorkers = pipelineWorkers
        }
      where
        pipelineWorkers :: NE.NonEmpty (VC.Pipeline ClassifyM)
        pipelineWorkers =
            let
              mpiWorkers = Conc.mpiWorkers slv tagsClassify MPI.commWorld
              mapStM f = runInIO . fmap f . restoreM
              asDeserialized = Conc.mapWorkerIO (mapStM F.fromFrameRowStoreRec)
            in
              fmap (Conc.workerToPipe . asDeserialized) mpiWorkers

    multithreadedMode :: RunInBase ClassifyM IO -> VC.ConcurrencyOpts ClassifyM
    multithreadedMode runInIO = VC.ConcurrencyOpts
        { VC.fastaChunkSize = Opts.fastaChunkSize (Opts.concurrencyOpts cfg)
        , VC.pipelineWorkers = pipelineWorkers
        }
      where
        pipelineWorkers :: NE.NonEmpty (VC.Pipeline ClassifyM)
        pipelineWorkers =
            let
              workerBody chunk = liftIO $ runInIO $
                  VC.syncPipeline workDir vpfsFile (P.each chunk)
            in
              Conc.replicate1 (fromIntegral nworkers) (P.mapM workerBody)


withCfgWorkDir :: LiftedBase IO r
               => Config
               -> (Path Directory -> Eff r a)
               -> Eff r a
withCfgWorkDir cfg fm =
    case Opts.workDir cfg of
      Nothing ->
          FS.withTmpDir "." "vpf-work" fm
      Just wd -> do
          lift $ D.createDirectoryIfMissing True (untag wd)
          fm wd


withProdigalCfg :: (Lifted IO r, Member Fail r)
               => Config
               -> Eff (Cmd Prodigal ': Exc ProdigalError ': r) a
               -> Eff r a
withProdigalCfg cfg m = do
    handleProdigalErrors $ do
      prodigalCfg <- prodigalConfig (Opts.prodigalPath cfg) []
      execProdigal prodigalCfg m
  where
    handleProdigalErrors :: (Lifted IO r, Member Fail r)
                         => Eff (Exc ProdigalError ': r) a -> Eff r a
    handleProdigalErrors m = do
        res <- runError m

        case res of
          Right a -> return a
          Left e -> do
            putErrLn $ "prodigal error: " ++ show e
            die


withHMMSearchCfg :: (Lifted IO r, Member Fail r)
                 => Config
                 -> Eff (Cmd HMMSearch ': Exc HMMSearchError ': r) a
                 -> Eff r a
withHMMSearchCfg cfg m = do
    handleHMMSearchErrors $ do
      hmmsearchCfg <- hmmsearchConfig (Opts.hmmerConfig cfg) []
      execHMMSearch hmmsearchCfg m
  where
    handleHMMSearchErrors :: (Lifted IO r, Member Fail r)
                          => Eff (Exc HMMSearchError ': r) a -> Eff r a
    handleHMMSearchErrors m = do
        res <- runError m

        case res of
          Right a -> return a
          Left e -> do
            putErrLn $ "hmmsearch error: " ++ show e
            die


handleFastaParseErrors :: (Lifted IO r, Member Fail r)
                       => Eff (Exc FA.ParseError ': r) a
                       -> Eff r a
handleFastaParseErrors m = do
    res <- runError m

    case res of
      Right a -> return a

      Left (FA.ExpectedNameLine found) -> do
        putErrLn $ "FASTA parsing error: expected name line but found " ++ show found
        die

      Left (FA.ExpectedSequenceLine []) -> do
        putErrLn $ "FASTA parsing error: expected sequence but found EOF"
        die

      Left (FA.ExpectedSequenceLine (l:_)) -> do
        putErrLn $ "FASTA parsing error: expected sequence but found " ++ show l
        die


handleDSVParseErrors :: (Lifted IO r, Member Fail r)
                     => Eff (Exc DSV.ParseError ': r) a
                     -> Eff r a
handleDSVParseErrors m = do
    res <- runError m

    case res of
      Right a -> return a
      Left (DSV.ParseError ctx row) -> do
        putErrLn $ "DSV parse error in " ++ show ctx ++ ": "
                     ++ "could not parse row " ++ show row
        die
