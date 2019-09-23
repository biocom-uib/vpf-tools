{-# language BlockArguments #-}
{-# language CPP #-}
{-# language MonoLocalBinds #-}
{-# language OverloadedStrings #-}
module Main where

import Control.Applicative ((<**>))
import Control.Concurrent (getNumCapabilities)
import Control.Eff (Eff, Member, Lift, Lifted, lift, runLift)
import Control.Eff.Reader.Strict (Reader, runReader)
import Control.Eff.Exception (Exc, runError, Fail, die, runFail)
import Control.Monad (forM_)
import qualified Control.Monad.Catch as MC
import Control.Monad.IO.Class (MonadIO(liftIO))
import Control.Monad.Trans.Control (MonadBaseControl, RunInBase, liftBaseWith)

import qualified Data.ByteString as BS
import Data.Foldable (toList)
import qualified Data.Map as Map
import Data.Maybe (fromMaybe, listToMaybe)
import qualified Data.List.NonEmpty as NE
import Data.Text (Text)
import qualified Data.Text.Encoding         as T
import qualified Text.Regex.Base            as PCRE
import qualified Text.Regex.PCRE.ByteString as PCRE

import qualified System.Directory as D
import qualified System.IO   as IO
import System.Exit (exitWith, ExitCode(..))

import qualified Pipes         as P
import qualified Pipes.Prelude as P

import qualified Options.Applicative as OptP

import qualified VPF.Concurrency.Async as Conc

import VPF.Eff.Cmd (Cmd)
import qualified VPF.Ext.HMMER.Search as HMM
import qualified VPF.Ext.Prodigal     as Pr

import VPF.Formats
import qualified VPF.Frames.DSV    as DSV

import qualified VPF.Model.VirusClass as VC

import qualified VPF.Util.Fasta as FA
import qualified VPF.Util.FS    as FS

import qualified Opts


#ifdef VPF_ENABLE_MPI
import qualified Control.Distributed.MPI.Store as MPI
import Control.Monad.Trans.Control (StM)

import Pipes.Safe (runSafeT)

import qualified VPF.Concurrency.MPI as Conc
#endif


type Config = Opts.Config

type SearchHitsM = Eff
   '[ Exc FA.ParseError
    , Cmd HMM.HMMSearch
    , Exc HMM.HMMSearchError
    , Cmd Pr.Prodigal
    , Exc Pr.ProdigalError
    , Exc DSV.ParseError
    , Exc VC.CheckpointLoadError
    , Fail
    , Lift IO
    ]

type ProcessHitsM = Eff
   '[ Exc FA.ParseError
    , Reader VC.ModelConfig
    , Exc DSV.ParseError
    , Exc VC.CheckpointLoadError
    , Fail
    , Lift IO
    ]


putErrLn :: MonadIO m => String -> m ()
putErrLn = liftIO . IO.hPutStrLn IO.stderr


parserInfo :: OptP.Parser a -> OptP.ParserInfo a
parserInfo parser =
    OptP.info (parser <**> OptP.helper) $
        OptP.fullDesc
        <> OptP.progDesc "Classify virus sequences using an existing VPF classification"
        <> OptP.header "vpf-class: VPF-based virus sequence classifier"

parseOpts :: OptP.Parser a -> IO a
parseOpts = OptP.execParser . parserInfo


#ifndef VPF_ENABLE_MPI

type Ranks = ()

main :: IO ()
main = do
    cfg <- parseOpts =<< Opts.configParserIO

    ec <- masterClassify cfg () `MC.catch` \(e :: MC.SomeException) -> do
        putErrLn "exception was caught in master task"
        MC.throwM e

    exitWith ec

#else

type Ranks = [MPI.Rank]

tagsSearchHits :: Conc.JobTags Int Int
                [FA.FastaEntry Nucleotide]
                (StM SearchHitsM [ ( VC.GenomeChunkKey
                                   , Path (FASTA Aminoacid)
                                   , Path (HMMERTable HMM.ProtSearchHitCols)
                                   )
                                 ])
tagsSearchHits = Conc.JobTags (Conc.JobTagIn 0) (Conc.JobTagOut 0)


mpiAbortWith :: Int -> String -> IO a
mpiAbortWith ec msg = do
    IO.hFlush IO.stdout
    IO.hPutStrLn IO.stderr msg
    IO.hFlush IO.stderr
    MPI.abort MPI.commWorld ec
    exitWith (ExitFailure ec)


main :: IO ()
main = MPI.mainMPI $ do
    let comm = MPI.commWorld
    rank <- MPI.commRank comm
    size <- MPI.commSize comm

    let slaves = [succ MPI.rootRank .. pred size]
    cfg <- parseOpts =<< Opts.configParserIO slaves

    case rank of
      0 -> do
          ec <- masterClassify cfg slaves `MC.catch` \(e :: MC.SomeException) -> do
                      putErrLn "exception was caught in master task"
                      mpiAbortWith 1 (show e)

          case ec of
            ExitSuccess   -> return ()
            ExitFailure i -> mpiAbortWith i "errors were produced in master task, aborting"

      r -> do
          ec <- slaveClassify cfg r `MC.catch` \(e :: MC.SomeException) -> do
                      putErrLn "exception was caught in slave task"
                      mpiAbortWith 2 (show e)

          case ec of
            ExitSuccess   -> return ()
            ExitFailure i -> mpiAbortWith i "errors were produced in slave task, aborting"

#endif


masterClassify :: Config -> Ranks -> IO ExitCode
masterClassify cfg slaves = do
    let genomesFile = Opts.genomesFile cfg
        vpfsFile    = Opts.vpfsFile cfg

    let classFiles = Map.intersectionWith (,) (Opts.vpfClassFiles cfg)
                                              (Opts.scoreSampleFiles cfg)
        outputDir = Opts.outputDir cfg

    r <- withCfgWorkDir cfg $ \workDir ->
        runLift $
        runFail $
        handleCheckpointLoadError $
          VC.runClassification workDir genomesFile $ \resume -> \case
              VC.SearchHitsStep run -> do
                  putErrLn "searching hits"

                  checkpoint <-
                      handleDSVParseErrors $
                      withProdigalCfg cfg $
                      withHMMSearchCfg cfg $
                      handleFastaParseErrors $ do
                          concOpts <- newSearchHitsConcOpts cfg (Just slaves) workDir vpfsFile
                          run genomesFile concOpts

                  resume checkpoint

              VC.ProcessHitsStep run -> do
                  putErrLn "processing hits"
                  modelCfg <- newModelCfg cfg

                  checkpoint <-
                      handleDSVParseErrors $
                      runReader modelCfg $
                      handleFastaParseErrors $
                          run =<< newProcessHitsConcOpts cfg workDir

                  resume checkpoint

              VC.PredictMembershipStep run -> do
                  putErrLn "predicting memberships"

                  let concOpts = VC.PredictMembershipConcurrencyOpts {
                          VC.predictingNumWorkers = fromIntegral $
                              Opts.numWorkers (Opts.concurrencyOpts cfg)
                      }

                  handleDSVParseErrors $
                      run classFiles outputDir concOpts

    case r of
      Nothing ->
          return (ExitFailure 3)
      Just outputs -> do
          forM_ outputs $ \(Tagged output) ->
              putErrLn $ "written " ++ output
          return ExitSuccess


#ifdef VPF_ENABLE_MPI

slaveClassify :: Config -> MPI.Rank -> IO ExitCode
slaveClassify cfg _ = do
    let vpfsFile = Opts.vpfsFile cfg

    r <- runLift $
        withCfgWorkDir cfg $ \workDir ->
        runFail $
        handleCheckpointLoadError $
        handleDSVParseErrors $
        withProdigalCfg cfg $
        withHMMSearchCfg cfg $
        handleFastaParseErrors $
            worker workDir vpfsFile

    case r of
      Just () -> return ExitSuccess
      Nothing -> return (ExitFailure 4)
  where
    worker :: Path Directory -> Path HMMERModel -> SearchHitsM ()
    worker workDir vpfsFile = do
      concOpts <- newSearchHitsConcOpts cfg Nothing workDir vpfsFile

      liftBaseWith $ \runInIO ->
          runSafeT $ Conc.makeProcessWorker MPI.rootRank tagsSearchHits MPI.commWorld $ \chunk ->
              liftIO $ runInIO $
                  VC.asyncSearchHits (fmap Right $ P.each chunk) concOpts

#endif


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


newSearchHitsConcOpts :: Config
                      -> Maybe Ranks
                      -> Path Directory
                      -> Path HMMERModel
                      -> SearchHitsM (VC.SearchHitsConcurrencyOpts SearchHitsM)
newSearchHitsConcOpts cfg slaves workDir vpfsFile =
    liftBaseWith $ \runInIO -> return $
#ifndef VPF_ENABLE_MPI
        case slaves of
          Nothing  -> multithreadedMode runInIO
          Just ()  -> multithreadedMode runInIO
#else
        case slaves of
          Nothing  -> multithreadedMode runInIO
          Just []  -> multithreadedMode runInIO
          Just slv -> mpiMode runInIO (NE.fromList slv)
#endif
  where
    nworkers :: Int
    nworkers = max 1 $ Opts.numWorkers (Opts.concurrencyOpts cfg)

#ifdef VPF_ENABLE_MPI
    mpiMode :: RunInBase SearchHitsM IO -> NE.NonEmpty MPI.Rank -> VC.SearchHitsConcurrencyOpts SearchHitsM
    mpiMode _ slv = VC.SearchHitsConcurrencyOpts
        { VC.fastaChunkSize   = Opts.fastaChunkSize (Opts.concurrencyOpts cfg) * nworkers
        , VC.searchingWorkers = pipelineWorkers
        }
      where
        pipelineWorkers :: NE.NonEmpty (VC.SearchHitsPipeline SearchHitsM)
        pipelineWorkers =
            fmap Conc.workerToPipe $ Conc.mpiWorkers slv tagsSearchHits MPI.commWorld
#endif

    multithreadedMode :: RunInBase SearchHitsM IO -> VC.SearchHitsConcurrencyOpts SearchHitsM
    multithreadedMode runInIO = VC.SearchHitsConcurrencyOpts
        { VC.fastaChunkSize   = Opts.fastaChunkSize (Opts.concurrencyOpts cfg)
        , VC.searchingWorkers = pipelineWorkers
        }
      where
        pipelineWorkers :: NE.NonEmpty (VC.SearchHitsPipeline SearchHitsM)
        pipelineWorkers =
            let
              workerBody chunk = liftIO $ runInIO $ do
                  hitsFiles <- VC.searchGenomeHits workDir vpfsFile (P.each chunk)
                  return [hitsFiles]
            in
              Conc.replicate1 (fromIntegral nworkers) (P.mapM workerBody)


newProcessHitsConcOpts :: Config -> Path Directory -> ProcessHitsM (VC.ProcessHitsConcurrencyOpts ProcessHitsM)
newProcessHitsConcOpts _ workDir = do
    nworkers <- liftIO $ getNumCapabilities

    liftBaseWith $ \runInIO -> do
        let workerBody (key, protsFile, hitsFile) = liftIO $ runInIO $ do
              aggHitsFile <- VC.processHits workDir key protsFile hitsFile
              return (key, aggHitsFile)


        return $ VC.ProcessHitsConcurrencyOpts {
            VC.processingHitsWorkers = Conc.replicate1 (fromIntegral nworkers) (P.mapM workerBody)
        }


withCfgWorkDir :: (MonadIO m, MonadBaseControl IO m)
               => Config
               -> (Path Directory -> m a)
               -> m a
withCfgWorkDir cfg fm =
    case Opts.workDir cfg of
      Nothing ->
          FS.withTmpDir "." "vpf-work" fm
      Just wd -> do
          liftIO $ D.createDirectoryIfMissing True (untag wd)
          fm wd


withProdigalCfg :: (Lifted IO r, Member Fail r)
               => Config
               -> Eff (Cmd Pr.Prodigal ': Exc Pr.ProdigalError ': r) a
               -> Eff r a
withProdigalCfg cfg m = do
    handleProdigalErrors $ do
        prodigalCfg <- Pr.prodigalConfig
                           (Opts.prodigalPath cfg)
                           ["-p", Opts.prodigalProcedure cfg]

        Pr.execProdigal prodigalCfg m
  where
    handleProdigalErrors :: (Lifted IO r, Member Fail r)
                         => Eff (Exc Pr.ProdigalError ': r) a -> Eff r a
    handleProdigalErrors m = do
        res <- runError m

        case res of
          Right a -> return a
          Left e -> do
            putErrLn $ "prodigal error: " ++ show e
            die


withHMMSearchCfg :: (Lifted IO r, Member Fail r)
                 => Config
                 -> Eff (Cmd HMM.HMMSearch ': Exc HMM.HMMSearchError ': r) a
                 -> Eff r a
withHMMSearchCfg cfg m = do
    handleHMMSearchErrors $ do
        hmmsearchCfg <- HMM.hmmsearchConfig (Opts.hmmerConfig cfg) []
        HMM.execHMMSearch hmmsearchCfg m
  where
    handleHMMSearchErrors :: (Lifted IO r, Member Fail r)
                          => Eff (Exc HMM.HMMSearchError ': r) a -> Eff r a
    handleHMMSearchErrors m = do
        res <- runError m

        case res of
          Right a -> return a
          Left e -> do
              putErrLn $ "hmmsearch error: " ++ show e
              die


handleCheckpointLoadError :: (Lifted IO r, Member Fail r)
                          => Eff (Exc VC.CheckpointLoadError ': r) a
                          -> Eff r a
handleCheckpointLoadError m = do
    res <- runError m

    case res of
      Right a -> return a

      Left (VC.CheckpointJSONLoadError e) -> do
          putErrLn $ "Error loading checkpoint: " ++ e
          putErrLn "  try deleting the checkpoint file and restarting from scratch"
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
