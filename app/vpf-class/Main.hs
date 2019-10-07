{-# language BlockArguments #-}
{-# language CPP #-}
{-# language GeneralizedNewtypeDeriving #-}
{-# language MonoLocalBinds #-}
{-# language OverloadedStrings #-}
module Main where

import Control.Concurrent (getNumCapabilities)

import Control.Effect (Carrier, Member)
import Control.Effect.Errors (Error, ExceptsT, handleErrorCase, runLastExceptT, throwError)
import Control.Effect.Reader (Reader)

import Control.Monad (forM_)
import qualified Control.Monad.Catch as MC
import Control.Monad.IO.Class (MonadIO(liftIO))
import Control.Monad.Trans.Control (MonadBaseControl(liftBaseWith), RunInBase)
import Control.Monad.Trans.Identity (IdentityT, runIdentityT)
import Control.Monad.Trans.Reader (ReaderT, runReaderT)

import qualified Data.ByteString as BS
import Data.Foldable (toList)
import Data.Maybe (fromMaybe, listToMaybe)
import qualified Data.List.NonEmpty as NE
import qualified Data.Yaml.Aeson as Y
import Data.Store (Store)
import Data.Text (Text)
import qualified Data.Text.Encoding         as T
import qualified Text.Regex.Base            as PCRE
import qualified Text.Regex.PCRE.ByteString as PCRE

import qualified System.Directory as D
import qualified System.IO   as IO
import System.Exit (exitWith, ExitCode(..))

import qualified Pipes         as P
import qualified Pipes.Prelude as P

import qualified VPF.Concurrency.Async as Conc

import qualified VPF.Ext.HMMER        as HMM
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

newtype DieMsg = DieMsg String
    deriving Store

type Die = Error DieMsg


putErrLn :: MonadIO m => String -> m ()
putErrLn = liftIO . IO.hPutStrLn IO.stderr

dieWith :: (Carrier sig m, Member Die sig) => String -> m a
dieWith = throwError . DieMsg


type family Trans ts m where
    Trans '[]       m = m
    Trans (t ': ts) m = t (Trans ts m)


type SearchHitsStack =
    '[ Pr.ProdigalT
     , HMM.HMMSearchT
     , ExceptsT '[FA.ParseError, DSV.ParseError, VC.CheckpointLoadError, DieMsg]
     , IdentityT
     ]

type SearchHitsM = Trans SearchHitsStack IO


type ProcessHitsStack =
    '[ ReaderT VC.ModelConfig
     , ExceptsT '[FA.ParseError, DSV.ParseError, VC.CheckpointLoadError, DieMsg]
     , IdentityT
     ]

type ProcessHitsM = Trans ProcessHitsStack IO


#ifndef VPF_ENABLE_MPI

type Ranks = ()

main :: IO ()
main = do
    cfg <- Opts.parseArgs

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
    cfg <- Opts.parseArgs slaves

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
masterClassify cfg slaves = runIdentityT $ dyingWithExitCode 3 $ do
    dataFilesIndex <- loadDataFilesIndex cfg

    let genomesFile = Opts.genomesFile cfg
        outputDir = Opts.outputDir cfg

        classFiles = Opts.classificationFiles dataFilesIndex
        vpfsFile    = Opts.vpfsFile dataFilesIndex

    outputs <- withCfgWorkDir cfg $ \workDir ->
        handleCheckpointLoadError $
          VC.runClassification workDir genomesFile $ \resume -> \case
              VC.SearchHitsStep run -> do
                  putErrLn "searching hits"

                  checkpoint <-
                      handleDSVParseErrors $
                      handleFastaParseErrors $
                      withHMMSearchCfg cfg $
                      withProdigalCfg cfg $ do
                          concOpts <- newSearchHitsConcOpts cfg (Just slaves) workDir vpfsFile
                          run genomesFile concOpts

                  resume checkpoint

              VC.ProcessHitsStep run -> do
                  putErrLn "processing hits"
                  modelCfg <- newModelCfg cfg

                  checkpoint <-
                      handleDSVParseErrors $
                      handleFastaParseErrors $
                      flip runReaderT modelCfg $
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

    forM_ outputs $ \(Tagged output) ->
        putErrLn $ "written " ++ output


#ifdef VPF_ENABLE_MPI

slaveClassify :: Config -> MPI.Rank -> IO ExitCode
slaveClassify cfg _ = runIdentityT $ dyingWithExitCode 4 $ do
    dataFilesIndex <- loadDataFilesIndex cfg

    let vpfsFile = Opts.vpfsFile dataFilesIndex

    withCfgWorkDir cfg $ \workDir ->
        handleCheckpointLoadError $
        handleDSVParseErrors $
        handleFastaParseErrors $
        withHMMSearchCfg cfg $
        withProdigalCfg cfg $
            worker workDir vpfsFile
  where
    worker :: Path Directory -> Path HMMERModel -> SearchHitsM ()
    worker workDir vpfsFile = do
      concOpts <- newSearchHitsConcOpts cfg Nothing workDir vpfsFile

      liftBaseWith $ \runInIO ->
          runSafeT $ Conc.makeProcessWorker MPI.rootRank tagsSearchHits MPI.commWorld $ \chunk ->
              liftIO $ runInIO $
                  VC.asyncSearchHits (fmap Right $ P.each chunk) concOpts

#endif


newModelCfg :: (MonadIO m, Carrier sig m, Member Die sig) => Config -> m VC.ModelConfig
newModelCfg cfg = do
    virusNameExtractor <- compileRegex (Opts.virusNameRegex cfg)

    return VC.ModelConfig
        { VC.modelEValueThreshold    = Opts.evalueThreshold cfg

        , VC.modelVirusNameExtractor = \protName ->
            fromMaybe (error $ "Could not extract virus name from protein: " ++ show protName)
                      (virusNameExtractor protName)
        }


compileRegex :: (MonadIO m, Carrier sig m, Member Die sig) => Text -> m (Text -> Maybe Text)
compileRegex src = do
    erx <- liftIO $ PCRE.compile (PCRE.compUTF8 + PCRE.compAnchored)
                                 (PCRE.execAnchored + PCRE.execNoUTF8Check)
                                 (T.encodeUtf8 src)
    case erx of
      Left (_, err) ->
          dieWith $ "Could not compile the regex " ++ show src ++ ": " ++ err

      Right rx -> return $ \text -> do
          let btext = T.encodeUtf8 text
          arr <- PCRE.matchOnce rx btext
          (off, len) <- listToMaybe (toList arr)

          return (T.decodeUtf8 (BS.drop off (BS.take len btext)))


newSearchHitsConcOpts ::
    Config
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


newProcessHitsConcOpts ::
    Config
    -> Path Directory
    -> ProcessHitsM (VC.ProcessHitsConcurrencyOpts ProcessHitsM)
newProcessHitsConcOpts _ workDir = do
    nworkers <- liftIO $ getNumCapabilities

    liftBaseWith $ \runInIO -> do
        let workerBody (key, protsFile, hitsFile) = liftIO $ runInIO $ do
              aggHitsFile <- VC.processHits workDir key protsFile hitsFile
              return (key, aggHitsFile)


        return $ VC.ProcessHitsConcurrencyOpts {
            VC.processingHitsWorkers = Conc.replicate1 (fromIntegral nworkers) (P.mapM workerBody)
        }


withCfgWorkDir :: (MonadIO m, MonadBaseControl IO m) => Config -> (Path Directory -> m a) -> m a
withCfgWorkDir cfg fm =
    case Opts.workDir cfg of
      Nothing ->
          FS.withTmpDir "." "vpf-work" fm
      Just wd -> do
          liftIO $ D.createDirectoryIfMissing True (untag wd)
          fm wd


dyingWithExitCode :: MonadIO m => Int -> ExceptsT '[DieMsg] m () -> m ExitCode
dyingWithExitCode ec m = do
    r <- runLastExceptT m

    case r of
      Right ()        -> return ExitSuccess
      Left (DieMsg e) -> do
          putErrLn e
          return (ExitFailure ec)


loadDataFilesIndex :: (MonadIO m, Carrier sig m, Member Die sig) => Config -> m Opts.DataFilesIndex
loadDataFilesIndex cfg = do
    r <- liftIO $ Opts.loadDataFilesIndex (Opts.dataFilesIndexFile cfg)

    case r of
      Right idx ->
          return idx
      Left err ->
          let indentedPretty = unlines . map ('\t':) . lines . Y.prettyPrintParseException
          in  dieWith $ "error loading data file index:\n" ++ indentedPretty err


withProdigalCfg :: (MonadIO m, Carrier sig m, Member Die sig) => Config -> Pr.ProdigalT m a -> m a
withProdigalCfg cfg m = do
    res <- Pr.runProdigalT (Opts.prodigalPath cfg) ["-p", Opts.prodigalProcedure cfg] m

    case res of
      Right a -> return a
      Left e  -> dieWith $ "prodigal error: " ++ show e


withHMMSearchCfg :: (MonadIO m, Carrier sig m, Member Die sig) => Config -> HMM.HMMSearchT m a -> m a
withHMMSearchCfg cfg m = do
    let hmmerConfig = HMM.HMMERConfig (Opts.hmmerPrefix cfg)

    res <- HMM.runHMMSearchT hmmerConfig [] m

    case res of
      Right a -> return a
      Left e  -> dieWith $ "hmmsearch error: " ++ show e


handleCheckpointLoadError ::
    ( Monad m
    , Carrier sig (ExceptsT es m)
    , Member Die sig
    )
    => ExceptsT (VC.CheckpointLoadError ': es) m a
    -> ExceptsT es m a
handleCheckpointLoadError = handleErrorCase $ \case
    VC.CheckpointJSONLoadError e ->
        dieWith $
            "Error loading checkpoint: " ++ e ++ "\n" ++
            "  try deleting the checkpoint file and restarting from scratch"


handleFastaParseErrors ::
    ( Monad m
    , Carrier sig (ExceptsT es m)
    , Member Die sig
    )
    => ExceptsT (FA.ParseError ': es) m a
    -> ExceptsT es m a
handleFastaParseErrors = handleErrorCase $ \case
    FA.ExpectedNameLine found ->
        dieWith $ "FASTA parsing error: expected name line but found " ++ show found

    FA.ExpectedSequenceLine [] ->
        dieWith $ "FASTA parsing error: expected sequence but found EOF"

    FA.ExpectedSequenceLine (l:_) ->
        dieWith $ "FASTA parsing error: expected sequence but found " ++ show l


handleDSVParseErrors ::
    ( Monad m
    , Carrier sig (ExceptsT es m)
    , Member Die sig
    )
    => ExceptsT (DSV.ParseError ': es) m a
    -> ExceptsT es m a
handleDSVParseErrors = handleErrorCase $ \case
    DSV.ParseError ctx row -> do
        dieWith $
            "DSV parse error in " ++ show ctx ++ ": " ++
            "could not parse row " ++ show row
