{-# language BlockArguments #-}
{-# language CPP #-}
{-# language GeneralizedNewtypeDeriving #-}
{-# language MonoLocalBinds #-}
{-# language OverloadedStrings #-}
{-# language StaticPointers #-}
module Main where

import Control.Algebra (Has)
import Control.Carrier.Error.Excepts (Throw, ExceptsT, handleErrorCase, runLastExceptT, throwError)

import Control.Monad (forM_)
import Control.Monad.IO.Class (MonadIO(liftIO))
import Control.Monad.Morph (hoist)
import Control.Monad.Trans.Control (MonadBaseControl)
import Control.Monad.Trans.Reader (ReaderT, runReaderT)

import qualified Data.ByteString as BS
import Data.Constraint (Dict(..))
import Data.Foldable (toList)
import Data.Maybe (fromMaybe, listToMaybe)
import qualified Data.Yaml.Aeson as Y
import Data.Store (Store)
import Data.Text (Text)
import qualified Data.Text.Encoding         as T
import qualified Text.Regex.Base            as PCRE
import qualified Text.Regex.PCRE.ByteString as PCRE

import qualified System.Directory as D
import qualified System.IO   as IO
import System.Exit (exitWith, ExitCode(..))

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
import Control.Carrier.Distributed.MPI
#else
import Control.Carrier.Distributed.SingleProcess
#endif


type Config = Opts.Config


newtype DieMsg = DieMsg String
    deriving Store


type Die = Throw DieMsg


putErrLn :: MonadIO m => String -> m ()
putErrLn = liftIO . IO.hPutStrLn IO.stderr


dieWith :: Has Die sig m => String -> m a
dieWith = throwError . DieMsg


type family Trans ts m where
    Trans '[]       m = m
    Trans (t ': ts) m = t (Trans ts m)


type Stack =
    '[ Pr.ProdigalT
     , HMM.HMMSearchT
     , ReaderT VC.WorkDir
     , ExceptsT '[FA.ParseError, DSV.ParseError, VC.CheckpointLoadError, DieMsg]
     ]

type M = Trans Stack IO


main :: IO ()
main = do
    cfg <- Opts.parseArgs []

    ec <-
        dyingWithExitCode 1 $
        withCfgWorkDir cfg $
        hoist (handleCheckpointLoadError . handleDSVParseErrors . handleFastaParseErrors) $
        withHMMSearchCfg cfg $
        withProdigalCfg cfg $
            mainClassify cfg


    exitWith ec


mainClassify :: Config -> M ()
mainClassify cfg = do
    dataFilesIndex <- loadDataFilesIndex cfg

    let genomesFile = Opts.genomesFile cfg
        outputDir = Opts.outputDir cfg

        classFiles = Opts.classificationFiles dataFilesIndex
        vpfsFile = Opts.vpfsFile dataFilesIndex

        concurrencyOpts = Opts.concurrencyOpts cfg

#ifdef VPF_ENABLE_MPI
    runMpiWorld finalizeOnError id $ do
#else
    runSingleProcessT $ do
#endif
        outputs <- VC.runClassification genomesFile $ \resume -> \case
            VC.SearchHitsStep run -> do
                putErrLn "searching hits"

                let concOpts = VC.SearchHitsConcurrencyOpts
                      { VC.numSearchingWorkers = Opts.numWorkers concurrencyOpts
                      , VC.fastaChunkSize      = Opts.fastaChunkSize concurrencyOpts
                      }

                resume =<< run (static Dict) concOpts vpfsFile genomesFile

            VC.ProcessHitsStep run -> do
                putErrLn "processing hits"
                modelCfg <- newModelCfg cfg

                let concOpts = VC.ProcessHitsConcurrencyOpts
                      { VC.numProcessingHitsWorkers = Opts.numWorkers concurrencyOpts
                      }

                resume =<< runReaderT (run concOpts) modelCfg

            VC.PredictMembershipStep run -> do
                putErrLn "predicting memberships"

                let concOpts = VC.PredictMembershipConcurrencyOpts
                      { VC.numPredictingWorkers = Opts.numWorkers concurrencyOpts
                      }

                run concOpts classFiles outputDir

        forM_ outputs $ \(Tagged output) ->
            putErrLn $ "written " ++ output


newModelCfg :: (MonadIO m, Has Die sig m) => Config -> m VC.ModelConfig
newModelCfg cfg = do
    virusNameExtractor <- compileRegex (Opts.virusNameRegex cfg)

    return VC.ModelConfig
        { VC.modelEValueThreshold    = Opts.evalueThreshold cfg

        , VC.modelVirusNameExtractor = \protName ->
            fromMaybe (error $ "Could not extract virus name from protein: " ++ show protName)
                      (virusNameExtractor protName)
        }


compileRegex :: (MonadIO m, Has Die sig m) => Text -> m (Text -> Maybe Text)
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


withCfgWorkDir :: (MonadIO m, MonadBaseControl IO m) => Config -> ReaderT VC.WorkDir m a -> m a
withCfgWorkDir cfg fm =
    case Opts.workDir cfg of
      Nothing ->
          FS.withTmpDir "." "vpf-work" (runReaderT fm . VC.WorkDir)
      Just wd -> do
          liftIO $ D.createDirectoryIfMissing True (untag wd)
          runReaderT fm (VC.WorkDir wd)


dyingWithExitCode :: MonadIO m => Int -> ExceptsT '[DieMsg] m () -> m ExitCode
dyingWithExitCode ec m = do
    r <- runLastExceptT m

    case r of
      Right ()        -> return ExitSuccess
      Left (DieMsg e) -> do
          putErrLn e
          return (ExitFailure ec)


loadDataFilesIndex :: (MonadIO m, Has Die sig m) => Config -> m Opts.DataFilesIndex
loadDataFilesIndex cfg = do
    r <- liftIO $ Opts.loadDataFilesIndex (Opts.dataFilesIndexFile cfg)

    case r of
      Right idx ->
          return idx
      Left err ->
          let indentedPretty = unlines . map ('\t':) . lines . Y.prettyPrintParseException
          in  dieWith $ "error loading data file index:\n" ++ indentedPretty err


withProdigalCfg :: (MonadIO m, Has Die sig m) => Config -> Pr.ProdigalT m a -> m a
withProdigalCfg cfg m = do
    res <- Pr.runProdigalT (Opts.prodigalPath cfg) ["-p", Opts.prodigalProcedure cfg] m

    case res of
      Right a -> return a
      Left e  -> dieWith $ "prodigal error: " ++ show e


withHMMSearchCfg :: (MonadIO m, Has Die sig m) => Config -> HMM.HMMSearchT m a -> m a
withHMMSearchCfg cfg m = do
    let hmmerConfig = HMM.HMMERConfig (Opts.hmmerPrefix cfg)

    res <- HMM.runHMMSearchT hmmerConfig [] m

    case res of
      Right a -> return a
      Left e  -> dieWith $ "hmmsearch error: " ++ show e


handleCheckpointLoadError ::
    ( Monad m
    , Has Die sig (ExceptsT es m)
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
    , Has Die sig (ExceptsT es m)
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
    , Has Die sig (ExceptsT es m)
    )
    => ExceptsT (DSV.ParseError ': es) m a
    -> ExceptsT es m a
handleDSVParseErrors = handleErrorCase $ \case
    DSV.ParseError ctx row -> do
        dieWith $
            "DSV parse error in " ++ show ctx ++ ": " ++
            "could not parse row " ++ show row
