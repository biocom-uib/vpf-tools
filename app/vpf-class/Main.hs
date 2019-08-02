{-# language OverloadedStrings #-}
{-# language MonoLocalBinds #-}
module Main where

import Control.Applicative ((<**>))
import qualified Control.Distributed.MPI.Store as MPI
import Control.Eff (Eff, Member, Lift, Lifted, LiftedBase, lift, runLift)
import Control.Eff.Reader.Strict (Reader, runReader)
import Control.Eff.Exception (Exc, runError, Fail, die, runFail)
import Control.Lens (Lens, view, over, mapped, (&))
import Control.Monad ((<=<))
import qualified Control.Monad.Catch as MC
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Control (StM, liftBaseWith, restoreM)

import qualified Data.List.NonEmpty as NE

import qualified Data.Array                 as Array
import qualified Data.ByteString            as BS
import Data.Foldable (toList)
import Data.Maybe (fromMaybe, listToMaybe)
import Data.Text (Text)
import qualified Data.Text.Encoding         as T
import qualified Text.Regex.Base            as PCRE
import qualified Text.Regex.PCRE.ByteString as PCRE

import qualified Options.Applicative as OptP

import System.Exit (exitWith, ExitCode(..))

import Frames (FrameRec, Record)

import VPF.Eff.Cmd (Cmd, runCmd)
import VPF.Ext.HMMER.Search (HMMSearch, HMMSearchError, hmmsearchConfig, execHMMSearch)
import VPF.Ext.Prodigal (Prodigal, ProdigalError, prodigalConfig, execProdigal)

import VPF.Formats
import qualified VPF.Model.Class      as Cls
import qualified VPF.Model.Class.Cols as Cls
import qualified VPF.Model.Cols       as M
import qualified VPF.Model.VirusClass as VC

import qualified VPF.Concurrency.Async as Conc
import qualified VPF.Concurrency.MPI   as Conc

import qualified VPF.Util.Dplyr as D
import qualified VPF.Util.DSV   as DSV
import qualified VPF.Util.Fasta as FA
import qualified VPF.Util.FS    as FS
import VPF.Util.Vinyl (rsubset')

import Pipes ((>->))
import qualified Pipes         as P
import qualified Pipes.Core    as P
import qualified Pipes.Prelude as P
import qualified Pipes.Safe    as PS

import qualified System.Directory as D

import qualified Opts


type OutputCols = VC.PredictedCols '[M.VirusName, M.ModelName, M.NumHits]
type RawOutputCols = VC.RawPredictedCols '[M.VirusName, M.ModelName, M.NumHits]

type Config = Opts.Config (DSV "\t" OutputCols)

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
                [FA.FastaEntry]
                (StM ClassifyM (D.FrameRowStoreRec '[M.VirusName, M.ModelName, M.NumHits]))
tagsClassify = Conc.JobTags (Conc.JobTagIn 0) (Conc.JobTagOut 0)


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
          result <- masterClassify cfg slaves `MC.catch` \e -> do
            print (e :: MC.SomeException)
            MPI.abort comm 1
            exitWith (ExitFailure 1)

          case result of
            Just _  -> return ()
            Nothing -> MPI.abort comm 1

      r -> do
          result <- slaveClassify cfg r slaves `MC.catch` \e -> do
            print (e :: MC.SomeException)
            MPI.abort comm 1
            exitWith (ExitFailure 1)

          case result of
            Just _  -> return ()
            Nothing -> MPI.abort comm 1
  where
    opts parser = OptP.info (parser <**> OptP.helper) $
        OptP.fullDesc
        <> OptP.progDesc "Classify virus sequences using an existing VPF classification"
        <> OptP.header "vpf-class: VPF-based virus sequence classifier"


compileRegex :: (Lifted IO r, Member Fail r) => Text -> Eff r (Text -> Maybe Text)
compileRegex src = do
    erx <- lift $ PCRE.compile (PCRE.compUTF8 + PCRE.compAnchored)
                               (PCRE.execAnchored + PCRE.execNoUTF8Check)
                               (T.encodeUtf8 src)
    case erx of
      Left (_, err) -> do
          lift $ putStrLn $ "Could not compile the regex " ++ show src ++ ": " ++ err
          die

      Right rx -> return $ \text -> do
          let btext = T.encodeUtf8 text
          arr <- PCRE.matchOnce rx btext
          (off, len) <- listToMaybe (toList arr)

          return (T.decodeUtf8 (BS.drop off (BS.take len btext)))


masterClassify :: Config -> [MPI.Rank] -> IO (Maybe ())
masterClassify cfg slaves =
    runLift $ runFail $ handleDSVParseErrors $ do
        modelCfg <- initModelCfg cfg

        hitCounts <- runReader modelCfg $
            case Opts.inputFiles cfg of
              Opts.GivenHitsFile hitsFile ->
                  VC.runModel (VC.GivenHitsFile hitsFile)

              Opts.GivenSequences vpfsFile genomesFile ->
                  withCfgWorkDir cfg $ \workDir ->
                  withProdigalCfg cfg $
                  withHMMSearchCfg cfg $
                  handleFastaParseErrors $ do
                    concOpts <- buildConcOpts workDir vpfsFile

                    VC.runModel (VC.GivenGenomes genomesFile concOpts)

        cls <- Cls.loadClassification (Opts.vpfClassFile cfg)

        let predictedCls = VC.predictClassification hitCounts cls

            rawPredictedCls = over (mapped.rsubset') (view Cls.rawClassification)
                                   predictedCls
                            & D.reorder @RawOutputCols

            tsvOpts = DSV.defWriterOptions '\t'

        lift $
          case Opts.outputFile cfg of
            Opts.StdDevice ->
                DSV.writeDSV tsvOpts FS.stdoutWriter rawPredictedCls
            Opts.FSPath fp ->
                PS.runSafeT $
                  DSV.writeDSV tsvOpts (FS.fileWriter (untag fp)) rawPredictedCls
  where
    buildConcOpts :: Path Directory
                  -> Path HMMERModel
                  -> ClassifyM (VC.ConcurrencyOpts ClassifyM)
    buildConcOpts workDir vpfsFile =
        liftBaseWith $ \runInIO -> return VC.ConcurrencyOpts
            { VC.fastaChunkSize =
                Opts.fastaChunkSize (Opts.concurrencyOpts cfg)
                  * length slaves
            , VC.pipelineWorkers =
                if Opts.useMPI (Opts.concurrencyOpts cfg) && not (null slaves) then
                    let mapStM f = runInIO . fmap f . restoreM in
                    fmap (Conc.workerToPipe . Conc.mapWorkerIO (mapStM D.fromFrameRowStoreRec)) $
                      Conc.mpiWorkers (NE.fromList slaves) tagsClassify MPI.commWorld
                else
                  Conc.replicate1
                    (fromIntegral $ Opts.numWorkers (Opts.concurrencyOpts cfg))
                    (P.mapM (\chunk -> liftIO $ runInIO $ do
                        hitsFile <- VC.searchGenomeHits workDir vpfsFile (P.each chunk)
                        VC.processHMMOut hitsFile))
            }


slaveClassify :: Config -> MPI.Rank -> [MPI.Rank] -> IO (Maybe ())
slaveClassify cfg rank slaves =
    runLift $ runFail $ handleDSVParseErrors $ do
        modelCfg <- initModelCfg cfg

        runReader modelCfg $
            case Opts.inputFiles cfg of
              Opts.GivenHitsFile hitsFile -> return ()

              Opts.GivenSequences vpfsFile genomesFile ->
                  withCfgWorkDir cfg $ \workDir ->
                  withProdigalCfg cfg $
                  withHMMSearchCfg cfg $
                  handleFastaParseErrors $ do
                    concOpts <- buildConcOpts workDir vpfsFile

                    liftBaseWith $ \runInIO ->
                      PS.runSafeT $
                        Conc.makeProcessWorker MPI.rootRank tagsClassify MPI.commWorld $ \chunk ->
                          liftIO $ runInIO $
                            fmap D.toFrameRowStoreRec $
                              VC.asyncPipeline (fmap Right $ P.each chunk) concOpts
  where
    buildConcOpts :: Path Directory
                  -> Path HMMERModel
                  -> ClassifyM (VC.ConcurrencyOpts ClassifyM)
    buildConcOpts workDir vpfsFile =
      liftBaseWith $ \runInIO -> return VC.ConcurrencyOpts
          { VC.fastaChunkSize =
              Opts.fastaChunkSize (Opts.concurrencyOpts cfg)
          , VC.pipelineWorkers =
              Conc.replicate1
                (fromIntegral $ Opts.numWorkers (Opts.concurrencyOpts cfg))
                (P.mapM (\chunk -> liftIO $ runInIO $ do
                    hitsFile <- VC.searchGenomeHits workDir vpfsFile (P.each chunk)
                    VC.processHMMOut hitsFile))
          }


initModelCfg :: (Lifted IO r, Member Fail r) => Config -> Eff r VC.ModelConfig
initModelCfg cfg = do
  virusNameExtractor <- compileRegex (Opts.virusNameRegex cfg)

  return VC.ModelConfig
        { VC.modelEValueThreshold    = Opts.evalueThreshold cfg
        , VC.modelVirusNameExtractor = \protName ->
            fromMaybe (error $ "Could not extract virus name from protein: " ++ show protName)
                      (virusNameExtractor protName)
        }


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
            lift $ putStrLn $ "prodigal error: " ++ show e
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
            lift $ putStrLn $ "hmmsearch error: " ++ show e
            die


handleFastaParseErrors :: (Lifted IO r, Member Fail r)
                       => Eff (Exc FA.ParseError ': r) a
                       -> Eff r a
handleFastaParseErrors m = do
    res <- runError m

    case res of
      Right a -> return a

      Left (FA.ExpectedNameLine found) -> do
        lift $ putStrLn $
          "FASTA parsing error: expected name line but found " ++ show found
        die

      Left (FA.ExpectedSequenceLine []) -> do
        lift $ putStrLn $
          "FASTA parsing error: expected sequence but found EOF"
        die

      Left (FA.ExpectedSequenceLine (l:_)) -> do
        lift $ putStrLn $
          "FASTA parsing error: expected sequence but found " ++ show l
        die


handleDSVParseErrors :: (Lifted IO r, Member Fail r)
                     => Eff (Exc DSV.ParseError ': r) a
                     -> Eff r a
handleDSVParseErrors m = do
    res <- runError m

    case res of
      Right a -> return a
      Left (DSV.ParseError ctx row) -> do
        lift $ do
          putStrLn $ "could not parse row " ++ show row
          putStrLn $ " within " ++ show ctx
        die
