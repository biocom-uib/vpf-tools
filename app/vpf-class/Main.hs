{-# language OverloadedStrings #-}
{-# language MonoLocalBinds #-}
module Main where

import Control.Applicative ((<**>))
import Control.Eff
import Control.Eff.Reader.Strict
import Control.Eff.Exception
import Control.Lens (Lens', view, over, mapped)
import Control.Monad.IO.Class (liftIO)

import qualified Data.Foldable as F
import qualified Data.Text.IO as T

import Frames.CSV (produceCSV, consumeTextLines)

import qualified Options.Applicative as OptP

import VPF.Eff.Cmd
import VPF.Ext.HMMER.Search
import VPF.Ext.HMMER.TableFormat (RowParseError(..))
import VPF.Ext.Prodigal
import VPF.Formats
import VPF.Model.Class
import VPF.Model.VirusClass
import VPF.Util.Frames (produceDSV)
import VPF.Util.FS (withTmpDir)
import VPF.Util.Vinyl (rsubset')

import Pipes ((>->))
import qualified Pipes.Core    as P
import qualified Pipes.Prelude as P
import qualified Pipes.Safe    as P

import qualified System.Directory as D

import qualified Opts


type Config = Opts.Config (FASTA Nucleotide) PredictedClassificationCols


main :: IO ()
main = OptP.execParser opts >>= classify
  where
    opts = OptP.info (Opts.configParser <**> OptP.helper) $
        OptP.fullDesc
        <> OptP.progDesc "Classify virus metagenome from VPF models"
        <> OptP.header "vpf-class: VPF-based virus classifier"


classify :: Config -> IO ()
classify cfg =
    case Opts.workDir cfg of
      Nothing ->
          withTmpDir "." "vpf-work" (classifyIn cfg)

      Just wd -> do
          D.createDirectoryIfMissing True (untag wd)
          classifyIn cfg wd


classifyIn :: Config  -> Path Directory -> IO ()
classifyIn cfg workDir =
    runLift $ ignoreFail $ do
        let modelCfg = ModelConfig
              { modelWorkDir         = workDir
              , modelEValueThreshold = Opts.evalueThreshold cfg
              }

        hitCounts <- runModelEff cfg $ runReader modelCfg $
            runModel (Opts.hmmerModelFile cfg)
                     (Opts.inputSequences cfg)

        let hitCountsFrame = hitCountsToFrame hitCounts

        cls <- lift loadClassification

        let predictedCls = predictClassification hitCountsFrame cls

            rawPredictedCls = over (mapped.rsubset') (view rawClassification)
                                   predictedCls

            tsvProducer = produceDSV "\t" rawPredictedCls


        lift $ P.runSafeT $ P.runEffect $
          case Opts.outputFile cfg of
            Opts.StdDevice -> tsvProducer >-> P.mapM_ (liftIO . T.putStrLn)
            Opts.FSPath p  -> tsvProducer >-> consumeTextLines (untag p)


runModelEff :: (Lifted IO r, Member Fail r)
            => Config
            -> Eff (Exc RowParseError
                    ': Cmd HMMSearch
                    ': Cmd Prodigal
                    ': Exc HMMSearchError
                    ': Exc ProdigalError
                    ': r)
                   a
            -> Eff r a
runModelEff cfg m =
    handleProdigalErrors $
      handleHMMSearchErrors $ do
        prodigalCfg <- prodigalConfig (Opts.prodigalPath cfg) []
        hmmsearchCfg <- hmmsearchConfig (Opts.hmmsearchPath cfg) []

        execProdigal prodigalCfg $
          execHMMSearch hmmsearchCfg $
            handleParseErrors m
  where
    handleParseErrors :: (Lifted IO r, Member Fail r)
                      => Eff (Exc RowParseError ': r) a -> Eff r a
    handleParseErrors m = do
        res <- runError m

        case res of
          Right a -> return a
          Left (RowParseError row) -> do
            lift $ putStrLn $ "could not parse row: " ++ show row
            die

    handleProdigalErrors :: (Lifted IO r, Member Fail r)
                         => Eff (Exc ProdigalError ': r) a -> Eff r a
    handleProdigalErrors m = do
        res <- runError m

        case res of
          Right a -> return a
          Left e -> do
            lift $ putStrLn $ "prodigal error: " ++ show e
            die

    handleHMMSearchErrors :: (Lifted IO r, Member Fail r)
                          => Eff (Exc HMMSearchError ': r) a -> Eff r a
    handleHMMSearchErrors m = do
        res <- runError m

        case res of
          Right a -> return a
          Left e -> do
            lift $ putStrLn $ "hmmsearch error: " ++ show e
            die

