{-# language OverloadedStrings #-}
{-# language MonoLocalBinds #-}
module Main where

import Control.Applicative ((<**>))
import Control.Eff (Eff, Member, Lifted, LiftedBase, lift, runLift)
import Control.Eff.Reader.Strict (runReader)
import Control.Eff.Exception (Exc, runError, Fail, die, ignoreFail)
import Control.Lens (view, over, mapped, (&))

import qualified Options.Applicative as OptP

import VPF.Eff.Cmd (Cmd, runCmd)
import VPF.Ext.HMMER.Search (HMMSearch, HMMSearchError, hmmsearchConfig, execHMMSearch)
import VPF.Ext.Prodigal (Prodigal, ProdigalError, prodigalConfig, execProdigal)

import VPF.Formats
import qualified VPF.Model.Class      as Cls
import qualified VPF.Model.Cols       as M
import qualified VPF.Model.VirusClass as VC

import qualified VPF.Util.Dplyr as D
import qualified VPF.Util.DSV   as DSV
import qualified VPF.Util.FS    as FS
import VPF.Util.Vinyl (rsubset')

import Pipes ((>->))
import qualified Pipes.Core    as P
import qualified Pipes.Prelude as P
import qualified Pipes.Safe    as P

import qualified System.Directory as D

import qualified Opts


type OutputCols = VC.PredictedCols '[M.VirusName, M.ModelName, M.NumHits]
type RawOutputCols = VC.RawPredictedCols '[M.VirusName, M.ModelName, M.NumHits]

type Config = Opts.Config (DSV "\t" OutputCols)


main :: IO ()
main = OptP.execParser opts >>= classify
  where
    opts = OptP.info (Opts.configParser <**> OptP.helper) $
        OptP.fullDesc
        <> OptP.progDesc "Classify virus metagenome from VPF models"
        <> OptP.header "vpf-class: VPF-based virus classifier"


classify :: Config -> IO ()
classify cfg =
    runLift $ ignoreFail $ handleParseErrors $ do
        let modelCfg = VC.ModelConfig
              { VC.modelEValueThreshold = Opts.evalueThreshold cfg
              }

        hitCounts <- runReader modelCfg $
            case Opts.inputFiles cfg of
              Opts.GivenHitsFile hitsFile ->
                  VC.runModel (VC.GivenHitsFile hitsFile)

              Opts.GivenSequences vpfModels genomes ->
                  withCfgWorkDir cfg $ \workDir ->
                    withProdigalCfg cfg $
                      withHMMSearchCfg cfg $
                        VC.runModel (VC.GivenGenomes workDir vpfModels genomes)

        cls <- Cls.loadClassification (Opts.vpfClassFile cfg)

        let predictedCls = VC.predictClassification hitCounts cls

            rawPredictedCls = over (mapped.rsubset') (view Cls.rawClassification)
                                   predictedCls
                            & D.reorder @RawOutputCols

            tsvOpts = DSV.defWriterOptions '\t'

        lift $
          case Opts.outputFile cfg of
            Opts.StdDevice ->
                DSV.writeDSV tsvOpts DSV.stdoutWriter rawPredictedCls
            Opts.FSPath fp ->
                P.runSafeT $
                  DSV.writeDSV tsvOpts (DSV.fileWriter (untag fp)) rawPredictedCls


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


handleParseErrors :: (Lifted IO r, Member Fail r)
                  => Eff (Exc DSV.RowParseError ': r) a -> Eff r a
handleParseErrors m = do
    res <- runError m

    case res of
      Right a -> return a
      Left (DSV.RowParseError ctx row) -> do
        lift $ do
          putStrLn $ "could not parse row " ++ show row
          putStrLn $ " within " ++ show ctx
        die
