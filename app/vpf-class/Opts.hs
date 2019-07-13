{-# language ApplicativeDo #-}
{-# language RecordWildCards #-}
module Opts where

import Options.Applicative

import VPF.Formats
import VPF.Ext.HMMER (HMMERConfig(HMMERConfig))
import VPF.Ext.HMMER.Search (ProtSearchHitCols)
import VPF.Model.Class (RawClassificationCols)


data ArgPath t = FSPath (Path t) | StdDevice
  deriving (Eq, Ord)

instance Show (ArgPath t) where
  show (FSPath p) = show (untag p)
  show StdDevice  = "\"-\""


data InputFiles =
    GivenSequences { vpfModelsFile :: Path HMMERModel, genomesFile :: Path (FASTA Nucleotide) }
  | GivenHitsFile { hitsFile :: Path (HMMERTable ProtSearchHitCols) }

data Config outfmt = Config
  { hmmerConfig     :: HMMERConfig
  , prodigalPath    :: FilePath
  , evalueThreshold :: Double
  , inputFiles      :: InputFiles
  , vpfClassFile    :: Path (DSV "\t" RawClassificationCols)
  , outputFile      :: ArgPath outfmt
  , workDir         :: Maybe (Path Directory)
  }


argPathReader :: ReadM (ArgPath t)
argPathReader = maybeReader $ \s ->
    case s of
      "-" -> Just StdDevice
      _   -> Just (FSPath (Tagged s))


configParser :: Parser (Config outfmt)
configParser = do
    prodigalPath <- strOption $
        long "prodigal"
        <> metavar "PRODIGAL"
        <> value "prodigal"
        <> help "Path to the prodigal executable (or in $PATH)"

    hmmerConfig <- fmap HMMERConfig $ optional $ strOption $
        long "hmmer-prefix"
        <> metavar "HMMER"
        <> help "Prefix to the HMMER installation (e.g. HMMER/bin/hmmsearch should exist)"

    evalueThreshold <- option auto $
        long "evalue"
        <> short 'E'
        <> metavar "THRESHOLD"
        <> value 1e-3
        <> help "Accept hits with e-value <= THRESHOLD (1e-3)"

    workDir <- optional $ fmap Tagged $ strOption $
        long "workdir"
        <> short 'd'
        <> metavar "DIR"
        <> help "Generate temporary files in DIR instead of creating a temporary one"

    inputFiles <- givenSequences <|> givenHitsFile

    vpfClassFile <- strOption $
        long "vpf-class"
        <> short 'c'
        <> metavar "CLASS_FILE"
        <> help "Tab-separated file containing the classification of the VPFs"

    outputFile <- option argPathReader $
        long "output"
        <> short 'o'
        <> metavar "OUTPUT"
        <> value StdDevice
        <> help "Path of the output directory, stdout by default (-)"

    pure Config {..}

  where
    givenSequences :: Parser InputFiles
    givenSequences = do
      vpfModelsFile <- strOption $
          long "vpf-models"
          <> short 'm'
          <> metavar "VPF_HMM"
          <> help ".hmms file containing query VPF models"

      genomesFile <- strOption $
          long "input-seqs"
          <> short 'i'
          <> metavar "SEQS_FILE"
          <> help "FASTA file containg full input sequences"

      pure GivenSequences {..}

    givenHitsFile :: Parser InputFiles
    givenHitsFile = do
        hitsFile <- strOption $
           long "hits"
           <> short 'h'
           <> metavar "HITS_FILE"
           <> help "HMMER tblout file containing protein search hits against the VPF models"

        pure GivenHitsFile {..}
