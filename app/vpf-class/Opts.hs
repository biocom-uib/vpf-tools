{-# language ApplicativeDo #-}
{-# language RecordWildCards #-}
module Opts where

import Options.Applicative

import VPF.Formats


data ArgPath t = FSPath (Path t) | StdDevice
  deriving (Eq, Ord)

instance Show (ArgPath t) where
  show (FSPath p) = show (untag p)
  show StdDevice  = "\"-\""


data Config input outputCols = Config
  { hmmsearchPath   :: FilePath
  , prodigalPath    :: FilePath
  , evalueThreshold :: Double
  , hmmerModelFile  :: Path HMMERModel
  , inputSequences  :: Path input
  , outputFile      :: ArgPath outputCols
  , workDir         :: Maybe (Path Directory)
  }


argPathReader :: ReadM (ArgPath t)
argPathReader = maybeReader $ \s ->
    case s of
      "-" -> Just StdDevice
      _   -> Just (FSPath (Tagged s))


configParser :: Parser (Config input outputCols)
configParser = do
    prodigalPath <- strOption $
        long "prodigal"
        <> metavar "PRODIGAL"
        <> value "prodigal"
        <> help "Path to the prodigal executable (or in $PATH)"

    hmmsearchPath <- strOption $
        long "hmmsearch"
        <> metavar "HMMSEARCH"
        <> value "hmmsearch"
        <> help "Path to the hmmsearch executable (or in $PATH)"

    evalueThreshold <- option auto $
        long "evalue"
        <> short 'E'
        <> metavar "THRESHOLD"
        <> value 1e-3
        <> help "Accept hits with e-value <= THRESHOLD"

    workDir <- optional $ fmap Tagged $ strOption $
        long "workdir"
        <> short 'd'
        <> metavar "DIR"
        <> help "Generate temporary files in DIR instead of creating a temporary one"

    hmmerModelFile <- strArgument $
        metavar "HMM_FILE"
        <> help ".hmm file containing VPF models"

    inputSequences <- strArgument $
        metavar "SEQS_FILE"
        <> help "FASTA file containg metagenomic search sequences"

    outputFile <- option argPathReader $
        long "output"
        <> short 'o'
        <> metavar "OUTPUT"
        <> value StdDevice
        <> help "Path of the output directory, stdout by default (-)"

    pure Config {..}

