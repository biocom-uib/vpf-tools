{-# language ApplicativeDo #-}
{-# language RecordWildCards #-}
module Opts where

import Control.Concurrent (getNumCapabilities)
import Options.Applicative

import VPF.Formats
import VPF.Ext.HMMER (HMMERConfig(HMMERConfig))
import VPF.Ext.HMMER.Search (ProtSearchHitCols)
import VPF.Model.Class (RawClassificationCols)

import qualified VPF.Model.VirusClass as VC


data ArgPath t = FSPath (Path t) | StdDevice
  deriving (Eq, Ord)

instance Show (ArgPath t) where
  show (FSPath p) = show (untag p)
  show StdDevice  = "\"-\""


data InputFiles =
    GivenSequences { vpfsFile :: Path HMMERModel, genomesFile :: Path (FASTA Nucleotide) }
  | GivenHitsFile { hitsFile :: Path (HMMERTable ProtSearchHitCols) }


data Config outfmt = Config
  { hmmerConfig     :: HMMERConfig
  , prodigalPath    :: FilePath
  , evalueThreshold :: Double
  , inputFiles      :: InputFiles
  , vpfClassFile    :: Path (DSV "\t" RawClassificationCols)
  , outputFile      :: ArgPath outfmt
  , workDir         :: Maybe (Path Directory)
  , concurrencyOpts :: VC.ConcurrencyOpts
  }


argPathReader :: ReadM (ArgPath t)
argPathReader = maybeReader $ \s ->
    case s of
      "-" -> Just StdDevice
      _   -> Just (FSPath (Tagged s))


defaultConcurrencyOpts :: IO VC.ConcurrencyOpts
defaultConcurrencyOpts = do
    maxSearchingWorkers <- getNumCapabilities
    let fastaChunkSize = 1

    return VC.ConcurrencyOpts {..}


configParserIO :: IO (Parser (Config outfmt))
configParserIO = fmap configParser defaultConcurrencyOpts

configParser :: VC.ConcurrencyOpts -> Parser (Config outfmt)
configParser defConcOpts = do
    prodigalPath <- strOption $
        long "prodigal"
        <> metavar "PRODIGAL"
        <> hidden
        <> showDefault
        <> value "prodigal"
        <> help "Path to the prodigal executable (or in $PATH)"

    hmmerConfig <- fmap HMMERConfig $ optional $ strOption $
        long "hmmer-prefix"
        <> metavar "HMMER"
        <> hidden
        <> showDefault
        <> help "Prefix to the HMMER installation (e.g. HMMER/bin/hmmsearch should exist)"

    evalueThreshold <- option auto $
        long "evalue"
        <> short 'E'
        <> metavar "THRESHOLD"
        <> hidden
        <> showDefault
        <> value 1e-3
        <> help "Accept hits with e-value <= THRESHOLD"

    workDir <- optional $ fmap Tagged $ strOption $
        long "workdir"
        <> short 'd'
        <> metavar "DIR"
        <> hidden
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
        <> showDefault
        <> help "Output file or - for STDOUT"

    concurrencyOpts <- concOpts

    pure Config {..}

  where
    givenSequences :: Parser InputFiles
    givenSequences = do
      vpfsFile <- strOption $
          long "vpf"
          <> short 'v'
          <> metavar "VPF_HMMS"
          <> help ".hmms file containing query VPF profiles"

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
          <> help "HMMER tblout file containing protein search hits against the VPFs"

      pure GivenHitsFile {..}


    concOpts :: Parser VC.ConcurrencyOpts
    concOpts = do
      maxSearchingWorkers <- option auto $
          long "workers"
          <> metavar "NW"
          <> value (VC.maxSearchingWorkers defConcOpts)
          <> hidden
          <> showDefault
          <> help "Number of parallel workers (prodigal/hmmsearch) processing the FASTA file"

      fastaChunkSize <- option auto $
          long "chunk-size"
          <> metavar "CHUNK_SZ"
          <> value (VC.fastaChunkSize defConcOpts)
          <> hidden
          <> showDefault
          <> help "Number of sequences to be processed at once by each parallel worker"

      pure VC.ConcurrencyOpts {..}
