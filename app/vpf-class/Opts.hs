{-# language ApplicativeDo #-}
{-# language RecordWildCards #-}
{-# language StrictData #-}
module Opts where

import Control.Concurrent (getNumCapabilities)
import Data.Text (Text)
import qualified Data.Text as T
import Options.Applicative

import qualified Control.Distributed.MPI.Store  as MPI

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
    GivenSequences { vpfsFile :: Path HMMERModel, genomesFile :: Path (FASTA Nucleotide) }
  | GivenHitsFile { hitsFile :: Path (HMMERTable ProtSearchHitCols) }

data ConcurrencyOpts = ConcurrencyOpts
  { fastaChunkSize :: Int
  , numWorkers :: Int
  , useMPI :: Bool
  }



data Config outfmt = Config
  { hmmerConfig     :: HMMERConfig
  , prodigalPath    :: FilePath
  , evalueThreshold :: Double
  , inputFiles      :: InputFiles
  , virusNameRegex  :: Text
  , vpfClassFile    :: Path (DSV "\t" RawClassificationCols)
  , outputFile      :: ArgPath outfmt
  , workDir         :: Maybe (Path Directory)
  , concurrencyOpts :: ConcurrencyOpts
  }


argPathReader :: ReadM (ArgPath t)
argPathReader = maybeReader $ \s ->
    case s of
      "-" -> Just StdDevice
      _   -> Just (FSPath (Tagged s))


defaultConcurrencyOpts :: [MPI.Rank] -> IO ConcurrencyOpts
defaultConcurrencyOpts slaves = do
    numWorkers <- getNumCapabilities
    let fastaChunkSize = 1
    let useMPI = False

    return ConcurrencyOpts {..}


configParserIO :: [MPI.Rank] -> IO (Parser (Config outfmt))
configParserIO = fmap configParser . defaultConcurrencyOpts

configParser :: ConcurrencyOpts -> Parser (Config outfmt)
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

    virusNameRegex <- fmap T.pack $ strOption $
        long "virus-pattern"
        <> metavar "REGEX"
        <> value "(.*)(?=_\\d+)"
        <> hidden
        <> showDefault
        <> help "PCRE regex matching the virus identifier from a gene identifier (options PCRE_ANCHORED | PCRE_UTF8)"

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


    concOpts :: Parser ConcurrencyOpts
    concOpts = do
      numWorkers <- option auto $
          long "workers"
          <> metavar "NW"
          <> value (numWorkers defConcOpts)
          <> hidden
          <> showDefault
          <> help "Number of parallel workers processing the FASTA file"

      fastaChunkSize <- option auto $
          long "chunk-size"
          <> metavar "CHUNK_SZ"
          <> value (fastaChunkSize defConcOpts)
          <> hidden
          <> showDefault
          <> help "Number of sequences to be processed at once by each parallel worker"

      useMPI <- switch $
          long "mpi"
          <> hidden
          <> showDefault
          <> help "Enable MPI mode"

      pure ConcurrencyOpts {..}
