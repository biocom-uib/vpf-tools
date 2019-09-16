{-# language ApplicativeDo #-}
{-# language CPP #-}
{-# language RecordWildCards #-}
{-# language StrictData #-}
module Opts where

import Control.Concurrent (getNumCapabilities)
import Control.Monad.Trans.Except (Except, throwE)
import Control.Monad.Trans.Reader (ReaderT(..))

import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Text (Text)
import qualified Data.Text as T

import Options.Applicative
import Options.Applicative.Types

#ifdef VPF_ENABLE_MPI
import qualified Control.Distributed.MPI.Store  as MPI
#endif

import VPF.Formats
import VPF.Ext.HMMER (HMMERConfig(HMMERConfig))
import VPF.Model.Class (RawClassificationCols)
import qualified VPF.Model.Cols as M


data ArgPath t = FSPath (Path t) | StdDevice
  deriving (Eq, Ord)

instance Show (ArgPath t) where
  show (FSPath p) = show (untag p)
  show StdDevice  = "\"-\""


data InputFiles =
    GivenSequences { vpfsFile :: Path HMMERModel, genomesFile :: Path (FASTA Nucleotide) }
  -- | GivenHitsFile { hitsFile :: Path (HMMERTable ProtSearchHitCols) }

data ConcurrencyOpts = ConcurrencyOpts
  { fastaChunkSize :: Int
  , numWorkers :: Int
  }


data Config = Config
  { hmmerConfig      :: HMMERConfig
  , prodigalPath     :: FilePath
  , evalueThreshold  :: Double
  , inputFiles       :: InputFiles
  , virusNameRegex   :: Text
  , vpfClassFiles    :: Map Text (Path (DSV "\t" RawClassificationCols))
  , scoreSampleFiles :: Map Text (Path (DSV "\t" '[M.VirusHitScore]))
  , outputPrefix     :: FilePath
  , workDir          :: Maybe (Path Directory)
  , concurrencyOpts  :: ConcurrencyOpts
  }


argPathReader :: ReadM (ArgPath t)
argPathReader = maybeReader $ \s ->
    case s of
      "-" -> Just StdDevice
      _   -> Just (FSPath (Tagged s))


kvReader :: ReadM a -> ReadM b -> ReadM (a, b)
kvReader ra rb = ReadM $ ReaderT $ \s ->
    case break (== '=') s of
      (sa, '=':sb) -> liftA2 (,) (feedReadM ra sa) (feedReadM rb sb)
      _            -> throwE (ErrorMsg "could not parse key/value pair")
  where
    feedReadM :: ReadM a -> String -> Except ParseError a
    feedReadM ma s = runReaderT (unReadM ma) s


#ifdef VPF_ENABLE_MPI
defaultConcurrencyOpts :: [MPI.Rank] -> IO ConcurrencyOpts
defaultConcurrencyOpts _ = do
#else
defaultConcurrencyOpts :: IO ConcurrencyOpts
defaultConcurrencyOpts = do
#endif
    numWorkers <- getNumCapabilities
    let fastaChunkSize = 1

    return ConcurrencyOpts {..}


#ifdef VPF_ENABLE_MPI
configParserIO :: [MPI.Rank] -> IO (Parser Config)
configParserIO = fmap configParser . defaultConcurrencyOpts
#else
configParserIO :: IO (Parser Config)
configParserIO = fmap configParser defaultConcurrencyOpts
#endif

configParser :: ConcurrencyOpts -> Parser Config
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

    inputFiles <- givenSequences -- <|> givenHitsFile

    virusNameRegex <- fmap T.pack $ strOption $
        long "virus-pattern"
        <> metavar "REGEX"
        <> value "(.*)(?=_\\d+)"
        <> hidden
        <> showDefault
        <> help "PCRE regex matching the virus identifier from a gene identifier (options PCRE_ANCHORED | PCRE_UTF8)"

    vpfClassFiles <- fmap Map.fromList $ some $ option (kvReader str str) $
        long "vpf-classes"
        <> short 'c'
        <> metavar "CLASS_FILE"
        <> help "Tab-separated file containing the classification of the VPFs"

    scoreSampleFiles <- fmap Map.fromList $ some $ option (kvReader str str) $
        long "scores"
        <> short 's'
        <> metavar "SCORE_FILE"
        <> help "Score samples (one per line) to take percentiles on, same format as --vpf-classes"

    outputPrefix <- strOption $
        long "output"
        <> short 'o'
        <> metavar "OUTPUT"
        <> help "Output file prefix (e.g. -c family=fam.tsv -o output would produce output.family.tsv)"

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

    -- givenHitsFile :: Parser InputFiles
    -- givenHitsFile = do
    --   hitsFile <- strOption $
    --       long "hits"
    --       <> short 'h'
    --       <> metavar "HITS_FILE"
    --       <> help "HMMER tblout file containing protein search hits against the VPFs"

    --   pure GivenHitsFile {..}

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

      pure ConcurrencyOpts {..}
