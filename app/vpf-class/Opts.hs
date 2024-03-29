{-# OPTIONS_GHC -Wno-orphans #-}
{-# language ApplicativeDo #-}
{-# language CPP #-}
{-# language DeriveGeneric #-}
{-# language RecordWildCards #-}
{-# language StrictData #-}
module Opts where

import GHC.Generics (Generic)

import Control.Concurrent (getNumCapabilities)
import Control.Lens (Traversal')
import qualified Control.Lens as L

import Data.Map.Strict (Map)
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Yaml.Aeson as Y

import System.Environment (lookupEnv)
import qualified System.FilePath as FP

import Options.Applicative

import VPF.Formats
import VPF.Frames.Types
import qualified VPF.Model.Class      as Cls
import qualified VPF.Model.Class.Cols as Cls


data DataFilesIndex = DataFilesIndex
    { classificationFiles :: Map (Field Cls.ClassKey) Cls.ClassificationFiles
    , vpfsFile            :: Path HMMERModel
    }
    deriving Generic

instance Y.ToJSON Cls.ClassificationFiles
instance Y.FromJSON Cls.ClassificationFiles

instance Y.ToJSON DataFilesIndex
instance Y.FromJSON DataFilesIndex


traverseDataFilesIndex :: Traversal' DataFilesIndex FilePath
traverseDataFilesIndex f (DataFilesIndex cf vpfs) =
    DataFilesIndex <$> traverse (Cls.traverseClassificationFiles f) cf <*> L._Wrapped f vpfs


loadDataFilesIndex :: Path (YAML DataFilesIndex) -> IO (Either Y.ParseException DataFilesIndex)
loadDataFilesIndex indexFile = do
    r <- Y.decodeFileEither (untag indexFile)
    return $ L.over (L.mapped . traverseDataFilesIndex) makeRelativeToIndex r
  where
    indexDir :: FilePath
    indexDir = FP.takeDirectory (untag indexFile)

    makeRelativeToIndex :: FilePath -> FilePath
    makeRelativeToIndex p
      | FP.isRelative p = indexDir FP.</> p
      | otherwise       = p


data ConcurrencyOpts = ConcurrencyOpts
    { fastaChunkSize :: Int
    , numWorkers :: Int
    }


data Config = Config
    { hmmerPrefix        :: Maybe (Path Directory)
    , prodigalPath       :: Path Executable
    , prodigalProcedure  :: String
    , prodigalMaskNs     :: Bool
    , evalueThreshold    :: Double
    , genomesFile        :: Path (FASTA Nucleotide)
    , virusNameRegex     :: Text
    , dataFilesIndexFile :: Path (YAML DataFilesIndex)
    , outputDir          :: Path Directory
    , workDir            :: Maybe (Path Directory)
    , concurrencyOpts    :: ConcurrencyOpts
    }


parseArgs :: IO Config
parseArgs = do
    parser <- configParserIO
    execParser (parserInfo parser)
  where
    parserInfo :: Parser a -> ParserInfo a
    parserInfo parser =
        info (parser <**> helper) $
            fullDesc
            <> progDesc "Classify virus sequences using an existing VPF classification"
            <> header "vpf-class: VPF-based virus sequence classifier"
#ifdef VPF_ENABLE_MPI
            <> footer "NOTE: This build is OpenMPI-enabled. The support is\
                      \ experimental and the --workers and --chunk-size flags\
                      \ are used for each individual worker."
#else
            <> footer "NOTE: OpenMPI support is disabled in this build."
#endif


configParserIO :: IO (Parser Config)
configParserIO = configParser <$> defaultConcurrencyOpts <*> defaultFilesIndexFile


defaultConcurrencyOpts :: IO ConcurrencyOpts
defaultConcurrencyOpts = do
    numWorkers <- getNumCapabilities
    let fastaChunkSize = 1

    return ConcurrencyOpts {..}


defaultFilesIndexFile :: IO (Maybe (Path (YAML DataFilesIndex)))
defaultFilesIndexFile = do
    fmap Tagged <$> lookupEnv "VPF_CLASS_DATA_INDEX"


pathOption :: Mod OptionFields (Path t) -> Parser (Path t)
pathOption = strOption


configParser :: ConcurrencyOpts -> Maybe (Path (YAML DataFilesIndex)) -> Parser Config
configParser defConcOpts defDataIndex = do
    prodigalPath <- pathOption $
        long "prodigal"
        <> metavar "PRODIGAL"
        <> hidden
        <> showDefault
        <> value (Tagged "prodigal")
        <> help "Path to the prodigal executable (or in $PATH)"

    prodigalProcedure <- strOption $
        long "prodigal-procedure"
        <> metavar "PROCEDURE"
        <> hidden
        <> showDefault
        <> value "meta"
        <> help "Prodigal procedure (-p) to use (for version 2.6.3: single or meta)"

    prodigalMaskNs <- switch $
        long "prodigal-mask-n"
        <> hidden
        <> help "Mask N bases during protein prediction (prodigal option -m)"

    hmmerPrefix <- optional $ pathOption $
        long "hmmer-prefix"
        <> metavar "HMMER"
        <> hidden
        <> showDefault
        <> help "Prefix to the HMMER installation (e.g. HMMER/bin/hmmsearch must exist)"

    evalueThreshold <- option auto $
        long "evalue"
        <> short 'E'
        <> metavar "THRESHOLD"
        <> hidden
        <> showDefault
        <> value 1e-3
        <> help "Accept hits with e-value <= THRESHOLD"

    workDir <- optional $ pathOption $
        long "work-dir"
        <> short 'd'
        <> metavar "DIR"
        <> hidden
        <> help "Generate temporary files in DIR instead of creating a temporary one"

    dataFilesIndexFile <- pathOption $
        case defDataIndex of
            Nothing   -> dataFilesIndexOpt
            Just path -> dataFilesIndexOpt <> value path <> hidden <> showDefault

    genomesFile <- pathOption $
        long "input-seqs"
        <> short 'i'
        <> metavar "SEQS_FILE"
        <> help "FASTA file containg full input sequences"

    virusNameRegex <- fmap T.pack $ strOption $
        long "virus-pattern"
        <> metavar "REGEX"
        <> value "(.*)(?=_\\d+)"
        <> hidden
        <> showDefault
        <> help "PCRE regex matching the virus identifier from a gene identifier (options PCRE_ANCHORED | PCRE_UTF8)"

    outputDir <- pathOption $
        long "output-dir"
        <> short 'o'
        <> metavar "OUTPUT_DIR"
        <> help "Output directory"

    concurrencyOpts <- concOpts

    pure Config {..}
  where
    dataFilesIndexOpt :: Mod OptionFields (Path (YAML DataFilesIndex))
    dataFilesIndexOpt =
        long "data-index"
        <> metavar "DATA_INDEX"
        <> help "YAML file containing references to all required data files"

    concOpts :: Parser ConcurrencyOpts
    concOpts = do
        numWorkers <- option auto $
            long "workers"
            <> metavar "NW"
            <> value (numWorkers defConcOpts)
            <> hidden
            <> showDefault
            <> help "Number of threads to use"

        fastaChunkSize <- option auto $
            long "chunk-size"
            <> metavar "CHUNK_SZ"
            <> value (fastaChunkSize defConcOpts)
            <> hidden
            <> showDefault
            <> help "Number of sequences to be processed at once by prodigal/hmmsearch"

        pure ConcurrencyOpts {..}
