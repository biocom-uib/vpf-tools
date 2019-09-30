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

import qualified System.FilePath as FP

import Options.Applicative

#ifdef VPF_ENABLE_MPI
import qualified Control.Distributed.MPI.Store  as MPI
#endif

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
    , evalueThreshold    :: Double
    , genomesFile        :: Path (FASTA Nucleotide)
    , virusNameRegex     :: Text
    , dataFilesIndexFile :: Path (YAML DataFilesIndex)
    , outputDir          :: Path Directory
    , workDir            :: Maybe (Path Directory)
    , concurrencyOpts    :: ConcurrencyOpts
    }


#ifdef VPF_ENABLE_MPI
parseArgs :: [MPI.Rank] -> IO Config
parseArgs slaves = do
    parser <- configParserIO slaves
#else
parseArgs :: IO Config
parseArgs = do
    parser <- configParserIO
#endif
    execParser (parserInfo parser)
  where
    parserInfo :: Parser a -> ParserInfo a
    parserInfo parser =
        info (parser <**> helper) $
            fullDesc
            <> progDesc "Classify virus sequences using an existing VPF classification"
            <> header "vpf-class: VPF-based virus sequence classifier"


#ifdef VPF_ENABLE_MPI
configParserIO :: [MPI.Rank] -> IO (Parser Config)
configParserIO = fmap configParser . defaultConcurrencyOpts
#else
configParserIO :: IO (Parser Config)
configParserIO = fmap configParser defaultConcurrencyOpts
#endif


#ifdef VPF_ENABLE_MPI
defaultConcurrencyOpts :: [MPI.Rank] -> IO ConcurrencyOpts
defaultConcurrencyOpts _slaves = do
#else
defaultConcurrencyOpts :: IO ConcurrencyOpts
defaultConcurrencyOpts = do
#endif
    numWorkers <- getNumCapabilities
    let fastaChunkSize = 1

    return ConcurrencyOpts {..}


configParser :: ConcurrencyOpts -> Parser Config
configParser defConcOpts = do
    prodigalPath <- fmap Tagged $ strOption $
        long "prodigal"
        <> metavar "PRODIGAL"
        <> hidden
        <> showDefault
        <> value "prodigal"
        <> help "Path to the prodigal executable (or in $PATH)"

    prodigalProcedure <- strOption $
        long "prodigal-procedure"
        <> metavar "PROCEDURE"
        <> hidden
        <> showDefault
        <> value "meta"
        <> help "Prodigal procedure (-p) to use (for version 2.6.3: single or meta)"

    hmmerPrefix <- optional $ strOption $
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
        long "work-dir"
        <> short 'd'
        <> metavar "DIR"
        <> hidden
        <> help "Generate temporary files in DIR instead of creating a temporary one"

    dataFilesIndexFile <- strOption $
        long "data-index"
        <> metavar "DATA_INDEX"
        <> help ".yaml file containing references to all required data files"

    genomesFile <- strOption $
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

    outputDir <- fmap Tagged $ strOption $
        long "output-dir"
        <> short 'o'
        <> metavar "OUTPUT_DIR"
        <> help "Output directory (e.g. -c family=fam.tsv -o output would produce output/family.tsv)"

    concurrencyOpts <- concOpts

    pure Config {..}
  where
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
