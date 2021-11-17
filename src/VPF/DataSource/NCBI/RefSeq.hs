{-# language GeneralizedNewtypeDeriving #-}
{-# language ViewPatterns #-}
module VPF.DataSource.NCBI.RefSeq
    ( RefSeqDownloadList(..)
    , refSeqFtpSourceConfig
    , refSeqViralGenomicList
    , refSeqViralProteinsList
    , matchRefSeqCatalog
    , seqFileMatchesCfg
    , buildDownloadList
    , syncRefSeq
    , CatalogCols
    , loadRefSeqCatalog
    , listRefSeqSeqFiles
    ) where

import Control.Monad (when, forM_)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Except (runExceptT, throwE, ExceptT(ExceptT))

import Data.Bits ((.|.))
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BS8
import Data.Coerce (coerce)
import Data.Function ((&))
import Data.Maybe (isJust)
import Data.List (isInfixOf, union, sort)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Vinyl (rcast)
import Data.Yaml qualified as Y

import Frames (FrameRec, Record, Rec)
import Frames.InCore (RecVec)

import Network.FTP.Client qualified as FTP

import Streaming.ByteString qualified as BSS
import Streaming.Prelude qualified as S
import Streaming.Zip qualified as SZ

import System.FilePath ((</>), takeFileName)
import System.FilePath.Posix qualified as Posix
import System.Directory qualified as Dir
import System.IO qualified as IO

import Text.Regex.PCRE qualified as PCRE

import VPF.DataSource.GenericFTP
import VPF.DataSource.NCBI (ncbiSourceConfig)
import VPF.Frames.Dplyr qualified as F
import VPF.Frames.DSV qualified as DSV
import VPF.Frames.Types (FieldSubset)
import VPF.Formats
import VPF.Util.FS qualified as FS


data RefSeqDownloadList = RefSeqDownloadList
    { refSeqIncludeCatalog :: Bool
    , refSeqDirectories    :: [String]
    , refSeqSeqFormats     :: [ByteString]
    }


instance Semigroup RefSeqDownloadList where
    RefSeqDownloadList c1 dirs1 formats1 <> RefSeqDownloadList c2 dirs2 formats2 =
        RefSeqDownloadList (c1 || c2) (union dirs1 dirs2) (union formats1 formats2)


refSeqFtpSourceConfig :: RefSeqDownloadList -> FtpSourceConfig
refSeqFtpSourceConfig cfg = ncbiSourceConfig (buildDownloadList cfg)


refSeqViralGenomicList :: RefSeqDownloadList
refSeqViralGenomicList = RefSeqDownloadList False ["viral"] [BS8.pack "genomic.gbff"]


refSeqViralProteinsList :: RefSeqDownloadList
refSeqViralProteinsList = RefSeqDownloadList False ["viral"] [BS8.pack "protein.gpff"]


refSeqFileRegex :: PCRE.Regex
refSeqFileRegex =
    PCRE.makeRegexOpts
        (PCRE.compAnchored .|. PCRE.compExtended)
        PCRE.execAnchored
        "([^0-9]+) \\. ([0-9]+(?:\\.[0-9]+)?) \\. ([^0-9]+) \\. ([^.]+)"


refSeqCatalogRegex :: PCRE.Regex
refSeqCatalogRegex =
    PCRE.makeRegex "RefSeq-release([0-9]+)\\.catalog\\.gz"


refSeqFilesInstalledRegex :: PCRE.Regex
refSeqFilesInstalledRegex =
    PCRE.makeRegex "release([0-9]+)\\.files.installed"


-- returns version number (if matched)
matchRefSeqCatalog :: PCRE.RegexLike PCRE.Regex source => source -> Maybe source
matchRefSeqCatalog filename = do
    mr <- PCRE.matchM refSeqCatalogRegex filename

    case PCRE.mrSubList mr of
        [releaseNum] -> Just releaseNum
        _            -> Nothing


-- returns version number (if matched)
matchRefSeqFilesInstalled :: PCRE.RegexLike PCRE.Regex source => source -> Maybe source
matchRefSeqFilesInstalled filename = do
    mr <- PCRE.matchM refSeqFilesInstalledRegex filename

    case PCRE.mrSubList mr of
        [releaseNum] -> Just releaseNum
        _            -> Nothing


-- expecting base name
seqFileMatchesCfg :: RefSeqDownloadList -> ByteString -> Bool
seqFileMatchesCfg cfg filename =
    case PCRE.matchM refSeqFileRegex filename of
        Just mr | [_dir, _increment, format, compression] <- PCRE.mrSubList mr ->
            and [format `elem` refSeqSeqFormats cfg, compression == BS8.pack "gz"]

        _ -> False


buildDownloadList :: RefSeqDownloadList -> DownloadList
buildDownloadList cfg = DownloadList \h -> runExceptT $ S.toList_ do
    S.yield (FtpRelPath (basePath Posix.</> "RELEASE_NUMBER"), NoChecksum)

    catalogDirList <- liftIO $ mlsd h (basePath Posix.</> "release-catalog")

    when (refSeqIncludeCatalog cfg) do
        case filter (isJust . matchRefSeqCatalog) catalogDirList of
            [] -> lift $ throwE "no catalogs found"

            [catalog] ->
                S.yield (FtpRelPath (basePath Posix.</> "release-catalog" Posix.</> BS8.unpack catalog), NoChecksum)

            catalogs  -> lift $ throwE ("multiple catalogs found: " ++ show catalogs)

    crcs <-
        case filter (isJust . matchRefSeqFilesInstalled) catalogDirList of
            [filesInstalled] -> liftIO $
                parseChecksums CRC <$>
                    FTP.retr h (basePath Posix.</> "release-catalog" Posix.</> BS8.unpack filesInstalled)

            _ -> mempty

    forM_ (refSeqDirectories cfg) \dir -> do
        let absDir = basePath Posix.</> dir

        files <- liftIO $ mlsd h absDir

        case filter (seqFileMatchesCfg cfg) files of
            [] ->
                lift $ throwE ("no files matched in directory " ++ show dir)
            matches -> do
                forM_ matches \match ->
                    S.yield (FtpRelPath (absDir Posix.</> BS8.unpack match), findChecksumFor match crcs)
  where
    basePath = "refseq/release"

    mlsd :: FTP.Handle -> String -> IO [ByteString]
    mlsd h dir = map (BS8.pack . FTP.mrFilename) <$> FTP.mlsd h (basePath Posix.</> dir)


syncRefSeq :: RefSeqDownloadList -> LogAction String -> Path Directory -> IO (Either String [ChangelogEntry])
syncRefSeq = syncGenericFTP . refSeqFtpSourceConfig


type CatalogCols =
    '[ '("taxid",             Int)
    ,  '("species_name",      Text)
    ,  '("accession_version", Text)
    ,  '("release_directory", Text)
    ,  '("refseq_status",     Text)
    ,  '("length",            Int)
    ,  '("removed_status",    Text)
    ]


loadRefSeqCatalog ::
    ( RecVec cols
    , FieldSubset Rec cols CatalogCols
    )
    => RefSeqDownloadList
    -> Path Directory
    -> IO (Either DSV.ParseError (FrameRec cols))
loadRefSeqCatalog cfg (untag -> downloadDir) = do
    [catalog] <-
        filter (isJust . matchRefSeqCatalog) <$>
            Dir.listDirectory (downloadDir </> "release-catalog")

    let catalogPath :: Path (TSV CatalogCols)
        catalogPath = Tagged (downloadDir </> "release-catalog" </> catalog)

    FS.withBinaryFile catalogPath IO.ReadMode \h ->
        BSS.fromHandle h
            & SZ.gunzip
            & FS.toTextLines
            & DSV.parsedRowStream
                (DSV.defParserOptions '\t') { DSV.hasHeader = False }
                catalogPath
            & S.filter checkReleaseDir
            & S.map rcast
            & DSV.fromParsedRowStreamSoA
  where
    cfgReleaseDirsText :: [Text]
    cfgReleaseDirsText = map Text.pack (refSeqDirectories cfg)

    checkReleaseDir :: Record CatalogCols -> Bool
    checkReleaseDir row =
        let releaseDirs = F.get @"release_directory" row
        in any (`Text.isInfixOf` releaseDirs) cfgReleaseDirsText


listRefSeqSeqFiles ::
    RefSeqDownloadList
    -> Path Directory
    -> IO (Either Y.ParseException [Path (GZip GenBank)])
listRefSeqSeqFiles cfg downloadDir = runExceptT do
    files <- ExceptT $ listDownloadedFiles downloadDir

    return $ coerce $ filter (filenameMatch . takeFileName) (sort files)
  where
    filenameMatch :: String -> Bool
    filenameMatch fname =
        (isInfixOf ".gbff." fname || isInfixOf ".gpff." fname)
            && seqFileMatchesCfg cfg (BS8.pack fname)
