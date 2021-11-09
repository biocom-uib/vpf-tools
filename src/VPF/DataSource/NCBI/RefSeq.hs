{-# language GeneralizedNewtypeDeriving #-}
{-# language TupleSections #-}
{-# language ViewPatterns #-}
module VPF.DataSource.NCBI.RefSeq where

import Control.Monad (forM)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Except (runExceptT, throwE, ExceptT(ExceptT))

import Data.Bits ((.|.))
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BS8
import Data.Coerce (coerce)
import Data.Function ((&))
import Data.Maybe (isJust)
import Data.List (isInfixOf, union, sort)
import Data.Semigroup (Any)
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
import VPF.Frames.Dplyr qualified as F
import VPF.Frames.DSV qualified as DSV
import VPF.Frames.Types (FieldSubset)
import VPF.Formats
import VPF.Util.FS qualified as FS
import VPF.DataSource.NCBI (ncbiSourceConfig)


data RefSeqDownloadList = RefSeqDownloadList
    { refSeqDirectories :: [String]
    , refSeqSeqFormats  :: [ByteString]
    }


instance Semigroup RefSeqDownloadList where
    RefSeqDownloadList dirs1 formats1 <> RefSeqDownloadList dirs2 formats2 =
        RefSeqDownloadList (union dirs1 dirs2) (union formats1 formats2)


refSeqFtpSourceConfig :: RefSeqDownloadList -> FtpSourceConfig
refSeqFtpSourceConfig cfg =
    (ncbiSourceConfig (refSeqDownloadList cfg)) { ftpBasePath = "/refseq/release" }


refSeqViralGenomicList :: RefSeqDownloadList
refSeqViralGenomicList = RefSeqDownloadList ["viral"] [BS8.pack "genomic.gbff"]


refSeqViralProteinsList :: RefSeqDownloadList
refSeqViralProteinsList = RefSeqDownloadList ["viral"] [BS8.pack "protein.gpff"]


refSeqFileRegex :: PCRE.Regex
refSeqFileRegex =
    PCRE.makeRegexOpts
        (PCRE.compAnchored .|. PCRE.compExtended)
        PCRE.execAnchored
        "([^0-9]+) \\. ([0-9]+(?:\\.[0-9]+)?) \\. ([^0-9]+) \\. ([^.]+)"


refSeqCatalogRegex :: PCRE.Regex
refSeqCatalogRegex =
    PCRE.makeRegex "RefSeq-release([0-9]+)\\.catalog\\.gz"


-- returns version number (if matched)
matchRefSeqCatalog :: PCRE.RegexLike PCRE.Regex source => source -> Maybe source
matchRefSeqCatalog filename = do
    mr <- PCRE.matchM refSeqCatalogRegex filename

    case PCRE.mrSubList mr of
        [releaseNum] -> Just releaseNum
        _            -> Nothing


-- expecting base name
fileMatchesCfg :: RefSeqDownloadList -> ByteString -> Bool
fileMatchesCfg cfg filename =
    case PCRE.matchM refSeqFileRegex filename of
        Just mr | [_dir, _increment, format, compression] <- PCRE.mrSubList mr ->
            and [format `elem` refSeqSeqFormats cfg, compression == BS8.pack "gz"]

        _ -> False


refSeqDownloadList :: RefSeqDownloadList -> DownloadList
refSeqDownloadList cfg = DownloadList \h -> runExceptT do
    catalogDirList <- map (BS8.pack . FTP.mrFilename) <$> FTP.mlsd h "release-catalog"

    let catalogs = filter (isJust . matchRefSeqCatalog) catalogDirList

    catalogPath <-
        case catalogs of
            [catalog] ->
                return $ FtpRelPath ("release-catalog" Posix.</> BS8.unpack catalog)

            [] -> throwE "no catalogs found"
            _  -> throwE ("multiple catalogs found: " ++ show catalogs)


    fileList <- fmap concat $
        forM (refSeqDirectories cfg) \dir -> do
            files <- lift $ map (BS8.pack . FTP.mrFilename) <$> FTP.mlsd h dir

            let toRelPath file = FtpRelPath (dir Posix.</> BS8.unpack file)

            case [toRelPath file | file <- files, fileMatchesCfg cfg file] of
                []       -> throwE ("no files matched in directory " ++ show dir)
                matches  -> return matches

    return (catalogPath : fileList)


syncRefSeq :: RefSeqDownloadList -> LogAction String -> Path Directory -> IO (Either String Any)
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
            & DSV.parseEitherRows
                (DSV.defParserOptions '\t') { DSV.hasHeader = False }
                catalogPath
            & S.filter (either (const True) checkReleaseDir)
            & S.map (fmap rcast)
            & DSV.fromEitherRowStreamAoS
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

    return $ coerce $ filter fileMatch (sort files)
  where
    fileMatch :: String -> Bool
    fileMatch fp =
        (isInfixOf ".gbff." fp || isInfixOf ".gpff." fp)
            && fileMatchesCfg cfg (BS8.pack (takeFileName fp))
