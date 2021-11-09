{-# language ViewPatterns #-}
module VPF.DataSource.NCBI.GenBank
  ( GenBankDownloadList(..)
  , genBankSourceConfig
  , genBankViralConfig
  , genBankReleaseViralConfig
  , genBankTpaOnlyConfig
  , tpaExtraFiles
  , tpaExtraFilesLocal
  , syncGenBank
  , loadNewlyDeletedAccessionsList
  , listGenBankSeqFiles
  ) where

import GHC.TypeLits (Symbol)

import Control.Monad.Trans (lift)
import Control.Monad.Trans.Except (runExceptT, ExceptT(ExceptT))
import Control.Monad.Trans.Resource (runResourceT)

import Data.Coerce (coerce)
import Data.Function ((&))
import Data.Kind
import Data.List (sort, union, isPrefixOf, isSuffixOf)
import Data.Semigroup (Any)
import Data.Text (Text)
import Data.Text.Encoding qualified as Text
import Data.Yaml qualified as Y

import Frames (FrameRec)

import Network.FTP.Client qualified as FTP

import Streaming.ByteString qualified as BSS
import Streaming.ByteString.Char8 qualified as BSS8
import Streaming.Prelude qualified as S
import Streaming.Zip qualified as SZ

import System.FilePath ((</>), takeFileName)
import System.FilePath.Posix qualified as Posix

import VPF.DataSource.GenericFTP
import VPF.DataSource.NCBI
import VPF.Formats
import VPF.Frames.DSV qualified as DSV


data GenBankDownloadList = GenBankDownloadList
    { genBankReleaseFileMatch :: String -> Bool
    , genBankIncludeTPA :: Bool
    }


genBankSourceConfig :: GenBankDownloadList -> FtpSourceConfig
genBankSourceConfig cfg = ncbiSourceConfig $
    DownloadList \h -> buildDownloadList h cfg


genBankViralConfig :: GenBankDownloadList
genBankViralConfig = GenBankDownloadList
    { genBankReleaseFileMatch = \name ->
        ("gbvrl" `isPrefixOf` name || "gbphg" `isPrefixOf` name)
            && ".seq.gz" `isSuffixOf` name
    , genBankIncludeTPA = True
    }


genBankReleaseViralConfig :: GenBankDownloadList
genBankReleaseViralConfig = genBankViralConfig { genBankIncludeTPA = False }


genBankTpaOnlyConfig :: GenBankDownloadList
genBankTpaOnlyConfig = GenBankDownloadList (const False) True


tpaExtraFiles :: [FtpRelPath]
tpaExtraFiles = coerce ["tpa/release/tpa_cu.gbff.gz"]


tpaExtraFilesLocal :: [FilePath]
tpaExtraFilesLocal = map toLocalRelPath tpaExtraFiles


buildDownloadList :: FTP.Handle -> GenBankDownloadList -> IO (Either String [FtpRelPath])
buildDownloadList h cfg = runExceptT do
    fileNameList <- lift $ map FTP.mrFilename <$> FTP.mlsd h "genbank/"
    let fileList = map ("genbank" Posix.</>) fileNameList

    let releaseNumberFile = "genbank/GB_Release_Number"
        deletedEntriesFile = "genbank/gbdel.txt.gz"
        selectedFiles = filter (genBankReleaseFileMatch cfg . Posix.takeFileName) fileList

        tpa = if genBankIncludeTPA cfg then tpaExtraFiles else []

    return (coerce ([releaseNumberFile, deletedEntriesFile] `union` selectedFiles) ++ tpa)


syncGenBank :: GenBankDownloadList -> LogAction String -> Path Directory -> IO (Either String Any)
syncGenBank = syncGenericFTP . genBankSourceConfig


type GbDelCols :: [(Symbol, Type)]
type GbDelCols = '[ '("file", Text), '("deleted_accession", Text)]


loadNewlyDeletedAccessionsList :: Path Directory -> IO (Either DSV.ParseError (FrameRec GbDelCols))
loadNewlyDeletedAccessionsList (Tagged downloadDir) =
    runResourceT $
        BSS.readFile fp
            & SZ.gunzip
            & BSS8.lines
            & S.mapped BSS.toStrict
            & S.map Text.decodeUtf8
            & DSV.parseEitherRows opts (Tagged @(DSV "|" GbDelCols) fp)
            & DSV.fromEitherRowStreamAoS
  where
    fp = downloadDir </> "genbank" </> "gbdel.txt.gz"

    opts = (DSV.defParserOptions '|') { DSV.hasHeader = False }


listGenBankSeqFiles ::
    GenBankDownloadList
    -> Path Directory
    -> IO (Either Y.ParseException [Path (GZip GenBank)])
listGenBankSeqFiles cfg downloadDir = runExceptT do
    files <- ExceptT $ listDownloadedFiles downloadDir

    return $ coerce $ filter (\p -> includeFile p && isGbffGz p) (sort files)
  where
    isGbffGz :: FilePath -> Bool
    isGbffGz p = "seq.gz" `isSuffixOf` p || "gbff.gz" `isSuffixOf` p

    includeReleaseFile :: FilePath -> Bool
    includeReleaseFile p = genBankReleaseFileMatch cfg (takeFileName p)

    includeFile :: FilePath -> Bool
    includeFile
        | genBankIncludeTPA cfg = \p -> includeReleaseFile p || p `elem` tpaFiles
        | otherwise             = includeReleaseFile

    tpaFiles :: [FilePath]
    tpaFiles = map (untag downloadDir </>) tpaExtraFilesLocal
