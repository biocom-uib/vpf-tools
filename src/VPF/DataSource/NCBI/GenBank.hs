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
  , GbDelCols
  , loadNewlyDeletedAccessionsList
  , listGenBankSeqFiles
  ) where

import GHC.TypeLits (Symbol)

import Control.Monad (when, forM_)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (runExceptT, ExceptT(ExceptT))
import Control.Monad.Trans.Resource (runResourceT)

import Data.Coerce (coerce)
import Data.Function ((&))
import Data.Kind
import Data.List (sort, isPrefixOf, isSuffixOf)
import Data.Text (Text)
import Data.Text.Encoding qualified as Text
import Data.Yaml qualified as Y

import Frames (FrameRec)

import Network.FTP.Client qualified as FTP

import Streaming.ByteString qualified as BSS
import Streaming.ByteString.Char8 qualified as BSS8
import Streaming.Prelude qualified as S
import Streaming.Zip qualified as SZ

import System.FilePath ((</>), takeFileName, takeDirectory)
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
genBankSourceConfig cfg = ncbiSourceConfig (buildDownloadList cfg)


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


buildDownloadList :: GenBankDownloadList -> DownloadList
buildDownloadList cfg = DownloadList \h -> runExceptT $ S.toList_ do
    fileNameList <- liftIO $ map FTP.mrFilename <$> FTP.mlsd h "genbank/"

    S.yield (FtpRelPath "genbank/GB_Release_Number", NoChecksum)
    S.yield (FtpRelPath "genbank/gbdel.txt.gz",      NoChecksum)

    forM_ fileNameList \fname ->
        when (genBankReleaseFileMatch cfg fname) $
            S.yield (FtpRelPath ("genbank" Posix.</> fname), NoChecksum)

    when (genBankIncludeTPA cfg) $
        S.each [(f, NoChecksum) | f <- tpaExtraFiles]


syncGenBank :: GenBankDownloadList -> LogAction String -> Path Directory -> IO (Either String [ChangelogEntry])
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
    includeReleaseFile p =
        takeDirectory p == "genbank"
            && genBankReleaseFileMatch cfg (takeFileName p)

    includeFile :: FilePath -> Bool
    includeFile
        | genBankIncludeTPA cfg = \p -> includeReleaseFile p || p `elem` tpaFiles
        | otherwise             = includeReleaseFile

    tpaFiles :: [FilePath]
    tpaFiles = map (untag downloadDir </>) tpaExtraFilesLocal
