{-# language QuasiQuotes #-}
{-# language TemplateHaskell #-}
{-# language ViewPatterns #-}
module VPF.DataSource.NCBI.GenBank where

import Control.Monad (forM_, when)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Except (runExceptT, ExceptT(ExceptT))
import Control.Monad.Trans.Resource (MonadResource)

import Data.Coerce (coerce)
import Data.List (sort, union, isPrefixOf, isSuffixOf)
import Data.Semigroup (Any)

import Network.FTP.Client qualified as FTP

import Streaming (Stream)
import Streaming.ByteString (ByteStream)

import System.Directory qualified as Dir
import System.FilePath ((</>))

import Text.URI.QQ (uri)

import VPF.DataSource.GenericFTP
import VPF.Formats
import VPF.Util.GBFF (parseGenBankFileWith)


newtype GenBankSourceConfig = GenBankSourceConfig
    { genBankFileMatch :: String -> Bool
    }


genBankSourceConfig :: GenBankSourceConfig -> FtpSourceConfig
genBankSourceConfig cfg =
    $$(ftpSourceConfigFromURI [uri|ftp://ftp.ncbi.nlm.nih.gov/genbank/|]) \h ->
        buildDownloadList h cfg


genBankViralConfig :: GenBankSourceConfig
genBankViralConfig = GenBankSourceConfig \p ->
    "gbvrl" `isPrefixOf` p && ".seq.gz" `isSuffixOf` p


buildDownloadList :: FTP.Handle -> GenBankSourceConfig -> IO (Either String [FtpRelPath])
buildDownloadList h cfg = runExceptT do
    fileList <- lift $ map FTP.mrFilename <$> FTP.mlsd h "."

    let releaseNumberFile = "GB_Release_Number"
        selectedFiles = filter (genBankFileMatch cfg) fileList

    return (coerce ([releaseNumberFile] `union` selectedFiles))


syncGenBank :: GenBankSourceConfig -> LogAction String -> Path Directory -> IO (Either String Any)
syncGenBank = syncGenericFTP . genBankSourceConfig


loadGenBankGbWith ::
    ( Functor f
    , MonadResource m
    )
    => GenBankSourceConfig
    -> Path Directory
    -> (FilePath -> ByteStream m () -> Stream f m (Either e ()))
    -> Stream f m (Either e ())
loadGenBankGbWith cfg (untag -> downloadDir) parseGbStream = runExceptT do
    files <- liftIO $ Dir.listDirectory downloadDir

    forM_ (sort files) \file ->
        when (genBankFileMatch cfg file) $
            ExceptT $ parseGenBankFileWith True (downloadDir </> file) parseGbStream