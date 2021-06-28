{-# language GeneralizedNewtypeDeriving #-}
{-# language QuasiQuotes #-}
{-# language TemplateHaskell #-}
module VPF.DataSource.RefSeq where

import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BS

import Network.FTP.Client qualified as FTP

import Text.Regex.PCRE qualified as PCRE
import Text.URI.QQ (uri)

import VPF.DataSource.GenericFTP


refSeqVirusFtpSourceConfig :: FtpSourceConfig
refSeqVirusFtpSourceConfig =
    $$(ftpSourceConfigFromURI [uri|ftp://ftp.ncbi.nlm.nih.gov/refseq/release/|])
        buildDownloadList


refSeqFileRegex :: PCRE.Regex
refSeqFileRegex = PCRE.makeRegex
    "([^0-9]+) \\. ([0-9]+) (:?\\.([0-9]+))? \\. ([^.]+) \\. ([^.]+)"


data RefSeqFile

matchRefSeqFile ::


buildDownloadList :: FTP.Handle -> String -> IO [FtpRelPath]
buildDownloadList h refSeqDir = do
    map (FtpRelPath . BS.unpack) . BS.lines <$> FTP.list h ["/refseq/release/"]
