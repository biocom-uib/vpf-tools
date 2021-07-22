{-# language GeneralizedNewtypeDeriving #-}
{-# language OverloadedStrings #-}
{-# language QuasiQuotes #-}
{-# language TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}
module VPF.DataSource.VirusHostDB where

import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BS

import Frames (FrameRec)

import System.FilePath ((</>))
import Text.URI.QQ (uri)

import Control.Carrier.Error.Excepts (ExceptsT)
import VPF.DataSource.GenericFTP
import VPF.Formats
import VPF.Frames.DSV qualified as DSV
import VPF.Util.FS (withFileRead)


virusHostDbFtpSourceConfig :: FtpSourceConfig
virusHostDbFtpSourceConfig =
    $$(ftpSourceConfigFromURI [uri|ftp://ftp.genome.jp/pub/db/virushostdb/|]) \_h ->
        return
            [ "README"
            , "non-segmented_virus_list.tsv"
            , "segmented_virus_list.tsv"
            , "virus_genome_type.tsv"
            , "virushostdb.formatted.genomic.fna.gz"
            , "virushostdb.tsv"
            ]


data FormattedFastaName = FormattedFastaName
    { fastaSequenceAccession :: ByteString
    , fastaHostName          :: ByteString
    , fastaVirusLineage      :: ByteString
    , fastaHostLineage       :: ByteString
    , fastaOtherFields       :: [ByteString]
    }


parseFormattedFastaName :: ByteString -> Maybe FormattedFastaName
parseFormattedFastaName formatted =
    case BS.split '|' formatted of
        acc : host : vlin : hlin : other -> Just (FormattedFastaName acc host vlin hlin other)
        _ -> Nothing


syncVirusHostDb :: (String -> IO ()) -> Path Directory -> IO ()
syncVirusHostDb = syncGenericFTP virusHostDbFtpSourceConfig


type VirusHostDbCols = '[]

tryLoadVirusHostDb :: Path Directory -> ExceptsT '[DSV.ParseError, String] IO (FrameRec VirusHostDbCols)
tryLoadVirusHostDb (untag -> downloadDir) = do
    let tsvPath = downloadDir </> "virushostdb.tsv"
        tsvConfig = (DSV.defParserOptions '\t') { DSV.hasHeader = False }

    withFileRead (Tagged tsvPath) \h -> do
        undefined
