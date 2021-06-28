{-# language GeneralizedNewtypeDeriving #-}
{-# language OverloadedStrings #-}
{-# language QuasiQuotes #-}
{-# language TemplateHaskell #-}
module VPF.DataSource.VirusHostDB where

import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BS

import Text.URI.QQ (uri)

import VPF.DataSource.GenericFTP


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


syncVirusHostDB :: (String -> IO ()) -> FilePath -> IO ()
syncVirusHostDB = syncGenericFTP virusHostDbFtpSourceConfig
