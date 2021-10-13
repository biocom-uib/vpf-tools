{-# language GeneralizedNewtypeDeriving #-}
{-# language OverloadedStrings #-}
{-# language QuasiQuotes #-}
{-# language TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}
module VPF.DataSource.VirusHostDB where

import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BS
import Data.Semigroup (Any)
import Data.Text (Text)

import Frames (FrameRec, Rec)
import Frames.InCore (RecVec)

import System.FilePath ((</>))
import Text.URI.QQ (uri)

import VPF.DataSource.GenericFTP
import VPF.Formats
import VPF.Frames.DSV qualified as DSV
import VPF.Frames.Types (FieldSubset)


virusHostDbFtpSourceConfig :: FtpSourceConfig
virusHostDbFtpSourceConfig =
    $$(ftpSourceConfigFromURI [uri|ftp://ftp.genome.jp/pub/db/virushostdb/|]) \_h ->
        return $ Right
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


syncVirusHostDb :: LogAction String -> Path Directory -> IO (Either String Any)
syncVirusHostDb = syncGenericFTP virusHostDbFtpSourceConfig


type VirusHostDbCols =
    '[ '("virus_tax_id",    Int)
    ,  '("virus_name",      Text)
    ,  '("virus_lineage",   Text)
    ,  '("refseq_id",       Text)
    ,  '("KEGG_GENOME",     Text)
    ,  '("KEGG_DISEASE",    Text)
    ,  '("DISEASE",         Text)
    ,  '("host_tax_id",     Text) -- Int, but nullable
    ,  '("host_name",       Text)
    ,  '("host_lineage",    Text)
    ,  '("pmid",            Text)
    ,  '("evidence",        Text)
    ,  '("sample_type",     Text)
    ,  '("source_organism", Text)
    ]


loadVirusHostDb ::
    ( FieldSubset Rec cols VirusHostDbCols
    , RecVec cols
    )
    => Path Directory
    -> IO (Either DSV.ParseError (FrameRec cols))
loadVirusHostDb (Tagged downloadDir) =
    DSV.readSubframe (DSV.defParserOptions '\t') tsvPath
  where
    tsvPath :: Path (DSV "\t" VirusHostDbCols)
    tsvPath = Tagged $ downloadDir </> "virushostdb.tsv"
