{-# language OverloadedStrings #-}
{-# language QuasiQuotes #-}
{-# language TemplateHaskell #-}
module VPF.DataSource.NCBI where

import Data.Text (Text)
import Text.URI.QQ (uri)

import VPF.DataSource.GenericFTP
import VPF.Formats


ncbiSourceConfig :: DownloadList -> FtpSourceConfig
ncbiSourceConfig = $$(ftpSourceConfigFromURI [uri|ftp://ftp.ncbi.nlm.nih.gov/|])


nrFastaPath :: Path (GZip (FASTA 'Aminoacid))
nrFastaPath =
    Tagged $ toLocalRelPath "/blast/db/FASTA/nr.gz"


type ProteinToTaxIdCols = '[ '("accession.version", Text), '("taxid", Int)]

proteinToTaxIdPath :: Path (GZip (TSV ProteinToTaxIdCols))
proteinToTaxIdPath =
    Tagged $ toLocalRelPath "/pub/taxonomy/accession2taxid/prot.accession2taxid.FULL.gz"

