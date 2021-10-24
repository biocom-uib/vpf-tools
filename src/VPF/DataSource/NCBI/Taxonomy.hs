{-# language QuasiQuotes #-}
{-# language TemplateHaskell #-}
{-# language ViewPatterns #-}
module VPF.DataSource.NCBI.Taxonomy where

import Codec.Archive.Tar qualified as Tar
import Codec.Compression.GZip qualified as GZ

import Control.Monad (when)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (runExceptT, ExceptT(ExceptT))

import Data.ByteString.Lazy qualified as LBS
import Data.Maybe (fromMaybe)
import Data.Semigroup (Any (getAny))
import Data.Text (Text)
import Data.Text qualified as Text

import Frames (Rec, FrameRec, ColumnHeaders, Record)
import Frames.CSV (ReadRec)
import Frames.InCore (RecVec)

import Streaming (Stream, Of)
import Streaming.Prelude qualified as S

import System.Directory qualified as Dir
import System.FilePath ((</>))

import Text.URI.QQ (uri)

import VPF.DataSource.GenericFTP
import VPF.Formats
import VPF.Frames.DSV qualified as DSV
import VPF.Frames.Dplyr.Row qualified as F
import VPF.Frames.Types (FieldSubset, FieldsOf)


taxonomySourceConfig :: FtpSourceConfig
taxonomySourceConfig =
    $$(ftpSourceConfigFromURI [uri|ftp://ftp.ncbi.nlm.nih.gov/pub/taxonomy/|]) \_ ->
        return (Right [FtpRelPath "taxdump.tar.gz"])


taxonomyExtractDir :: String
taxonomyExtractDir = "taxdump"


extractedDmpPath :: Path Directory -> String -> Path (DSV "|" cols)
extractedDmpPath (untag -> downloadDir) name =
    Tagged (downloadDir </> taxonomyExtractDir </> (name ++ ".dmp"))


syncTaxonomy :: LogAction String -> Path Directory -> IO (Either String Any)
syncTaxonomy log (untag -> downloadDir) = runExceptT do
    dirty <- ExceptT $ syncGenericFTP taxonomySourceConfig log (Tagged downloadDir)

    liftIO $
        when (getAny dirty) do
            log $ "Taxonomy files changed, replacing " ++ (downloadDir </> taxonomyExtractDir)

            let destDir = downloadDir </> taxonomyExtractDir

            destExists <- Dir.doesDirectoryExist destDir
            when destExists $
                Dir.removeDirectoryRecursive destDir

            compressedLBS <- LBS.readFile (downloadDir </> "taxdump.tar.gz")
            Tar.unpack destDir (Tar.read (GZ.decompress compressedLBS))

    return dirty


loadTaxonomyDmpFileWith ::
    ( ColumnHeaders allCols
    , FieldSubset Rec cols' cols
    , ReadRec allCols
    , RecVec cols'
    )
    => (Stream (Of (Record allCols)) IO (Either DSV.ParseError ())
        -> Stream (Of (Record cols)) IO (Either DSV.ParseError ()))
    -> Path (DSV "|" allCols)
    -> IO (Either DSV.ParseError (FrameRec cols'))
loadTaxonomyDmpFileWith f dmpPath = do
    let err :: a
        err = error "Bad trailing delimiter in .dmp file"

        dmpOpts :: DSV.ParserOptions
        dmpOpts = (DSV.defParserOptions '|')
            { DSV.hasHeader = False
            , DSV.rowTokenizer =
                -- no quoting in this format it seems
                Text.splitOn fieldSep . fromMaybe err . Text.stripSuffix rowSuffix
            }

    DSV.readSubframeWith dmpOpts f dmpPath
  where
    rowSuffix = Text.pack "\t|"
    fieldSep = Text.pack "\t|\t"


type TaxonomyNodesCols =
    '[ '("tax_id",                        Int)   -- node id in GenBank taxonomy database
    ,  '("parent tax_id",                 Int)   -- parent node id in GenBank taxonomy database
    ,  '("rank",                          Text)  -- rank of this node (superkingdom, kingdom, ...)
    ,  '("embl code",                     Text)  -- locus-name prefix; not unique
    ,  '("division id",                   Int)   -- see division.dmp file
    ,  '("inherited div flag",            Bool)  -- 0 if node inherits division from parent
    ,  '("genetic code id",               Int)   -- see gencode.dmp file
    ,  '("inherited GC  flag",            Bool)  -- 0 if node inherits genetic code from parent
    ,  '("mitochondrial genetic code id", Int)   -- see gencode.dmp file
    ,  '("inherited MGC flag",            Bool)  -- 0 if node inherits mitochondrial gencode from parent
    ,  '("GenBank hidden flag",           Bool)  -- 0 if name is suppressed in GenBank entry lineage
    ,  '("hidden subtree root flag",      Bool)  -- 0 if this subtree has no sequence data yet
    ,  '("comments",                      Text)  -- free-text comments and citations
    ]

loadTaxonomyNodesWith ::
    ( FieldSubset Rec cols' cols
    , RecVec cols'
    )
    => (Stream (Of (Record TaxonomyNodesCols)) IO (Either DSV.ParseError ())
      -> Stream (Of (Record cols)) IO (Either DSV.ParseError ()))
    -> Path Directory
    -> IO (Either DSV.ParseError (FrameRec cols'))
loadTaxonomyNodesWith f downloadDir =
    loadTaxonomyDmpFileWith f (extractedDmpPath downloadDir "nodes")


type TaxonomyNamesCols =
    '[ '("tax_id",      Int)   -- the id of node associated with this name
    ,  '("name_txt",    Text)  -- name itself
    ,  '("unique name", Text)  -- the unique variant of this name if name not unique
    ,  '("name class",  Text)  -- (synonym, common name, ...)
    ]

loadTaxonomyNamesWith ::
    ( FieldSubset Rec cols' cols
    , RecVec cols'
    )
    => (Stream (Of (Record TaxonomyNamesCols)) IO (Either DSV.ParseError ())
      -> Stream (Of (Record cols)) IO (Either DSV.ParseError ()))
    -> Path Directory
    -> IO (Either DSV.ParseError (FrameRec cols'))
loadTaxonomyNamesWith f downloadDir =
    loadTaxonomyDmpFileWith f (extractedDmpPath downloadDir "names")


loadTaxonomyScientificNames ::
    Path Directory
    -> IO (Either DSV.ParseError (FrameRec (FieldsOf TaxonomyNamesCols '["tax_id", "unique name"])))
loadTaxonomyScientificNames =
    loadTaxonomyNamesWith (S.filter isScientificName)
  where
    isScientificName row = F.get @"name class" row == Text.pack "scientific name"

