{-# language TemplateHaskell #-}
{-# language ViewPatterns #-}
module VPF.DataSource.NCBI.Taxonomy where

import Codec.Archive.Tar qualified as Tar
import Codec.Compression.GZip qualified as GZ

import Control.Monad (when)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (runExceptT, ExceptT(ExceptT))

import Data.ByteString.Lazy qualified as LBS
import Data.Foldable
import Data.Graph.Inductive qualified as FGL
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.IntMap.Strict (IntMap)
import Data.IntMap.Strict qualified as IntMap
import Data.Maybe (fromMaybe)
import Data.Semigroup (Any (getAny))
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Vinyl qualified as V

import Frames (Rec, FrameRec, ColumnHeaders, Record)
import Frames.CSV (ReadRec)
import Frames.InCore (RecVec)

import Streaming (Stream, Of)
import Streaming.Prelude qualified as S

import System.Directory qualified as Dir
import System.FilePath ((</>))

import VPF.DataSource.GenericFTP
import VPF.DataSource.NCBI
import VPF.Formats
import VPF.Frames.DSV qualified as DSV
import VPF.Frames.Dplyr.Row qualified as F
import VPF.Frames.Types (FieldSubset, FieldsOf)


taxonomySourceConfig :: FtpSourceConfig
taxonomySourceConfig =
    (ncbiSourceConfig downloadList) { ftpBasePath = "/pub/taxonomy/" }
  where
    downloadList = DownloadList \_ ->
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


data TaxonomyGraph = TaxonomyGraph
    { taxonomyRanks :: IntMap Text
    , taxonomyGr    :: FGL.UGr
    , taxonomyRoot  :: FGL.Node
    }


buildTaxonomyGraph ::
    FrameRec (FieldsOf TaxonomyNodesCols '["tax_id", "parent tax_id", "rank"])
    -> TaxonomyGraph
buildTaxonomyGraph df =
    let lnodes = IntMap.toList $ IntMap.fromList (concatMap edgeLEndpoints ledges)
        ledges = toList (fmap recLEdge df)
        g = FGL.mkGraph lnodes (filter (not . isLoop) ledges)

        ranks = IntMap.fromList $ toList (fmap recRankAssoc df)
    in
        TaxonomyGraph
            { taxonomyRanks = ranks
            , taxonomyGr = g
            , taxonomyRoot =
                case FGL.match 1 g of
                    (Just ([], _, (), _:_), _) -> 1
                    _                          -> error "buildTaxonomyGraph: root is not 1!"
            }
  where
    isLoop :: FGL.UEdge -> Bool
    isLoop (u, v, _) = u == v

    recLEdge :: Record (FieldsOf TaxonomyNodesCols '["tax_id", "parent tax_id", "rank"]) -> FGL.UEdge
    recLEdge (V.Field taxid V.:& V.Field ptaxid V.:& _) = (ptaxid, taxid, ())

    edgeLEndpoints :: FGL.UEdge -> [FGL.UNode]
    edgeLEndpoints (u, v, _) = [(u, ()), (v, ())]

    recRankAssoc :: Record (FieldsOf TaxonomyNodesCols '["tax_id", "parent tax_id", "rank"]) -> (Int, Text)
    recRankAssoc (V.Field taxid V.:& _ V.:& V.Field rank V.:& V.RNil) = (taxid, rank)


checkTaxonomyTree :: TaxonomyGraph -> Bool
checkTaxonomyTree (TaxonomyGraph _ g _) =
    FGL.isConnected g && FGL.noNodes g == length (FGL.labEdges g) + 1


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


data TaxNameClass = TaxNameClass { uniqueNameInClass :: !Text, taxNameClass :: !Text }


buildTaxIdToNameMapWith :: forall a.
    (Text -> TaxNameClass -> a)
    -> FrameRec TaxonomyNamesCols
    -> IntMap [a]
buildTaxIdToNameMapWith p = foldr f mempty  -- foldr to preserve order with (:)
  where
    f :: Record TaxonomyNamesCols -> IntMap [a] -> IntMap [a]
    f (V.Field taxid V.:& V.Field name V.:& V.Field uniqueName V.:& V.Field nameClass V.:& V.RNil) =
        flip IntMap.alter taxid \case
            Nothing -> Just [p name (TaxNameClass uniqueName nameClass)]
            Just ns -> Just (p name (TaxNameClass uniqueName nameClass) : ns)


buildNameToTaxIdMapWith :: forall a.
    (Int -> TaxNameClass -> a)
    -> FrameRec TaxonomyNamesCols
    -> HashMap Text [a]
buildNameToTaxIdMapWith p = foldr f mempty  -- foldr to preserve order with (:)
  where
    f :: Record TaxonomyNamesCols -> HashMap Text [a] -> HashMap Text [a]
    f (V.Field taxid V.:& V.Field name V.:& V.Field uniqueName V.:& V.Field nameClass V.:& V.RNil) =
        flip HashMap.alter name \case
            Nothing -> Just [p taxid (TaxNameClass uniqueName nameClass)]
            Just ns -> Just (p taxid (TaxNameClass uniqueName nameClass) : ns)


type TaxonomyMergedIdsCols =
    '[ '("old_tax_id", Int)  -- id of nodes that have been merged
    ,  '("new_tax_id", Int)  -- id of nodes which is the result of merging
    ]


loadTaxonomyMergedIds ::
    Path Directory
    -> IO (Either DSV.ParseError (FrameRec TaxonomyMergedIdsCols))
loadTaxonomyMergedIds downloadDir =
    loadTaxonomyDmpFileWith @TaxonomyMergedIdsCols id
        (extractedDmpPath downloadDir "merged")


buildMergedIdsMap :: FrameRec TaxonomyMergedIdsCols -> IntMap Int
buildMergedIdsMap df =
    IntMap.fromList [(old, new) | V.Field old V.:& V.Field new V.:& _ <- toList df]


data Taxonomy = Taxonomy
    { taxonomyGraph          :: TaxonomyGraph
    , taxonomyTaxIdToNameMap :: IntMap [(Text, TaxNameClass)]
    , taxonomyNameToTaxIdMap :: HashMap Text [(Int, TaxNameClass)]
    , taxonomyMergedIds      :: IntMap Int
    }


loadFullTaxonomy :: Path Directory -> IO (Either DSV.ParseError Taxonomy)
loadFullTaxonomy downloadDir = runExceptT do
    graph <- ExceptT $ loadTaxonomyNodesWith id downloadDir
    names <- ExceptT $ loadTaxonomyNamesWith id downloadDir
    merged <- ExceptT $ loadTaxonomyMergedIds downloadDir

    return Taxonomy
        { taxonomyGraph          = buildTaxonomyGraph graph
        , taxonomyTaxIdToNameMap = buildTaxIdToNameMapWith (,) names
        , taxonomyNameToTaxIdMap = buildNameToTaxIdMapWith (,) names
        , taxonomyMergedIds      = buildMergedIdsMap merged
        }


taxIdLineage :: Taxonomy -> Int -> [Int]
taxIdLineage taxdb taxid =
    case FGL.rdfs [taxid] (taxonomyGr (taxonomyGraph taxdb)) of
        h:t | h == taxonomyRoot (taxonomyGraph taxdb) -> t
        l                                             -> l


taxIdRank :: Taxonomy -> Int -> Maybe Text
taxIdRank taxdb taxid = IntMap.lookup taxid (taxonomyRanks (taxonomyGraph taxdb))


findTaxIdsForName :: Taxonomy -> Bool -> Text -> [(Int, TaxNameClass)]
findTaxIdsForName taxdb includeOld name
    | includeOld = res
    | otherwise  = [tup | tup@(taxid, _) <- res, not (IntMap.member taxid (taxonomyMergedIds taxdb))]
  where
    res :: [(Int, TaxNameClass)]
    res = fromMaybe [] $ HashMap.lookup name (taxonomyNameToTaxIdMap taxdb)


findNamesForTaxId :: Taxonomy -> Int -> [(Text, TaxNameClass)]
findNamesForTaxId taxdb taxid =
    fromMaybe [] (IntMap.lookup taxid (taxonomyTaxIdToNameMap taxdb))
