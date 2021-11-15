{-# language DeriveFunctor #-}
{-# language GeneralizedNewtypeDeriving #-}
{-# language OverloadedLists #-}
{-# language StrictData #-}
{-# language TemplateHaskell #-}
{-# language UndecidableInstances #-}
{-# language ViewPatterns #-}
module VPF.DataSource.NCBI.Taxonomy
  ( TaxonomySourceConfig
  , syncTaxonomy
  , loadTaxonomyDmpFileWith
  , streamTaxonomyDmpFile
  , TaxonomyNodesCols
  , taxonomyNodesPath
  , TaxonomyNamesCols
  , taxonomyNamesPath
  , TaxonomyMergedIdsCols
  , taxonomyMergedIdsPath
  , Taxonomy(..)
  , TaxNameClass(..)
  , ListWithTaxNameClass
  , onlyScientificNames
  , listWithTaxNameClass
  , loadFullTaxonomy
  , taxIdVertex
  , taxIdLineage
  , taxIdRank
  , findTaxIdsForName
  , findNamesForTaxId
  ) where

import Codec.Archive.Tar qualified as Tar
import Codec.Compression.GZip qualified as GZ

import Control.Applicative (liftA2)
import Control.Foldl qualified as L
import Control.Lens (Fold)
import Control.Monad (when)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Except (runExceptT, ExceptT(ExceptT))
import Control.Monad.Trans.Resource qualified as ResourceT

import Data.ByteString.Lazy qualified as LBS
import Data.Function ((&))
import Data.Functor.Contravariant (phantom)
import Data.Graph.Haggle qualified as G
import Data.Graph.Haggle.Algorithms.DFS qualified as G
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.IntMap.Strict (IntMap)
import Data.IntMap.Strict qualified as IntMap
import Data.Maybe (fromMaybe)
import Data.Semigroup (Any (getAny))
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Vector.Unboxed qualified as VU
import Data.Vector.Unboxed.Deriving qualified as VU (derivingUnbox)
import Data.Vinyl qualified as V

import Frames (Rec, FrameRec, ColumnHeaders, Record)
import Frames.CSV (ReadRec)
import Frames.InCore (RecVec)

import GHC.Exts (IsList(..))

import Streaming (Stream, Of)
import Streaming.Prelude qualified as S

import System.Directory qualified as Dir
import System.FilePath ((</>))
import System.IO qualified as IO

import Unsafe.Coerce (unsafeCoerce)

import VPF.DataSource.GenericFTP
import VPF.DataSource.NCBI
import VPF.Formats
import VPF.Frames.DSV qualified as DSV
import VPF.Frames.Types (FieldSubset, FieldsOf)
import VPF.Util.Foldl qualified as L
import VPF.Util.FS qualified as FS
import VPF.Util.Streaming qualified as S


$(VU.derivingUnbox "Vertex"
    [t| G.Vertex -> Int |]
    [| unsafeCoerce |]
    [| unsafeCoerce |])


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


dmpParserOpts :: DSV.ParserOptions
dmpParserOpts =
    (DSV.defParserOptions '|')
        { DSV.hasHeader = False
        , DSV.rowTokenizer =
            -- no quoting in this format it seems
            Text.splitOn fieldSep . fromMaybe err . Text.stripSuffix rowSuffix
        }
  where
    err :: a
    err = error "Bad trailing delimiter in .dmp file"

    rowSuffix = Text.pack "\t|"
    fieldSep = Text.pack "\t|\t"


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
    DSV.readSubframeWith dmpParserOpts f dmpPath


streamTaxonomyDmpFile ::
    ( ColumnHeaders allCols
    , FieldSubset Rec cols allCols
    , ReadRec allCols
    , ResourceT.MonadResource m
    )
    => Path (DSV "|" allCols)
    -> Stream (Of (Record cols)) m (Either DSV.ParseError ())
streamTaxonomyDmpFile dmpPath = do
    (releaseKey, h) <- lift $ ResourceT.liftResourceT $
        ResourceT.allocate (IO.openFile (untag dmpPath) IO.ReadMode) IO.hClose

    r <- FS.hStreamTextLines h
        & DSV.parsedRowStream dmpParserOpts dmpPath
        & S.map V.rcast

    lift $ ResourceT.liftResourceT $
        ResourceT.release releaseKey

    return r


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


taxonomyNodesPath :: Path Directory -> Path (DSV "|" TaxonomyNodesCols)
taxonomyNodesPath downloadDir = extractedDmpPath downloadDir "nodes"


data TaxonomyGraph = TaxonomyGraph
    }


ranksMapFold :: forall cols.
    cols ~ FieldsOf TaxonomyNodesCols '["tax_id", "parent tax_id", "rank"]
    => L.Fold (Record cols) (IntMap Text)
ranksMapFold = L.premap recAssoc L.intMap
  where
    recRankAssoc :: Record cols -> (Int, Text)
    recRankAssoc (V.Field taxid V.:& _ V.:& V.Field rank V.:& V.RNil) = (taxid, rank)


data TaxonomyMGraph m = TaxonomyMGraph
    (IntMap G.Vertex)
    (G.VertexLabeledMGraph (G.MSimpleBiDigraph m) Int m)


data TaxonomyGraph = TaxonomyGraph
    (IntMap G.Vertex)
    (G.VertexLabeledGraph G.SimpleBiDigraph Int


graphFold :: forall cols s.
    cols ~ FieldsOf TaxonomyNodesCols '["tax_id", "parent tax_id", "rank"]
    => L.FoldM (ST s) (Record cols) TaxonomyGraph
graphFold =
    L.FoldM (G.newVertexLabeledGraph G.newSimpleBiDigraph) addEdge return
  where
    addEdge ::
        TaxonomyMGraph (ST s)
        -> Record cols
        -> ST s (TaxonomyMGraph (ST s))
    addEdge (TaxonomyMGraph vm g) rec = do
        let (src, dst) = recEdge rec

        srcv <- G.addLabeledVertex g src
        dstv <- case IntMap.lookup dst vm of
            Just v -> return v
            Nothing -> G.addLabeledVertex g dst

        G.addEdge srcv

        toTaxonomyGraph :: IntMap Text -> FGL.UGr -> TaxonomyGraph
        toTaxonomyGraph ranks gr = TaxonomyGraph
            { taxonomyRanks = ranks
            , taxonomyGr    = gr
            , taxonomyRoot  = assertedRoot gr
            }
    in
        liftA2 toTaxonomyGraph ranksFold ugrFold
  where
    isLoop
    isLoop :: (Int, Int) -> Bool
    isLoop (u, v) = u == v

    recEdge :: Record cols -> (Int, Int)
    recEdge (V.Field taxid V.:& V.Field ptaxid V.:& _) = (ptaxid, taxid)


findLabeledRoot ::
    G.VertexLabeledGraph G.SimpleBiDigraph Int
    -> VU.Vector G.Vertex
    -> Maybe (Int, G.Vertex)
findLabeledRoot g vs =
    case FGL.match 1 gr of
        (Just ([], _, (), _:_), _) -> Just undefined
        _                          -> Nothing


type TaxonomyNamesCols =
    '[ '("tax_id",      Int)   -- the id of node associated with this name
    ,  '("name_txt",    Text)  -- name itself
    ,  '("unique name", Text)  -- the unique variant of this name if name not unique
    ,  '("name class",  Text)  -- (synonym, common name, ...)
    ]


taxonomyNamesPath :: Path Directory -> Path (DSV "|" TaxonomyNamesCols)
taxonomyNamesPath downloadDir = extractedDmpPath downloadDir "names"


data TaxNameClass = TaxNameClass
    { uniqueNameInClass :: {-# unpack #-} Text
    , taxNameClass      :: {-# unpack #-} Text
    }
    deriving (Eq, Ord, Show)


taxIdToNameMapFold :: L.Fold (Text, TaxNameClass) a -> L.Fold (Record TaxonomyNamesCols) (IntMap a)
taxIdToNameMapFold = L.premap recAssoc . L.foldByKeyIntMap
  where
    recAssoc :: Record TaxonomyNamesCols -> (Int, (Text, TaxNameClass))
    recAssoc (V.Field taxid V.:& V.Field name V.:& V.Field uniqueName V.:& V.Field nameClass V.:& V.RNil) =
        (taxid, (name, ) $! TaxNameClass uniqueName nameClass)


nameToTaxIdMapFold :: L.Fold (Int, TaxNameClass) a -> L.Fold (Record TaxonomyNamesCols) (HashMap Text a)
nameToTaxIdMapFold = L.premap recAssoc . L.foldByKeyHashMap
  where
    recAssoc :: Record TaxonomyNamesCols -> (Text, (Int, TaxNameClass))
    recAssoc (V.Field taxid V.:& V.Field name V.:& V.Field uniqueName V.:& V.Field nameClass V.:& V.RNil) =
        (name, (taxid, ) $! TaxNameClass uniqueName nameClass)


type TaxonomyMergedIdsCols =
    '[ '("old_tax_id", Int)  -- id of nodes that have been merged
    ,  '("new_tax_id", Int)  -- id of nodes which is the result of merging
    ]


taxonomyMergedIdsPath :: Path Directory -> Path (DSV "|" TaxonomyMergedIdsCols)
taxonomyMergedIdsPath downloadDir = extractedDmpPath downloadDir "merged"


mergedIdsToVertexMapFold ::
    IntMap G.Vertex
    -> L.Fold (Record TaxonomyMergedIdsCols) (IntMap G.Vertex)
mergedIdsToVertexMapFold init = L.Fold step init id
  where
    step :: IntMap G.Vertex -> Record (Int, Int) -> IntMap G.Vertex
    step m (V.Field old V.:& V.Field new V.:& V.RNil) =
        case IntMap.lookup new m of
            Nothing -> m
            Just v  -> IntMap.insert old v m


mergedIdsMapFold :: L.Fold (Record TaxonomyMergedIdsCols) (IntMap Int)
mergedIdsMapFold = L.premap recAssoc L.intMap
  where
    recAssoc :: Record TaxonomyMergedIdsCols -> (Int, Int)
    recAssoc (V.Field old V.:& V.Field new V.:& V.RNil) = (old, new)



-- In-memory Taxonomy DB

data Taxonomy f = Taxonomy
    { taxonomyGraph          :: G.VertexLabeledMGraph G.MSimpleBiDigraph Int
    , taxonomyTaxidToVertex  :: IntMap G.Vertex
    , taxonomyRoot           :: (Int, G.Vertex)
    , taxonomyRanks          :: IntMap Text
    , taxonomyTaxIdToNameMap :: IntMap (f Text)
    , taxonomyNameToTaxIdMap :: HashMap Text (f Int)
    , taxonomyMergedIds      :: IntMap Int
    }


newtype ListWithTaxNameClass a = ListWithTaxNameClass [(a, TaxNameClass)]
    deriving newtype (Eq, Ord, Show, Semigroup, Monoid)
    deriving stock Functor


instance IsList (ListWithTaxNameClass a) where
    type Item (ListWithTaxNameClass a) = (a, TaxNameClass)
    fromList = ListWithTaxNameClass
    toList (ListWithTaxNameClass xs) = xs


onlyScientificNames :: L.Fold (a, TaxNameClass) r -> L.Fold (a, TaxNameClass) r
onlyScientificNames =
    L.prefilter \(_, TaxNameClass _ cls) -> cls == Text.pack "scientific name"


listWithTaxNameClass :: L.Fold (a, TaxNameClass) (ListWithTaxNameClass a)
listWithTaxNameClass = ListWithTaxNameClass <$> L.list


loadFullTaxonomy ::
    (forall a. L.Fold (a, TaxNameClass) (f a))
    -> Path Directory
    -> IO (Either DSV.ParseError (Taxonomy f))
loadFullTaxonomy fold downloadDir = ResourceT.runResourceT $ runExceptT do
    !graph <- ExceptT $
        streamTaxonomyDmpFile (taxonomyNodesPath downloadDir)
            & L.purely S.fold graphFold
            & fmap S.wrapEitherOf

    (!taxid2name, !name2taxid) <- ExceptT $
        streamTaxonomyDmpFile (taxonomyNamesPath downloadDir)
            & L.purely S.fold (liftA2 (,) (taxIdToNameMapFold fold) (nameToTaxIdMapFold fold))
            & fmap S.wrapEitherOf

    !merged <- ExceptT $
        streamTaxonomyDmpFile (taxonomyMergedIdsPath downloadDir)
            & L.purely S.fold mergedIdsMapFold
            & fmap S.wrapEitherOf

    return Taxonomy
        { taxonomyGraph          = graph
        , taxonomyTaxIdToNameMap = taxid2name
        , taxonomyNameToTaxIdMap = name2taxid
        , taxonomyMergedIds      = merged
        }


-- Taxonomy Queries

checkTaxonomyTree :: Taxonomy f -> Bool
checkTaxonomyTree taxdb =
    G.isConnected g && G.numNodes g == G.numEdges g + 1
  where
    g = taxonomyGraph taxdb


taxIdVertex :: Taxonomy f -> Maybe G.Vertex
taxidVertex taxdb taxid =
    IntMap.lookup taxid (taxonomyTaxidToVertexMap tg)


taxIdLineage :: Taxonomy f -> Int -> [Int]
taxIdLineage taxdb taxid =
    case G.dfs [taxid] g of
        h:t | h == root -> t
        l               -> l
  where
    g = taxonomyGr (taxonomyGraph taxdb)
    root = taxonomyRoot (taxonomyGraph taxdb)


taxIdRank :: Taxonomy f -> Int -> Maybe Text
taxIdRank taxdb taxid =
    case IntMap.lookup taxid ranks of
        Nothing
            | Just newTaxid <- IntMap.lookup taxid (taxonomyMergedIds taxdb)
            -> IntMap.lookup newTaxid ranks
        r -> r
  where
    ranks = taxonomyRanks (taxonomyGraph taxdb)


findTaxIdsForName :: Taxonomy ListWithTaxNameClass -> Bool -> Text -> ListWithTaxNameClass Int
findTaxIdsForName taxdb includeOld name
    | includeOld = ListWithTaxNameClass res
    | otherwise  = ListWithTaxNameClass [tup | tup@(taxid, _) <- res, not (IntMap.member taxid (taxonomyMergedIds taxdb))]
  where
    res :: [(Int, TaxNameClass)]
    res = toList $ fromMaybe mempty $ HashMap.lookup name (taxonomyNameToTaxIdMap taxdb)


findNamesForTaxId :: Monoid (f Text) => Taxonomy f -> Int -> f Text
findNamesForTaxId taxdb taxid =
    fromMaybe mempty (IntMap.lookup taxid (taxonomyTaxIdToNameMap taxdb))
