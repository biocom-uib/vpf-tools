{-# language DeriveFunctor #-}
{-# language DeriveFoldable #-}
{-# language DeriveTraversable #-}
{-# language GeneralizedNewtypeDeriving #-}
{-# language OverloadedLists #-}
{-# language StrictData #-}
{-# language TemplateHaskell #-}
{-# language UndecidableInstances #-}
{-# language ViewPatterns #-}
module VPF.DataSource.NCBI.Taxonomy
  ( TaxonomyDownloadList(..)
  , taxonomySourceConfig
  , buildDownloadList
  , syncTaxonomy
  , loadTaxonomyDmpFileWith
  , streamTaxonomyDmpFile
  , TaxonomyNodesCols
  , taxonomyNodesPath
  , TaxonomyGraph(..)
  , TaxonomyNamesCols
  , taxonomyNamesPath
  , TaxonomyMergedIdsCols
  , taxonomyMergedIdsPath
  -- loading
  , Taxonomy(..)
  , TaxNameClass(..)
  , ListWithTaxNameClass
  , onlyScientificNames
  , listWithTaxNameClass
  , loadTaxonomyDB
  , streamProtAccessionToTaxId
  -- queries
  , checkTaxonomyTree
  , taxIdToVertex
  , taxIdLineage
  , taxIdRank
  , findTaxIdsForName
  , findNamesForTaxId
  , withoutTaxNameClass
  ) where

import Codec.Archive.Tar qualified as Tar
import Codec.Compression.GZip qualified as GZ

import Control.Applicative (liftA2)
import Control.Foldl qualified as L
import Control.Lens (_1)
import Control.Monad (when)
import Control.Monad.IO.Class (liftIO)
import Control.Monad.Ref qualified as Ref
import Control.Monad.ST.Strict (ST, stToIO)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Except (runExceptT, ExceptT(ExceptT))
import Control.Monad.Trans.Resource qualified as ResourceT

import Data.ByteString.Char8 qualified as BS8
import Data.ByteString.Lazy qualified as LBS
import Data.Function ((&))
import Data.Functor (($>))
import Data.Functor.Plus (Plus, Alt, (<!>))
import Data.Functor.Plus qualified as Plus
import Data.Graph.Haggle qualified as G
import Data.Graph.Haggle.Algorithms.DFS qualified as G
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.IntMap.Strict (IntMap)
import Data.IntMap.Strict qualified as IntMap
import Data.Maybe (fromMaybe, mapMaybe)
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text
import Data.Vinyl qualified as V

import Frames (Rec, FrameRec, ColumnHeaders, Record)
import Frames.CSV (ReadRec)
import Frames.InCore (RecVec)

import GHC.Exts as GHC (IsList(..))

import Network.FTP.Client qualified as FTP

import Streaming (Stream, Of)
import Streaming.ByteString as BSS
import Streaming.ByteString.Char8 qualified as BSS8
import Streaming.Prelude qualified as S
import Streaming.Zip qualified as SZ

import System.Directory qualified as Dir
import System.FilePath ((</>), takeFileName)
import System.FilePath.Posix qualified as Posix
import System.IO qualified as IO

import Witherable (Filterable)
import Witherable qualified as Witherable

import VPF.DataSource.GenericFTP
import VPF.DataSource.NCBI
import VPF.Formats
import VPF.Frames.DSV qualified as DSV
import VPF.Frames.Types (FieldSubset, FieldsOf)
import VPF.Util.Foldl qualified as L
import VPF.Util.FS qualified as FS
import VPF.Util.Streaming qualified as S


data TaxonomyDownloadList = TaxonomyDownloadList
    { taxonomyIncludeProt2Taxid :: Bool
    }


taxonomySourceConfig :: TaxonomyDownloadList -> FtpSourceConfig
taxonomySourceConfig = ncbiSourceConfig . buildDownloadList


basePath :: FtpRelPath
basePath = FtpRelPath "pub/taxonomy"


localBasePath :: FilePath
localBasePath = toLocalRelPath basePath


taxonomyExtractDirName :: String
taxonomyExtractDirName = "taxdump"


extractedDmpPath :: Path Directory -> String -> Path (DSV "|" cols)
extractedDmpPath (untag -> downloadDir) name =
    Tagged (downloadDir </> localBasePath </> taxonomyExtractDirName </> (name ++ ".dmp"))


buildDownloadList :: TaxonomyDownloadList -> DownloadList
buildDownloadList cfg = DownloadList \h -> runExceptT $ S.toList_ do
    taxdumpMd5 <- liftIO $
        parseChecksumsFor (BS8.pack "taxdump.tar.gz") MD5 <$>
            FTP.retr h (rel "taxdump.tar.gz.md5")

    S.yield (FtpRelPath (rel "taxdump.tar.gz"), taxdumpMd5)

    when (taxonomyIncludeProt2Taxid cfg) do
        md5 <- liftIO $
            parseChecksumsFor (BS8.pack "prot.accession2taxid.gz") MD5 <$>
                FTP.retr h (rel "accession2taxid/prot.accession2taxid.gz.md5")

        S.yield (FtpRelPath (rel "accession2taxid/prot.accession2taxid.gz"), md5)
  where
    rel = (ftpRelPath basePath Posix.</>)


syncTaxonomy :: TaxonomyDownloadList -> LogAction String -> Path Directory -> IO (Either String [ChangelogEntry])
syncTaxonomy cfg log (untag -> downloadDir) = runExceptT do
    changes <- ExceptT $ syncGenericFTP (taxonomySourceConfig cfg) log (Tagged downloadDir)

    liftIO do
        let taxdumpChanged = any ((== "taxdump.tar.gz") . takeFileName . changelogEntryPath) changes
            destDir = downloadDir </> localBasePath </> taxonomyExtractDirName

        when taxdumpChanged do
            log $ "Taxonomy files changed, replacing " ++ destDir

            destExists <- Dir.doesDirectoryExist destDir
            when destExists $
                Dir.removeDirectoryRecursive destDir

            compressedLBS <- LBS.readFile (downloadDir </> localBasePath </> "taxdump.tar.gz")
            Tar.unpack destDir (Tar.read (GZ.decompress compressedLBS))

    return changes


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


ranksMapFold :: forall cols.
    cols ~ FieldsOf TaxonomyNodesCols '["tax_id", "parent tax_id", "rank"]
    => L.Fold (Record cols) (IntMap Text)
ranksMapFold = L.premap recAssoc L.intMap
  where
    recAssoc :: Record cols -> (Int, Text)
    recAssoc (V.Field taxid V.:& _ V.:& V.Field rank V.:& V.RNil) = (taxid, rank)


data TaxonomyMGraph m = TaxonomyMGraph
    (Ref.Ref m (IntMap G.Vertex))
    (G.VertexLabeledMGraph G.MSimpleBiDigraph Int m)


data TaxonomyGraph = TaxonomyGraph
    { taxonomyGraphVertexMap   :: IntMap G.Vertex
    , taxonomyGraphGraph       :: G.VertexLabeledGraph G.SimpleBiDigraph Int
    , taxonomyGraphVertexCount :: Int
    , taxonomyGraphEdgeCount   :: Int
    }


graphFold :: forall cols s.
    cols ~ FieldsOf TaxonomyNodesCols '["tax_id", "parent tax_id", "rank"]
    => L.FoldM (ST s) (Record cols) TaxonomyGraph
graphFold =
    L.FoldM (\tg rec -> stepAddEdge tg rec $> tg) init freeze
  where
    init :: ST s (TaxonomyMGraph (ST s))
    init = do
        vm <- Ref.newRef mempty
        g <- G.newVertexLabeledGraph G.newMSimpleBiDigraph
        return (TaxonomyMGraph vm g)

    stepAddEdge :: TaxonomyMGraph (ST s) -> Record cols -> ST s ()
    stepAddEdge (TaxonomyMGraph vmRef g) rec = do
        let (src, dst) = recEdge rec

        srcv <- addVertex src

        when (not (isLoop (src, dst))) do
            dstv <- addVertexIfMissing dst
            _ <- G.addEdge g srcv dstv
            return ()
      where
        addVertexIfMissing :: Int -> ST s G.Vertex
        addVertexIfMissing taxid = do
            vm <- Ref.readRef vmRef
            case IntMap.lookup taxid vm of
                Just vid -> return vid
                Nothing -> addVertex taxid

        addVertex :: Int -> ST s G.Vertex
        addVertex taxid = do
            vid <- G.addLabeledVertex g taxid
            Ref.modifyRef' vmRef (IntMap.insert taxid vid)
            return vid

        isLoop :: (Int, Int) -> Bool
        isLoop (u, v) = u == v

        recEdge :: Record cols -> (Int, Int)
        recEdge (V.Field taxid V.:& V.Field ptaxid V.:& _) = (ptaxid, taxid)

    freeze :: TaxonomyMGraph (ST s) -> ST s TaxonomyGraph
    freeze (TaxonomyMGraph vmRef g) = do
        vc <- G.countVertices g
        ec <- G.countEdges g
        vm <- Ref.readRef vmRef
        immG <- G.freeze g
        return (TaxonomyGraph vm immG vc ec)


findTaxonomyRoot :: TaxonomyGraph -> Maybe (Int, G.Vertex)
findTaxonomyRoot (TaxonomyGraph vm g _ _)
    | Just vid <- IntMap.lookup 1 vm
    , [] <- G.predecessors g vid
    , _:_ <- G.successors g vid
    = Just (1, vid)

    | otherwise = Nothing


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


mergedIdsMapFold :: L.Fold (Record TaxonomyMergedIdsCols) (IntMap Int)
mergedIdsMapFold = L.premap recAssoc L.intMap
  where
    recAssoc :: Record TaxonomyMergedIdsCols -> (Int, Int)
    recAssoc (V.Field old V.:& V.Field new V.:& V.RNil) = (old, new)



-- In-memory Taxonomy DB

data Taxonomy f = Taxonomy
    { taxonomyGraph            :: TaxonomyGraph
    , taxonomyRoot             :: ~(Int, G.Vertex)
    , taxonomyRanksMap         :: IntMap Text
    , taxonomyTaxIdToNameMap   :: IntMap (f Text)
    , taxonomyNameToTaxIdMap   :: HashMap Text (f Int)
    , taxonomyMergedIdsMap     :: IntMap Int
    }


newtype ListWithTaxNameClass a = ListWithTaxNameClass [(a, TaxNameClass)]
    deriving newtype (Eq, Ord, Show, Semigroup, Monoid)
    deriving stock (Functor, Foldable, Traversable)


instance IsList (ListWithTaxNameClass a) where
    type Item (ListWithTaxNameClass a) = (a, TaxNameClass)
    fromList = ListWithTaxNameClass
    toList (ListWithTaxNameClass xs) = xs

instance Alt ListWithTaxNameClass where
    l1 <!> l2 = GHC.fromList $ GHC.toList l1 <!> GHC.toList l2

instance Plus ListWithTaxNameClass where
    zero = mempty

instance Filterable ListWithTaxNameClass where
    mapMaybe f = GHC.fromList . mapMaybe (_1 f) . GHC.toList


onlyScientificNames :: L.Fold (a, TaxNameClass) r -> L.Fold (a, TaxNameClass) r
onlyScientificNames =
    L.prefilter \(_, TaxNameClass _ cls) -> cls == Text.pack "scientific name"


listWithTaxNameClass :: L.Fold (a, TaxNameClass) (ListWithTaxNameClass a)
listWithTaxNameClass = ListWithTaxNameClass <$> L.list


withoutTaxNameClass :: L.Fold a b -> L.Fold (a, TaxNameClass) b
withoutTaxNameClass = L.premap fst


loadTaxonomyDB ::
    Path Directory
    -> (forall a. L.Fold (a, TaxNameClass) (f a))
    -> IO (Either DSV.ParseError (Taxonomy f))
loadTaxonomyDB downloadDir fold = ResourceT.runResourceT $ runExceptT do
    let graphFoldIO = L.hoists (lift . stToIO) graphFold
        ranksFoldIO = L.generalize ranksMapFold

    (!graph, !ranks) <- ExceptT $
        streamTaxonomyDmpFile (taxonomyNodesPath downloadDir)
            & L.impurely S.foldM (liftA2 (,) graphFoldIO ranksFoldIO)
            & fmap S.wrapEitherOf

    liftIO $ IO.hPutStrLn IO.stderr "loaded graph"

    !merged <- ExceptT $
        streamTaxonomyDmpFile (taxonomyMergedIdsPath downloadDir)
            & L.purely S.fold mergedIdsMapFold
            & fmap S.wrapEitherOf

    liftIO $ IO.hPutStrLn IO.stderr "loaded merged"

    (!taxid2name, !name2taxid) <- ExceptT $
        streamTaxonomyDmpFile (taxonomyNamesPath downloadDir)
            & L.purely S.fold (liftA2 (,) (taxIdToNameMapFold fold) (nameToTaxIdMapFold fold))
            & fmap S.wrapEitherOf

    liftIO $ IO.hPutStrLn IO.stderr "loaded names"

    return Taxonomy
        { taxonomyGraph          = graph
        , taxonomyRoot           = fromMaybe rootNotFound (findTaxonomyRoot graph)
        , taxonomyRanksMap       = ranks
        , taxonomyTaxIdToNameMap = taxid2name
        , taxonomyNameToTaxIdMap = name2taxid
        , taxonomyMergedIdsMap   = merged
        }
  where
    rootNotFound = error "loadTaxonomyDB: taxonomy root is not 1"


type ProtAccessionToTaxId =
    '[ '("accession",         Text)
    ,  '("accession.version", Text)
    ,  '("taxid",             Int)
    ,  '("gi",                Int)
    ]


protAccessionToTaxIdPath :: Path (GZip (TSV ProtAccessionToTaxId))
protAccessionToTaxIdPath = Tagged (localBasePath </> "prot.accession2taxid.gz")


streamProtAccessionToTaxId ::
    ResourceT.MonadResource m
    => Path Directory
    -> Stream (Of (Record ProtAccessionToTaxId)) m (Either DSV.ParseError ())
streamProtAccessionToTaxId downloadDir = do
    (releaseKey, h) <- lift $ ResourceT.liftResourceT $
        ResourceT.allocate (IO.openBinaryFile (untag mappingPath) IO.ReadMode) IO.hClose

    r <- BSS.fromHandle h
        & SZ.gunzip
        & BSS8.lines
        & S.mapped BSS.toStrict
        & S.map Text.decodeUtf8
        & DSV.parsedRowStream (DSV.defParserOptions '\t') mappingPath

    lift $ ResourceT.liftResourceT $
        ResourceT.release releaseKey

    return r
  where
    mappingPath :: Path (TSV ProtAccessionToTaxId)
    mappingPath = Tagged (untag downloadDir </> untag protAccessionToTaxIdPath)



-- Taxonomy Queries

checkTaxonomyTree :: Taxonomy f -> Bool
checkTaxonomyTree taxdb =
    G.isConnected g && taxonomyGraphVertexCount tg == taxonomyGraphEdgeCount tg + 1
  where
    tg = taxonomyGraph taxdb
    g = taxonomyGraphGraph tg


lookupTaxId :: Taxonomy f -> Int -> (Int -> Maybe a) -> Maybe a
lookupTaxId taxdb taxid lookup =
    case lookup taxid of
        Nothing
          | Just taxid' <- IntMap.lookup taxid (taxonomyMergedIdsMap taxdb)
          -> lookupTaxId taxdb taxid' lookup

        r -> r


taxIdToVertex :: Taxonomy f -> Int -> Maybe G.Vertex
taxIdToVertex taxdb taxid =
    lookupTaxId taxdb taxid (`IntMap.lookup` taxonomyGraphVertexMap (taxonomyGraph taxdb))


taxIdLineage :: Taxonomy f -> Int -> [Int]
taxIdLineage taxdb taxid =
    case taxIdToVertex taxdb taxid of
        Nothing -> []
        Just vid ->
            case G.dfs g [vid] of
                h:t | h == root -> mapMaybe toTaxId t
                l               -> mapMaybe toTaxId l
  where
    g = taxonomyGraphGraph (taxonomyGraph taxdb)

    root :: G.Vertex
    (_, root) = taxonomyRoot taxdb

    toTaxId :: G.Vertex -> Maybe Int
    toTaxId = G.vertexLabel g


taxIdRank :: Taxonomy f -> Int -> Maybe Text
taxIdRank taxdb taxid =
    lookupTaxId taxdb taxid (`IntMap.lookup` taxonomyRanksMap taxdb)


findTaxIdsForName :: forall f. (Plus f, Filterable f) => Taxonomy f -> Bool -> Text -> f Int
findTaxIdsForName taxdb includeOld name
    | includeOld = res
    | otherwise  = Witherable.filter (\taxid -> not $ IntMap.member taxid mergedIds) res
  where
    mergedIds = taxonomyMergedIdsMap taxdb

    res :: f Int
    res = fromMaybe Plus.zero $ HashMap.lookup name (taxonomyNameToTaxIdMap taxdb)


findNamesForTaxId :: Monoid (f Text) => Taxonomy f -> Int -> f Text
findNamesForTaxId taxdb taxid =
    fromMaybe mempty (IntMap.lookup taxid (taxonomyTaxIdToNameMap taxdb))
