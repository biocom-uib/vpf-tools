{-# language DeriveFunctor #-}
{-# language GeneralizedNewtypeDeriving #-}
{-# language OverloadedLists #-}
{-# language StrictData #-}
{-# language TemplateHaskell #-}
{-# language UndecidableInstances #-}
{-# language ViewPatterns #-}
module VPF.DataSource.NCBI.Taxonomy where

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

import GHC.Exts (IsList(..))

import Streaming (Stream, Of)
import Streaming.Prelude qualified as S

import System.Directory qualified as Dir
import System.FilePath ((</>))
import System.IO qualified as IO

import VPF.DataSource.GenericFTP
import VPF.DataSource.NCBI
import VPF.Formats
import VPF.Frames.DSV qualified as DSV
import VPF.Frames.Types (FieldSubset, FieldsOf)
import VPF.Util.Foldl qualified as L
import VPF.Util.FS qualified as FS
import VPF.Util.Streaming qualified as S


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
    { taxonomyRanks :: IntMap Text
    , taxonomyGr    :: FGL.UGr
    , taxonomyRoot  :: ~FGL.Node
    }


taxonomyGraphFold :: forall cols.
    cols ~ FieldsOf TaxonomyNodesCols '["tax_id", "parent tax_id", "rank"]
    => L.Fold (Record cols) TaxonomyGraph
taxonomyGraphFold =
    let ranksFold :: L.Fold (Record cols) (IntMap Text)
        ranksFold = L.premap recRankAssoc L.intMap

        unodesFold :: L.Fold (Record cols) [FGL.UNode]
        unodesFold = IntMap.toList <$> L.handles recUNodes L.intMap

        uedgesFold :: L.Fold (Record cols) [FGL.UEdge]
        uedgesFold = L.premap recUEdge $ L.handles (L.filtered (not . isLoop)) L.list

        ugrFold :: L.Fold (Record cols) FGL.UGr
        ugrFold = liftA2 FGL.mkGraph unodesFold uedgesFold
    in
        liftA2 (\ranks gr ->
            TaxonomyGraph
                { taxonomyRanks = ranks
                , taxonomyGr    = gr
                , taxonomyRoot  =
                    case FGL.match 1 gr of
                        (Just ([], _, (), _:_), _) -> 1
                        _                          -> error "taxonomyGraphFold: root is not 1!"
                })
            ranksFold
            ugrFold


  where
    isLoop :: FGL.UEdge -> Bool
    isLoop (u, v, _) = u == v

    recUEdge :: Record cols -> FGL.UEdge
    recUEdge (V.Field taxid V.:& V.Field ptaxid V.:& _) = (ptaxid, taxid, ())

    recUNodes :: Fold (Record cols) FGL.UNode
    recUNodes f (V.Field taxid V.:& V.Field ptaxid V.:& _) = phantom $ f (ptaxid, ()) *> f (taxid, ())

    recRankAssoc :: Record cols -> (Int, Text)
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


taxonomyNamesPath :: Path Directory -> Path (DSV "|" TaxonomyNamesCols)
taxonomyNamesPath downloadDir = extractedDmpPath downloadDir "names"


data TaxNameClass = TaxNameClass { uniqueNameInClass :: Text, taxNameClass :: Text }
    deriving (Eq, Ord, Show)


taxIdToNameMapFold :: forall a.
    L.Fold (Text, TaxNameClass) a
    -> L.Fold (Record TaxonomyNamesCols) (IntMap a)
taxIdToNameMapFold (L.Fold g z (p :: x -> a)) = L.Fold (flip f) mempty (IntMap.map p)
  where
    f :: Record TaxonomyNamesCols -> IntMap x -> IntMap x
    f (V.Field taxid V.:& V.Field name V.:& V.Field uniqueName V.:& V.Field nameClass V.:& V.RNil) =
        flip IntMap.alter taxid \case
            Nothing -> Just (g z (name, TaxNameClass uniqueName nameClass))
            Just a  -> Just (g a (name, TaxNameClass uniqueName nameClass))


nameToTaxIdMapFold :: forall a.
    L.Fold (Int, TaxNameClass) a
    -> L.Fold (Record TaxonomyNamesCols) (HashMap Text a)
nameToTaxIdMapFold (L.Fold g z (p :: x -> a)) = L.Fold (flip f) mempty (HashMap.map p)
  where
    f :: Record TaxonomyNamesCols -> HashMap Text x -> HashMap Text x
    f (V.Field taxid V.:& V.Field name V.:& V.Field uniqueName V.:& V.Field nameClass V.:& V.RNil) =
        flip HashMap.alter name \case
            Nothing -> Just (g z (taxid, TaxNameClass uniqueName nameClass))
            Just a  -> Just (g a (taxid, TaxNameClass uniqueName nameClass))


type TaxonomyMergedIdsCols =
    '[ '("old_tax_id", Int)  -- id of nodes that have been merged
    ,  '("new_tax_id", Int)  -- id of nodes which is the result of merging
    ]


taxonomyMergedIdsPath :: Path Directory -> Path (DSV "|" TaxonomyMergedIdsCols)
taxonomyMergedIdsPath downloadDir = extractedDmpPath downloadDir "merged"


mergedIdsMapFold :: L.Fold (Record TaxonomyMergedIdsCols) (IntMap Int)
mergedIdsMapFold =
    L.premap recAssoc $
        L.Fold (flip $ uncurry IntMap.insert) mempty id
  where
    recAssoc :: Record TaxonomyMergedIdsCols -> (Int, Int)
    recAssoc (V.Field old V.:& V.Field new V.:& _) = (old, new)


data Taxonomy f = Taxonomy
    { taxonomyGraph          :: TaxonomyGraph
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
    L.handles $
        L.filtered \(_, TaxNameClass _ cls) -> cls == Text.pack "scientific name"


listWithTaxNameClass :: L.Fold (a, TaxNameClass) (ListWithTaxNameClass a)
listWithTaxNameClass = ListWithTaxNameClass <$> L.list


loadFullTaxonomy ::
    (forall a. L.Fold (a, TaxNameClass) (f a))
    -> Path Directory
    -> IO (Either DSV.ParseError (Taxonomy f))
loadFullTaxonomy fold downloadDir = ResourceT.runResourceT $ runExceptT do
    !graph <- ExceptT $
        streamTaxonomyDmpFile (taxonomyNodesPath downloadDir)
            & L.purely S.fold taxonomyGraphFold
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


taxIdLineage :: Taxonomy f -> Int -> [Int]
taxIdLineage taxdb taxid =
    case FGL.dfs [taxid] (taxonomyGr (taxonomyGraph taxdb)) of
        h:t | h == taxonomyRoot (taxonomyGraph taxdb) -> t
        l                                             -> l


taxIdRank :: Taxonomy f -> Int -> Maybe Text
taxIdRank taxdb taxid = IntMap.lookup taxid (taxonomyRanks (taxonomyGraph taxdb))


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
