{-# language OverloadedLabels #-}
{-# language OverloadedStrings #-}
{-# language QuasiQuotes #-}
{-# language RecordWildCards #-}
{-# language Strict #-}
{-# language TupleSections #-}
module VPF.DataSource.ICTV where

import Prelude hiding (last)
import GHC.TypeLits (KnownSymbol)

import Control.Lens
import Control.Monad (when)

import Codec.Xlsx qualified as Xlsx

import Data.Bifunctor
import Data.ByteString.Lazy qualified as LBS
import Data.Foldable (foldl', forM_)
import Data.Function
import Data.List (sortBy)
import Data.List.NonEmpty (NonEmpty(..), nonEmpty, last)
import Data.Map.Strict qualified as Map
import Data.Maybe
import Data.Proxy
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Time (UTCTime)
import Data.Time.Format (defaultTimeLocale, parseTimeM, rfc822DateFormat)
import Data.Vector (Vector)
import Data.Vector qualified as Vector
import Data.Vector.Mutable qualified as MVector

import Network.HTTP.Req qualified as Req

import Frames (FrameRec)
import Frames.RecF (rtraverse)
import Data.Vinyl.Functor
import Frames.InCore (toAoS)

import Text.Feed.Import (parseFeedSource)
import Text.Feed.Types (Feed(..))
import Text.RSS.Syntax qualified as RSS
import Text.URI (mkURI)

import VPF.Frames.Types (FieldK, Rec(..), type (++))


ictvVmrFeedUrl :: Req.Url 'Req.Https
ictvVmrFeedScheme :: Req.Option 'Req.Https
(ictvVmrFeedUrl, ictvVmrFeedScheme) =
    [Req.urlQ|https://talk.ictvonline.org/taxonomy/vmr/m/vmr-file-repository/rss|]


data VmrRevisionMeta = forall scheme. VmrRevisionMeta
    { revisionTitle     :: Text
    , revisionDate      :: UTCTime
    , revisionUrl       :: Req.Url scheme
    , revisionSchemeOpt :: Req.Option scheme
    }


instance Show VmrRevisionMeta where
    show (VmrRevisionMeta title date url _) =
        "VmrRevisionMeta { revisionTitle = " ++ show title
          ++ ", revisionDate = " ++ show date
          ++ ", revisionUrl = " ++ show url
          ++ "}"


getVmrFeed :: Req.HttpResponse response => Req.Req response
getVmrFeed =
    Req.req Req.GET ictvVmrFeedUrl Req.NoReqBody Proxy ictvVmrFeedScheme


-- sorted by date (ascending)
parseVmrFeed :: LBS.ByteString -> Maybe (NonEmpty VmrRevisionMeta)
parseVmrFeed xml = do
    RSSFeed rss <- parseFeedSource xml

    let items :: [RSS.RSSItem]
        items = RSS.rssItems (RSS.rssChannel rss)

    nonEmpty $ sortBy (compare `on` revisionDate) (mapMaybe itemToRevision items)
  where
    itemToRevision :: RSS.RSSItem -> Maybe VmrRevisionMeta
    itemToRevision item = do
        title <- RSS.rssItemTitle item

        pubDateText <- RSS.rssItemPubDate item
        date <- parseTimeM True defaultTimeLocale rfc822DateFormat (Text.unpack pubDateText)

        link <- RSS.rssItemLink item
        uri <- mkURI link
        eurl <- Req.useURI uri

        case eurl of
            Left (url, opt) -> return (VmrRevisionMeta title date url opt)
            Right (url, opt) -> return (VmrRevisionMeta title date url opt)


-- assumes sorted by date (ascending)
findLatestVmrRevision :: NonEmpty VmrRevisionMeta -> VmrRevisionMeta
findLatestVmrRevision = last


getVmrRevision :: VmrRevisionMeta -> Req.Req Req.LbsResponse
getVmrRevision (VmrRevisionMeta {..}) = do
    let downloadUrl = revisionUrl Req./: Text.pack "download"

    Req.req Req.GET downloadUrl Req.NoReqBody Proxy revisionSchemeOpt


data VmrRevisionColumn = VmrRevisionColumn
    { vmrColumnTitle  :: Text
    , vmrColumnValues :: Vector Text
    }
    deriving Show


data VmrRevision = VmrRevision
    { vmrMetadata :: VmrRevisionMeta
    , vmrColumns  :: [VmrRevisionColumn]
    }
    deriving Show


parseVmrRevision :: VmrRevisionMeta -> LBS.ByteString -> Either String VmrRevision
parseVmrRevision meta xlsxData = do
    xlsx <- first show $ Xlsx.toXlsxEitherFast xlsxData
    ws1 <- maybe (Left "no worksheets") Right $ xlsx ^? Xlsx.xlSheets . ix 0 . _2

    let cellMap :: Map.Map (Int, Int) Xlsx.Cell
        cellMap = ws1 ^. Xlsx.wsCells

        maxRow, maxCol :: Int
        (maxRow, maxCol) =
            foldl' (\(!mr, !mc) (r, c) -> (max mr r, max mc c)) (1, 1)
                (Map.keys cellMap)

        cells :: [((Int, Int), Text)]
        cells = Map.toAscList cellMap
            & map (_2 %~ maybe Text.empty cellText . view Xlsx.cellValue)
            & fillGaps [(row, col) | row <- [1..maxRow], col <- [1..maxCol]] Text.empty

        df :: Vector Text
        df = Vector.create do
            v <- MVector.new (maxRow*maxCol)

            forM_ cells \((row, col), !text) ->
                MVector.write v ((col-1)*maxRow + (row-1)) text

            return v

        sliceColumn :: Int -> VmrRevisionColumn
        sliceColumn col =
            let title = df Vector.! ((col-1)*maxRow)
                values = Vector.slice ((col-1)*maxRow + 1) (maxRow-1) df
            in
                VmrRevisionColumn title values

    let columns = map sliceColumn [1..maxCol]

    return (VmrRevision meta columns)
  where
    cellText :: Xlsx.CellValue -> Text
    cellText (Xlsx.CellText t)   = t
    cellText (Xlsx.CellDouble d) = Text.pack (show d)
    cellText (Xlsx.CellBool b)   = Text.pack (show b)
    cellText (Xlsx.CellRich rs)  = foldOf (folded . Xlsx.richTextRunText) rs
    cellText (Xlsx.CellError _)  = Text.empty


    fillGaps :: Ord i => [i] -> a -> [(i, a)] -> [(i, a)]
    fillGaps []     _         jas = jas
    fillGaps is     fillValue []  = map (, fillValue) is
    fillGaps (i:is) fillValue ((j,a):jas)
      | i == j    = (j, a) : fillGaps is fillValue jas
      | i <  j    = (i, fillValue) : fillGaps is fillValue ((j, a) : jas)
      | otherwise = (j, a) : fillGaps (i:is) fillValue jas


type TaxonomyCols :: [FieldK]
type TaxonomyCols =
    [ '("realm",      Text)
    , '("subrealm",   Text)
    , '("kingdom",    Text)
    , '("subkingdom", Text)
    , '("phylum",     Text)
    , '("subphylum",  Text)
    , '("class",      Text)
    , '("subclass",   Text)
    , '("order",      Text)
    , '("suborder",   Text)
    , '("family",     Text)
    , '("subfamily",  Text)
    , '("genus",      Text)
    , '("subgenus",   Text)
    , '("species",    Text)
    ]

type VmrAnnotationCols :: [FieldK]
type VmrAnnotationCols =
    [ '("virus_name",        Text)
    , '("genbank_accession", Text)
    , '("refseq_accession",  Text)
    , '("coverage",          Text)
    , '("composition",       Text)
    , '("host_source",       Text)
    ]


type VmrRevisionCols :: [FieldK]
type VmrRevisionCols = TaxonomyCols ++ VmrAnnotationCols


attemptRevisionToFrame :: VmrRevision -> Either String (FrameRec VmrRevisionCols)
attemptRevisionToFrame revision = do
    nrows <- commonLength

    colsFrame <- rtraverse getCompose $
           findColAndIndex @"realm"             "realm"
        :& findColAndIndex @"subrealm"          "subrealm"
        :& findColAndIndex @"kingdom"           "kingdom"
        :& findColAndIndex @"subkingdom"        "subkingdom"
        :& findColAndIndex @"phylum"            "phylum"
        :& findColAndIndex @"subphylum"         "subphylum"
        :& findColAndIndex @"class"             "class"
        :& findColAndIndex @"subclass"          "subclass"
        :& findColAndIndex @"order"             "order"
        :& findColAndIndex @"suborder"          "suborder"
        :& findColAndIndex @"family"            "family"
        :& findColAndIndex @"subfamily"         "subfamily"
        :& findColAndIndex @"genus"             "genus"
        :& findColAndIndex @"subgenus"          "subgenus"
        :& findColAndIndex @"species"           "species"
        :& findColAndIndex @"virus_name"        "virus name(s)"
        :& findColAndIndex @"genbank_accession" "virus genbank accession"
        :& findColAndIndex @"refseq_accession"  "virus refseq accession"
        :& findColAndIndex @"coverage"          "genome coverage"
        :& findColAndIndex @"composition"       "genome composition"
        :& findColAndIndex @"host_source"       "host source"
        :& RNil

    return (toAoS nrows colsFrame)
  where
    commonLength :: Either String Int
    commonLength =
        case vmrColumns revision of
            []       -> Left "no columns found"
            col:cols -> do
                let l = Vector.length (vmrColumnValues col)

                forM_ cols \col' -> do
                    let l' = Vector.length (vmrColumnValues col')

                    when (l /= l') $
                        Left $
                            "column length mismatch: column " <> Text.unpack (vmrColumnTitle col)
                                <> " has " <> show l
                            <> " entries, but column " <> Text.unpack (vmrColumnTitle col')
                                <> " has " <> show l' <> " entries"
                Right l

    findColAndIndex :: forall s.
        KnownSymbol s
        => Text
        -> (Either String :. ((->) Int) :. ElField) '(s, Text)
    findColAndIndex name =
        Compose $ fmap (\values -> Compose $ Field . Vector.unsafeIndex values) (findCol name)

    findCol :: Text -> Either String (Vector Text)
    findCol name = maybe (Left ("column not found: " <> Text.unpack name)) Right (Map.lookup name colMap)

    colMap :: Map.Map Text (Vector Text)
    colMap = Map.fromList
        [ (Text.toLower (vmrColumnTitle col), vmrColumnValues col)
        | col <- vmrColumns revision
        ]


parseAccessionList :: Text -> Maybe [(Text, Text)]
parseAccessionList cellText =
    fmap concat $ traverse parsePair (Text.splitOn (Text.pack ";") cellText)
  where
    parsePair :: Text -> Maybe [(Text, Text)]
    parsePair item =
        case map Text.strip (Text.splitOn ":" item) of
            []           -> Nothing
            [acc]
                | Text.null acc -> Just []
                | otherwise     -> Just [(mempty, acc)]
            label : accs -> Just $ map (label, ) accs
