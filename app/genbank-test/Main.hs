{-# language OverloadedLabels #-}
{-# language TupleSections #-}

import GHC.TypeLits (AppendSymbol, KnownSymbol)

import Control.Applicative (liftA2)
import Control.Category ((>>>))
import Control.Foldl qualified as Fold
import Control.Lens hiding ((:>))
import Control.Monad (forM_)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Except (ExceptT(ExceptT), runExceptT)
import Control.Monad.Trans.Resource qualified as ResourceT

import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BS8
import Data.Foldable
import Data.Graph.Inductive qualified as FGL
import Data.HashMap.Strict (HashMap)
import Data.HashMap.Strict qualified as HashMap
import Data.HashSet (HashSet)
import Data.HashSet qualified as HashSet
import Data.List.NonEmpty (NonEmpty ((:|)))
import Data.Text (Text)
import Data.Text qualified as Text
import Data.Text.Encoding qualified as Text
import Data.Text.IO qualified as Text
import Data.Vinyl (Rec(..), (=:))
import Data.Vinyl qualified as V
import Data.Vinyl.TypeLevel qualified as V

import Streaming (Stream, Of(..))
import Streaming.Prelude qualified as S

import System.Directory (createDirectoryIfMissing)
import System.FilePath (takeDirectory)
import System.Exit (exitFailure)
import System.IO qualified as IO

import VPF.DataSource.ICTV qualified as ICTV
import VPF.DataSource.NCBI.GenBank qualified as GenBank
import VPF.DataSource.NCBI.RefSeq qualified as RefSeq
import VPF.DataSource.NCBI.Taxonomy qualified as Taxonomy
import VPF.Formats
import VPF.Frames.Dplyr qualified as F
import VPF.Frames.DSV qualified as DSV
import VPF.Frames.Types (Record, FrameRec)
import VPF.Model.Training.DataSetup
import VPF.Util.FS
import VPF.Util.GBFF qualified as GBFF
import VPF.Util.Streaming


wrapError :: Of a (Either e ()) -> Either e a
wrapError (a :> e) =
    case e of
        Left err -> Left err
        Right () -> Right a


collectAccessions ::
    Monad m
    => Stream (Of (FilePath, [GBFF.GenBankField])) m (Either e ())
    -> m (Either e (HashMap GBFF.Accession GBFF.Accession))
collectAccessions =
    S.map snd
    >>> S.concat
    >>> S.map (over GBFF.genBankFieldStrings BS.copy)
    >>> S.mapMaybe \case
            GBFF.AccessionField (acc:accs) -> Just (map (, acc) (acc:accs))
            _                              -> Nothing
    >>> S.concat
    >>> Fold.purely S.fold Fold.hashMap
    >>> fmap wrapError


refSeqDownloadDir :: Path Directory
refSeqDownloadDir = Tagged "./downloads/refseq"


refSeqEntries ::
    ResourceT.MonadResource m
    => RefSeq.RefSeqDownloadList
    -> Stream (Of (FilePath, [GBFF.GenBankField])) m (Either GBFF.ParseError_ ())
refSeqEntries cfg = cleanupErrors $ runExceptT do
    fileList <- liftIO $ (>>= rightOrDie) $
        RefSeq.listRefSeqSeqFiles cfg refSeqDownloadDir

    forM_ fileList \(Tagged file) ->
        ExceptT $
            GBFF.parseGenBankFileWith True file GBFF.parseGenBankStream
                & S.map (file, )
  where
    cleanupErrors =
        (>>= _Left (lift . GBFF.parseErrorWithLeftoversPeek 1000))


genBankDownloadDir :: Path Directory
genBankDownloadDir = Tagged "./downloads/genbank"


genBankEntries ::
    ResourceT.MonadResource m
    => GenBank.GenBankDownloadList
    -> Stream (Of (FilePath, [GBFF.GenBankField])) m (Either GBFF.ParseError_ ())
genBankEntries cfg = cleanupErrors $ runExceptT do
    fileList <- liftIO $ (>>= rightOrDie) $
        GenBank.listGenBankSeqFiles cfg genBankDownloadDir

    forM_ fileList \(Tagged file) ->
        ExceptT $
            GBFF.parseGenBankFileWith True file GBFF.parseGenBankStream
                & S.map (file, )
  where
    cleanupErrors =
        (>>= _Left (lift . GBFF.parseErrorWithLeftoversPeek 1000))


_countAccessionsMain :: IO ()
_countAccessionsMain = do
    res <- ResourceT.runResourceT $
        runExceptT do
            refseq <- ExceptT $ collectAccessions (refSeqEntries RefSeq.refSeqViralGenomicList)
            gb <- ExceptT $ collectAccessions (genBankEntries GenBank.genBankViralConfig)

            return (refseq, gb)

    case res of
        Left e ->
            putStrLn $ GBFF.showParseErrorWithLeftovers e
        Right (refseqAccessions :: t, gbAccessions :: t) -> do
            putStrLn $ "Found " ++ show (length refseqAccessions) ++ " accessions in RefSeq"
            putStrLn $ "Found " ++ show (length gbAccessions) ++ " accessions in GenBank"


_findNonredundantProtsMain :: IO ()
_findNonredundantProtsMain = do
    res <- ResourceT.runResourceT $ runExceptT do
        ExceptT $
            refSeqEntries RefSeq.refSeqViralProteinsList
                & S.map snd
                & S.filter
                    (anyOf (traverse . GBFF.genBankFieldStrings) containsNonredundant)
                & S.map formatOutput
                & S.mapM_ (liftIO . BS8.putStrLn)

    print res
  where
    recordAccessions :: Traversal' [GBFF.GenBankField] GBFF.Accession
    recordAccessions = traverse . GBFF._AccessionField . traverse

    containsNonredundant :: BS.ByteString -> Bool
    containsNonredundant = BS.isInfixOf (BS8.pack "WP_")

    commaSep :: [BS.ByteString] -> BS.ByteString
    commaSep = BS8.intercalate (BS8.pack ",")

    formatOutput :: [GBFF.GenBankField] -> BS.ByteString
    formatOutput fields =
        let protNames = toListOf (recordAccessions . GBFF._Accession) fields
            dbsources = toListOf (traverse . GBFF._DbSourceField . GBFF._RawLines . traverse) fields
        in commaSep protNames <> BS8.pack "\t" <> commaSep dbsources


type AccessionMappingCols =
    '[ '("primary_accession", Text)
    ,  '("alias_accession", Text)
    ,  '("file", Text)
    ]

type NoProteinGenomeCols =
    '[ '("genome", Text)
    ,  '("sequence_size", Int)
     ]

type ProteinMappingCols =
    '[ '("genome", Text)
    ,  '("protein", Text)
     ]


streamMappings :: MonadIO m
    => Stream (Of (FilePath, [GBFF.GenBankField])) m r
    -> Stream
        (Of
            ( [Record AccessionMappingCols]
            , Either (Record NoProteinGenomeCols) [Record ProteinMappingCols]
            ))
        m
        r
streamMappings = S.map (accessionsToRecords . entryAccessions)
  where
    accessionsToRecords ::
        (Text, NonEmpty Text, [Text], Int)
        -> ( [Record AccessionMappingCols]
            , Either (Record NoProteinGenomeCols) [Record ProteinMappingCols]
            )
    accessionsToRecords (path, primary :| aliases, prots, seqSize) =
        case prots of
            [] ->
                (accessionRecords, Left (V.fieldRec (#genome V.=: primary, #sequence_size V.=: seqSize)))
            _ ->
                (accessionRecords, Right protRecords)
              where
                protRecords = [V.fieldRec (#genome V.=: primary, #protein V.=: prot) | prot <- prots]
      where
        accessionRecords =
            [V.fieldRec (#primary_accession V.=: primary, #alias_accession V.=: alias, #file V.=: path) | alias <- primary:aliases]

    entryAccessions :: (FilePath, [GBFF.GenBankField]) -> (Text, NonEmpty Text, [Text], Int)
    entryAccessions (path, entry) =
        case entry ^.. traverse . GBFF._AccessionField . traverse . GBFF._Accession . to Text.decodeUtf8 of
            [] ->
                error $ "entry without accession: " ++ show entry ++ " in " ++ path

            primary:aliases ->
                let proteinIds = entry ^.. folded
                        . GBFF._FeaturesField
                        . folded
                        . filteredBy (GBFF.featureName . only (BS8.pack "CDS"))
                        . GBFF.featureQualifiers
                        . to (\quals ->
                            case quals^..cdsProteinIds of
                                [protId] -> Just protId
                                protId:_ -> error $ "CDS with multiple protein_id qualifiers: " ++ show protId
                                []       -> Nothing)
                        . _Just
                        . to Text.decodeUtf8

                in (Text.pack path, primary :| aliases, proteinIds, entryOriginLength entry)


    entryOriginLength :: [GBFF.GenBankField] -> Int
    entryOriginLength entry =
        case entry ^.. traverse . GBFF._OriginField of
            [] -> 0
            [[]] -> 0
            [ls] ->
                let GBFF.SequenceLine ix chunks = last ls
                in ix + sum (map BS.length chunks) - 1
            _ ->
                error $ "found entry with multiple ORIGINs: "
                    ++ show (entry ^.. traverse . GBFF._AccessionField)



    cdsProteinIds :: Fold [GBFF.FeatureQualifier] BS.ByteString
    cdsProteinIds =
        folded
        . filteredBy (GBFF.qualifierName . only (BS8.pack "protein_id"))
        . GBFF.qualifierValue
        . _Just
        . GBFF._FreeText
        . to mconcat


data MappingPaths = MappingPaths
    { accessionMappingPath :: Path (TSV AccessionMappingCols)
    , noProtsListPath      :: Path (TSV NoProteinGenomeCols)
    , protsMappingPath     :: Path (TSV ProteinMappingCols)
    }


data Mappings = Mappings
    { accessionMapping :: FrameRec AccessionMappingCols
    , noProtsList      :: FrameRec NoProteinGenomeCols
    , protsMapping     :: FrameRec ProteinMappingCols
    }


instance Semigroup Mappings where
    Mappings a n p <> Mappings a' n' p' = Mappings (a <> a') (n <> n') (p <> p')


instance Monoid Mappings where
    mempty = Mappings mempty mempty mempty


refSeqMappingPaths :: MappingPaths
refSeqMappingPaths = MappingPaths
    { accessionMappingPath = Tagged "./mapping/ncbi/refseq/primary_accessions.tsv"
    , noProtsListPath      = Tagged "./mapping/ncbi/refseq/no_prots_list.tsv"
    , protsMappingPath     = Tagged "./mapping/ncbi/refseq/protein_mapping.tsv"
    }


genBankReleaseMappingPaths :: MappingPaths
genBankReleaseMappingPaths = MappingPaths
    { accessionMappingPath = Tagged "./mapping/ncbi/genbank_release/primary_accessions.tsv"
    , noProtsListPath      = Tagged "./mapping/ncbi/genbank_release/no_prots_list.tsv"
    , protsMappingPath     = Tagged "./mapping/ncbi/genbank_release/protein_mapping.tsv"
    }


tpaMappingPaths :: MappingPaths
tpaMappingPaths = MappingPaths
    { accessionMappingPath = Tagged "./mapping/ncbi/tpa/primary_accessions.tsv"
    , noProtsListPath      = Tagged "./mapping/ncbi/tpa/no_prots_list.tsv"
    , protsMappingPath     = Tagged "./mapping/ncbi/tpa/protein_mapping.tsv"
    }


_generateAllMappings :: IO ()
_generateAllMappings =
    ResourceT.runResourceT do
        e <- runExceptT do
            ExceptT $
                writeMappings
                    (refSeqEntries RefSeq.refSeqViralGenomicList)
                    refSeqMappingPaths

            -- ExceptT $
            --     writeMappings
            --         (genBankEntries GenBank.genBankReleaseViralConfig)
            --         genBankReleaseMappingPaths

            -- ExceptT $
            --     writeMappings
            --         (genBankEntries GenBank.genBankTpaOnlyConfig)
            --         tpaMappingPaths

        case e of
            Left e -> do
                liftIO $ putStrLn $ GBFF.showParseErrorWithLeftovers e
            Right () ->
                liftIO $ putStrLn "done"
  where
    writeMappings entries paths =
        withWriteTagged (accessionMappingPath paths) \accH ->
        withWriteTagged (noProtsListPath paths) \noProtH ->
        withWriteTagged (protsMappingPath paths) \protsH ->
            streamMappings entries
                & S.unzip
                & writeAsTSV accH . S.concat
                & S.partitionEithers
                & writeAsTSV noProtH
                & writeAsTSV protsH . S.concat

    withWriteTagged :: ResourceT.MonadResource m => Path format -> (Tagged format IO.Handle -> m r) -> m r
    withWriteTagged (Tagged fp) f = do
        liftIO $ createDirectoryIfMissing True (takeDirectory fp)

        (releaseKey, h) <- ResourceT.liftResourceT $
            ResourceT.allocate (IO.openFile fp IO.WriteMode) IO.hClose

        r <- f (Tagged h)
        ResourceT.liftResourceT $ ResourceT.release releaseKey
        return r

    writeAsTSV (Tagged h :: Tagged (TSV cols) IO.Handle) stream =
        hWriteTextLines h $ DSV.streamDSVLines @cols (DSV.defWriterOptions '\t') stream


loadMappings :: MappingPaths -> IO (Either DSV.ParseError Mappings)
loadMappings paths = runExceptT do
    accMapp <- ExceptT $ DSV.readFrame (DSV.defParserOptions '\t') (accessionMappingPath paths)
    noProts <- ExceptT $ DSV.readFrame (DSV.defParserOptions '\t') (noProtsListPath paths)
    protsMapp <- ExceptT $ DSV.readFrame (DSV.defParserOptions '\t') (protsMappingPath paths)

    return (Mappings accMapp noProts protsMapp)


rightOrDie :: Show e => Either e a -> IO a
rightOrDie (Right a) = return a
rightOrDie (Left e) = do
    IO.hPutStrLn IO.stderr $ "Error: " ++ show e
    exitFailure


justOrDie :: String -> Maybe a -> IO a
justOrDie _ (Just a) = return a
justOrDie e Nothing = do
    IO.hPutStrLn IO.stderr $ "Error: " ++ e
    exitFailure


type GenBankAccessionStatsCols = AccessionStatsCols "genbank_"
type RefSeqAccessionStatsCols = AccessionStatsCols "refseq_"
type AllAccessionStatsCols =
    '("virus_name", Text) ': GenBankAccessionStatsCols V.++ RefSeqAccessionStatsCols

type AccessionStatsCols prefix =
    '[ '(prefix `AppendSymbol` "num_accessions",        Int)
    ,  '(prefix `AppendSymbol` "num_proteins",          Int)
    ,  '(prefix `AppendSymbol` "accession_keys",        Text)
    ,  '(prefix `AppendSymbol` "found_accessions",      Int)
    ,  '(prefix `AppendSymbol` "found_accessions_tpa",  Int)
    ,  '(prefix `AppendSymbol` "deleted_accessions",    Int)
    ,  '(prefix `AppendSymbol` "unknown_accessions",    Int)
    ,  '(prefix `AppendSymbol` "num_noprot_accessions", Int)
    ]

checkMissingAccessions :: IO ()
checkMissingAccessions = do
    !refseqAccs <- rightOrDie =<< accessionSet refSeqMappingPaths
    !refseqProts <- rightOrDie =<< proteinMap refSeqMappingPaths
    !rsNoProts <- rightOrDie =<< noProtsSet refSeqMappingPaths

    IO.hPutStrLn IO.stderr "loaded refseq accessions"

    !genbankRelAccs <- rightOrDie =<< accessionSet genBankReleaseMappingPaths
    !genbankRelProts <- rightOrDie =<< proteinMap genBankReleaseMappingPaths
    !gbNoProts <- rightOrDie =<< noProtsSet genBankReleaseMappingPaths
    !gbDeleted <- rightOrDie =<< runExceptT do
        gbdel <- ExceptT $ GenBank.loadNewlyDeletedAccessionsList genBankDownloadDir
        return $ HashSet.fromList (toListOf (folded . F.field @"deleted_accession") gbdel)

    IO.hPutStrLn IO.stderr "loaded genbank accessions"

    !tpaAccs <- rightOrDie =<< accessionSet tpaMappingPaths
    !tpaProts <- rightOrDie =<< proteinMap tpaMappingPaths
    !tpaNoProts <- rightOrDie =<< noProtsSet tpaMappingPaths

    let !genbankProts = HashMap.unionWith HashSet.union genbankRelProts tpaProts

    IO.hPutStrLn IO.stderr "loaded TPA accessions"

    vmr <- rightOrDie =<< getLatestVmr

    S.each (F.select @'["virus_name", "genbank_accession", "refseq_accession"] vmr)
        & S.zipWith (\i rec -> (i, rec, rec)) (S.enumFrom (1 :: Int))

        & S.map (_3 %~ \rec -> do
            gb <- renameFieldStats @"" @"genbank_" <$>
                fieldStats
                    (F.get @"genbank_accession" rec)
                    (genbankRelAccs, genbankProts, gbNoProts)
                    (tpaAccs, tpaNoProts)
                    gbDeleted

            rs <- renameFieldStats @"" @"refseq_" <$>
                fieldStats
                    (F.get @"refseq_accession" rec)
                    (refseqAccs, refseqProts, rsNoProts)
                    (tpaAccs, tpaNoProts)
                    HashSet.empty

            return (V.rappend gb rs))

        & S.mapMaybeM (\case
            (i, rec, Nothing) -> do
                IO.hPutStrLn IO.stderr $ "Error parsing accessions at ICTV line " ++ show i ++ ": " ++ show rec
                return Nothing
            (_, virusName V.:& rec, Just stats) -> return $ Just (over F.elfield Text.strip virusName V.:& stats))
        & DSV.streamDSVLines @AllAccessionStatsCols (DSV.defWriterOptions '\t')
        & putTextLines
  where
    accessionSet :: MappingPaths -> IO (Either DSV.ParseError (HashSet Text))
    accessionSet paths = runExceptT do
        alias <- ExceptT $
            DSV.readSubframe @'[ '("alias_accession", Text)] (DSV.defParserOptions '\t')
                (accessionMappingPath paths)

        return $ HashSet.fromList $
            toListOf (folded . F.field @"alias_accession") alias


    noProtsSet :: MappingPaths -> IO (Either DSV.ParseError (HashSet Text))
    noProtsSet paths = runExceptT do
        genomes <- ExceptT $
            DSV.readSubframe @'[ '("genome", Text)] (DSV.defParserOptions '\t')
                (noProtsListPath paths)

        return $ HashSet.fromList $
            (toListOf (folded . F.field @"genome") genomes)


    proteinMap :: MappingPaths -> IO (Either DSV.ParseError (HashMap Text (HashSet Text)))
    proteinMap paths = do
        let insertOrAppend :: Text -> Text -> HashMap Text (HashSet Text) -> HashMap Text (HashSet Text)
            insertOrAppend prot = HashMap.alter \case
                Nothing    -> Just (HashSet.singleton prot)
                Just prots -> Just (HashSet.insert prot prots)

        withFileRead (protsMappingPath paths) $
            hStreamTextLines
                >>> DSV.parsedRowStream (DSV.defParserOptions '\t') (protsMappingPath paths)
                >>> S.fold (\m rec -> insertOrAppend (F.get @"protein" rec) (F.get @"genome" rec) m)
                        HashMap.empty
                        id
                >>> fmap wrapEitherOf


    renameFieldStats ::
        ( V.StripFieldNames (AccessionStatsCols prefix)
        , V.StripFieldNames (AccessionStatsCols prefix')
        )
        => Record (AccessionStatsCols prefix)
        -> Record (AccessionStatsCols prefix')
    renameFieldStats = V.withNames . V.stripNames


    fieldStats ::
        Text
        -> (HashSet Text, HashMap Text (HashSet Text), HashSet Text)
        -> (HashSet Text, HashSet Text)
        -> HashSet Text
        -> Maybe (Record (AccessionStatsCols ""))
    fieldStats field (known, protMap, knownNoProts) (tpa, tpaNoProts) deleted = do
        (keys, accs) <- unzip <$> ICTV.parseAccessionList field

        let numAccs = length accs

            upd acc
                | HashSet.member acc known   = F.field @"found_accessions" +~ 1
                    >>> if HashSet.member acc knownNoProts then F.field @"num_noprot_accessions" +~ 1 else id
                | HashSet.member acc tpa     = F.field @"found_accessions_tpa" +~ 1
                    >>> if HashSet.member acc tpaNoProts then F.field @"num_noprot_accessions" +~ 1 else id
                | HashSet.member acc deleted = F.field @"deleted_accessions" +~ 1
                | otherwise                  = F.field @"unknown_accessions" +~ 1

            updProts acc =
                case HashMap.lookup acc protMap of
                    Nothing    -> id
                    Just prots -> F.field @"num_proteins" %~ HashSet.union prots

            init = V.fieldRec
                ( #num_accessions        =: numAccs
                , #num_proteins          =: HashSet.empty
                , #accession_keys        =: Text.intercalate (Text.singleton ',') (filter (not . Text.null) keys)
                , #found_accessions      =: 0
                , #found_accessions_tpa  =: 0
                , #deleted_accessions    =: 0
                , #unknown_accessions    =: 0
                , #num_noprot_accessions =: 0
                )

        return $ foldl' (\stats acc -> updProts acc (upd acc stats)) init accs
            & F.field @"num_proteins" %~ HashSet.size


verifyTaxonomy :: IO ()
verifyTaxonomy = do
    let downloadDir = Tagged "./downloads/taxonomy/"

    -- _ <- rightOrDie =<< Taxonomy.syncTaxonomy (IO.hPutStrLn IO.stderr) downloadDir
    taxdb <- rightOrDie =<<
        Taxonomy.loadFullTaxonomy (Taxonomy.onlyScientificNames Taxonomy.listWithTaxNameClass) True downloadDir

    IO.hPutStrLn IO.stderr "Loaded taxonomy graph"

    print $ Taxonomy.checkTaxonomyTree (Taxonomy.taxonomyGraph taxdb)

    vmr <- rightOrDie =<< getLatestVmr

    S.each (vmrTaxNames vmr)
        & S.map (\name -> (name, name))
        & S.map (_2 %~ Taxonomy.findTaxIdsForName taxdb False)
        & S.map (_2 %~ Text.intercalate (Text.pack ";") . map (Text.pack . show . fst))
        & S.map (\(name, taxids) -> Text.intercalate (Text.pack "\t") [name, taxids])
        & S.mapM_ Text.putStrLn


main :: IO ()
main = verifyTaxonomy
