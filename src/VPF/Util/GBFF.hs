{-# language GeneralizedNewtypeDeriving #-}
{-# language OverloadedStrings #-}
{-# language PackageImports #-}
{-# language TemplateHaskell #-}
{-# language TupleSections #-}
module VPF.Util.GBFF where

import Control.Applicative
import Control.Lens (Traversal', makeLenses, makePrisms)
import Control.Monad (guard, replicateM)
import Control.Monad.Trans (lift)
import Control.Monad.Trans.Except (runExceptT, ExceptT(ExceptT))
import Control.Monad.Trans.Resource (MonadResource)
import Control.Monad.Trans.Resource qualified as ResourceT

import Data.Attoparsec.ByteString (Parser)
import Data.Attoparsec.ByteString qualified as A
import Data.Attoparsec.ByteString.Char8 qualified as A8

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BS8
import Data.ByteString.Internal qualified as BS8 (c2w)

import Data.Functor.Identity (Identity, runIdentity)
import Data.Hashable (Hashable)
import Data.Int (Int64)

import Streaming (Stream, Of, effect)
import Streaming.ByteString (ByteStream)
import Streaming.ByteString qualified as BSS
import Streaming.Zip qualified as SZ
import "streaming-attoparsec" Data.Attoparsec.ByteString.Streaming qualified as SA

import System.IO qualified as IO


newtype RawLine = RawLine ByteString
    deriving newtype Show

newtype RawLines = RawLines [ByteString]
    deriving newtype Show

newtype Accession = Accession ByteString
    deriving newtype (Eq, Ord, Hashable, Show)

newtype VersionedAccession = VersionedAccession ByteString
    deriving newtype (Eq, Ord, Hashable, Show)


newtype GenBankHeader = GenBankHeader RawLines
    deriving Show


data GenBankRecord = GenBankRecord
    { _genBankFields :: [GenBankField]
    , _genBankRecordSource :: ByteString
    }


data GenBankField
    = LocusField      RawLine
    | DefinitionField RawLines
    | AccessionField  [Accession]
    | VersionField    VersionedAccession
    | DbLinkField     RawLines
    | DbSourceField   RawLines
    | KeywordsField   RawLines
    | SourceField     RawLines [OrganismSubfield]
    | ReferenceField  RawLines [ReferenceSubfield]
    | CommentField    RawLines
    | PrimaryField    RawLines
    | FeaturesField   [Feature]
    | ContigField     RawLines
    | OriginField     [SequenceLine]
    --  | UnknownField    ByteString RawLines
    deriving Show


newtype OrganismSubfield
    = OrganismSubfield RawLines
    deriving Show


data ReferenceSubfield
    = ReferenceAuthors    RawLines
    | ReferenceConsortium RawLines
    | ReferenceTitle      RawLines
    | ReferenceJournal    RawLines
    | ReferencePubMed     RawLines
    | ReferenceRemark     RawLines
    | ReferenceMedline    RawLines
    deriving Show


data Feature = Feature
    { _featureName       :: ByteString
    , _featureLocation   :: RawLine
    , _featureQualifiers :: [FeatureQualifier]
    }
    deriving Show

data FeatureQualifier = FeatureQualifier
    { _qualifierName :: ByteString
    , _qualifierValue :: Maybe QualifierValue
    }
    deriving Show

data QualifierValue = FreeText [ByteString] | LiteralText ByteString | Numeral !Int
    deriving Show

data SequenceLine = SequenceLine
    { _sequenceLineStart :: !Int
    , _sequenceLineChunks :: [ByteString]
    }
    deriving Show


makePrisms ''RawLine
makePrisms ''RawLines
makePrisms ''Accession
makePrisms ''VersionedAccession
makePrisms ''GenBankHeader
makeLenses ''GenBankRecord
makePrisms ''GenBankField
makePrisms ''OrganismSubfield
makePrisms ''ReferenceSubfield
makeLenses ''Feature
makeLenses ''FeatureQualifier
makePrisms ''QualifierValue
makeLenses ''SequenceLine


genBankFieldStrings :: forall f. Applicative f => (ByteString -> f ByteString) -> GenBankField -> f GenBankField
genBankFieldStrings f = \case
    LocusField rl         -> LocusField <$> _RawLine f rl
    DefinitionField rl    -> DefinitionField <$> lineStrings f rl
    AccessionField acs    -> AccessionField <$> (traverse._Accession) f acs
    VersionField va       -> VersionField <$> _VersionedAccession f va
    DbLinkField rl        -> DbLinkField <$> lineStrings f rl
    DbSourceField rl      -> DbSourceField <$> lineStrings f rl
    KeywordsField rl      -> KeywordsField <$> lineStrings f rl
    SourceField rl oss    -> SourceField <$> lineStrings f rl <*> (traverse._OrganismSubfield.lineStrings) f oss
    ReferenceField rl rss -> ReferenceField <$> lineStrings f rl <*> (traverse.refFieldStrings) f rss
    CommentField rl       -> CommentField <$> lineStrings f rl
    PrimaryField rl       -> PrimaryField <$> lineStrings f rl
    FeaturesField feas    -> FeaturesField <$> (traverse.featureStrings) f feas
    ContigField rl        -> ContigField <$> lineStrings f rl
    OriginField sl        -> OriginField <$> (traverse.sequenceLineChunks.traverse) f sl
    --UnknownField bs rl    -> UnknownField <$> f bs <*> lineStrings f rl
  where
    lineStrings :: Traversal' RawLines ByteString
    lineStrings = _RawLines.traverse

    refFieldStrings :: Traversal' ReferenceSubfield ByteString
    refFieldStrings f = \case
        ReferenceAuthors rl    -> ReferenceAuthors <$> lineStrings f rl
        ReferenceConsortium rl -> ReferenceConsortium <$> lineStrings f rl
        ReferenceTitle rl      -> ReferenceTitle <$> lineStrings f rl
        ReferenceJournal rl    -> ReferenceJournal <$> lineStrings f rl
        ReferencePubMed rl     -> ReferencePubMed <$> lineStrings f rl
        ReferenceRemark rl     -> ReferenceRemark <$> lineStrings f rl
        ReferenceMedline rl    -> ReferenceMedline <$> lineStrings f rl

    featureStrings :: Traversal' Feature ByteString
    featureStrings f = \case
        Feature name loc quals -> Feature <$> f name <*> _RawLine f loc <*> (traverse.qualStrings) f quals

    qualStrings :: Traversal' FeatureQualifier ByteString
    qualStrings f = \case
        FeatureQualifier qual val -> FeatureQualifier <$> f qual <*> (traverse.qualValStrings) f val

    qualValStrings :: Traversal' QualifierValue ByteString
    qualValStrings f = \case
        FreeText bss   -> FreeText <$> traverse f bss
        LiteralText bs -> LiteralText <$> f bs
        n@(Numeral _)  -> pure n

-- USAGE

data ParseError m r = ParseError
    { parseErrorFilename :: FilePath
    , parseErrorContexts :: [String]
    , parseErrorMessage  :: String
    , parseLeftovers     :: ByteStream m r
    }

type ParseError_ = ParseError Identity ()


instance Show (ParseError m r) where
    show err = unlines $
        [ "Parse error at file " ++ parseErrorFilename err ++ ": " ++ parseErrorMessage err
        , "Context:"
        ]
        ++ map ('\t':) (parseErrorContexts err)


parseErrorWithLeftoversPeek :: Monad m => Int64 -> ParseError m r -> m (ParseError n ())
parseErrorWithLeftoversPeek peekSize e = do
    peek <- BSS.toStrict_ (BSS.take peekSize (parseLeftovers e))
    return e { parseLeftovers = BSS.fromStrict peek }


showParseErrorWithLeftovers :: ParseError Identity r -> String
showParseErrorWithLeftovers e = unlines
    [ show e
    , "Leftovers: " ++ BS8.unpack (runIdentity (BSS.toStrict_ (parseLeftovers e)))
    ]


parseGenBankFileWith ::
    ( Functor f
    , MonadResource m
    )
    => Bool
    -> FilePath
    -> (FilePath -> ByteStream m () -> Stream f m (Either e ()))
    -> Stream f m (Either e ())
parseGenBankFileWith gzipped filePath parse = runExceptT do
    (releaseKey, h) <- lift . lift $
        ResourceT.allocate
            (IO.openBinaryFile filePath IO.ReadMode)
            IO.hClose

    ExceptT $ parse filePath (fromHandle h)

    ResourceT.release releaseKey
  where
    fromHandle
      | gzipped   = SZ.gunzip . BSS.fromHandle
      | otherwise = BSS.fromHandle


parseGenBankStreamWithSource ::
    Monad m
    => FilePath
    -> ByteStream m r
    -> m (Either (ParseError m r)
            ( Maybe GenBankHeader
            , Stream (Of GenBankRecord) m (Either (ParseError m r) r)
            ))
parseGenBankStreamWithSource filename stream = do
    (headerRes, stream') <- SA.parse (A.option Nothing (Just <$> headerP)) stream

    return $ case headerRes of
        Left (ctx, e) ->
            Left (ParseError filename ctx e stream')

        Right header ->
            Right
                ( header
                , leftoversToError filename <$> SA.parsed (toRecord <$> A.match entryP) stream'
                )
  where
    toRecord :: (ByteString, [GenBankField]) -> GenBankRecord
    toRecord = uncurry (flip GenBankRecord)


parseGenBankStreamBodyWithSource ::
    Monad m
    => FilePath
    -> ByteStream m r
    -> Stream (Of GenBankRecord) m (Either (ParseError m r) r)
parseGenBankStreamBodyWithSource filename stream = effect do
    headerRes <- parseGenBankStreamWithSource filename stream

    case headerRes of
        Left e ->
            return (pure (Left e))

        Right (_header, stream') ->
            return stream'


parseGenBankStream ::
    Monad m
    => FilePath
    -> ByteStream m r
    -> Stream (Of [GenBankField]) m (Either (ParseError m r) r)
parseGenBankStream filename stream = effect do
    (headerRes, stream') <- SA.parse (A.option Nothing (Just <$> headerP)) stream

    case headerRes of
        Left (ctx, e) ->
            return $ pure (Left (ParseError filename ctx e stream'))

        Right _header ->
            return $ leftoversToError filename <$> SA.parsed entryP stream'


leftoversToError :: FilePath -> Either (SA.Errors, ByteStream m r) r -> Either (ParseError m r) r
leftoversToError filename (Left ((ctx, e), l)) = Left (ParseError filename ctx e l)
leftoversToError _        (Right r)            = Right r


headerP :: Parser GenBankHeader
headerP = (A.<?> "header") do
    ls <- replicateM 10 intactRestOfLine

    let statLine = ls !! 7
    guard $ BS8.length statLine >= 66
    guard $ BS8.pack "loci," `BS8.isPrefixOf` BS8.drop 9 statLine
    guard $ BS8.pack "bases," `BS8.isPrefixOf` BS8.drop 27 statLine
    guard $ BS8.pack "reported sequences" `BS8.isPrefixOf` BS8.drop 48 statLine

    return (GenBankHeader (RawLines ls))



entryP :: Parser [GenBankField]
entryP = do
    fields <- A.many1 fieldP

    entrySeparatorP <|> do
        line <- intactRestOfLine
        fail ("Cannot parse field line: " ++ show line)

    return fields


entrySeparatorP :: Parser ()
entrySeparatorP = (A.<?> "entry separator (//)" ) do
    _ <- A8.string "//"
    _ <- restOfLine
    return ()


fieldP :: Parser GenBankField
fieldP = do
    kw <- BS8.strip <$> A.take 10 A.<?> "keyword"
    guard $ not (BS.isPrefixOf "//" kw)

    _ <- A8.string "  "

    case kw of
        "LOCUS"      -> locusP
        "DEFINITION" -> definitionP
        "ACCESSION"  -> accessionP
        "VERSION"    -> versionP
        "DBLINK"     -> dblinkP
        "DBSOURCE"   -> dbsourceP
        "KEYWORDS"   -> keywordsP
        "SOURCE"     -> sourceP
        "REFERENCE"  -> referenceP
        "COMMENT"    -> commentP
        "PRIMARY"    -> primaryP
        "FEATURES"   -> featuresP
        "CONTIG"     -> contigP
        "ORIGIN"     -> originP
        kw           -> fail ("Unknown field: " ++ show kw)


locusP :: A.Parser GenBankField
locusP = LocusField . RawLine <$> restOfLine
    A.<?> "LOCUS line"


definitionP :: A.Parser GenBankField
definitionP = DefinitionField <$> restOfField
    A.<?> "DEFINITION field"


accessionP :: A.Parser GenBankField
accessionP = (A.<?> "ACCESSION field") do
    firstLine <- accessionLineP

    otherLines <- A.many' do
        _ <- A8.string (BS8.replicate 10 ' ')
        accessionLineP

    return (AccessionField (firstLine ++ concat otherLines))
  where
    accessionLineP :: A.Parser [Accession]
    accessionLineP = do
        A8.skipSpace

        accessions <- accessionNumberP `A.sepBy1` A8.char ' '
        _ <- restOfLine

        return $ concatMap splitAccessionRange accessions

    accessionNumberP :: A.Parser ByteString
    accessionNumberP = A8.takeWhile1 (not . A8.isSpace)
        A.<?> "accession"

    zeros :: [ByteString]
    zeros = iterate (BS8.cons '0') BS.empty

    splitAccessionRange :: ByteString -> [Accession]
    splitAccessionRange acc
        | '-' `BS8.notElem` acc = [Accession acc]
        | otherwise =
            let (acc1, dashAcc2) = BS8.break (== '-') acc
                acc2 = BS.drop 1 dashAcc2

                (acc1alpha, acc1num) = BS8.span A8.isAlpha_ascii acc1
                acc2num = BS8.dropWhile A8.isAlpha_ascii acc1

                numLength = BS.length acc1num

                padNum bs
                    | BS.length bs >= numLength = bs
                    | otherwise = (zeros !! (numLength - BS.length bs)) <> bs

                intToAcc = Accession . (acc1alpha <>) . padNum . BS8.pack . show
            in
                case (BS8.readInt acc1num, BS8.readInt acc2num) of
                    (Just (acc1int, _), Just (acc2int, _)) ->
                        map intToAcc [acc1int..acc2int]
                    _ ->
                        [Accession acc1, Accession acc2]


versionP :: A.Parser GenBankField
versionP = VersionField . VersionedAccession <$> restOfLine
    A.<?> "VERSION field"


dblinkP :: A.Parser GenBankField
dblinkP = DbLinkField <$> restOfField
    A.<?> "DBLINK field"


dbsourceP :: A.Parser GenBankField
dbsourceP = DbSourceField <$> restOfField
    A.<?> "DBSOURCE field"


keywordsP :: A.Parser GenBankField
keywordsP = KeywordsField <$> restOfField
    A.<?> "KEYWORDS field"


sourceP :: A.Parser GenBankField
sourceP = SourceField <$> restOfField <*> A.many1 organismP
    A.<?> "SOURCE field"
  where
    organismP = do
        _ <- A.string "  ORGANISM "
        OrganismSubfield <$> restOfField


referenceP :: A.Parser GenBankField
referenceP = (A.<?> "REFERENCE field") do
    refLines <- restOfField

    subfields <- A.many1 subfieldsP

    return (ReferenceField refLines subfields)
  where
    subfieldsP = (A.<?> "one REFERENCE subfield") do
        _ <- A.string "  "
        subkw <- BS8.strip <$> A.take 10

        case subkw of
            "AUTHORS" -> ReferenceAuthors    <$> restOfField
            "CONSRTM" -> ReferenceConsortium <$> restOfField
            "TITLE"   -> ReferenceTitle      <$> restOfField
            "JOURNAL" -> ReferenceJournal    <$> restOfField
            "PUBMED"  -> ReferencePubMed     <$> restOfField
            "REMARK"  -> ReferenceRemark     <$> restOfField
            "MEDLINE" -> ReferenceMedline    <$> restOfField
            _         -> fail $ "Unknown reference subfield: " ++ show subkw


commentP :: A.Parser GenBankField
commentP = CommentField <$> restOfField
    A.<?> "COMMENT field"


primaryP :: A.Parser GenBankField
primaryP = PrimaryField <$> restOfField
    A.<?> "PRIMARY field"


featuresP :: A.Parser GenBankField
featuresP = (A.<?> "FEATURES field") do
    A8.skipSpace
    _ <- A8.string "Location/Qualifiers"
    _ <- restOfLine
    FeaturesField <$> A.many1 featureP
  where
    featureP :: A.Parser Feature
    featureP = (A.<?> "feature") do
        _ <- A8.string "     "
        name <- BS8.strip <$> A.take 16
        loc <- RawLine <$> restOfLine
        quals <- A.many' qualifierP
        return (Feature name loc quals)

    qualifierP :: A.Parser FeatureQualifier
    qualifierP = (A.<?> "feature qualifier") do
        _ <- A.string (BS8.replicate 21 ' ')

        qualName <- A8.char '/' *> qualifierNameP

        qualValue <- A8.peekChar >>= \case
            Just '=' -> Just <$> (A8.char '=' *> qualifierValueP)
            _        -> return Nothing

        _ <- restOfLine

        return (FeatureQualifier qualName qualValue)

    qualifierNameP :: A.Parser ByteString
    qualifierNameP = A.takeWhile1 (\c -> c /= BS8.c2w '=' && not (A8.isEndOfLine c))
        A.<?> "qualifier name"

    qualifierValueP :: A.Parser QualifierValue
    qualifierValueP =
        FreeText <$> quotedTextP
        <|> Numeral <$> A8.signed A8.decimal
        <|> LiteralText . BS8.strip <$> A.takeWhile (not . A8.isEndOfLine)
        A.<?> "qualifier value"

    quotedTextP :: A.Parser [ByteString]
    quotedTextP = (A.<?> "quoted free text") do
        _ <- A8.char '"'

        let chunk = fmap mconcat . A.many' $
                ("\"" <$ A.string "\"\"")
                <|>
                A.takeWhile1 (\c -> c /= BS8.c2w '"' && not (A8.isEndOfLine c))

            sep = A8.endOfLine >> A.string (BS8.replicate 21 ' ')

        value <- chunk `A.sepBy` sep
        _ <- A8.char '"'

        return value


contigP :: A.Parser GenBankField
contigP = ContigField <$> restOfField
    A.<?> "CONTIG field"


originP :: A.Parser GenBankField
originP = (A.<?> "ORIGIN field") do
    _ <- restOfLine

    seqLines <- A.many' do
        A8.char ' ' *> A8.skipWhile (== ' ')
        startIndex <- A8.decimal
        A8.char ' ' *> A8.skipWhile (== ' ')
        chunks <- A8.takeWhile1 (not . A8.isSpace) `A8.sepBy1'` A8.char ' '
        A8.skipWhile (== ' ')
        A8.endOfLine
        return (SequenceLine startIndex chunks)

    return (OriginField seqLines)


-- unknownP :: ByteString -> A.Parser GenBankField
-- unknownP kw = UnknownField kw <$> restOfField
--     A.<?> ("unknown field " ++ show kw)


restOfField :: A.Parser RawLines
restOfField = RawLines <$> liftA2 (:) restOfLine continuationLines


restOfLine :: A.Parser ByteString
restOfLine = BS8.strip <$> intactRestOfLine

intactRestOfLine :: A.Parser ByteString
intactRestOfLine = A.takeWhile (not . A8.isEndOfLine) <* A8.endOfLine


continuationLines :: A.Parser [ByteString]
continuationLines = A.many' continuationLine
  where
    continuationLine = do
        _ <- A.string (BS8.replicate 10 ' ')
        restOfLine
