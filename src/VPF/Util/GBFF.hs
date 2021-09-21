{-# language GeneralizedNewtypeDeriving #-}
{-# language OverloadedStrings #-}
{-# language PackageImports #-}
{-# language TupleSections #-}
module VPF.Util.GBFF where

import Control.Applicative
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

import Data.Hashable (Hashable)

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
    { genBankFields :: [GenBankField]
    , genBankRecordSource :: ByteString
    }


data GenBankField
    = LocusField      RawLine
    | DefinitionField RawLines
    | AccessionField  [Accession]
    | VersionField    VersionedAccession
    | DbLinkField     RawLines
    | KeywordsField   RawLines
    | SourceField     RawLines [OrganismSubfield]
    | ReferenceField  RawLines [ReferenceSubfield]
    | CommentField    RawLines
    | PrimaryField    RawLines
    | FeaturesField   [Feature]
    | ContigField     RawLines
    | OriginField     SequenceLines
    | UnknownField    ByteString RawLines
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
    { featureName       :: ByteString
    , featureLocation   :: RawLine
    , featureQualifiers :: [FeatureQualifier]
    }
    deriving Show

data FeatureQualifier = FeatureQualifier ByteString (Maybe QualifierValue)
    deriving Show

data QualifierValue = FreeText [ByteString] | LiteralText ByteString | Numeral Int
    deriving Show

newtype SequenceLines = SequenceLines RawLines
    deriving Show


copyGenBankField :: GenBankField -> GenBankField
copyGenBankField field =
    case field of
        LocusField rl         -> LocusField (copyLine rl)
        DefinitionField rl    -> DefinitionField (copyLines (copyLines rl))
        AccessionField acs    -> AccessionField (map copyAccession acs)
        VersionField va       -> VersionField (copyVersionedAccession va)
        DbLinkField rl        -> DbLinkField (copyLines rl)
        KeywordsField rl      -> KeywordsField (copyLines rl)
        SourceField rl oss    -> SourceField (copyLines rl) (map copyOrgSubfield oss)
        ReferenceField rl rss -> ReferenceField (copyLines rl) (map copyRefField rss)
        CommentField rl       -> CommentField (copyLines rl)
        PrimaryField rl       -> PrimaryField (copyLines rl)
        FeaturesField feas    -> FeaturesField (map copyFeature feas)
        ContigField rl        -> ContigField (copyLines rl)
        OriginField sl        -> OriginField (copySequenceLines sl)
        UnknownField bs rl    -> UnknownField (BS.copy bs) (copyLines rl)
  where
    copyLine :: RawLine -> RawLine
    copyLine (RawLine bs) = RawLine (BS.copy bs)

    copyLines :: RawLines -> RawLines
    copyLines (RawLines bss) = RawLines (map BS.copy bss)

    copyAccession :: Accession -> Accession
    copyAccession (Accession bs) = Accession (BS.copy bs)

    copyVersionedAccession :: VersionedAccession -> VersionedAccession
    copyVersionedAccession (VersionedAccession bs) = VersionedAccession (BS.copy bs)

    copyOrgSubfield :: OrganismSubfield -> OrganismSubfield
    copyOrgSubfield (OrganismSubfield rl) = OrganismSubfield (copyLines rl)

    copyRefField :: ReferenceSubfield -> ReferenceSubfield
    copyRefField (ReferenceAuthors rl)    = ReferenceAuthors (copyLines rl)
    copyRefField (ReferenceConsortium rl) = ReferenceConsortium (copyLines rl)
    copyRefField (ReferenceTitle rl)      = ReferenceTitle (copyLines rl)
    copyRefField (ReferenceJournal rl)    = ReferenceJournal (copyLines rl)
    copyRefField (ReferencePubMed rl)     = ReferencePubMed (copyLines rl)
    copyRefField (ReferenceRemark rl)     = ReferenceRemark (copyLines rl)
    copyRefField (ReferenceMedline rl)    = ReferenceMedline (copyLines rl)

    copyFeature :: Feature -> Feature
    copyFeature (Feature name loc quals) =
        Feature (BS.copy name) (copyLine loc) (map copyQual quals)

    copyQual :: FeatureQualifier -> FeatureQualifier
    copyQual (FeatureQualifier qual val) = FeatureQualifier (BS.copy qual) (fmap copyQualVal val)

    copyQualVal :: QualifierValue -> QualifierValue
    copyQualVal (FreeText bss)   = FreeText (map BS.copy bss)
    copyQualVal (LiteralText bs) = LiteralText (BS.copy bs)
    copyQualVal n@(Numeral _)    = n

    copySequenceLines :: SequenceLines -> SequenceLines
    copySequenceLines (SequenceLines rl) = SequenceLines (copyLines rl)

-- USAGE

data ParseError m r = ParseError
    { parseErrorFilename :: FilePath
    , parseErrorContexts :: [String]
    , parseErrorMessage  :: String
    , parseLeftovers     :: ByteStream m r
    }


instance Show (ParseError m r) where
    show err = unlines $
        [ "Parse error at file " ++ parseErrorFilename err ++ ": " ++ parseErrorMessage err
        , "Context:"
        ]
        ++ parseErrorContexts err


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
    seqLines <- A.many' $ A8.char ' ' *> restOfLine

    return (OriginField (SequenceLines (RawLines seqLines)))


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
