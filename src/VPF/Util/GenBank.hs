{-# language GeneralizedNewtypeDeriving #-}
{-# language OverloadedStrings #-}
{-# language PackageImports #-}
{-# language TupleSections #-}
module VPF.Util.GenBank where

import Control.Applicative
import Control.Monad (guard, replicateM)

import Data.Attoparsec.ByteString (Parser)
import Data.Attoparsec.ByteString qualified as A
import Data.Attoparsec.ByteString.Char8 qualified as A8

import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BS8
import Data.ByteString.Internal qualified as BS8 (c2w)

import Streaming (Stream, Of, effect)
import Streaming.ByteString (ByteStream)
import "streaming-attoparsec" Data.Attoparsec.ByteString.Streaming qualified as SA

import Data.Maybe


newtype RawLine = RawLine ByteString
    deriving newtype Show

newtype RawLines = RawLines [ByteString]
    deriving newtype Show

newtype Accession = Accession ByteString
    deriving newtype Show

newtype VersionedAccession = VersionedAccession ByteString
    deriving newtype Show


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


-- USAGE

--- withBinaryFile (Tagged "/home/biel/documents/UIB/Doc/research/jgi/genbank_gbvrl1.seq.gz") ReadMode $
---     BSS.fromHandle
---     >>> SZ.gunzip
---     >>> AS.parse headerP
---     >>> fmap snd
---     >>> BSS.mwrap
---     >>> AS.parsed entryP


data ParseError m r = ParseError
    { parseErrorFilename :: FilePath
    , parseErrorContexts :: [String]
    , parseErrorMessage  :: String
    , parseLeftovers     :: ByteStream m r
    }


parseGenBankStream ::
    Monad m
    => FilePath
    -> ByteStream m r
    -> m (Either (ParseError m r)
            ( GenBankHeader
            , Stream (Of GenBankRecord) m (Either (ParseError m r) r)
            ))
parseGenBankStream filename stream = do
    (headerRes, stream') <- SA.parse headerP stream

    return $ case headerRes of
        Left (ctx, e) ->
            Left (ParseError filename ctx e stream')

        Right header ->
            Right
                ( header
                , ignoringLeftovers <$> SA.parsed (toRecord <$> A.match entryP) stream'
                )
  where
    toRecord :: (ByteString, [GenBankField]) -> GenBankRecord
    toRecord = uncurry (flip GenBankRecord)

    ignoringLeftovers :: Either (SA.Errors, ByteStream m r) r -> Either (ParseError m r) r
    ignoringLeftovers (Left ((ctx, e), l)) = Left (ParseError filename ctx e l)
    ignoringLeftovers (Right r)            = Right r



parseGenBankStream_ ::
    Monad m
    => FilePath
    -> ByteStream m r
    -> Stream (Of GenBankRecord) m (Either (ParseError m r) r)
parseGenBankStream_ filename stream = effect do
    headerRes <- parseGenBankStream filename stream

    case headerRes of
        Left e ->
            return (pure (Left e))

        Right (_header, stream') ->
            return stream'


headerP :: Parser GenBankHeader
headerP =
    GenBankHeader <$> RawLines <$> replicateM 10 restOfLine


entryP :: Parser [GenBankField]
entryP = do
    fields <- A.many1 fieldP
    entrySeparatorP
    return fields


entrySeparatorP :: Parser ()
entrySeparatorP = (A.<?> "entry separator" ) do
    _ <- A8.string "//" A.<?> "entry separator (//)"
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
        "FEATURES"   -> featuresP
        "CONTIG"     -> contigP
        "ORIGIN"     -> originP
        kw           -> unknownP kw


locusP :: A.Parser GenBankField
locusP = LocusField . RawLine <$> restOfLine
    A.<?> "LOCUS line"


definitionP :: A.Parser GenBankField
definitionP = DefinitionField <$> restOfField
    A.<?> "DEFINITION field"


accessionP :: A.Parser GenBankField
accessionP = (A.<?> "ACCESSION field") do
    A8.skipSpace

    accessions <- accessionNumberP `A.sepBy1` A8.char ' '
    _ <- restOfLine

    return $ AccessionField (concatMap splitAccessionRange accessions)
  where
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

    subfields <- catMaybes <$> A.many1 subfieldsP

    return (ReferenceField refLines subfields)
  where
    subfieldsP = (A.<?> "one REFERENCE subfield") do
        _ <- A.string "  "
        subkw <- BS8.strip <$> A.take 10

        case subkw of
            "AUTHORS" -> Just . ReferenceAuthors    <$> restOfField
            "CONSRTM" -> Just . ReferenceConsortium <$> restOfField
            "TITLE"   -> Just . ReferenceTitle      <$> restOfField
            "JOURNAL" -> Just . ReferenceJournal    <$> restOfField
            "PUBMED"  -> Just . ReferencePubMed     <$> restOfField
            "REMARK"  -> Just . ReferenceRemark     <$> restOfField
            _         -> return Nothing


commentP :: A.Parser GenBankField
commentP = CommentField <$> restOfField
    A.<?> "COMMENT field"


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
    seqLines <- A.many' $ A.string "   " *> restOfLine

    return (OriginField (SequenceLines (RawLines seqLines)))


unknownP :: ByteString -> A.Parser GenBankField
unknownP kw = UnknownField kw <$> restOfField
    A.<?> ("unknown field " ++ show kw)


restOfField :: A.Parser RawLines
restOfField = RawLines <$> liftA2 (:) restOfLine continuationLines


restOfLine :: A.Parser ByteString
restOfLine = BS8.strip <$> A.takeWhile (not . A8.isEndOfLine) <* A8.endOfLine


continuationLines :: A.Parser [ByteString]
continuationLines = A.many' continuationLine
  where
    continuationLine = do
        _ <- A.string (BS8.replicate 10 ' ')
        restOfLine
