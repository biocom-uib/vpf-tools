{-# language AllowAmbiguousTypes #-}
{-# language DeriveGeneric #-}
{-# language OverloadedStrings #-}
{-# language Strict #-}
module VPF.Util.Fasta
  ( FastaEntry
  , ParseError(..)
  , entryName
  , removeNameComments
  , entrySeqNumBases
  , fastaFileReader
  , fastaFileWriter
  -- , parsedFastaEntries
  -- , fastaLines
  ) where

import GHC.Generics (Generic)
import GHC.Stack (HasCallStack)

import Control.Lens (zoom)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (runExceptT, throwE)

import Data.Char (isAlpha)
import Data.Store (Store)
import Data.Text (Text)
import qualified Data.Text as T

import Pipes (Consumer, Pipe, Producer, runEffect, (>->))
import qualified Pipes.Parse   as P
import qualified Pipes.Prelude as P
import qualified Pipes.Safe    as P

import VPF.Formats
import VPF.Util.FS (fileReader, fileWriter)


data FastaEntry acid = FastaEntry Text [Text]
  deriving (Eq, Ord, Show, Generic)

instance Store (FastaEntry acid)

data ParseError
    = ExpectedNameLine     FilePath Int Text
    | ExpectedSequenceLine FilePath Int [Text]
  deriving (Show, Generic)

instance Store ParseError


entryName :: HasCallStack => FastaEntry acid -> Text
entryName (FastaEntry n _)
  | T.isPrefixOf (T.pack ">") n = T.tail n
  | otherwise                   = error "entryName: badly constructed FastaEntry"


removeNameComments :: HasCallStack => Text -> Text
removeNameComments nameText =
    case T.splitOn (T.pack " #") nameText of
      [name]          -> T.strip name
      (name:_comment) -> T.strip name
      []              -> error "removeNameComments: bad name line"


class AcidBaseLength acid where
    seqNumBases :: Text -> Int

instance AcidBaseLength Aminoacid where
    seqNumBases =
        T.foldl' (\acc c -> if knownAcid c then acc+1 else acc) 0
      where
        knownAcid 'X' = False
        knownAcid c
          | not (isAlpha c) = False
          | otherwise       = True


entrySeqNumBases :: forall acid. AcidBaseLength acid => FastaEntry acid -> Int
entrySeqNumBases (FastaEntry _ seq) = sum (map (seqNumBases @acid) seq)


fastaFileReader :: forall acid m. P.MonadSafe m
                => Path (FASTA acid)
                -> Producer (FastaEntry acid) m (Either ParseError ())
fastaFileReader p = parsedFastaEntries p (fileReader (untag p))


fastaFileWriter :: P.MonadSafe m => Path (FASTA acid) -> Consumer (FastaEntry acid) m ()
fastaFileWriter p = fastaLines >-> fileWriter (untag p)


fastaLines :: Monad m => Pipe (FastaEntry acid) Text m r
fastaLines = P.map (\(FastaEntry name seq) -> name : seq) >-> P.concat


parsedFastaEntries :: forall m acid r.
    Monad m
    => Path (FASTA acid)
    -> Producer Text m r
    -> Producer (FastaEntry acid) m (Either ParseError r)
parsedFastaEntries fp producer = do
    let linenums :: Producer Int m r
        linenums = P.unfoldr (\i -> return (Right (i, i+1))) 1

        enumerated :: Producer (Int, Text) m r
        enumerated = P.zip linenums producer >-> P.filter (not . T.null . snd)

    (r, p') <- P.parsed (fastaParser fp) enumerated

    case r of
      Nothing -> lift $ fmap Right $ runEffect (p' >-> P.drain)
      Just e  -> return (Left e)


fastaParser ::
    Monad m
    => Path (FASTA acid)
    -> P.Parser (Int, Text) m (Either (Maybe ParseError) (FastaEntry acid))
fastaParser fp = runExceptT $ do
    (linenum, nameLine) <- parseNameLine
    sequenceLines <- parseSequenceLines (linenum+1)

    return (FastaEntry nameLine sequenceLines)

  where
    parseNameLine = do
      mline <- lift P.draw

      case mline of
        Nothing -> throwE Nothing -- finished parsing

        Just (linenum, line)
          | isNameLine line -> return (linenum, line)
          | otherwise       -> throwE (Just (ExpectedNameLine (untag fp) linenum line))

    parseSequenceLines linenum = do
      sequenceLines <- lift $ zoom (P.span (isSequenceLine . snd)) P.drawAll

      case sequenceLines of
        [] -> throwE (Just (ExpectedSequenceLine (untag fp) linenum []))
        ls -> return (map snd ls)

    isNameLine = T.isPrefixOf ">"
    isSequenceLine = not . isNameLine
