{-# language AllowAmbiguousTypes #-}
{-# language DeriveGeneric #-}
{-# language OverloadedStrings #-}
{-# language StrictData #-}
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
import Control.Monad.Catch (MonadThrow, throwM)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (ExceptT, runExceptT, throwE)

import Data.Char (isAlpha)
import Data.Store (Store)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Void

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
    = ExpectedNameLine     Text
    | ExpectedSequenceLine [Text]
  deriving (Show, Generic)

instance Store ParseError


entryName :: HasCallStack => FastaEntry acid -> Text
entryName (FastaEntry n _)
  | T.isPrefixOf (T.pack ">") n = T.tail n
  | otherwise                   = error "entryName: badly constructed FastaEntry"

removeNameComments :: Text -> Text
removeNameComments nameText =
    case T.splitOn (T.pack " #") nameText of
      [name]          -> T.strip name
      (name:_comment) -> T.strip name


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
fastaFileReader p = parsedFastaEntries (fileReader (untag p))


fastaFileWriter :: P.MonadSafe m => Path (FASTA acid) -> Consumer (FastaEntry acid) m ()
fastaFileWriter p = fastaLines >-> fileWriter (untag p)


fastaLines :: Monad m => Pipe (FastaEntry acid) Text m r
fastaLines = P.map (\(FastaEntry name seq) -> name : seq) >-> P.concat


parsedFastaEntries :: Monad m
                   => Producer Text m r
                   -> Producer (FastaEntry acid) m (Either ParseError r)
parsedFastaEntries producer = do
    (r, p') <- P.parsed fastaParser $ producer >-> P.filter (not . T.null)

    case r of
      Nothing -> lift $ fmap Right $ runEffect (p' >-> P.drain)
      Just e  -> return (Left e)


fastaParser :: Monad m => P.Parser Text m (Either (Maybe ParseError) (FastaEntry acid))
fastaParser = runExceptT $ do
    nameLine <- parseNameLine
    sequenceLines <- parseSequenceLines

    return (FastaEntry nameLine sequenceLines)

  where
    parseNameLine = do
      mline <- lift P.draw

      case mline of
        Nothing -> throwE Nothing -- finished parsing

        Just line
          | isNameLine line -> return line
          | otherwise       -> throwE (Just (ExpectedNameLine line))

    parseSequenceLines = do
      sequenceLines <- lift $ zoom (P.span isSequenceLine) P.drawAll

      case sequenceLines of
        [] -> throwE (Just (ExpectedSequenceLine []))
        ls -> return ls

    isNameLine = T.isPrefixOf ">"
    isSequenceLine = not . isNameLine
