{-# language OverloadedStrings #-}
{-# language StrictData #-}
module VPF.Util.Fasta
  ( FastaEntry(..)
  , ParseError(..)
  , fastaSeqLength
  , parsedFastaEntries
  , fastaLines
  ) where

import Control.Lens (zoom)
import Control.Monad.Catch (MonadThrow, throwM)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.Except (ExceptT, runExceptT, throwE)

import Data.Text (Text)
import qualified Data.Text as T
import Data.Void

import Pipes (Pipe, Producer, runEffect, (>->))
import qualified Pipes.Parse   as P
import qualified Pipes.Prelude as P



data FastaEntry = FastaEntry Text [Text]

data ParseError
    = ExpectedNameLine     Text
    | ExpectedSequenceLine [Text]

fastaSeqLength :: FastaEntry -> Int
fastaSeqLength (FastaEntry _ seq) = sum (map T.length seq)


parsedFastaEntries :: Monad m => Producer Text m r -> Producer FastaEntry m (Either ParseError r)
parsedFastaEntries producer = do
    (r, p') <- P.parsed fastaParser $ producer >-> P.filter (not . T.null)

    case r of
      Nothing -> lift $ fmap Right $ runEffect (p' >-> P.drain)
      Just e  -> return (Left e)


fastaLines :: Monad m => Pipe FastaEntry Text m r
fastaLines = P.map (\(FastaEntry name seq) -> name : seq) >-> P.concat


fastaParser :: Monad m => P.Parser Text m (Either (Maybe ParseError) FastaEntry)
fastaParser = runExceptT $ do
    nameLine <- parseNameLine
    sequenceLines <- parseSequenceLines

    return (FastaEntry nameLine sequenceLines)

  where
    parseNameLine = do
      nameLines <- lift $ zoom (P.span isNameLine) P.drawAll

      case nameLines of
        []         -> throwE Nothing -- finished parsing
        [nameLine] -> return nameLine
        (_:ls)     -> throwE (Just (ExpectedSequenceLine ls))

    parseSequenceLines = do
      sequenceLines <- lift $ zoom (P.span isSequenceLine) P.drawAll

      case sequenceLines of
        [] -> throwE (Just (ExpectedSequenceLine []))
        ls -> return ls

    isNameLine = T.isPrefixOf ">"
    isSequenceLine = not . isNameLine
