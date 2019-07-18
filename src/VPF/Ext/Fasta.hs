{-# language OverloadedStrings #-}
module VPF.Ext.Fasta
  ( FastaEntry(..)
  , fastaSeqLength
  , parseFastaEntries
  , fastaLines
  ) where

import Control.Lens (zoom)
import Control.Monad.Trans.Maybe (runMaybeT)

import Data.Text (Text)
import qualified Data.Text as T
import Data.Void

import Pipes (Pipe, Producer, lift, (>->))
import qualified Pipes.Parse   as P
import qualified Pipes.Prelude as P



data FastaEntry = FastaEntry !Text ![Text]

fastaSeqLength :: FastaEntry -> Int
fastaSeqLength (FastaEntry _ seq) = sum (map T.length seq)


parseFastaEntries :: Monad m => Producer Text m r -> Producer FastaEntry m (Producer Text m r)
parseFastaEntries = P.parsed_ fastaParser


fastaLines :: Monad m => Pipe FastaEntry Text m r
fastaLines = P.map (\(FastaEntry name seq) -> name : seq) >-> P.concat


fastaParser :: Monad m => P.Parser Text m (Maybe FastaEntry)
fastaParser = runMaybeT $ do
    [] <- lift $ zoom (P.span isSequenceLine) P.drawAll
    Just nameLine <- lift P.draw
    seqLines <- lift $ zoom (P.span isSequenceLine) P.drawAll

    return $ FastaEntry nameLine seqLines
  where
    isSequenceLine = not . T.isPrefixOf ">"
