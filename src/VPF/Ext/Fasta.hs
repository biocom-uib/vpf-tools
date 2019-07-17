{-# language OverloadedStrings #-}
module VPF.Ext.Fasta
  ( fastaGroups
  , ungroupFasta
  ) where

import Control.Lens (zoom)
import Control.Monad.Trans.Maybe (runMaybeT)

import Data.Text (Text)
import qualified Data.Text as T
import Data.Void

import Pipes (Pipe, Producer, lift)
import qualified Pipes.Parse   as P
import qualified Pipes.Prelude as P


fastaGroups :: Monad m => Producer Text m r -> Producer [Text] m (Producer Text m r)
fastaGroups = P.parsed_ fastaParser


ungroupFasta :: (Functor m, Foldable f) => Pipe (f Text) Text m r
ungroupFasta = P.concat


fastaParser :: Monad m => P.Parser Text m (Maybe [Text])
fastaParser = runMaybeT $ do
    rows1 <- lift $ zoom (P.span isSequenceLine) P.drawAll
    Just nameLine <- lift P.draw
    seqLines <- lift $ zoom (P.span isSequenceLine) P.drawAll

    return $ rows1 ++ (nameLine : seqLines)
  where
    isSequenceLine = not . T.isPrefixOf ">"
