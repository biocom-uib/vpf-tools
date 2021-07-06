{-# language OverloadedStrings #-}
{-# language Strict #-}
module VPF.Ext.HMMER.TableFormat where

import Control.Algebra (Has)
import Control.Effect.Throw (Throw)
import Control.Monad.Catch (MonadThrow)
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Trans.Resource (MonadResource)

import Data.Coerce (coerce)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vector (Vector)

import qualified Data.Vinyl.TypeLevel as V

import Frames (Record, FrameRec)
import qualified Frames                as Fr
import qualified Frames.ColumnTypeable as CSV
import qualified Frames.CSV            as CSV
import qualified Frames.ShowCSV        as CSV
import Frames.InCore (VectorFor, RecVec)

import Streaming (Stream, Of)

import VPF.Formats
import VPF.Frames.DSV qualified as DSV


data Accession = NoAccession | Accession Text
  deriving (Eq, Ord)

instance Fr.Readable Accession where
    fromText s
      | s == "-"  = return NoAccession
      | otherwise = return (Accession s)

instance CSV.Parseable Accession

instance Show Accession where
    show NoAccession   = "-"
    show (Accession t) = show t

instance CSV.ShowCSV Accession where
    showCSV NoAccession   = "-"
    showCSV (Accession t) = CSV.showCSV t

type instance VectorFor Accession = Vector


tokenizeRowWithMaxCols :: Int -> Text -> [Text]
tokenizeRowWithMaxCols = iterBreak
  where
    break :: Text -> (Text, Text)
    break s =
      let (a, b) = T.break (==' ') s
          !b'    = T.dropWhile (==' ') b
      in (a, b')

    iterBreak :: Int -> Text -> [Text]
    iterBreak       0 _ = []
    iterBreak maxCols s =
        case break s of
          ("", _) -> []
          (a, "") -> [a]
          (a, b)  -> a : iterBreak (maxCols-1) b


tblParserOptions :: Int -> DSV.ParserOptions
tblParserOptions maxCols = DSV.ParserOptions
    { DSV.isComment    = T.isPrefixOf "#"
    , DSV.hasHeader    = False
    , DSV.rowTokenizer = tokenizeRowWithMaxCols maxCols
    }


tableAsDSV :: Path (HMMERTable rs) -> Path (DSV " " rs)
tableAsDSV = coerce


produceEitherRows :: forall rs m.
    ( MonadResource m
    , Fr.ColumnHeaders rs, CSV.ReadRec rs, V.NatToInt (V.RLength rs)
    )
    => Path (HMMERTable rs)
    -> Stream (Of (Either DSV.ParseError (Record rs))) m ()
produceEitherRows fp =
    DSV.streamEitherRows (tblParserOptions maxCols) (tableAsDSV fp)
  where
    maxCols = V.natToInt @(V.RLength rs)


streamTableRows :: forall rs m.
    ( MonadResource m
    , MonadThrow m
    , Fr.ColumnHeaders rs, CSV.ReadRec rs, V.NatToInt (V.RLength rs)
    )
    => Path (HMMERTable rs)
    -> Stream (Of (Record rs)) m ()
streamTableRows fp =
    DSV.throwLeftsM $
        DSV.streamEitherRows (tblParserOptions maxCols) (tableAsDSV fp)
  where
    maxCols = V.natToInt @(V.RLength rs)


readTable ::
    ( Fr.ColumnHeaders rs, CSV.ReadRec rs, V.NatToInt (V.RLength rs)
    , Has (Throw DSV.ParseError) m
    , MonadIO m
    , RecVec rs
    )
    => Path (HMMERTable rs)
    -> m (FrameRec rs)
readTable =
    DSV.inCoreAoSExc . streamTableRows
