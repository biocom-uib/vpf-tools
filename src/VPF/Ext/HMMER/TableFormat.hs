{-# language OverloadedStrings #-}
{-# language Strict #-}
module VPF.Ext.HMMER.TableFormat where

import Control.Category ((>>>))

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
import VPF.Util.FS qualified as FS


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


parseEitherRows :: forall rs m.
    ( Monad m
    , CSV.ReadRec rs
    , Fr.ColumnHeaders rs
    , V.NatToInt (V.RLength rs)
    )
    => Path (HMMERTable rs)
    -> Stream (Of Text) m ()
    -> Stream (Of (Either DSV.ParseError (Record rs))) m ()
parseEitherRows fp =
    DSV.parseEitherRows (tblParserOptions maxCols) (tableAsDSV fp)
  where
    maxCols = V.natToInt @(V.RLength rs)


readTableWith ::
    ( Fr.ColumnHeaders cols
    , CSV.ReadRec cols
    , V.NatToInt (V.RLength cols)
    , RecVec cols'
    )
    => (Stream (Of (Either DSV.ParseError (Record cols))) IO ()
        -> Stream (Of (Either DSV.ParseError (Record cols'))) IO ())
    -> Path (HMMERTable cols)
    -> IO (Either DSV.ParseError (FrameRec cols'))
readTableWith f fp =
    FS.withFileRead fp $
        FS.hStreamTextLines
            >>> parseEitherRows fp
            >>> f
            >>> DSV.fromEitherRowStreamAoS


readTable ::
    ( Fr.ColumnHeaders cols
    , CSV.ReadRec cols
    , V.NatToInt (V.RLength cols)
    , RecVec cols
    )
    => Path (HMMERTable cols)
    -> IO (Either DSV.ParseError (FrameRec cols))
readTable = readTableWith id
