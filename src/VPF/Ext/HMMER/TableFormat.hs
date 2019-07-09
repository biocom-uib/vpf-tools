{-# language DeriveDataTypeable #-}
{-# language OverloadedStrings #-}
module VPF.Ext.HMMER.TableFormat where

import Control.Monad.Primitive (PrimMonad)
import Control.Monad.Trans.Except (ExceptT(..), runExceptT)
import Control.Monad.IO.Class (MonadIO)
import Control.Monad.Catch (MonadMask)

import Data.Text (Text)
import qualified Data.Text as T
import Data.Typeable (Typeable)
import qualified Data.Vector as V (Vector)

import Data.Vinyl (Rec, ElField, rtraverse)
import Data.Vinyl.Functor (Compose(..), (:.))
import Data.Vinyl.Lens (type (<:), rcast)
import Data.Vinyl.TypeLevel (NatToInt(natToInt), RLength)

import Frames (Record, FrameRec)
import qualified Frames                as Fr
import qualified Frames.ColumnTypeable as CSV
import qualified Frames.CSV            as CSV
import qualified Frames.InCore         as FiC

import Pipes ((>->), Pipe, Producer)
import qualified Pipes as P
import qualified Pipes.Prelude as P
import qualified Pipes.Parse as P
import Pipes.Safe (MonadSafe, SafeT)
import qualified Pipes.Safe.Prelude as Safe


import VPF.Formats


data Accession = NoAccession | Accession Text
  deriving (Eq, Ord)

instance Show Accession where
  show NoAccession   = "-"
  show (Accession t) = show t

instance Fr.Readable Accession where
  fromText s
    | s == "-"  = return NoAccession
    | otherwise = return (Accession s)

instance CSV.Parseable Accession

type instance FiC.VectorFor Accession = V.Vector


newtype RowParseError = RowParseError Text
  deriving (Eq, Ord, Show)


tokenizeRowWithMaxCols :: Int -> Text -> [Text]
tokenizeRowWithMaxCols = iterBreak
  where
    break :: Text -> (Text, Text)
    break s =
      let (a, b) = T.break (==' ') s
          !b'    = T.dropWhile (==' ') b
      in (a, b')

    iterBreak :: Int -> Text -> [Text]
    iterBreak       0 s = []
    iterBreak maxCols s =
        case break s of
          ("", _) -> []
          (a, "") -> [a]
          (a, b)  -> a : iterBreak (maxCols-1) b



readTableRow :: forall rs. (NatToInt (RLength rs), CSV.ReadRec rs)
              => Text -> Rec (Either Text :. ElField) rs
readTableRow = CSV.readRec . tokenizeRowWithMaxCols maxCols
  where
    maxCols = natToInt @(RLength rs)


pipeEitherTableRows :: (Monad m, NatToInt (RLength rs), CSV.ReadRec rs)
                    => Pipe Text (Either RowParseError (Record rs)) m ()
pipeEitherTableRows = P.map $ \row ->
    case rtraverse getCompose (readTableRow row) of
        Left  _   -> Left (RowParseError row)
        Right rec -> Right rec


produceEitherTableRows :: (MonadSafe m, NatToInt (RLength rs), CSV.ReadRec rs)
                       => Path (WithComments (HMMERTable rs))
                       -> Producer (Either RowParseError (Record rs)) m ()
produceEitherTableRows fp =
    CSV.produceTextLines (untag fp)
    >-> P.filter (\row -> not ("#" `T.isPrefixOf` row))
    >-> pipeEitherTableRows


produceTableRows :: forall rs m.
                 ( NatToInt (RLength rs)
                 , CSV.ReadRec rs
                 , MonadIO m
                 , MonadMask m
                 )
                 => Path (WithComments (HMMERTable rs))
                 -> Producer (Record rs) (SafeT (ExceptT RowParseError m)) ()
produceTableRows fp =
    produceEitherTableRows fp
    >-> P.mapM throwLefts
  where
    throwLefts :: Either e a -> SafeT (ExceptT e m) a
    throwLefts = P.lift . ExceptT . return


inCoreEitherAoS :: forall m rs.
                ( FiC.RecVec rs
                , MonadIO m
                , MonadMask m
                , PrimMonad m
                )
                => Producer (Record rs) (SafeT (ExceptT RowParseError m)) ()
                -> m (Either RowParseError (FrameRec rs))
inCoreEitherAoS = runExceptT . Fr.inCoreAoS


readAsFrame :: forall rs rs' m.
            ( NatToInt (RLength rs)
            , CSV.ReadRec rs
            , FiC.RecVec rs'
            , rs' <: rs
            , MonadIO m
            , MonadMask m
            , PrimMonad m
            )
            => Path (WithComments (HMMERTable rs))
            -> Pipe (Record rs) (Record rs') m ()
            -> m (Either RowParseError (FrameRec rs'))
readAsFrame fp pipe =
    inCoreEitherAoS (produceTableRows fp >-> P.map rcast)
  where
    pipe' :: Pipe (Record rs) (Record rs') (SafeT (ExceptT Text m)) ()
    pipe' = P.hoist P.lift (P.hoist P.lift pipe)
