module VPF.Model.Class
  ( rawClassification
  , rawClassificationStream
  , loadRawClassification
  , classificationStream
  , loadClassification
  , Classification
  , ClassificationCols
  , RawClassification
  , RawClassificationCols
  ) where

import Control.Eff
import Control.Eff.Exception (Exc)
import Control.Lens (Iso', from, iso, (^.))
import Control.Monad.IO.Class (MonadIO)

import Frames (Frame, inCoreAoS)
import Frames.CSV (readTableOpt)

import Pipes (Producer, (>->))
import qualified Pipes.Prelude as P
import Pipes.Safe (MonadSafe)

import VPF.Model.Class.Cols (Classification, RawClassification,
                             ClassificationCols, RawClassificationCols,
                             fromRawClassification,
                             toRawClassification)

import VPF.Formats
import qualified VPF.Util.DSV as DSV


rawClassification :: Iso' Classification RawClassification
rawClassification = iso toRawClassification fromRawClassification


rawClassificationStream :: (MonadIO m, MonadSafe m)
                        => Path (DSV "\t" RawClassificationCols)
                        -> Producer RawClassification m ()
rawClassificationStream fp =
    DSV.produceEitherRows fp opts >-> DSV.throwLeftsM
  where
    opts = (DSV.defParserOptions '\t') { DSV.hasHeader = False }

loadRawClassification :: (Lifted IO r, Member (Exc DSV.RowParseError) r)
                      => Path (DSV "\t" RawClassificationCols)
                      -> Eff r (Frame RawClassification)
loadRawClassification = DSV.inCoreAoSExc . rawClassificationStream


classificationStream :: MonadSafe m
                     => Path (DSV "\t" RawClassificationCols)
                     -> Producer Classification m ()
classificationStream fp =
    rawClassificationStream fp >-> P.map (^. from rawClassification)


loadClassification :: (Lifted IO r, Member (Exc DSV.RowParseError) r)
                   => Path (DSV "\t" RawClassificationCols)
                   -> Eff r (Frame Classification)
loadClassification = DSV.inCoreAoSExc . classificationStream
