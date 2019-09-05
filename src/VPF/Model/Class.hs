module VPF.Model.Class
  ( rawClassification
  , rawClassificationStream
  , loadRawClassification
  , loadClassification
  , Classification
  , ClassificationCols
  , RawClassification
  , RawClassificationCols
  ) where

import Control.Eff
import Control.Eff.Exception (Exc)
import Control.Lens (Iso', from, iso, view, (%~))
import Control.Monad.IO.Class (MonadIO)

import Frames (Frame)

import Pipes (Producer, (>->))
import Pipes.Safe (MonadSafe)

import qualified VPF.Frames.Dplyr as F

import VPF.Model.Class.Cols (Classification, RawClassification,
                             ClassificationCols, RawClassificationCols,
                             fromRawClassification,
                             toRawClassification)

import VPF.Formats
import qualified VPF.Frames.DSV as DSV


rawClassification :: Iso' (Frame Classification) (Frame RawClassification)
rawClassification = iso (F.reindexed' @"model_name" %~ toRawClassification)
                        (F.reindexed' @"model_name" %~ fromRawClassification)


rawClassificationStream :: (MonadIO m, MonadSafe m)
                        => Path (DSV "\t" RawClassificationCols)
                        -> Producer RawClassification m ()
rawClassificationStream fp =
    DSV.produceEitherRows fp opts >-> DSV.throwLeftsM
  where
    opts = (DSV.defParserOptions '\t') { DSV.hasHeader = False }


loadRawClassification :: (Lifted IO r, Member (Exc DSV.ParseError) r)
                      => Path (DSV "\t" RawClassificationCols)
                      -> Eff r (Frame RawClassification)
loadRawClassification = DSV.inCoreAoSExc . rawClassificationStream


loadClassification :: (Lifted IO r, Member (Exc DSV.ParseError) r)
                   => Path (DSV "\t" RawClassificationCols)
                   -> Eff r (Frame Classification)
loadClassification = fmap (view (from rawClassification)) . DSV.inCoreAoSExc . rawClassificationStream
