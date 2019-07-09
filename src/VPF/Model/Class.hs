module VPF.Model.Class
  ( rawClassification
  , rawClassificationStream
  , loadRawClassification
  , classificationStream
  , loadClassification
  , RawClassification
  , Classification
  ) where

import Control.Lens (Iso', from, iso, (^.))

import Frames (Frame, inCoreAoS)
import Frames.CSV (readTableOpt)

import Pipes (Producer, (>->))
import qualified Pipes.Prelude as P
import Pipes.Safe (MonadSafe)

import VPF.Model.Class.Cols (Classification, RawClassification,
                             rawClassificationParser,
                             fromRawClassification,
                             toRawClassification)



rawClassification :: Iso' Classification RawClassification
rawClassification = iso toRawClassification fromRawClassification


rawClassificationStream :: MonadSafe m => Producer RawClassification m ()
rawClassificationStream = readTableOpt rawClassificationParser "../data/classification.tsv"

loadRawClassification :: IO (Frame RawClassification)
loadRawClassification = inCoreAoS rawClassificationStream


classificationStream :: MonadSafe m => Producer Classification m ()
classificationStream = rawClassificationStream >-> P.map (^. from rawClassification)

loadClassification :: IO (Frame Classification)
loadClassification = inCoreAoS classificationStream
