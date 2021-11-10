module VPF.Util.Foldl where

import Control.Foldl qualified as L

import Data.IntMap.Strict (IntMap)
import Data.IntMap.Strict qualified as IntMap


intMap :: L.Fold (Int, a) (IntMap a)
intMap = L.Fold (flip (uncurry IntMap.insert)) mempty id
