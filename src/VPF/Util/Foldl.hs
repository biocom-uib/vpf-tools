module VPF.Util.Foldl where

import Control.Foldl qualified as L

import Data.IntMap.Strict (IntMap)
import Data.IntMap.Strict qualified as IntMap


intMap :: L.Fold (Int, a) (IntMap a)
intMap = L.Fold (\m (i, a) -> IntMap.insert i a m) mempty id


foldByKeyIntMap :: forall a b. L.Fold a b -> L.Fold (Int, a) (IntMap b)
foldByKeyIntMap (L.Fold step begin (end :: x -> b)) = L.Fold step' begin' end'
  where
    step' :: IntMap x -> (Int, a) -> IntMap x
    step' m (i, a) =
        IntMap.alter
            (\case
                Just x  -> Just (step x a)
                Nothing -> Just (step begin a))
            i m

    begin' :: IntMap x
    begin' = mempty

    end' :: IntMap x -> IntMap b
    end' = fmap end
