{-# language DeriveGeneric #-}
{-# language TemplateHaskell #-}
{-# language StaticPointers #-}
{-# language Strict #-}
{-# language UndecidableInstances #-}
module Control.Carrier.Distributed.SingleProcess
  ( SingleProcessT
  , runSingleProcessT
  , LocalWorker
  ) where

import GHC.Generics (Generic)

import Control.Carrier.MTL.TH (deriveMonadTrans, deriveCarrier)
import Control.Distributed.SClosure
import Control.Effect.Distributed

import Data.Store (Store)


newtype SingleProcessT n m a = SingleProcessT { runSingleProcessT :: m a }


deriveMonadTrans ''SingleProcessT


data LocalWorker = LocalWorker
    deriving (Generic, Typeable, Show)

instance Store LocalWorker

instance SInstance (Show LocalWorker) where
    sinst = static Dict

instance SInstance (Serializable LocalWorker) where
    sinst = static Dict


interpretSingleProcessT :: (Monad m, n ~ m) => Distributed n LocalWorker (SingleProcessT n m) a -> SingleProcessT n m a
interpretSingleProcessT (GetNumWorkers k)                 = k 1
interpretSingleProcessT (WithWorkers block k)             = k . pure =<< block LocalWorker
interpretSingleProcessT (RunInWorker LocalWorker _ clo k) = k =<< SingleProcessT (seval clo)


deriveCarrier 'interpretSingleProcessT
