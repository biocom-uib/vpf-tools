{-# language DeriveGeneric #-}
{-# language NoPolyKinds #-}
{-# language StaticPointers #-}
{-# language Strict #-}
{-# language TemplateHaskell #-}
{-# language UndecidableInstances #-}
module Control.Carrier.Distributed.SingleProcess
  ( SingleProcessT
  , runSingleProcessT
  , LocalWorker
  ) where

import GHC.Generics (Generic)

import Control.Algebra
import Control.Carrier.MTL.TH (deriveMonadTrans)
import Control.Distributed.SClosure
import Control.Effect.Distributed

import Control.Algebra.Helpers (algUnwrapL)

import Data.Store (Store)


newtype SingleProcessT m a = SingleProcessT { runSingleProcessT :: m a }

deriveMonadTrans ''SingleProcessT


data LocalWorker = LocalWorker
    deriving (Generic, Typeable, Show)

instance Store LocalWorker

instance SInstance (Show LocalWorker) where
    sinst = static Dict

instance SInstance (Serializable LocalWorker) where
    sinst = static Dict


instance Algebra ctx m => Algebra ctx (SingleProcessT m) where
    type Sig (SingleProcessT m) = Distributed m LocalWorker :+: Sig m

    alg = algUnwrapL id SingleProcessT \hdl sig ctx ->
        case sig of
            GetNumWorkers                 -> return (1 <$ ctx)
            WithWorkers block             -> hdl (block LocalWorker <$ ctx)
            RunInWorker LocalWorker _ clo -> SingleProcessT $ (<$ ctx) <$> seval clo
