{-# language TemplateHaskell #-}
{-# language UndecidableInstances #-}
module Control.Carrier.Distributed.SingleProcess
  ( SingleProcessT
  , runSingleProcessT
  , LocalWorker
  ) where

import Control.Distributed.SClosure (seval)

import Control.Carrier.MTL.TH (deriveMonadTrans, deriveCarrier)

import Control.Effect.Distributed


newtype SingleProcessT n m a = SingleProcessT { runSingleProcessT :: m a }


deriveMonadTrans ''SingleProcessT


data LocalWorker n = LocalWorker


interpretSingleProcessT :: (Monad m, n ~ m) => Distributed n LocalWorker (SingleProcessT n m) a -> SingleProcessT n m a
interpretSingleProcessT (GetNumWorkers k)                 = k 1
interpretSingleProcessT (WithWorkers block k)             = k . pure =<< block LocalWorker
interpretSingleProcessT (RunInWorker LocalWorker _ clo k) = k =<< SingleProcessT (seval clo)


deriveCarrier 'interpretSingleProcessT
