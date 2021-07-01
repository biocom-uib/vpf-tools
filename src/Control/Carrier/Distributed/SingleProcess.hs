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

import Data.Store (Store)
import Control.Algebra.Helpers (relayAlgebraUnwrap)


newtype SingleProcessT m a = SingleProcessT { runSingleProcessT :: m a }

deriveMonadTrans ''SingleProcessT


data LocalWorker = LocalWorker
    deriving (Generic, Typeable, Show)

instance Store LocalWorker

instance SInstance (Show LocalWorker) where
    sinst = static Dict

instance SInstance (Serializable LocalWorker) where
    sinst = static Dict


interpretSingleProcessT :: Monad m => Distributed m LocalWorker (SingleProcessT m) a -> SingleProcessT m a
interpretSingleProcessT GetNumWorkers                   = return 1
interpretSingleProcessT (WithWorkers block)             = pure <$> block LocalWorker
interpretSingleProcessT (RunInWorker LocalWorker _ clo) = SingleProcessT $ seval clo


instance (Monad m, Algebra ctx m) => Algebra ctx (SingleProcessT m) where
    type Sig (SingleProcessT m) = Distributed m LocalWorker :+: Sig m

    alg hdl sig ctx =
        case sig of
            L GetNumWorkers                   -> SingleProcessT $ fmap (<$ ctx) $ return 1
            L (WithWorkers block)             -> SingleProcessT $ runSingleProcessT $ fmap pure <$> hdl (block LocalWorker <$ ctx)
            L (RunInWorker LocalWorker _ clo) -> SingleProcessT $ fmap (<$ ctx) $ (seval clo)
            R (other :: (Sig m n a))          -> relayAlgebraUnwrap SingleProcessT hdl other ctx
              -- alg (runSingleProcessT . hdl) other ctx

