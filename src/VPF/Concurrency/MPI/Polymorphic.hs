{-# language DeriveGeneric #-}
{-# language RecordWildCards #-}
{-# language StaticPointers #-}
{-# language StrictData #-}
{-# language TemplateHaskell #-}
{-# language UndecidableInstances #-}
module VPF.Concurrency.MPI.Polymorphic where

import GHC.StaticPtr

import qualified Control.Distributed.MPI as CMPI
import qualified Control.Distributed.MPI.Store as MPI

import Control.Effect.Carrier
import Control.Effect.MTL.TH
import Control.Monad (when)
import Control.Monad.Catch (MonadMask, bracket, throwM)
import Control.Monad.IO.Class (MonadIO(..))
import qualified Control.Monad.Trans.Reader as MT

import Data.Functor.Identity
import Data.Constraint
import Data.IORef
import Data.Kind
import qualified Data.List.NonEmpty as NE

import VPF.Concurrency.MPI
import VPF.Concurrency.StoreClosure


data GenericJobTags m = GenericJobTags

newtype ScopedWorker s a b = ScopedWorker (Worker a b)

newtype ScopedBlock s m a = ScopedBlock (m a)

data MPIHandle = MPIHandle
    { rootRank   :: MPI.Rank
    , slaveRanks :: NE.NonEmpty MPI.Rank
    , rankTags   :: NE.NonEmpty (IORef MPI.Tag)
    , selfRank   :: MPI.Rank
    , mpiComm    :: MPI.Comm
    }


newtype MpiT m a = MpiT (MT.ReaderT MPIHandle m a)

deriveMonadTrans ''MpiT

data MPI n (m :: Type -> Type) (k :: Type) = forall a b.
    SendMPI (Serializer a) (Serializer b) (Closure n (a -> n b))
            (NE.NonEmpty (a -> n b) -> m k)

instance HFunctor (MPI n) where
    hmap f (SendMPI sa sb clo k) = SendMPI sa sb clo (f . k)

instance Effect (MPI n) where
    handle state handler (SendMPI sa sb clo k) = SendMPI sa sb clo (handler . (<$ state) . k)


interpretMpiT :: (MonadIO m, n ~ IO) => MPI n (MpiT m) a -> MpiT m a
interpretMpiT (SendMPI instA instB (cloF :: Closure n (a -> n b)) k)
  | Dict <- deRefStaticPtr instB = do

      h <- MpiT MT.ask

      let worker :: MPI.Rank -> MPI.Tag -> a -> IO b
          worker rank tag a = do
              let cloPreB = bindClosure cloF (pureClosureWith instA a)
                  dynCloPreB = makeDynClosure instB cloPreB

              maybeCloB <- fmap fromDynClosure $
                  sendrecvYielding_ (StreamItem dynCloPreB) rank tag rank tag (mpiComm h)

              case maybeCloB :: Maybe (Closure Identity b) of
                Just cloB -> return $ runIdentity $ evalClosure cloB
                Nothing   -> error "bad response closure"

      let ranks = slaveRanks h

      tags <-
          let postIncr tag = (succ tag, tag)
          in  liftIO $ mapM (\tagRef -> atomicModifyIORef' tagRef postIncr) (rankTags h)

      k $ NE.zipWith worker ranks tags


deriveCarrier 'interpretMpiT


data DidInit = DidInit | DidNotInit

runMpiT :: (MonadIO m, MonadMask m) => MPI.Comm -> MpiT m a -> m a
runMpiT mpiComm (MpiT e) = bracket (liftIO initMPI) (liftIO . finalizeMPI) $ \_ -> do
    h <- liftIO newHandle
    MT.runReaderT e h
  where
    newHandle :: IO MPIHandle
    newHandle = do
        let rootRank = MPI.rootRank

        selfRank <- liftIO $ MPI.commRank mpiComm
        size <- liftIO $ MPI.commSize mpiComm

        let slaveRanks = NE.fromList [succ rootRank .. pred size]
        rankTags <- liftIO $ traverse (\_ -> newIORef 0) slaveRanks

        return MPIHandle {..}

    initMPI :: IO DidInit
    initMPI = do
        isInit <- CMPI.initialized

        if isInit then
           return DidNotInit

        else do
           ts <- CMPI.initThread CMPI.ThreadMultiple

           when (ts < CMPI.ThreadMultiple) $
               throwM $ MPI.MPIException $
                   "CMPI.init: Insufficient thread support: requiring " ++
                   show CMPI.ThreadMultiple ++
                   ", but CMPI library provided only " ++ show ts
           return DidInit

    finalizeMPI :: DidInit -> IO ()
    finalizeMPI DidInit =
      do isFinalized <- CMPI.finalized
         if isFinalized
           then return ()
           else do CMPI.finalize
    finalizeMPI DidNotInit = return ()


runWorldMpiT :: (MonadIO m, MonadMask m) => MpiT m a -> m a
runWorldMpiT = runMpiT MPI.commWorld
