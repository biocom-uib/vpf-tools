{-# language GeneralizedNewtypeDeriving #-}
{-# language RecordWildCards #-}
{-# language StaticPointers #-}
{-# language StrictData #-}
{-# language UndecidableInstances #-}
module VPF.Concurrency.MPI.Polymorphic where

import GHC.StaticPtr

import qualified Control.Distributed.MPI as CMPI
import qualified Control.Distributed.MPI.Store as MPI

import Control.Eff
import Control.Eff.Extend
import Control.Monad (when)
import Control.Monad.IO.Class (MonadIO(..))
import Control.Monad.Catch (MonadMask, bracket, throwM)

import Control.Monad.Base (MonadBase)
import Control.Monad.Trans.Control (MonadBaseControl(..), RunInBase, liftBaseOp)

import Data.Functor.Identity
import Data.Constraint
import Data.Function (fix)
import Data.IORef
import Data.Kind
import qualified Data.List.NonEmpty as NE

import VPF.Concurrency.MPI
import VPF.Concurrency.StoreClosure


data GenericJobTags m = GenericJobTags

newtype ScopedWorker s a b = ScopedWorker (Worker a b)

newtype ScopedBlock s m a = ScopedBlock (m a)

data MPIHandle = MPIHandle
    { rootRank :: MPI.Rank
    , slaveRanks :: NE.NonEmpty MPI.Rank
    , rankTags :: NE.NonEmpty (IORef MPI.Tag)
    , selfRank :: MPI.Rank
    , mpiComm :: MPI.Comm
    }


data MPI (m :: Type -> Type) (a :: Type) where
    GetHandle :: MPI m MPIHandle

    Sending :: Serializer a
            -> Serializer b
            -> Closure m (a -> m b)
            -> MPI m (NE.NonEmpty (a -> IO b))


instance MonadIO io => Handle (MPI m) r c (MPIHandle -> io k) where
    handle step q req h = case req of
        GetHandle -> step (q ^$ h) h

        Sending instA instB (cloF :: Closure m (a -> m b))
          | Dict <- deRefStaticPtr instB -> do

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

            let workers = NE.zipWith worker ranks tags

            step (q ^$ workers) h


instance (MonadIO b, MonadBase b b, LiftedBase b r) => MonadBaseControl b (Eff (MPI m ': r)) where
    type StM (Eff (MPI m ': r)) a = StM (Eff r) a

    liftBaseWith f = do
        h <- send (GetHandle @m)
        raise $ liftBaseWith (\runInBase -> f (liftRunInBase h runInBase))
      where
        liftRunInBase :: MPIHandle -> RunInBase (Eff r) b -> RunInBase (Eff (MPI m ': r)) b
        liftRunInBase h runInBase e = runInBase (fix (handle_relay withMPI) e h)

    restoreM = raise . restoreM


withMPI :: a -> MPIHandle -> Eff r a
withMPI a _ = return a


data DidInit = DidInit | DidNotInit

runMPI :: (MonadMask (Eff r), MonadIO (Eff r)) => MPI.Comm -> Eff (MPI m ': r) a -> Eff r a
runMPI mpiComm e = bracket initMPI finalizeMPI $ do
    h <- liftIO newHandle

    fix (handle_relay withMPI) e h
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


runMPIWorld :: (MonadMask (Eff r), MonadIO (Eff r)) => Eff (MPI m ': r) a -> Eff r a
runMPIWorld = runMPI MPI.commWorld
