{-# language RecordWildCards #-}
{-# language StaticPointers #-}
{-# language StrictData #-}
{-# language TemplateHaskell #-}
{-# language UndecidableInstances #-}
module Control.Distributed.MpiT where

import qualified Control.Concurrent.Async.Lifted as Async
import qualified Control.Distributed.MPI as CMPI
import qualified Control.Distributed.MPI.Store as MPI

import Control.Effect.MTL.TH
import Control.Monad (when)
import qualified Control.Monad.Catch as MC
import Control.Monad.IO.Class (MonadIO(..))
import Control.Monad.Trans.Control (MonadBaseControl(..))
import qualified Control.Monad.Trans.Reader as MT

import Data.Bifunctor (second)
import Data.Constraint
import Data.Functor.Identity
import Data.IORef
import qualified Data.List.NonEmpty as NE

import qualified Pipes         as P
import qualified Pipes.Prelude as P

import Type.Reflection (Typeable)

import qualified VPF.Concurrency.MPI as PM
import Control.Distributed.StoreClosure
import Control.Effect.Distributed


data MPIHandle = MPIHandle
    { rootRank   :: MPI.Rank
    , slaveRanks :: NE.NonEmpty MPI.Rank
    , rankTags   :: NE.NonEmpty (IORef MPI.Tag)
    , selfRank   :: MPI.Rank
    , mpiComm    :: MPI.Comm
    }


newtype MpiMasterT n m a = MpiMasterT { runMpiMasterT :: MT.ReaderT MPIHandle m a }


deriveMonadTrans ''MpiMasterT


data Worker n = Worker MPI.Rank MPI.Tag

data Request m a = Request (Dict (Serializable a)) (m a)

type Response = Identity

getResponse :: Response a -> a
getResponse = runIdentity


interpretMpiMasterT ::
    (MonadIO m, MonadBaseControl IO m, MonadIO n, Typeable n)
    => Distributed n Worker (MpiMasterT n m) k
    -> MpiMasterT n m k
interpretMpiMasterT (WithWorkers (block :: forall (s :: *). Scoped s (Worker n) -> ScopeT s (MpiMasterT n m) a) k) = do
      h <- MpiMasterT MT.ask

      let worker :: MPI.Rank -> MPI.Tag -> MpiMasterT n m a
          worker rank tag = runScopeT (block (scope (Worker rank tag)))

      let ranks = slaveRanks h

      tags <-
          let postIncr tag = (succ tag, tag)
          in  liftIO $ mapM (\tagRef -> atomicModifyIORef' tagRef postIncr) (rankTags h)

      results <- Async.mapConcurrently id (NE.zipWith worker ranks tags)
      k results

interpretMpiMasterT (RunInWorker (Worker rank tag) (clo :: Closure (n a)) k)
  | sinst <- staticInstance @(Serializable a) = withInstance sinst $ do
      h <- MpiMasterT MT.ask

      let cloF = liftC2 (static Request) sinst clo

          dynCloF = reflectInstance (weakenInstance @(Typeable a) (static Impl) sinst) $
              makeDynClosure @a cloF

      maybeClo <- liftIO $ fmap fromDynClosure $
          PM.sendrecvYielding_ (PM.StreamItem dynCloF) rank tag rank tag (mpiComm h)

      case maybeClo of
        Just clo' -> k $ getResponse $ evalClosure clo'
        Nothing   -> error "bad response closure"


deriveCarrier 'interpretMpiMasterT


data DidInit = DidInit | DidNotInit


runMpi :: forall m.
    ( MonadIO m
    , MC.MonadMask m
    )
    => MPI.Comm
    -> MpiMasterT m m ()
    -> m ()
runMpi mpiComm (MpiMasterT e) = do
    self <- liftIO $ MPI.commRank mpiComm

    ((), ()) <- MC.generalBracket (liftIO initMPI) (\() -> liftIO . finalizeMPI) $ \_ -> do
        if self == MPI.rootRank then
            MC.bracket (liftIO newHandle) (liftIO . finalizeHandle) $
                MT.runReaderT e
        else
            workerProcess

    return ()
  where
    initMPI :: IO ()
    initMPI = do
        isInit <- CMPI.initialized

        when isInit $
            MC.throwM $ MPI.MPIException $ "initMPI: MPI is already initialized!"

        ts <- CMPI.initThread CMPI.ThreadMultiple

        when (ts < CMPI.ThreadMultiple) $
            MC.throwM $ MPI.MPIException $
                "CMPI.init: Insufficient thread support: requiring "
                  ++ show CMPI.ThreadMultiple
                  ++ ", but CMPI library provided only " ++ show ts


    finalizeMPI :: MC.ExitCase () -> IO ()
    finalizeMPI ec = do
        isFinalized <- CMPI.finalized

        if isFinalized then
            MC.throwM $ MPI.MPIException $ "finalizeMPI: MPI is already finalized!"

        else
            case ec of
              MC.ExitCaseSuccess () -> MPI.barrier mpiComm >> CMPI.finalize

    newHandle :: IO MPIHandle
    newHandle = do
        initMPI

        let rootRank = MPI.rootRank

        selfRank <- liftIO $ MPI.commRank mpiComm
        size <- liftIO $ MPI.commSize mpiComm

        let slaveRanks = NE.fromList [succ rootRank .. pred size]
        rankTags <- liftIO $ traverse (\_ -> newIORef 0) slaveRanks

        return MPIHandle {..}


    finalizeHandle :: MPIHandle -> IO ()
    finalizeHandle h = do
        let closeRank rank = MPI.send (PM.StreamEnd @(DynClosure (Request m))) rank 0 mpiComm
        mapM_ closeRank (slaveRanks h)


    workerProcess :: m ()
    workerProcess = do
        let eval :: DynClosure (Request m) -> DynClosure Response
            eval = undefined

        P.runEffect $
            PM.messagesFrom @(DynClosure (Request m)) MPI.anySource MPI.anyTag mpiComm
            P.>-> P.map (second eval)
            P.>-> PM.genericSender @(DynClosure Response) mpiComm


runMpiWorld :: (MonadIO m, MC.MonadMask m) => MpiMasterT m m () -> m ()
runMpiWorld = runMpi MPI.commWorld

