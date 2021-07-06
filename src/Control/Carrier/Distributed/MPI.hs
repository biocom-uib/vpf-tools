{-# language DeriveGeneric #-}
{-# language GeneralizedNewtypeDeriving #-}
{-# language QuantifiedConstraints #-}
{-# language RecordWildCards #-}
{-# language StaticPointers #-}
{-# language Strict #-}
{-# language TemplateHaskell #-}
{-# language UndecidableInstances #-}
module Control.Carrier.Distributed.MPI
  ( MpiMasterT
  , MPIWorker
  , WorkerException(..)
  , ExitHandlers
  , MonadBaseControlStore
  , abortOnError
  , finalizeOnError
  , runMpi
  , runMpiWorld
  ) where

import GHC.Generics (Generic)

import Control.Algebra
import Control.Algebra.Helpers (algUnwrapL)
import Control.Effect.Sum.Extra (injR)

import qualified Control.Concurrent.Async.Lifted as Async

import qualified Control.Distributed.MPI           as CMPI
import qualified Control.Distributed.MPI.Store     as MPI
import qualified Control.Distributed.MPI.Streaming as SMPI

import Control.Carrier.MTL.TH (deriveMonadTrans)

import Control.Lens (_2)
import Control.Monad (when)
import qualified Control.Monad.Catch as MC
import Control.Monad.IO.Class (MonadIO(..))
import Control.Monad.Trans.Control (MonadBaseControl(..))
import qualified Control.Monad.Trans.Reader as MT

import qualified Data.ByteString as BS
import Data.Constraint
import Data.Function ((&))
import Data.Functor.Apply (Apply)
import Data.IORef
import Data.Kind (Type)
import Data.Semigroup.Traversable (sequence1)
import Data.Store (Store)
import qualified Data.Store as Store
import qualified Data.List.NonEmpty as NE

import Streaming.Prelude qualified as S

import Control.Distributed.SClosure
import Control.Effect.Distributed


data MPIHandle (n :: Type -> Type) = MPIHandle
    { rootRank   :: MPI.Rank
    , slaveRanks :: NE.NonEmpty MPI.Rank
    , rankTags   :: NE.NonEmpty (IORef MPI.Tag)
    , selfRank   :: MPI.Rank
    , mpiComm    :: MPI.Comm
    }


newtype MpiMasterT n m a = MpiMasterT (MT.ReaderT (MPIHandle n) m a)


deriveMonadTrans ''MpiMasterT


data MPIWorker = MPIWorker MPI.Rank MPI.Tag
    deriving (Generic, Show)

instance Store MPI.Rank
instance Store MPI.Tag
instance Store MPIWorker


instance SInstance (Show MPIWorker) where
    sinst = static Dict

instance SInstance (Serializable MPIWorker) where
    sinst = static Dict


data Request m a = Request (Dict (Serializable a)) (m a)


data Response m = ResponseException !String | ResponseOk !BS.ByteString
    deriving (Generic)

instance Store (Response m)


newtype WorkerException = WorkerException { workerExceptionMessage :: String }
    deriving (Eq, Show)

instance MC.Exception WorkerException


newtype StM' m a = StM' { unStM' :: StM m a }

deriving instance Store (StM m a) => Store (StM' m a)


class MonadBaseControl b m => MonadBaseControlStore b m where
    instStoreBase :: forall a. Store a :- Store (StM' m a)

instance (MonadBaseControl b m, forall a. Store a => Store (StM' m a)) => MonadBaseControlStore b m where
    instStoreBase = Sub Dict


instance
    ( Apply ctx
    , Algebra ctx m
    , MonadBaseControlStore IO m
    , MonadIO m
    , MC.MonadThrow m
    , Typeable m
    , m ~ n
    )
    => Algebra ctx (MpiMasterT n m) where

    type Sig (MpiMasterT n m) = Distributed n MPIWorker :+: Sig m

    alg = algUnwrapL injR MpiMasterT \hdl sig ctx ->
        case sig of
            GetNumWorkers ->
                MpiMasterT $ MT.asks ((<$ ctx) . fromIntegral . length . slaveRanks)

            WithWorkers (block :: MPIWorker -> n0 a) -> do
                h <- MpiMasterT MT.ask

                let worker :: MPI.Rank -> MPI.Tag -> MpiMasterT n m (ctx a)
                    worker rank tag = hdl $ block (MPIWorker rank tag) <$ ctx

                let ranks = slaveRanks h

                tags <-
                    let postIncr tag = (succ tag, tag)
                    in  liftIO $ mapM (\tagRef -> atomicModifyIORef' tagRef postIncr) (rankTags h)

                rs <- Async.mapConcurrently id (NE.zipWith worker ranks tags)

                return (sequence1 rs)


            RunInWorker (MPIWorker rank tag) sdict (clo :: SClosure (n a)) ->
                withSDict sdict do
                    h <- MpiMasterT MT.ask

                    let reqClo :: SClosure (Request n a)
                        reqClo = slift2 (static Request) sdict clo

                        tdict :: SDict (Typeable a)
                        tdict = weakenSDict @(Typeable a) (static Impl) sdict

                        reqDynClo :: SDynClosure (Request n)
                        reqDynClo = reflectSDict tdict (makeSDynClosure reqClo)

                    response <- liftIO $ SMPI.sendrecvYielding_ (SMPI.StreamItem reqDynClo) rank tag rank tag (mpiComm h)

                    case response :: Response n of
                      ResponseException e ->
                          MC.throwM (WorkerException e)

                      ResponseOk encoded  -> do
                          let Sub instStoreStM' = instStoreBase @IO @m @a

                          a <- restoreM $
                              unStM' $
                                  withDict instStoreStM' $
                                      Store.decodeEx @(StM' m a) encoded

                          return (a <$ ctx)


-- interpretMpiMasterT :: forall n m k.
--     ( MonadBaseControlStore IO m
--     , MonadIO m
--     , MC.MonadThrow m
--     , Typeable m
--     , m ~ n
--     )
--     => Distributed n MPIWorker (MpiMasterT n m) k
--     -> MpiMasterT n m k
-- interpretMpiMasterT (GetNumWorkers k) =
--     k =<< MpiMasterT (MT.asks (fromIntegral . length . slaveRanks))
--
-- interpretMpiMasterT (WithWorkers (block :: MPIWorker -> MpiMasterT n m a) k) = do
--     h <- MpiMasterT MT.ask
--
--     let worker :: MPI.Rank -> MPI.Tag -> MpiMasterT n m a
--         worker rank tag = block (MPIWorker rank tag)
--
--     let ranks = slaveRanks h
--
--     tags <-
--         let postIncr tag = (succ tag, tag)
--         in  liftIO $ mapM (\tagRef -> atomicModifyIORef' tagRef postIncr) (rankTags h)
--
--     results <- Async.mapConcurrently id (NE.zipWith worker ranks tags)
--
--     k results
--
-- interpretMpiMasterT (RunInWorker (MPIWorker rank tag) sdict (clo :: SClosure (n a)) k) = do
--     withSDict sdict $ do
--         h <- MpiMasterT MT.ask
--
--         let reqClo :: SClosure (Request n a)
--             reqClo = slift2 (static Request) sdict clo
--
--             tdict :: SDict (Typeable a)
--             tdict = weakenSDict @(Typeable a) (static Impl) sdict
--
--             reqDynClo :: SDynClosure (Request n)
--             reqDynClo = reflectSDict tdict (makeSDynClosure reqClo)
--
--         response <- liftIO $ SMPI.sendrecvYielding_ (SMPI.StreamItem reqDynClo) rank tag rank tag (mpiComm h)
--
--         case response :: Response n of
--           ResponseException e -> MC.throwM (WorkerException e)
--           ResponseOk encoded  -> do
--               let Sub instStoreStM' = instStoreBase @IO @m @a
--
--               a <- restoreM $ unStM' $ withDict instStoreStM' $ Store.decodeEx @(StM' m a) encoded
--
--               k a


data ExitHandlers n = ExitHandlers
    { onExitAbort     :: MPIHandle n -> IO ()
    , onExitException :: MPIHandle n -> MC.SomeException -> IO ()
    }


abortOnError :: Int -> ExitHandlers n
abortOnError ec = ExitHandlers abort (\h _ -> abort h)
  where
    abort :: MPIHandle n -> IO ()
    abort h = MPI.abort (mpiComm h) ec


finalizeWorkers :: forall n. MPIHandle n -> IO ()
finalizeWorkers h = do
    let finalizeRank rank = MPI.send (SMPI.StreamEnd @(SDynClosure (Request n))) rank 0 (mpiComm h)
    mapM_ finalizeRank (slaveRanks h)


finalizeOnError :: ExitHandlers n
finalizeOnError = ExitHandlers finalizeWorkers (\h _ -> finalizeWorkers h)


runMpi :: forall m n b.
    ( MonadIO m
    , MC.MonadMask m
    , MonadBaseControlStore IO n
    , MonadIO n
    , Typeable n
    )
    => MPI.Comm
    -> ExitHandlers n
    -> (n () -> m b)
    -> MpiMasterT n m b
    -> m b
runMpi mpiComm exitHandlers runN (MpiMasterT e) = do
    MC.bracket (liftIO initMPI) (\() -> liftIO finalizeMPI) $ \_ -> do
        self <- liftIO $ MPI.commRank mpiComm

        if self == MPI.rootRank then do
            (b, ()) <- MC.generalBracket (liftIO newHandle) (\h -> liftIO . finalizeHandle h) $
                MT.runReaderT e
            return b
        else
            runN workerProcess
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


    finalizeMPI :: IO ()
    finalizeMPI = do
        isFinalized <- liftIO CMPI.finalized

        if isFinalized then
            MC.throwM $ MPI.MPIException $ "finalizeMPI: MPI is already finalized!"

        else do
            MPI.barrier mpiComm
            CMPI.finalize


    newHandle :: IO (MPIHandle n)
    newHandle = do
        let rootRank = MPI.rootRank
            selfRank = rootRank

        size <- liftIO $ MPI.commSize mpiComm

        let slaveRanks = NE.fromList [succ rootRank .. pred size]

        rankTags <- traverse (\_ -> newIORef 0) slaveRanks

        return MPIHandle {..}


    finalizeHandle :: MPIHandle n -> MC.ExitCase b -> IO ()
    finalizeHandle h ec = do
        case ec of
          MC.ExitCaseSuccess _   -> finalizeWorkers h
          MC.ExitCaseAbort       -> onExitAbort exitHandlers h
          MC.ExitCaseException e -> onExitException exitHandlers h e


    processRequest :: SDynClosure (Request n) -> n (Response n)
    processRequest dynReq = withSDynClosure dynReq $ \(Request Dict (na :: n a)) -> do
        res <- liftBaseWith $ \runInBase -> MC.try (runInBase na)

        return $ case res of
            Left e    -> ResponseException (show (e :: MC.SomeException))
            Right sta ->
                let Sub instStoreStM' = instStoreBase @IO @n @a
                in  ResponseOk $ withDict instStoreStM' $ Store.encode @(StM' n a) (StM' sta)


    workerProcess :: n ()
    workerProcess = do
        SMPI.messagesFrom @(SDynClosure (Request n)) MPI.rootRank MPI.anyTag mpiComm
            & S.mapM (_2 processRequest)
            & SMPI.genericSender @(Response n) mpiComm


runMpiWorld :: forall m n b.
    ( MonadIO m
    , MC.MonadMask m
    , MonadBaseControlStore IO n
    , MonadIO n
    , Typeable n
    )
    => ExitHandlers n
    -> (n () -> m b)
    -> MpiMasterT n m b
    -> m b
runMpiWorld = runMpi MPI.commWorld

