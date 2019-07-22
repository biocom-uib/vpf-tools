{-# language DeriveGeneric #-}
{-# language GeneralizedNewtypeDeriving #-}
module VPF.Util.MPI where

import GHC.Generics (Generic)

import qualified Control.Concurrent as Thread
import Control.Concurrent.Async (Async)
import qualified Control.Concurrent.Async       as Async
import qualified Control.Concurrent.STM.TBQueue as TBQ
import qualified Control.Concurrent.STM.TMVar   as TMVar
import qualified Control.Distributed.MPI.Store  as MPI
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.STM (atomically)
import qualified Control.Monad.STM as STM

import Data.Store (Store)
import qualified Data.Set as Set
import qualified Data.Vector as V
import Numeric.Natural (Natural)

import Pipes (Producer, Pipe, Consumer, (>->))
import qualified Pipes         as P
import qualified Pipes.Prelude as P
import qualified Pipes.Safe    as PS


newtype JobInput e a = JobInput e
  deriving (Eq, Ord, Show, Bounded, Enum)

newtype JobResult e a = JobResult  e
  deriving (Eq, Ord, Show, Bounded, Enum)


data StreamItem msg = StreamItem msg | StreamEnd
  deriving (Eq, Ord, Generic)

instance Store msg => Store (StreamItem msg)


messagesFrom :: forall tag m a. (Enum tag, PS.MonadSafe m, Store a)
             => MPI.Rank                      -- ^ where to listen for values
             -> tag                           -- ^ tag
             -> MPI.Comm                      -- ^ communicator
             -> Producer a m ()               -- ^ produces values of producer tag
messagesFrom rank tag comm = go
  where
    go :: Producer a m ()
    go = do
      msg <- liftIO $ MPI.recv_ rank (MPI.toTag tag) comm

      case msg of
        StreamItem a -> P.yield a >> go
        StreamEnd    -> return ()


makeWorker :: (PS.MonadSafe m, Enum tag, Enum tag', Store a, Store b)
           => MPI.Rank -> JobInput tag a -> JobResult tag' b -> MPI.Comm -> (a -> m b) -> m ()
makeWorker master jobTag resultTag comm f =
  P.runEffect $
    messagesFrom master jobTag comm
      >-> P.mapM f
      >-> P.mapM_ (\b -> liftIO $ MPI.send b master (MPI.toTag resultTag) comm)


delegate :: forall m a b tag tag' r. (PS.MonadSafe m, Enum tag, Enum tag', Store a, Store b)
         => [MPI.Rank]
         -> JobInput tag a
         -> JobResult tag' b
         -> MPI.Comm
         -> Natural
         -> Pipe a b m r
delegate []    _       _       _    _         = error "delegate called with no workers"
delegate ranks sendTag recvTag comm queueSize = do
  done <- liftIO $ TMVar.newEmptyTMVarIO

  jobs <- liftIO $ TBQ.newTBQueueIO queueSize
  results <- liftIO $ TBQ.newTBQueueIO queueSize

  PS.bracket (liftIO $ V.mapM (startRankSend jobs results done)
                              (V.fromList ranks))
             (liftIO . mapM_ Async.cancel) $ \senders -> do

    r <- insertJobs jobs results

    lastResults <- liftIO $ do
      atomically $ do
        TMVar.putTMVar done ()
        mapM_ Async.waitSTM senders
        TBQ.flushTBQueue results

    P.each lastResults

    return r
  where
    insertJobs :: TBQ.TBQueue a -> TBQ.TBQueue b -> Pipe a b m r
    insertJobs jobs results =
      P.for P.cat $ \a -> do
        newResults <- liftIO $ atomically $
            STM.orElse (do TBQ.writeTBQueue jobs a
                           return [])
                       (do STM.check . not =<< TBQ.isEmptyTBQueue results
                           TBQ.flushTBQueue results)

        P.each newResults

    startRankSend :: TBQ.TBQueue a
                  -> TBQ.TBQueue b
                  -> TMVar.TMVar ()
                  -> MPI.Rank
                  -> IO (Async.Async ())
    startRankSend jobs results done rank = Async.async go
      where
        go :: IO ()
        go = do
          newJob <- atomically $
              STM.orElse (Just <$> TBQ.readTBQueue jobs)
                         (Nothing <$ TMVar.readTMVar done)

          case newJob of
            Just job -> do
              r <- MPI.sendrecv_ (StreamItem job) rank (MPI.toTag sendTag) rank (MPI.toTag recvTag) comm
              atomically $ TBQ.writeTBQueue results r
              go

            Nothing -> do
              MPI.send (StreamEnd @a) rank (MPI.toTag sendTag) comm
              return ()
