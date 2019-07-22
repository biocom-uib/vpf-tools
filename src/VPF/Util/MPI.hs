{-# language DeriveGeneric #-}
{-# language GeneralizedNewtypeDeriving #-}
{-# language StrictData #-}
module VPF.Util.MPI where

import GHC.Generics (Generic)

import qualified Control.Concurrent as Thread
import Control.Concurrent.Async (Async)
import qualified Control.Concurrent.Async       as Async
import qualified Control.Concurrent.STM.TBQueue as TBQ
import qualified Control.Concurrent.STM.TMVar   as TMVar
import qualified Control.Distributed.MPI.Store  as MPI
import qualified Control.Monad.Catch as MC
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.STM (atomically)
import Control.Monad.Trans.Class (lift)
import qualified Control.Monad.STM as STM

import Data.Store (Store)
import qualified Data.Set as Set
import qualified Data.Vector as V
import Numeric.Natural (Natural)

import Pipes (Producer, Pipe, Consumer, (>->))
import qualified Pipes         as P
import qualified Pipes.Prelude as P
import qualified Pipes.Safe    as PS


newtype JobTagIn e a = JobTagIn e
  deriving (Eq, Ord, Show, Bounded, Enum)

newtype JobTagOut e a = JobTagOut  e
  deriving (Eq, Ord, Show, Bounded, Enum)

data JobTags e e' a b = JobTags (JobTagIn e a) (JobTagOut e' a)


data StreamItem a = StreamItem a | StreamEnd
  deriving (Eq, Ord, Show, Generic)

instance Store a => Store (StreamItem a)


messagesFrom :: forall tag m a. (Enum tag, PS.MonadSafe m, Store a)
             => MPI.Rank                      -- ^ where to listen for values
             -> tag                           -- ^ tag
             -> MPI.Comm                      -- ^ communicator
             -> Producer a m ()               -- ^ produces values of producer tag
messagesFrom rank tag comm = loop
  where
    tag' :: MPI.Tag
    tag' = MPI.toTag tag

    loop :: Producer a m ()
    loop = do
      msg <- liftIO $ MPI.recv_ rank tag' comm

      case msg of
        StreamItem a -> do
          P.yield a
          loop
        StreamEnd    -> return ()


messagesTo :: forall tag m a. (Enum tag, PS.MonadSafe m, Store a)
           => MPI.Rank
           -> tag
           -> MPI.Comm
           -> Consumer a m ()
messagesTo rank tag comm = do
    P.mapM_ (\s -> liftIO $ MPI.send (StreamItem s) rank tag' comm)
      `PS.finally` sendStreamEnd
  where
    tag' :: MPI.Tag
    tag' = MPI.toTag tag

    streamEnd :: StreamItem a
    streamEnd = StreamEnd

    sendStreamEnd :: PS.Base m ()
    sendStreamEnd = liftIO $ MPI.send (StreamEnd @a) rank tag' comm


makeWorker :: (PS.MonadSafe m, Enum tag, Enum tag', Store a, Store b)
           => MPI.Rank
           -> JobTags tag tag' a b
           -> MPI.Comm
           -> (a -> m b)
           -> m ()
makeWorker master (JobTags tagIn tagOut) comm f =
    P.runEffect $
        messagesFrom master tagIn comm
          >-> P.mapM f
          >-> messagesTo master tagOut comm


delegate :: forall m a b tag tag' r. (PS.MonadSafe m, Enum tag, Enum tag', Store a, Store b)
         => [MPI.Rank]
         -> JobTags tag tag' a b
         -> MPI.Comm
         -> Natural
         -> Pipe a b m r
delegate []    _                      _    _         = error "delegate called with no workers"
delegate ranks (JobTags tagIn tagOut) comm queueSize = do
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
    tagIn', tagOut' :: MPI.Tag
    tagIn'  = MPI.toTag tagIn
    tagOut' = MPI.toTag tagOut

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
    startRankSend jobs results done rank =
        Async.async $
            loop `MC.finally` MPI.send (StreamEnd @a) rank tagOut' comm
      where
        loop :: IO ()
        loop = do
          newJob <- atomically $
              STM.orElse (Just <$> TBQ.readTBQueue jobs)
                         (Nothing <$ TMVar.readTMVar done)

          case newJob of
            Just job -> do
              result <- MPI.sendrecv_ (StreamItem job) rank tagIn' rank tagOut' comm

              case result of
                StreamItem r -> do
                  atomically $ TBQ.writeTBQueue results r
                  loop

                StreamEnd -> return ()

            Nothing -> do
              return ()
