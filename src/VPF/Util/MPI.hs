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
import Control.Exception (SomeException)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.STM (atomically)
import Control.Monad.Trans.Class (lift)
import qualified Control.Monad.STM as STM

import Data.Monoid (Alt(..))
import Data.List (genericLength)
import Data.Store (Store)
import qualified Data.Set as Set
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
      msg <- liftIO $ MC.mask_ $ MPI.recv_ rank tag' comm

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
    P.mapM_ (\s -> liftIO $ MC.mask_ $ MPI.send (StreamItem s) rank tag' comm)
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
         -> Producer a m r
         -> Producer b m r
delegate []    _                      _    _         _        = error "delegate called with no workers"
delegate ranks (JobTags tagIn tagOut) comm queueSize producer = do
  done <- liftIO $ TMVar.newEmptyTMVarIO

  jobs <- liftIO $ TBQ.newTBQueueIO queueSize
  results <- liftIO $ TBQ.newTBQueueIO (genericLength ranks)

  r <- PS.bracket (setupSenders jobs results done) cancelSenders $ \senders -> do
           r <- insertJobs jobs results
           finishSenders results done senders
           return r

  lastResults <- liftIO $ atomically $ TBQ.flushTBQueue results
  P.each lastResults

  return r
  where
    tagIn', tagOut' :: MPI.Tag
    tagIn'  = MPI.toTag tagIn
    tagOut' = MPI.toTag tagOut

    insertJobs :: TBQ.TBQueue a -> TBQ.TBQueue b -> Producer b m r
    insertJobs jobs results = do
        r <- P.for producer $ \a -> do
            newResults <- liftIO $ atomically $ TBQ.flushTBQueue results
            P.each newResults
            liftIO $ atomically $ TBQ.writeTBQueue jobs a

        return r


    setupSenders :: TBQ.TBQueue a -> TBQ.TBQueue b -> TMVar.TMVar () -> PS.Base m [Async.Async ()]
    setupSenders jobs results done =
        liftIO $ mapM (startRankSend jobs results done) ranks


    finishSenders :: TBQ.TBQueue b -> TMVar.TMVar () -> [Async.Async ()] -> Producer b m ()
    finishSenders results done senders = do
        liftIO $ atomically $ TMVar.putTMVar done ()

        let consumeAndWait :: Async.Async () -> Producer b m ()
            consumeAndWait sender = do
              status <- liftIO $ Async.poll sender
              case status of
                Nothing -> do
                    bs <- liftIO $ atomically $ TBQ.flushTBQueue results
                    P.each bs
                    consumeAndWait sender
                Just r ->
                    case r of
                      Left e   -> MC.throwM e
                      Right () -> return ()

        mapM_ consumeAndWait senders


    cancelSenders :: [Async.Async ()] -> PS.Base m ()
    cancelSenders = liftIO . mapM_ Async.cancel


    startRankSend :: TBQ.TBQueue a
                  -> TBQ.TBQueue b
                  -> TMVar.TMVar ()
                  -> MPI.Rank
                  -> IO (Async.Async ())
    startRankSend jobs results done rank =
        Async.async $
            loop `MC.finally` MC.mask_ (MPI.send (StreamEnd @a) rank tagOut' comm)
      where
        loop :: IO ()
        loop = do
          newJob <- atomically $
              STM.orElse (Just <$> TBQ.readTBQueue jobs)
                         (Nothing <$ TMVar.readTMVar done)

          case newJob of
            Just job -> do
              result <- MC.mask_ $ MPI.sendrecv_ (StreamItem job) rank tagIn' rank tagOut' comm

              case result of
                StreamItem r -> do
                  atomically $ TBQ.writeTBQueue results r
                  loop

                StreamEnd -> return ()

            Nothing -> return ()
