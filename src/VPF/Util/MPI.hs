module VPF.Util.MPI where

import Control.Concurrent.Async (Async)
import qualified Control.Concurrent.Async as Async
import qualified Control.Concurrent.TBQueue as TBQ
import qualified Control.Concurrent.TQueue as TQ
import qualified Control.Concurrent.STM.TMVar as TMVar
import qualified Control.Distributed.MPI.Store as MPI
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.STM.TMVar (atomically)

import Data.Store (Store)
import qualified Data.Sequence as Seq
import Numeric.Natural (Natural)

import Pipes (Producer, Consumer, await, yield)
import Pipes.Safe (MonadSafe, bracket)



data StreamItem msg = StreamItem msg | EndStream
  deriving (Eq, Ord, Generic, Store)


recvAsync :: (Enum tag, MonadIO m, Store a)
          => MPI.Comm
          -> MPI.Rank
          -> tag
          -> m (Async a)
recvAsync comm rank tag =
    Async.async $ MPI.recv comm rank (MPI.toTag tag)


messagesFrom :: (Enum tag, MonadSafe m, Store a)
             => MPI.Comm                      -- ^ communicator
             -> MPI.Rank                      -- ^ where to listen for values
             -> tag                           -- ^ tag
             -> Producer a m ()               -- ^ produces values of producer tag
messagesFrom comm rank tag = go
  where
    go :: Producer a m ()
    go = do
        msg <- liftIO $ MPI.recv_ rank (MPI.toTag tag) comm

        case msg of
          StreamItem a -> yield a >> go
          EndStream    -> return ()

workStealing :: (MonadSafe m, Store a, Store b)
             => [MPI.Rank]
             -> MPI.Tag
             -> MPI.Comm
             -> Natural
             -> Pipe a b m r
workStealing _    []    _   _       = return ()
workStealing comm ranks tag bufSize = do
  submissions <- liftIO $ TMVar.newTMVar Seq.empty

  availRanks <- liftIO $ atomically $ do
      q <- TQ.newTQueue
      mapM_ (writeTQueue q) ranks
      q

  results <- liftIO $ TBQ.newTBQueueIO queueSize

  where
    loop = P.for P.cat $ \a -> do
      results <- liftIO $ do
        rank <- atomically $ TQ.readTQueue availRanks
        submit rank a
        atomically $ TQ.flushTBQueue results

      P.each newResults

    submit rank a = do
      liftIO $ MPI.isend a rank tag comm

