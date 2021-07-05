{-# language Strict #-}
module VPF.Util.Progress
  ( State
  , init
  , filterMessages
  , update
  , finish
  , tracking
  ) where

import Prelude hiding (init)

import qualified Control.Concurrent.MVar as MVar
import Control.Monad
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Catch (MonadMask, bracket)

import Data.Maybe (fromMaybe)

import qualified System.IO as IO


data State a = State (MVar.MVar a) (a -> String) (a -> a -> Bool)


report :: String -> IO ()
report msg =
    IO.hPutStr IO.stderr $ '\r' : msg


init :: Eq a => a -> (a -> String) -> IO (State a)
init start message = do
    var <- MVar.newMVar start
    report (message start)
    return (State var message (/=))


filterMessages :: (a -> a -> Bool) -> State a -> State a
filterMessages p' (State var message p) =
    State var message (\old new -> p old new && p' old new)


update :: State a -> (a -> a) -> IO ()
update (State var message p) upd = do
    MVar.modifyMVar_ var $ \a -> do
        let a' = upd a
        when (p a a') $
            report (message a')
        return a'


finish :: State a -> Maybe a -> IO ()
finish (State var message _) last = do
    a <- MVar.readMVar var
    let a' = fromMaybe a last

    report (message a')
    IO.hPutStrLn IO.stderr ""


tracking :: (Eq a, MonadIO m, MonadMask m) => a -> (a -> String) -> (State a -> m b) -> m b
tracking start message =
    bracket (liftIO $ init start message) (\p -> liftIO $ finish p Nothing)
