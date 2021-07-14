{-# language Strict #-}
module VPF.Util.FS where

import Control.Category ((>>>))
import Control.Exception (throwIO)
import Control.Monad (when)
import Control.Monad.Catch qualified as MC
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Resource (MonadResource)

import Data.ByteString (ByteString)
import Data.Tagged (Tagged(..), untag)
import Data.Text (Text)
import Data.Text.Encoding qualified as Text
import qualified Data.Text.IO as TIO

import Streaming (Stream, Of)
import Streaming.Prelude qualified as S

import qualified System.Directory as D
import qualified System.FilePath  as FP
import qualified System.IO        as IO
import qualified System.IO.Error  as IO
import qualified System.IO.Temp   as Temp

import Streaming.ByteString qualified as BSS
import Streaming.ByteString.Char8 qualified as BSS8

import VPF.Formats (Path, Directory, Executable)


resolveExecutable :: String -> IO (Maybe (Path Executable))
resolveExecutable path = do
    mpath' <- resolvePath

    case mpath' of
      Nothing -> return Nothing

      Just path' -> do
        hasPerm <- hasPermission path'

        if hasPerm then
          return (Just (Tagged path'))
        else
          return Nothing
  where
    resolvePath :: IO (Maybe FilePath)
    resolvePath = do
      exists <- D.doesFileExist path

      if exists then
        return (Just path)
      else
        D.findExecutable path

    hasPermission :: FilePath -> IO Bool
    hasPermission = fmap D.executable . D.getPermissions


whenNotExists :: MonadIO m => Path tag -> m () -> m ()
whenNotExists fp m = do
    exists <- liftIO $ D.doesFileExist (untag fp)

    when (not exists) m


withFile :: forall m a r.
    (MonadIO m, MC.MonadMask m)
    => Path a
    -> IO.IOMode
    -> (IO.Handle -> m r)
    -> m r
withFile p mode =
    MC.bracket
        (liftIO $ IO.openFile (untag p) mode)
        (liftIO . IO.hClose)


withFileRead :: forall m a r.
    (MonadIO m, MC.MonadMask m)
    => Path a
    -> (IO.Handle -> m r)
    -> m r
withFileRead p = withFile p IO.ReadMode


withFileWrite :: forall m a r.
    (MonadIO m, MC.MonadMask m)
    => Path a
    -> (IO.Handle -> m r)
    -> m r
withFileWrite p = withFile p IO.WriteMode


withTmpDir :: forall m a. (MonadIO m, MC.MonadMask m)
           => Path Directory
           -> String
           -> (Path Directory -> m a)
           -> m a
withTmpDir workdir template f =
    Temp.withTempDirectory (untag workdir) template (f . Tagged)


withTmpFile :: forall tag m a. (MonadIO m, MC.MonadMask m)
            => Path Directory
            -> String
            -> (Path tag -> m a)
            -> m a
withTmpFile workdir template f =
    Temp.withTempFile (untag workdir) template $ \fp h -> do
        liftIO $ IO.hClose h
        f (Tagged fp)


atomicCreateFile :: forall tag m a. (MonadIO m, MC.MonadMask m)
                 => Path tag
                 -> (Path tag -> m a)
                 -> m a
atomicCreateFile finalPath create =
    withTmpFile (Tagged workdir) template $ \path -> do
        a <- create path
        liftIO $ D.renameFile (untag path) (untag finalPath)
        return a
  where
    (workdir, finalFileName) = FP.splitFileName (untag finalPath)
    template = "tmp-" ++ finalFileName


emptyTmpFile :: forall tag m. MonadIO m
             => Path Directory -> String -> m (Path tag)
emptyTmpFile workdir =
    liftIO . fmap Tagged . Temp.emptyTempFile (untag workdir)



lineReader :: MonadIO m => IO Text -> Stream (Of Text) m ()
lineReader get = loop
  where
    loop = do
      line <- liftIO (MC.try get)

      case line of
        Right t -> do
          S.yield t
          loop

        Left e | IO.isEOFError e -> return ()
               | otherwise       -> liftIO $ throwIO e


streamLines :: MonadResource m => Path a -> Stream (Of ByteString) m ()
streamLines =
    untag
    >>> BSS.readFile
    >>> BSS8.lines
    >>> S.mapped BSS.toStrict


streamTextLines :: MonadResource m => Path a -> Stream (Of Text) m ()
streamTextLines = streamLines >>> S.map Text.decodeUtf8


putTextLines :: MonadIO m => Stream (Of Text) m r -> m r
putTextLines = S.mapM_ (liftIO . TIO.putStrLn)


writeTextLines :: (MonadIO m, MC.MonadMask m) => Path a -> Stream (Of Text) m r -> m r
writeTextLines p stream =
    withFileWrite p \h ->
        S.mapM_ (liftIO . TIO.hPutStrLn h) stream
