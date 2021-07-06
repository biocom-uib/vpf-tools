{-# language Strict #-}
module VPF.Util.FS where

import Control.Category ((>>>))
import Control.Exception (throwIO)
import Control.Monad (when)
import Control.Monad.Catch qualified as MC
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Base (liftBase)
import Control.Monad.Trans.Control (MonadBaseControl(..), liftBaseOp)
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

import VPF.Formats (Path, Directory)


resolveExecutable :: String -> IO (Maybe FilePath)
resolveExecutable path = do
  mpath' <- resolvePath

  case mpath' of
    Nothing -> return Nothing

    Just path' -> do
      hasPerm <- hasPermission path'

      if hasPerm then
        return (Just path')
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


withTmpDir :: forall m n a. (MonadIO m, MC.MonadMask m, MonadBaseControl m n)
           => Path Directory
           -> String
           -> (Path Directory -> n a)
           -> n a
withTmpDir workdir template f =
    liftBaseOp (Temp.withTempDirectory (untag workdir) template)
        (f . Tagged)


withTmpFile :: forall tag m n a. (MonadIO m, MC.MonadMask m, MonadBaseControl m n)
            => Path Directory
            -> String
            -> (Path tag -> n a)
            -> n a
withTmpFile workdir template = liftBaseOp op
  where
    hCloseM :: IO.Handle -> m ()
    hCloseM = liftIO . IO.hClose

    op :: (MonadIO m, MC.MonadMask m) => (Path tag -> m b) -> m b
    op f = Temp.withTempFile (untag workdir) template $ \fp h -> do
        hCloseM h
        f (Tagged fp)


atomicCreateFile :: forall tag m n a. (MonadIO m, MC.MonadMask m, MonadBaseControl m n)
                 => Path tag
                 -> (Path tag -> n a)
                 -> n a
atomicCreateFile finalPath create =
    withTmpFile (Tagged workdir) template $ \path -> do
        a <- create path
        liftBase $ liftIO $ D.renameFile (untag path) (untag finalPath)
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


streamLines :: MonadResource m => FilePath -> Stream (Of ByteString) m ()
streamLines =
    BSS.readFile
    >>> BSS8.lines
    >>> S.mapped BSS.toStrict


streamTextLines :: MonadResource m => FilePath -> Stream (Of Text) m ()
streamTextLines = streamLines >>> S.map Text.decodeUtf8


putTextLines :: MonadIO m => Stream (Of Text) m r -> m r
putTextLines = S.mapM_ (liftIO . TIO.putStrLn)


writeTextLines :: (MonadIO m, MC.MonadMask m) => FilePath -> Stream (Of Text) m r -> m r
writeTextLines fp stream =
    MC.bracket
        (liftIO $ IO.openFile fp IO.WriteMode)
        (liftIO . IO.hClose)
        (\h -> S.mapM_ (liftIO . TIO.hPutStrLn h) stream)
