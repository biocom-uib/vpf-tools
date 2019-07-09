module VPF.Util.FS where

import Control.Monad.Base (liftBase)
import Control.Monad.Catch (MonadMask)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Control (MonadBaseControl(..), liftBaseOp)

import Data.Tagged (Tagged(..), untag)

import qualified System.Directory as D
import System.IO (Handle, hClose)
import qualified System.IO.Temp as Temp

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
      else do
        D.findExecutable path

    hasPermission :: FilePath -> IO Bool
    hasPermission = fmap D.executable . D.getPermissions


withTmpDir :: forall m n a. (MonadIO m, MonadMask m, MonadBaseControl m n)
           => Path Directory
           -> String
           -> (Path Directory -> n a)
           -> n a
withTmpDir workdir template f =
    liftBaseOp (Temp.withTempDirectory (untag workdir) template)
        (f . Tagged)

withTmpFile :: forall tag m n a. (MonadIO m, MonadMask m, MonadBaseControl m n)
            => Path Directory
            -> String
            -> (Path tag -> n a)
            -> n a
withTmpFile workdir template = liftBaseOp op
  where
    hCloseM :: Handle -> m ()
    hCloseM = liftIO . hClose

    op :: (MonadIO m, MonadMask m) => (Path tag -> m b) -> m b
    op f = Temp.withTempFile (untag workdir) template $ \fp h -> do
        hCloseM h
        f (Tagged fp)


emptyTmpFile :: forall tag m. MonadIO m
             => Path Directory -> String -> m (Path tag)
emptyTmpFile workdir =
    liftIO . fmap Tagged . Temp.emptyTempFile (untag workdir)
