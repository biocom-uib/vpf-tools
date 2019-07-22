module VPF.Util.FS where

import Control.Monad (forever)
import Control.Monad.Catch (MonadCatch, MonadMask, try)
import Control.Monad.IO.Class (MonadIO, liftIO)
import Control.Monad.Trans.Control (MonadBaseControl(..), liftBaseOp)

import Data.Tagged (Tagged(..), untag)
import Data.Text (Text)
import qualified Data.Text.IO as TIO

import Pipes (Producer, Consumer)
import qualified Pipes              as P
import qualified Pipes.Prelude      as P
import qualified Pipes.Safe         as P
import qualified Pipes.Safe.Prelude as P

import qualified System.Directory as D
import qualified System.IO        as IO
import qualified System.IO.Error  as IO
import qualified System.IO.Temp   as Temp

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
    hCloseM :: IO.Handle -> m ()
    hCloseM = liftIO . IO.hClose

    op :: (MonadIO m, MonadMask m) => (Path tag -> m b) -> m b
    op f = Temp.withTempFile (untag workdir) template $ \fp h -> do
        hCloseM h
        f (Tagged fp)


emptyTmpFile :: forall tag m. MonadIO m
             => Path Directory -> String -> m (Path tag)
emptyTmpFile workdir =
    liftIO . fmap Tagged . Temp.emptyTempFile (untag workdir)



lineReader :: (MonadIO m, MonadCatch m) => IO Text -> Producer Text m ()
lineReader get = loop
  where
    loop = do
      line <- liftIO (try get)

      case line of
        Right t -> P.yield t >> loop

        Left e | IO.isEOFError e -> return ()
               | otherwise       -> P.throwM e


stdinReader :: (MonadIO m, MonadCatch m) => Producer Text m ()
stdinReader = lineReader (liftIO TIO.getLine)

fileReader :: P.MonadSafe m => FilePath -> Producer Text m ()
fileReader fp =
    P.withFile fp IO.ReadMode $
        lineReader . TIO.hGetLine


stdoutWriter :: MonadIO m => Consumer Text m r
stdoutWriter = P.mapM_ (liftIO . TIO.putStrLn)

fileWriter :: P.MonadSafe m => FilePath -> Consumer Text m r
fileWriter fp =
    P.withFile fp IO.WriteMode $ \h ->
        forever $ do
          line <- P.await
          liftIO (TIO.hPutStrLn h line)


