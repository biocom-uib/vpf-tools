{-# language GeneralizedNewtypeDeriving #-}
{-# language Strict #-}
{-# language TemplateHaskell #-}
{-# LANGUAGE ViewPatterns #-}
module VPF.DataSource.GenericFTP where

import GHC.Exts (IsString)

import Conduit

import Control.Exception qualified as Exception
import Control.Lens (iforM_)
import Control.Monad.Trans.Except (ExceptT(ExceptT), runExceptT, withExceptT)
import Control.Monad.Trans.Writer.Strict (execWriterT, tell)

import Data.Foldable
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Map.Merge.Strict qualified as Map
import Data.Monoid (Any(..))
import Data.Set qualified as Set
import Data.These
import Data.Text qualified as Text

import Data.Yaml.Aeson qualified as Y

import Language.Haskell.TH

import Network.FTP.Client qualified as FTP
import Network.FTP.Client.Conduit qualified as FTPConduit

import System.Directory qualified as Dir
import System.FilePath.Posix qualified as Posix
import System.FilePath ((</>), joinPath, takeDirectory)

import Text.URI qualified as URI
import Text.Read (readMaybe)

import VPF.Formats


newtype FtpRelPath = FtpRelPath { ftpRelPath :: FilePath }
    deriving newtype (Eq, Ord, Show, IsString)


toLocalRelPath :: FtpRelPath -> FilePath
toLocalRelPath = joinPath . Posix.splitDirectories . ftpRelPath


data FtpSourceConfig = FtpSourceConfig
    { ftpSecure :: Bool
    , ftpHost :: String
    , ftpPort :: Int
    , ftpLogin :: (String, String)
    , ftpBasePath :: FilePath
    , ftpDownloadList :: FTP.Handle -> IO (Either String [FtpRelPath])
    }


ftpSourceConfigFromURI :: URI.URI -> TExpQ ((FTP.Handle -> IO (Either String [FtpRelPath])) -> FtpSourceConfig)
ftpSourceConfigFromURI uri = do
    let secure =
          case URI.unRText <$> URI.uriScheme uri of
              Nothing -> False
              Just scheme
                | scheme == Text.pack "ftp"  -> False
                | scheme == Text.pack "ftps" -> True
                | otherwise ->
                    error ("unrecognized scheme: " ++ show scheme)


        Right authority = URI.uriAuthority uri

        host = Text.unpack (URI.unRText (URI.authHost authority))

        port = maybe 21 fromIntegral (URI.authPort authority)

        login@(_user, _pass) =
            case URI.authUserInfo authority of
                Just (URI.UserInfo usr (Just pass))  ->
                    ( Text.unpack (URI.unRText usr)
                    , Text.unpack (URI.unRText pass)
                    )
                Just (URI.UserInfo _usr Nothing) -> error "password required"
                Nothing -> ("anonymous", "anonymous")


        basePath =
            case URI.uriPath uri of
                Nothing -> "/"
                Just (trailingSlash, components) ->
                    let joined =
                          Text.intercalate (Text.pack "/")
                              (map URI.unRText (toList components))
                    in
                       Text.unpack if trailingSlash then joined else joined <> Text.pack "/"

    [|| FtpSourceConfig secure host port login basePath ||]


newtype FtpFileInfo = FtpFileInfo
    { getFtpFileInfo :: Map String String
    }
    deriving (Show, Y.FromJSON, Y.ToJSON)


newtype FtpFileInfos = FtpFileInfos
    { getFtpFileInfos :: Map FtpRelPath FtpFileInfo
    }
    deriving Show


instance Y.FromJSON FtpFileInfos where
    parseJSON = fmap toFtpFileInfos . Y.parseJSON
      where
        toFtpFileInfos :: Map String FtpFileInfo -> FtpFileInfos
        toFtpFileInfos = FtpFileInfos . Map.mapKeysMonotonic FtpRelPath


instance Y.ToJSON FtpFileInfos where
    toJSON = Y.toJSON . fromFtpFileInfos
      where
        fromFtpFileInfos :: FtpFileInfos -> Map String FtpFileInfo
        fromFtpFileInfos = Map.mapKeysMonotonic ftpRelPath . getFtpFileInfos


metadataFilename :: String
metadataFilename = "vpf-metadata.yaml"


retrieveMetadata :: FtpSourceConfig -> FTP.Handle -> IO (Either String FtpFileInfos)
retrieveMetadata cfg h = runExceptT do
    paths <- withExceptT ("error preparing file list: " ++) $
        ExceptT $ ftpDownloadList cfg h

    infos <- lift $ sourceToList $
        forM_ paths \path -> do
            resp <- liftIO $ FTP.mlst h (ftpRelPath path)
            yield (path, FtpFileInfo (FTP.mrFacts resp))

    return (FtpFileInfos (Map.fromList infos))


streamRetrToFile :: FTP.Handle -> FilePath -> FilePath -> IO ()
streamRetrToFile h remotePath downloadPath = do
    runResourceT . runConduit $
        FTPConduit.retr h remotePath
            .| sinkFileCautious downloadPath


readMetadata :: FilePath -> IO (Either Y.ParseException FtpFileInfos)
readMetadata = Y.decodeFileEither


writeMetadata :: FtpFileInfos -> FilePath -> IO ()
writeMetadata infos path = Y.encodeFile path infos


type LogAction a = a -> IO ()


syncGenericFTP :: FtpSourceConfig -> LogAction String -> Path Directory -> IO (Either String Any)
syncGenericFTP cfg log (untag -> downloadDir) =
    withFTP \h _welcome -> runExceptT do
        lift do
            loginResp <- FTP.login h "anonymous" "anonymous"
            log $ "Logged into " ++ ftpHost cfg ++ ": " ++ show loginResp
            _ <- FTP.cwd h (ftpBasePath cfg)
            return ()

        let metadataPath = downloadDir </> metadataFilename

        oldMetadata <- lift do
            existsOld <- Dir.doesFileExist metadataPath

            if existsOld then
                readMetadata (downloadDir </> metadataFilename) >>= \case
                    Right meta -> do
                        log "Loaded existing metadata"
                        return meta
                    Left err -> do
                        log $ "Error parsing existing metadata, deleting (error: " ++ show err ++ ")"
                        Dir.removeDirectoryRecursive downloadDir
                        return (FtpFileInfos Map.empty)
            else
                return (FtpFileInfos Map.empty)

        lift $ Dir.createDirectoryIfMissing True downloadDir

        newMetadata <- ExceptT $ retrieveMetadata cfg h

        let changes :: Map FtpRelPath (These FtpFileInfo FtpFileInfo)
            changes = Map.merge
                (Map.mapMissing \_ -> This)
                (Map.mapMissing \_ -> That)
                (Map.zipWithMatched \_ -> These)
                (getFtpFileInfos oldMetadata)
                (getFtpFileInfos newMetadata)

        (dirty, excs) <- lift . execWriterT $
            iforM_ changes \relPath change -> do
                let remotePath = ftpRelPath relPath
                    localPath = downloadDir </> toLocalRelPath relPath

                case change of
                    This _old -> do
                        liftIO do
                            log $ "Deleting file " ++ localPath
                            Dir.removeFile localPath
                        tell (Any True, [])

                    That _new -> do
                        result <- liftIO do
                            log $ "Retrieving file " ++ remotePath
                            Dir.createDirectoryIfMissing True (takeDirectory localPath)

                            Exception.try @Exception.IOException $
                                streamRetrToFile h remotePath localPath

                        case result of
                            Left e   -> tell (Any True, [(relPath, e)])
                            Right () -> tell (Any True, [])

                    These (FtpFileInfo old) (FtpFileInfo new)
                        | Just oldMod <- readMaybe @Int =<< Map.lookup "modify" old
                        , Just newMod <- readMaybe @Int =<< Map.lookup "modify" new
                        , oldMod >= newMod
                        ->
                            return ()

                    _ -> do
                        result <- liftIO do
                            log $ "Updating " ++ remotePath
                            Dir.removeFile localPath

                            Exception.try @Exception.IOException $
                                streamRetrToFile h remotePath localPath

                        case result of
                            Right () -> tell (Any True, [])
                            Left e   -> tell (Any True, [(relPath, e)])

        lift
            if getAny dirty then do
                writeMetadata (deleteFileInfos (map fst excs) newMetadata)
                    metadataPath

                case excs of
                    [] -> log "Update finished."
                    ((_ ,exc1) : _) -> do
                        forM_ excs \(path,exc) ->
                            log $ "Exception caught retrieving " ++ ftpRelPath path ++ ": " ++ show exc

                        log "Update finished with errors."
                        Exception.throwIO exc1
            else
                log "Already up to date."

        return dirty
  where
    withFTP :: (FTP.Handle -> FTP.FTPResponse -> IO a) -> IO a
    withFTP
        | ftpSecure cfg = FTP.withFTPS (ftpHost cfg) (ftpPort cfg)
        | otherwise     = FTP.withFTP (ftpHost cfg) (ftpPort cfg)

    deleteFileInfos :: [FtpRelPath] -> FtpFileInfos -> FtpFileInfos
    deleteFileInfos paths (FtpFileInfos infos) =
        FtpFileInfos $ Map.withoutKeys infos (Set.fromList paths)
