{-# language DeriveAnyClass #-}
{-# language DeriveGeneric #-}
{-# language GeneralizedNewtypeDeriving #-}
{-# language Strict #-}
{-# language TemplateHaskell #-}
{-# language ViewPatterns #-}
module VPF.DataSource.GenericFTP where

import GHC.Exts (IsString)

import Conduit

import Control.Applicative (liftA2)
import Control.Exception qualified as Exception
import Control.Foldl qualified as L
import Control.Lens (iforM_)
import Control.Monad.Trans.Except (ExceptT(ExceptT), runExceptT, withExceptT)
import Control.Monad.Trans.Writer.Strict (execWriterT, tell)

import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BS8
import Data.Foldable
import Data.List (unionBy)
import Data.Map.Strict (Map)
import Data.Map.Strict qualified as Map
import Data.Map.Merge.Strict qualified as Map
import Data.Maybe (fromMaybe)
import Data.Set qualified as Set
import Data.These
import Data.Text qualified as Text
import Data.Yaml.Aeson qualified as Y

import GHC.Generics (Generic)

import Language.Haskell.TH

import Network.FTP.Client qualified as FTP
import Network.FTP.Client.Conduit qualified as FTPConduit

import System.Directory qualified as Dir
import System.FilePath.Posix qualified as Posix
import System.FilePath ((</>), joinPath, takeDirectory)

import Text.URI qualified as URI
import Text.Read (readMaybe)

import VPF.Formats
import VPF.Util.Hash (Checksum(..), checksumToFoldM)


newtype FtpRelPath = FtpRelPath { ftpRelPath :: FilePath }
    deriving newtype (Eq, Ord, Show, IsString)


toLocalRelPath :: FtpRelPath -> FilePath
toLocalRelPath = joinPath . Posix.splitDirectories . ftpRelPath



newtype DownloadList = DownloadList
    { runFtpDownloadList :: FTP.Handle -> IO (Either String [(FtpRelPath, Checksum)])
    }

instance Semigroup DownloadList where
    DownloadList dl1 <> DownloadList dl2 =
        DownloadList (liftA2 (liftA2 (liftA2 union)) dl1 dl2)
      where
          union = unionBy (\(f1, _) (f2, _) -> f1 == f2)


data FtpSourceConfig = FtpSourceConfig
    { ftpSecure :: Bool
    , ftpHost :: String
    , ftpPort :: Int
    , ftpLogin :: (String, String)
    , ftpBasePath :: FilePath
    , ftpDownloadList :: DownloadList
    }


ftpSourceConfigFromURI :: URI.URI -> TExpQ (DownloadList -> FtpSourceConfig)
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


data FtpFileInfo = FtpFileInfo
    { ftpFileFacts    :: Map String String
    , ftpFileChecksum :: Checksum
    }
    deriving stock (Generic, Show)
    deriving anyclass (Y.FromJSON, Y.ToJSON)


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
        ExceptT $ runFtpDownloadList (ftpDownloadList cfg) h

    infos <- lift $ sourceToList $
        forM_ paths \(path, checksum) -> do
            info <- liftIO $ FTP.mrFacts <$> FTP.mlst h (ftpRelPath path)

            yield (path, FtpFileInfo info checksum)

    return (FtpFileInfos (Map.fromList infos))


streamRetrToFileAndFoldM :: FTP.Handle -> L.FoldM IO ByteString a -> FilePath -> FilePath -> IO a
streamRetrToFileAndFoldM h (L.FoldM step init end) remotePath downloadPath = do
    z <- init
    x <- Conduit.withSinkFileCautious downloadPath \sink ->
        Conduit.runConduit $
            FTPConduit.retr h remotePath
                Conduit..| Conduit.passthroughSink sink return
                Conduit..| Conduit.foldMC step z
    end x


data ChecksumError =


streamRetrToFileAndCheck :: FTP.Handle -> Checksum -> FilePath -> FilePath -> IO ()
streamRetrToFileAndCheck h cksum remotePath downloadPath = do
    check <- streamRetrToFileAndFoldM h (checksumToFoldM cksum) remotePath downloadPath
    when (not check) $
        Exception.throwIO (



readMetadata :: FilePath -> IO (Either Y.ParseException FtpFileInfos)
readMetadata = Y.decodeFileEither


writeMetadata :: FtpFileInfos -> FilePath -> IO ()
writeMetadata infos path = Y.encodeFile path infos


listDownloadedFiles :: Path Directory -> IO (Either Y.ParseException [FilePath])
listDownloadedFiles (Tagged downloadDir) = runExceptT do
    FtpFileInfos fileInfos <- ExceptT $ readMetadata (downloadDir </> metadataFilename)

    return $ map ((downloadDir </>) . toLocalRelPath) (Map.keys fileInfos)


type LogAction a = a -> IO ()

data ChangelogEntry = Deleted FilePath | Updated FilePath | Added FilePath

changelogEntryPath :: ChangelogEntry -> FilePath
changelogEntryPath = \case
    Deleted fp -> fp
    Updated fp -> fp
    Added fp   -> fp


parseChecksums :: (ByteString -> Checksum) -> ByteString -> Map ByteString Checksum
parseChecksums con contents =
    Map.fromList $
        [(name, con checksum) | [checksum, name] <- map BS8.words (BS8.lines contents)]


parseChecksumsFor :: ByteString -> (ByteString -> Checksum) -> ByteString -> Checksum
parseChecksumsFor name con contents =
    case [checksum | [checksum, name'] <- map BS8.words (BS8.lines contents), name' == name] of
        [s] -> con s
        _   -> NoChecksum


findChecksumFor :: ByteString -> Map ByteString Checksum -> Checksum
findChecksumFor name = fromMaybe NoChecksum . Map.lookup name


syncGenericFTP :: FtpSourceConfig -> LogAction String -> Path Directory -> IO (Either String [ChangelogEntry])
syncGenericFTP cfg log (Tagged downloadDir) =
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

        lift $ log "Retrieved current metadata"

        let changes :: Map FtpRelPath (These FtpFileInfo FtpFileInfo)
            changes = Map.merge
                (Map.mapMissing \_ -> This)
                (Map.mapMissing \_ -> That)
                (Map.zipWithMatched \_ -> These)
                (getFtpFileInfos oldMetadata)
                (getFtpFileInfos newMetadata)

        (changes, excs) <- lift . execWriterT $
            iforM_ changes \relPath change -> do
                let remotePath = ftpRelPath relPath
                    localPath = downloadDir </> toLocalRelPath relPath

                case change of
                    This _old -> do
                        liftIO do
                            log $ "Deleting file " ++ localPath
                            Dir.removeFile localPath
                        tell ([Deleted localPath], [])

                    That new -> do
                        result <- liftIO do
                            log $ "Retrieving file " ++ remotePath
                            Dir.createDirectoryIfMissing True (takeDirectory localPath)

                            Exception.try @Exception.IOException do
                                streamRetrToFileAndCheck h (ftpFileChecksum new) remotePath localPath

                        case result of
                            Right () -> tell ([Added localPath], [])
                            Left e   -> tell ([Added localPath], [(relPath, e)])

                    These (FtpFileInfo oldFacts _) (FtpFileInfo newFacts _)
                        | Just oldMod <- readMaybe @Int =<< Map.lookup "modify" oldFacts
                        , Just newMod <- readMaybe @Int =<< Map.lookup "modify" newFacts
                        , oldMod >= newMod
                        ->
                            return ()

                    These _old new -> do
                        result <- liftIO do
                            log $ "Updating " ++ remotePath
                            Dir.removeFile localPath

                            Exception.try @Exception.IOException do
                                streamRetrToFileAndCheck h (ftpFileChecksum new) remotePath localPath

                        case result of
                            Right () -> tell ([Updated localPath], [])
                            Left e   -> tell ([Updated localPath], [(relPath, e)])

        lift
            if null changes then
                log "Already up to date."

            else do
                writeMetadata (deleteFileInfos (map fst excs) newMetadata)
                    metadataPath

                case excs of
                    [] -> log "Update finished."
                    ((_ ,exc1) : _) -> do
                        forM_ excs \(path,exc) ->
                            log $ "Exception caught retrieving " ++ ftpRelPath path ++ ": " ++ show exc

                        log "Update finished with errors."
                        Exception.throwIO exc1

        return changes
  where
    withFTP :: (FTP.Handle -> FTP.FTPResponse -> IO a) -> IO a
    withFTP
        | ftpSecure cfg = FTP.withFTPS (ftpHost cfg) (ftpPort cfg)
        | otherwise     = FTP.withFTP (ftpHost cfg) (ftpPort cfg)

    deleteFileInfos :: [FtpRelPath] -> FtpFileInfos -> FtpFileInfos
    deleteFileInfos paths (FtpFileInfos infos) =
        FtpFileInfos (Map.withoutKeys infos (Set.fromList paths))

    checksum :: FilePath -> FtpFileInfo -> IO Bool
    checksum _ _ = return True
