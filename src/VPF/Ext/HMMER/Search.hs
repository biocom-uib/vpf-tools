{-# language AllowAmbiguousTypes #-}
{-# language DeriveGeneric #-}
{-# language RecordWildCards #-}
{-# language StandaloneDeriving #-}
{-# language Strict #-}
{-# language TemplateHaskell #-}
{-# language UndecidableInstances #-}
module VPF.Ext.HMMER.Search
  ( HMMSearch
  , hmmsearch
  , HMMSearchError(..)
  , HMMSearchConfig
  , hmmsearchPath
  , hmmsearchDefaultArgs
  , newHMMSearchConfig
  , HMMSearchT
  , runHMMSearchTWith
  , runHMMSearchT
  , ProtSearchHitCols
  , ProtSearchHit
  ) where

import GHC.Generics (Generic, Generic1)

import qualified Data.ByteString.Lazy as BL (toStrict)
import Data.Store (Store)
import Data.Text (Text)
import Data.Text.Encoding (decodeUtf8)

import Control.Algebra
import Control.Carrier.MTL.TH (deriveMonadTrans, deriveAlgebra)

import qualified Control.Monad.IO.Class      as MT
import qualified Control.Monad.Morph         as MT
import qualified Control.Monad.Trans.Except  as MT
import qualified Control.Monad.Trans.Reader  as MT

import System.Exit (ExitCode(..))
import qualified System.Process.Typed as Proc

import VPF.Formats
import VPF.Ext.HMMER (HMMERConfig, resolveHMMERTool)
import VPF.Ext.HMMER.Search.Cols (ProtSearchHitCols, ProtSearchHit)


data HMMSearchArgs = HMMSearchArgs
    { inputModelFile :: Path HMMERModel
    , inputSeqsFile  :: Path (FASTA Aminoacid)
    , outputFile     :: Path (HMMERTable ProtSearchHitCols)
    }
    deriving (Eq, Ord, Show, Generic)

instance Store HMMSearchArgs


data HMMSearch m k where
    HMMSearch :: HMMSearchArgs -> m k -> HMMSearch m k
    deriving (Generic1)

instance HFunctor HMMSearch
instance Functor f => Handles f HMMSearch


hmmsearch ::
    Has HMMSearch sig m
    => Path HMMERModel
    -> Path (FASTA Aminoacid)
    -> Path (HMMERTable ProtSearchHitCols)
    -> m ()
hmmsearch inputModelFile inputSeqsFile outputFile =
    send (HMMSearch HMMSearchArgs {..} (pure ()))


data HMMSearchConfig = HMMSearchConfig
    { hmmsearchPath        :: FilePath
    , hmmsearchDefaultArgs :: [String]
    }
    deriving (Eq, Ord, Show, Generic)

instance Store HMMSearchConfig


data HMMSearchError
    = HMMSearchError    { cmd :: HMMSearchArgs, exitCode :: Int, stderr :: Text }
    | HMMSearchNotFound { hmmerConfig :: HMMERConfig }
    deriving (Eq, Ord, Show, Generic)

instance Store HMMSearchError


newtype HMMSearchT m a = HMMSearchT (MT.ExceptT HMMSearchError (MT.ReaderT HMMSearchConfig m) a)

deriveMonadTrans ''HMMSearchT


newHMMSearchConfig ::
    MT.MonadIO m
    => HMMERConfig
    -> [String]
    -> MT.ExceptT HMMSearchError m HMMSearchConfig
newHMMSearchConfig cfg defaultArgs = do
    mpath' <- MT.liftIO $ resolveHMMERTool cfg "hmmsearch"

    case mpath' of
      Nothing    -> MT.throwE (HMMSearchNotFound cfg)
      Just path' -> return (HMMSearchConfig path' defaultArgs)


runHMMSearchTWith ::
    MT.MonadIO m
    => HMMSearchConfig
    -> HMMSearchT m a
    -> MT.ExceptT HMMSearchError m a
runHMMSearchTWith cfg (HMMSearchT m) = MT.hoist (\rm -> MT.runReaderT rm cfg) m


runHMMSearchT ::
    MT.MonadIO m
    => HMMERConfig
    -> [String]
    -> HMMSearchT m a
    -> m (Either HMMSearchError a)
runHMMSearchT cfg defaultArgs m = MT.runExceptT $ do
    cfg <- newHMMSearchConfig cfg defaultArgs
    runHMMSearchTWith cfg m


interpretHMMSearchT :: MT.MonadIO m => HMMSearch (HMMSearchT m) a -> HMMSearchT m a
interpretHMMSearchT (HMMSearch args@HMMSearchArgs{..} k) = do
    cfg <- HMMSearchT $ MT.lift MT.ask

    let cmdlineArgs =
          [ "--tformat", "FASTA"
          , "--cpu", "1"
          , "--tblout", untag outputFile
          , untag inputModelFile
          , untag inputSeqsFile
          ]

    (exitCode, stderr) <- MT.liftIO $
        Proc.readProcessStderr $
        Proc.setStdout Proc.nullStream $
        Proc.proc (hmmsearchPath cfg) (hmmsearchDefaultArgs cfg ++ cmdlineArgs)

    case exitCode of
      ExitSuccess    -> k
      ExitFailure ec -> HMMSearchT $ MT.throwE $ HMMSearchError args ec (decodeUtf8 (BL.toStrict stderr))

deriveAlgebra 'interpretHMMSearchT
