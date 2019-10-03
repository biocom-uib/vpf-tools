{-# language DeriveFunctor #-}
{-# language DeriveGeneric #-}
{-# language GeneralizedNewtypeDeriving #-}
{-# language RecordWildCards #-}
{-# language StandaloneDeriving #-}
{-# language TemplateHaskell #-}
module VPF.Ext.Prodigal
  ( Cmd
  , Prodigal
  , ProdigalConfig
  , ProdigalError(..)
  , prodigalPath
  , prodigalDefaultArgs
  , prodigal
  , newProdigalConfig
  , runProdigal
  ) where

import GHC.Generics (Generic)

import qualified Data.ByteString.Lazy as BL (toStrict)
import Data.Functor (($>))
import Data.Store (Store)
import Data.Text (Text)
import Data.Text.Encoding (decodeUtf8)

import Control.Effect
import Control.Effect.Carrier
import Control.Effect.MTL (relayTransControl)
import Control.Effect.MTL.TH (deriveMonadTrans)

import qualified Control.Monad.IO.Class      as MT
import qualified Control.Monad.Morph         as MT
import qualified Control.Monad.Trans.Except  as MT
import qualified Control.Monad.Trans.Reader  as MT

import System.Exit (ExitCode(..))
import qualified System.Process.Typed as Proc

import VPF.Eff.Cmd
import VPF.Formats
import VPF.Util.FS (resolveExecutable)


data ProdigalArgs = ProdigalArgs
    { inputFile           :: Path (FASTA Nucleotide)
    , outputAminoacidFile :: Path (FASTA Aminoacid)
    , outputGenBankFile   :: Maybe (Path GenBank)
    }
    deriving (Eq, Ord, Show, Generic)

instance Store ProdigalArgs


data Prodigal m k where
    Prodigal :: ProdigalArgs -> m k -> Prodigal m k

deriving instance Functor m => Functor (Prodigal m)

instance HFunctor Prodigal where
    hmap f (Prodigal args k) = Prodigal args (f k)

instance Effect Prodigal where
    handle state handler (Prodigal args k) = Prodigal args (handler (state $> k))


prodigal :: (Carrier sig m, Member Prodigal sig)
         => Path (FASTA Nucleotide)
         -> Path (FASTA Aminoacid)
         -> Maybe (Path GenBank)
         -> m ()
prodigal inputFile outputAminoacidFile outputGenBankFile =
    send (Prodigal ProdigalArgs{..} (pure ()))


data ProdigalConfig = ProdigalConfig
    { prodigalPath        :: Path Executable
    , prodigalDefaultArgs :: [String]
    }
    deriving (Eq, Ord, Show, Generic)

instance Store ProdigalConfig


data ProdigalError
    = ProdigalError    { cmd :: ProdigalArgs, exitCode :: !Int, stderr :: !Text }
    | ProdigalNotFound { search :: Path Executable }
    deriving (Eq, Ord, Show, Generic)

instance Store ProdigalError


newtype ProdigalT m a = ProdigalT (MT.ExceptT ProdigalError (MT.ReaderT ProdigalConfig m) a)
    deriving (Functor, Applicative, Monad)


deriveMonadTrans ''ProdigalT


newProdigalConfig :: MT.MonadIO m => Path Executable -> [String] -> MT.ExceptT ProdigalError m ProdigalConfig
newProdigalConfig path defaultArgs = do
    mpath' <- MT.liftIO $ resolveExecutable (untag path)

    case mpath' of
      Nothing    -> MT.throwE $ ProdigalNotFound path
      Just path' -> return $ ProdigalConfig (Tagged path') defaultArgs


runProdigalWith :: Monad m => ProdigalConfig -> ProdigalT m a -> MT.ExceptT ProdigalError m a
runProdigalWith cfg (ProdigalT m) = MT.hoist (\rm -> MT.runReaderT rm cfg) m


runProdigal :: MT.MonadIO m => Path Executable -> [String] -> ProdigalT m a -> m (Either ProdigalError a)
runProdigal path defaultArgs m = MT.runExceptT $ do
    cfg <- newProdigalConfig path defaultArgs
    runProdigalWith cfg m


throwProdigalError :: Monad m => ProdigalError -> ProdigalT m a
throwProdigalError = ProdigalT . MT.throwE


execProdigal :: MT.MonadIO m => ProdigalArgs -> ProdigalT m ()
execProdigal args@ProdigalArgs{..} = do
    cfg <- ProdigalT (MT.lift MT.ask)

    let cmdlineArgs =
          [ "-i", untag inputFile
          , "-a", untag outputAminoacidFile
          ]
          ++
          maybe [] (\path -> ["-o", untag path]) outputGenBankFile

    (exitCode, stderr) <- MT.liftIO $
        Proc.readProcessStderr $
        Proc.setStdout Proc.nullStream $
        Proc.proc (untag (prodigalPath cfg)) (prodigalDefaultArgs cfg ++ cmdlineArgs)

    case exitCode of
      ExitSuccess    -> return ()
      ExitFailure ec -> throwProdigalError $! ProdigalError args ec (decodeUtf8 (BL.toStrict stderr))


instance
    ( MT.MonadIO m
    , Carrier sig m, Effect sig
    )
    => Carrier (Prodigal :+: sig) (ProdigalT m) where

    eff (L (Prodigal args k)) = execProdigal args >> k
    eff (R other) = relayTransControl fmap other
