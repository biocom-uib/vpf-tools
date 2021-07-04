{-# language AllowAmbiguousTypes #-}
{-# language DeriveGeneric #-}
{-# language RecordWildCards #-}
{-# language Strict #-}
{-# language TemplateHaskell #-}
{-# language UndecidableInstances #-}
module VPF.Ext.Prodigal
  ( Prodigal
  , prodigal
  , ProdigalError(..)
  , ProdigalConfig
  , prodigalPath
  , prodigalDefaultArgs
  , newProdigalConfig
  , ProdigalT
  , runProdigalTWith
  , runProdigalT
  ) where

import GHC.Generics (Generic)

import qualified Data.ByteString.Lazy as BL (toStrict)
import Data.Store (Store)
import Data.Text (Text)
import Data.Text.Encoding (decodeUtf8)

import Control.Algebra
import Control.Carrier.MTL.TH (deriveMonadTrans)

import qualified Control.Monad.IO.Class      as MT
import qualified Control.Monad.Morph         as MT
import qualified Control.Monad.Trans.Except  as MT
import qualified Control.Monad.Trans.Reader  as MT

import System.Exit (ExitCode(..))
import qualified System.Process.Typed as Proc

import VPF.Formats
import VPF.Util.FS (resolveExecutable)
import Control.Effect.Sum.Extra (injR)
import Control.Algebra.Helpers (algUnwrapL)


data ProdigalArgs = ProdigalArgs
    { inputFile           :: Path (FASTA Nucleotide)
    , outputAminoacidFile :: Path (FASTA Aminoacid)
    , outputGenBankFile   :: Maybe (Path GenBank)
    }
    deriving (Eq, Ord, Show, Generic)

instance Store ProdigalArgs


data Prodigal m a where
    Prodigal :: ProdigalArgs -> Prodigal m ()


prodigal ::
    Has Prodigal m
    => Path (FASTA Nucleotide)
    -> Path (FASTA Aminoacid)
    -> Maybe (Path GenBank)
    -> m ()
prodigal inputFile outputAminoacidFile outputGenBankFile =
    send (Prodigal ProdigalArgs{..})


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

deriveMonadTrans ''ProdigalT


newProdigalConfig ::
    MT.MonadIO m
    => Path Executable
    -> [String]
    -> MT.ExceptT ProdigalError m ProdigalConfig
newProdigalConfig path defaultArgs = do
    mpath' <- MT.liftIO $ resolveExecutable (untag path)

    case mpath' of
      Nothing    -> MT.throwE $ ProdigalNotFound path
      Just path' -> return $ ProdigalConfig (Tagged path') defaultArgs


runProdigalTWith ::
    Monad m
    => ProdigalConfig
    -> ProdigalT m a
    -> MT.ExceptT ProdigalError m a
runProdigalTWith cfg (ProdigalT m) = MT.hoist (\rm -> MT.runReaderT rm cfg) m


runProdigalT ::
    MT.MonadIO m
    => Path Executable
    -> [String]
    -> ProdigalT m a
    -> m (Either ProdigalError a)
runProdigalT path defaultArgs m = MT.runExceptT $ do
    cfg <- newProdigalConfig path defaultArgs
    runProdigalTWith cfg m


interpretProdigalT :: MT.MonadIO m => Prodigal n a -> ProdigalT m a
interpretProdigalT (Prodigal args@ProdigalArgs{..}) = do
    cfg <- ProdigalT $ MT.lift MT.ask

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
      ExitFailure ec ->
          ProdigalT $ MT.throwE $ ProdigalError args ec (decodeUtf8 (BL.toStrict stderr))


instance
    ( MT.MonadIO m
    , ThreadAlgebra (Either ProdigalError) ctx m
    )
    => Algebra ctx (ProdigalT m) where

    type Sig (ProdigalT m) = Prodigal :+: Sig m

    alg = algUnwrapL injR ProdigalT \_hdl sig ctx ->
        (<$ ctx) <$> interpretProdigalT sig
