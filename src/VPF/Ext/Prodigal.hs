{-# language DeriveGeneric #-}
{-# language RecordWildCards #-}
{-# language StandaloneDeriving #-}
module VPF.Ext.Prodigal
  ( Cmd
  , Prodigal
  , ProdigalConfig
  , prodigalPath
  , prodigalDefaultArgs
  , prodigalConfig
  , ProdigalError(..)
  , prodigal
  , mockProdigal
  , execProdigal
  ) where

import GHC.Generics (Generic)

import qualified Data.ByteString.Lazy as BL (toStrict)
import Data.Store (Store)
import Data.Text (Text)
import Data.Text.Encoding (decodeUtf8)

import Control.Eff
import Control.Eff.Exception (Exc, throwError)

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


data Prodigal a where
  Prodigal :: ProdigalArgs -> Prodigal ()


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


type instance CmdEff Prodigal r = (Member (Exc ProdigalError) r, Lifted IO r)


prodigalConfig :: (Member (Exc ProdigalError) r, Lifted IO r)
               => Path Executable
               -> [String]
               -> Eff r ProdigalConfig
prodigalConfig path defaultArgs = do
  mpath' <- lift $ resolveExecutable (untag path)

  case mpath' of
    Nothing    -> throwError (ProdigalNotFound path)
    Just path' -> return (ProdigalConfig (Tagged path') defaultArgs)


prodigal :: Member (Cmd Prodigal) r
         => Path (FASTA Nucleotide)
         -> Path (FASTA Aminoacid)
         -> Maybe (Path GenBank)
         -> Eff r ()
prodigal inputFile outputAminoacidFile outputGenBankFile =
    exec (Prodigal ProdigalArgs{..})


mockProdigal :: CmdEff Prodigal r => Eff (Cmd Prodigal ': r) a -> Eff r a
mockProdigal = runCmd $ \(Prodigal args) -> lift $ putStrLn $ "running " ++ show args


execProdigal :: CmdEff Prodigal r => ProdigalConfig -> Eff (Cmd Prodigal ': r) a -> Eff r a
execProdigal cfg = runCmd $ \(Prodigal args@ProdigalArgs{..}) -> do
    let cmdlineArgs =
          [ "-i", untag inputFile
          , "-a", untag outputAminoacidFile
          ]
          ++
          maybe [] (\path -> ["-o", untag path]) outputGenBankFile

    (exitCode, stderr) <- lift $
        Proc.readProcessStderr $
        Proc.setStdout Proc.nullStream $
        Proc.proc (untag (prodigalPath cfg)) (prodigalDefaultArgs cfg ++ cmdlineArgs)

    case exitCode of
      ExitSuccess    -> return ()
      ExitFailure ec -> throwError $! ProdigalError args ec (decodeUtf8 (BL.toStrict stderr))
