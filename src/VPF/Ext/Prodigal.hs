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

import qualified Data.ByteString.Lazy as BL (toStrict)
import Data.Function (fix, (&))
import Data.Text (Text)
import Data.Text.Encoding (decodeUtf8)

import Control.Eff
import Control.Eff.Exception (Exc, throwError)

import System.Exit (ExitCode(..))
import qualified System.Process.Typed as Proc

import VPF.Eff.Cmd
import VPF.Formats
import VPF.Util.FS (resolveExecutable)



data Prodigal a where
  Prodigal :: { inputFile           :: Path (FASTA Nucleotide)
              , outputAminoacidFile :: Path (FASTA Aminoacid)
              , outputGenBankFile   :: Maybe (Path GenBank)
              }
           -> Prodigal ()

deriving instance Eq (Prodigal a)
deriving instance Ord (Prodigal a)
deriving instance Show (Prodigal a)


data ProdigalConfig = ProdigalConfig
  { prodigalPath        :: FilePath
  , prodigalDefaultArgs :: [String]
  }
  deriving (Eq, Show)


data ProdigalError
  = ProdigalError    { cmd :: Prodigal (), exitCode :: !Int, stderr :: !Text }
  | ProdigalNotFound { search :: FilePath }
  deriving (Eq, Ord, Show)


type instance CmdEff Prodigal r = (Member (Exc ProdigalError) r, Lifted IO r)


prodigalConfig :: (Member (Exc ProdigalError) r, Lifted IO r)
               => FilePath -> [String] -> Eff r ProdigalConfig
prodigalConfig path defaultArgs = do
  mpath' <- lift $ resolveExecutable path

  case mpath' of
    Nothing    -> throwError (ProdigalNotFound path)
    Just path' -> return (ProdigalConfig path' defaultArgs)


prodigal :: Member (Cmd Prodigal) r
         => Path (FASTA Nucleotide)
         -> Path (FASTA Aminoacid)
         -> Maybe (Path GenBank)
         -> Eff r ()
prodigal inputFile outputAminoacidFile outputGenBankFile = exec (Prodigal {..})


mockProdigal :: CmdEff Prodigal r => Eff (Cmd Prodigal ': r) a -> Eff r a
mockProdigal = runCmd $ \p@Prodigal{} -> lift $ putStrLn $ "running " ++ show p


execProdigal :: CmdEff Prodigal r => ProdigalConfig -> Eff (Cmd Prodigal ': r) a -> Eff r a
execProdigal cfg = runCmd $ \cmd@Prodigal{} -> do
    let args =
          [ "-i", untag (inputFile cmd)
          , "-a", untag (outputAminoacidFile cmd)
          ]
          ++
          maybe [] (\path -> ["-o", untag path]) (outputGenBankFile cmd)

    (exitCode, stderr) <- lift $
        Proc.readProcessStderr $
        Proc.setStdout Proc.nullStream $
        Proc.proc (prodigalPath cfg) (prodigalDefaultArgs cfg ++ args)

    case exitCode of
      ExitSuccess    -> return ()
      ExitFailure ec -> throwError $! ProdigalError cmd ec (decodeUtf8 (BL.toStrict stderr))
