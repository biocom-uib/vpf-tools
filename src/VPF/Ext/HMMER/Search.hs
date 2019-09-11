{-# language DeriveGeneric #-}
{-# language RecordWildCards #-}
{-# language StandaloneDeriving #-}
{-# language StrictData #-}
module VPF.Ext.HMMER.Search
  ( Cmd
  , HMMSearch
  , HMMSearchConfig
  , hmmsearchPath
  , hmmsearchDefaultArgs
  , hmmsearchConfig
  , HMMSearchError(..)
  , hmmsearch
  , mockHMMSearch
  , execHMMSearch
  , ProtSearchHitCols
  , ProtSearchHit
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
import VPF.Ext.HMMER (HMMERConfig, resolveHMMERTool)
import VPF.Ext.HMMER.Search.Cols (ProtSearchHitCols, ProtSearchHit)


data HMMSearchArgs = HMMSearchArgs
  { inputModelFile :: Path HMMERModel
  , inputSeqsFile  :: Path (FASTA Aminoacid)
  , outputFile     :: Path (HMMERTable ProtSearchHitCols)
  }
  deriving (Eq, Ord, Show, Generic)

instance Store HMMSearchArgs

data HMMSearch a where
  HMMSearch :: HMMSearchArgs -> HMMSearch ()


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


type instance CmdEff HMMSearch r = (Member (Exc HMMSearchError) r, Lifted IO r)


hmmsearchConfig :: (Member (Exc HMMSearchError) r, Lifted IO r)
                => HMMERConfig -> [String] -> Eff r HMMSearchConfig
hmmsearchConfig cfg defaultArgs = do
  mpath' <- lift $ resolveHMMERTool cfg "hmmsearch"

  case mpath' of
    Nothing    -> throwError (HMMSearchNotFound cfg)
    Just path' -> return (HMMSearchConfig path' defaultArgs)

hmmsearch :: Member (Cmd HMMSearch) r
          => Path HMMERModel
          -> Path (FASTA Aminoacid)
          -> Path (HMMERTable ProtSearchHitCols)
          -> Eff r ()
hmmsearch inputModelFile inputSeqsFile outputFile =
    exec (HMMSearch HMMSearchArgs {..})


mockHMMSearch :: CmdEff HMMSearch r => Eff (Cmd HMMSearch ': r) a -> Eff r a
mockHMMSearch = runCmd $ \(HMMSearch args) -> lift $ putStrLn $ "running " ++ show args

execHMMSearch :: CmdEff HMMSearch r => HMMSearchConfig -> Eff (Cmd HMMSearch ': r) a -> Eff r a
execHMMSearch cfg = runCmd $ \(HMMSearch args@HMMSearchArgs{..}) -> do
    let cmdlineArgs =
          [ "--tformat", "FASTA"
          , "--tblout", untag outputFile
          , untag inputModelFile
          , untag inputSeqsFile
          ]

    (exitCode, stderr) <- lift $
        Proc.readProcessStderr $
        Proc.setStdout Proc.nullStream $
        Proc.proc (hmmsearchPath cfg) (hmmsearchDefaultArgs cfg ++ cmdlineArgs)

    case exitCode of
      ExitSuccess    -> return ()
      ExitFailure ec -> throwError $! HMMSearchError args ec (decodeUtf8 (BL.toStrict stderr))
