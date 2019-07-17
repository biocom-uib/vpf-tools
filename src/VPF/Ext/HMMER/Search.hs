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

import qualified Data.ByteString.Lazy as BL (toStrict)
import Data.Function (fix)
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
import VPF.Util.FS (resolveExecutable)



data HMMSearch a where
  HMMSearch :: { inputModelFile :: Path HMMERModel
               , inputSeqsFile  :: Path (FASTA seq)
               , outputFile     :: Path (HMMERTable ProtSearchHitCols)
               }
            -> HMMSearch ()

instance Eq (HMMSearch a) where
  HMMSearch i1 s1 o1 == HMMSearch i2 s2 o2 = (i1, untag s1, o1) == (i2, untag s2, o2)

instance Ord (HMMSearch a) where
  HMMSearch i1 s1 o1 `compare` HMMSearch i2 s2 o2 = (i1, untag s1, o1) `compare` (i2, untag s2, o2)

deriving instance Show (HMMSearch a)



data HMMSearchConfig = HMMSearchConfig
  { hmmsearchPath        :: FilePath
  , hmmsearchDefaultArgs :: [String]
  }
  deriving (Eq, Show)


data HMMSearchError
    = HMMSearchError    { cmd :: HMMSearch (), exitCode :: Int, stderr :: Text }
    | HMMSearchNotFound { hmmerConfig :: HMMERConfig }
  deriving (Eq, Ord, Show)


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
          -> Path (FASTA seq)
          -> Path (HMMERTable ProtSearchHitCols)
          -> Eff r ()
hmmsearch inputModelFile inputSeqsFile outputFile = exec HMMSearch {..}


mockHMMSearch :: CmdEff HMMSearch r => Eff (Cmd HMMSearch ': r) a -> Eff r a
mockHMMSearch = runCmd $ \p@HMMSearch{} -> lift $ putStrLn $ "running " ++ show p

execHMMSearch :: CmdEff HMMSearch r => HMMSearchConfig -> Eff (Cmd HMMSearch ': r) a -> Eff r a
execHMMSearch cfg = runCmd $ \cmd@HMMSearch{..} -> do
    let args =
          [ "--tformat", "FASTA"
          , "--tblout", untag outputFile
          , untag inputModelFile
          , untag inputSeqsFile
          ]

    (exitCode, stderr) <- lift $
        Proc.readProcessStderr $
        Proc.setStdout Proc.nullStream $
        Proc.proc (hmmsearchPath cfg) (hmmsearchDefaultArgs cfg ++ args)

    case exitCode of
      ExitSuccess    -> return ()
      ExitFailure ec -> throwError $! HMMSearchError cmd ec (decodeUtf8 (BL.toStrict stderr))
