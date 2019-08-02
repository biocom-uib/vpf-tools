{-# language DeriveGeneric #-}
module VPF.Ext.HMMER where

import GHC.Generics (Generic)

import Data.Store (Store)
import System.FilePath ((</>))

import VPF.Formats
import VPF.Util.FS (resolveExecutable)


newtype HMMERConfig = HMMERConfig { hmmerPrefix :: Maybe (Path Directory) }
  deriving (Eq, Ord, Show, Generic)

instance Store HMMERConfig


resolveHMMERTool :: HMMERConfig -> String -> IO (Maybe FilePath)
resolveHMMERTool (HMMERConfig mpref) toolCmd =
    case mpref of
      Nothing   -> resolveExecutable toolCmd
      Just pref -> resolveExecutable (untag pref </> "bin" </> toolCmd)
