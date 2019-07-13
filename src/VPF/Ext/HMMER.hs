module VPF.Ext.HMMER where

import System.FilePath ((</>))

import VPF.Formats
import VPF.Util.FS (resolveExecutable)


newtype HMMERConfig = HMMERConfig { hmmerPrefix :: Maybe (Path Directory) }
  deriving (Eq, Ord, Show)


resolveHMMERTool :: HMMERConfig -> String -> IO (Maybe FilePath)
resolveHMMERTool (HMMERConfig mpref) toolCmd =
    case mpref of
      Nothing   -> resolveExecutable toolCmd
      Just pref -> resolveExecutable (untag pref </> "bin" </> toolCmd)
