module VPF.Model.Training.DataSetup where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (runExceptT, ExceptT (ExceptT))

import Data.HashSet (HashSet)
import Data.HashSet qualified as HashSet
import Data.Text (Text)
import Data.Text qualified as Text

import Frames (FrameRec, Record)

import Network.HTTP.Req qualified as Req

import System.IO qualified as IO

import VPF.DataSource.ICTV qualified as ICTV
import VPF.Frames.Dplyr qualified as F


getLatestVmr :: IO (Either String (FrameRec ICTV.VmrRevisionCols))
getLatestVmr = runExceptT do
    parsedRev <- ExceptT $ Req.runReq Req.defaultHttpConfig do
        feed <- ICTV.parseVmrFeed . Req.responseBody <$> ICTV.getVmrFeed @Req.LbsResponse

        case feed of
            Just vmrs -> do
                let latest = ICTV.findLatestVmrRevision vmrs

                liftIO $ IO.hPutStrLn IO.stderr $
                    "Downloading revision " ++ Text.unpack (ICTV.revisionTitle latest)
                        ++ " with date " ++ show (ICTV.revisionDate latest)

                rev <- Req.responseBody <$> ICTV.getVmrRevision latest

                return $ ICTV.parseVmrRevision latest rev

            Nothing ->
                return $ Left "no VMRs found in in the ICTV feed"

    ExceptT $
        return $ ICTV.attemptRevisionToFrame parsedRev


vmrTaxNames :: FrameRec ICTV.VmrRevisionCols -> HashSet Text
vmrTaxNames =
    HashSet.fromList . concatMap (filter (not . Text.null) . recTaxNames)
  where
    recTaxNames :: Record ICTV.VmrRevisionCols -> [Text]
    recTaxNames = F.give
        [ F.val @"realm"
        , F.val @"subrealm"
        , F.val @"kingdom"
        , F.val @"subkingdom"
        , F.val @"phylum"
        , F.val @"subphylum"
        , F.val @"class"
        , F.val @"subclass"
        , F.val @"order"
        , F.val @"suborder"
        , F.val @"family"
        , F.val @"subfamily"
        , F.val @"genus"
        , F.val @"subgenus"
        , F.val @"species"
        ]
