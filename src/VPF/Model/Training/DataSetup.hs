module VPF.Model.Training.DataSetup where

import Control.Monad.IO.Class (liftIO)
import Control.Monad.Trans.Except (runExceptT, ExceptT (ExceptT))

import Data.ByteString (ByteString)
import Data.Text qualified as Text

import Frames (FrameRec)

import Network.HTTP.Req qualified as Req

import VPF.DataSource.ICTV qualified as ICTV


getLatestVmr :: IO (Either String (FrameRec ICTV.VmrRevisionCols))
getLatestVmr = runExceptT do
    parsedRev <- ExceptT $ Req.runReq Req.defaultHttpConfig do
        feed <- ICTV.parseVmrFeed . Req.responseBody <$> ICTV.getVmrFeed @Req.LbsResponse

        case feed of
            Just vmrs -> do
                let latest = ICTV.findLatestVmrRevision vmrs

                liftIO $ putStrLn $
                    "Downloading revision " ++ Text.unpack (ICTV.revisionTitle latest)
                        ++ " with date " ++ show (ICTV.revisionDate latest)

                rev <- Req.responseBody <$> ICTV.getVmrRevision latest

                return $ ICTV.parseVmrRevision latest rev

            Nothing ->
                return $ Left "no VMRs found in in the ICTV feed"

    ExceptT $
        return $ ICTV.attemptRevisionToFrame parsedRev


collectAccessions :: FrameRec ICTV.VmrRevisionCols -> [ByteString]
collectAccessions = undefined
