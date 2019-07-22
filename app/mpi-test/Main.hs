module Main where

import Data.List (genericLength)
import Data.Text (Text)

import qualified Control.Distributed.MPI.Store as MPI

import Pipes ((>->))
import qualified Pipes         as P
import qualified Pipes.Prelude as P
import qualified Pipes.Safe    as PS

import qualified VPF.Util.Concurrent as Conc
import qualified VPF.Util.Fasta      as FA
import qualified VPF.Util.FS         as FS
import qualified VPF.Util.MPI        as M


data MyTag = MyTag
  deriving (Eq, Ord, Show, Bounded, Enum)

inputTag :: M.JobInput MyTag [FA.FastaEntry]
inputTag = M.JobInput MyTag

resultTag :: M.JobResult MyTag [Text]
resultTag = M.JobResult MyTag


main :: IO ()
main = MPI.mainMPI $ do
  let comm = MPI.commWorld
  rank <- MPI.commRank comm
  size <- MPI.commSize comm

  case MPI.fromRank rank of
    0 -> do
        r <- rootMain (succ rank `enumFromTo` pred size) comm
        case r of
          Right () -> return ()
          Left e   -> print e
    _ -> workerMain MPI.rootRank comm


rootMain :: [MPI.Rank] -> MPI.Comm -> IO (Either FA.ParseError ())
rootMain slaves comm = do
    PS.runSafeT $ P.runEffect $
        fastaProducer
        >-> M.delegate slaves inputTag resultTag comm (genericLength slaves * 2)
        >-> P.chain (P.liftIO . print)
        >-> P.for P.cat (\as -> sequence_ [P.yield a | a <- as])
        >-> FS.stdoutWriter
  where
    fastaProducer :: P.Producer [FA.FastaEntry] (PS.SafeT IO) (Either FA.ParseError ())
    fastaProducer =
        Conc.bufferedChunks 10 $ FA.parsedFastaEntries fileLines

    fileLines = FS.fileReader "../data-backup/All_Viral_Contigs_4filters_final.fasta"


workerMain :: MPI.Rank -> MPI.Comm -> IO ()
workerMain master comm = do
    myRank <- MPI.commRank comm

    PS.runSafeT $ M.makeWorker master inputTag resultTag comm $ \chunk -> do
        let ns = [n | FA.FastaEntry n _ <- chunk]
        P.liftIO $ putStrLn $ show myRank ++ " processed " ++ show (length ns) ++ " lines"
        return ns
