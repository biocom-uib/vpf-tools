module Main where

import Data.List (genericLength)
import Data.Text (Text)
import qualified Data.Text.IO as T

import Control.Concurrent (threadDelay)
import Control.Monad.IO.Class (liftIO)
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

inputTag :: M.JobTagIn MyTag [FA.FastaEntry]
inputTag = M.JobTagIn MyTag

resultTag :: M.JobTagOut MyTag [Text]
resultTag = M.JobTagOut MyTag

jobTags :: M.JobTags MyTag MyTag [FA.FastaEntry] [Text]
jobTags = M.JobTags (M.JobTagIn MyTag) (M.JobTagOut MyTag)


main :: IO ()
main = MPI.mainMPI $ do
  let comm = MPI.commWorld
  rank <- MPI.commRank comm
  size <- MPI.commSize comm

  case MPI.fromRank rank of
    0 -> do
        r <- rootMain [succ rank..pred size] comm
        print r
    _ -> workerMain MPI.rootRank rank comm


rootMain :: [MPI.Rank] -> MPI.Comm -> IO (Either FA.ParseError ())
rootMain slaves comm = do
    PS.runSafeT $ P.runEffect $
         M.delegate slaves jobTags comm (genericLength slaves + 1) fastaProducer
         >-> P.for P.cat (mapM_ P.yield)
         >-> P.mapM_ (liftIO . T.putStrLn)
  where
    fastaProducer :: P.Producer [FA.FastaEntry] (PS.SafeT IO) (Either FA.ParseError ())
    fastaProducer =
        Conc.bufferedChunks 10 $ FA.parsedFastaEntries fileLines

    fileLines = FS.fileReader "../vpf-data/All_Viral_Contigs_4filters_final.fasta"


workerMain :: MPI.Rank -> MPI.Rank -> MPI.Comm -> IO ()
workerMain master me comm = do
    PS.runSafeT $ M.makeWorker master jobTags comm $ \chunk -> do
        return [n | FA.FastaEntry n _ <- chunk]
