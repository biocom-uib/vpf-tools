{-# language DataKinds #-}
module Main where

import Data.Text (Text)
import Data.Semigroup.Foldable (foldMap1)
import qualified Data.Text.IO as T

import qualified Control.Monad.Catch as MC
import qualified Control.Foldl as L
import Control.Lens (folded)
import Control.Monad.IO.Class (liftIO)
import qualified Control.Distributed.MPI.Store as MPI

import qualified Data.List.NonEmpty as NE

import qualified Pipes      as P
import qualified Pipes.Safe as PS

import System.Exit (exitWith, ExitCode(..))

import Pipes.Concurrent.Async ((>||>), (>-|>))
import qualified Pipes.Concurrent.Async       as PA
import qualified Pipes.Concurrent.Synchronize as PA
import qualified VPF.Concurrency.MPI          as Conc

import VPF.Formats
import qualified VPF.Util.Fasta as FA


inputTag :: Conc.JobTagIn [FA.FastaEntry Nucleotide]
inputTag = Conc.JobTagIn 0

resultTag :: Conc.JobTagOut [Text]
resultTag = Conc.JobTagOut 0

jobTags :: Conc.JobTags [FA.FastaEntry Nucleotide] [Text]
jobTags = Conc.JobTags (Conc.JobTagIn 0) (Conc.JobTagOut 0)


main :: IO ()
main = MPI.mainMPI $ do
  let comm = MPI.commWorld
  rank <- MPI.commRank comm
  size <- MPI.commSize comm

  case rank of
    0 -> do
        r <- rootMain (NE.fromList [succ rank..pred size]) comm `MC.catch` \e -> do
               print (e :: MC.SomeException)
               MPI.abort comm 1
               exitWith (ExitFailure 1)
        print r
    _ -> workerMain MPI.rootRank rank comm


rootMain :: NE.NonEmpty MPI.Rank -> MPI.Comm -> IO (Maybe FA.ParseError)
rootMain slaves comm = do
    PS.runSafeT $ do
        let workers :: NE.NonEmpty (Conc.Worker [FA.FastaEntry Nucleotide] [Text])
            workers = Conc.mpiWorkers slaves jobTags comm

            asyncPrinter :: PA.AsyncConsumer' [Text] (PS.SafeT IO) ()
            asyncPrinter = PA.asyncFoldM (L.handlesM folded $ L.mapM_ (liftIO . T.putStrLn))

        (res, ()) <-
            PA.runAsyncEffect (nslaves+1) $
                PA.cmapOutput (Nothing <$) (PA.duplicatingAsyncProducer fastaProducer)
                >||>
                foldMap1 (\worker -> Conc.workerToPipe worker >-|> asyncPrinter)
                         workers

        return res
  where
    nslaves :: Num a => a
    nslaves = fromIntegral (length slaves)

    fastaProducer :: P.Producer [FA.FastaEntry Nucleotide] (PS.SafeT IO) (Maybe FA.ParseError)
    fastaProducer =
        fmap (either Just (\() -> Nothing)) $ PA.bufferedChunks 10 fastaEntries

    fastaEntries :: P.Producer (FA.FastaEntry Nucleotide) (PS.SafeT IO) (Either FA.ParseError ())
    fastaEntries = FA.fastaFileReader (Tagged "../vpf-data/All_Viral_Contigs_4filters_final.fasta")


workerMain :: MPI.Rank -> MPI.Rank -> MPI.Comm -> IO ()
workerMain master _ comm = do
    PS.runSafeT $ Conc.makeProcessWorker master jobTags comm $ \_ chunk -> do
        return (map FA.entryName chunk)
