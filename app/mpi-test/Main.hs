{-# language DataKinds #-}
module Main where

import Data.Monoid (Alt(..))
import Data.Text (Text)
import Data.Semigroup.Foldable (foldMap1)
import qualified Data.Text.IO as T

import qualified Control.Monad.Catch as MC
import qualified Control.Foldl as L
import Control.Lens (folded)
import Control.Monad.IO.Class (liftIO)
import qualified Control.Distributed.MPI.Store as MPI

import qualified Data.List.NonEmpty as NE

import qualified Pipes         as P
import qualified Pipes.Safe    as PS

import System.Exit (exitWith, ExitCode(..))

import VPF.Concurrency.Async ((>||>), (>-|>))
import qualified VPF.Concurrency.Async as Conc
import qualified VPF.Concurrency.MPI   as Conc
import qualified VPF.Concurrency.MPI.Polymorphic as Conc
import qualified VPF.Concurrency.Pipes as Conc

import VPF.Formats
import qualified VPF.Util.Fasta as FA


data MyTag = MyTag
  deriving (Eq, Ord, Show, Bounded, Enum)

inputTag :: Conc.JobTagIn MyTag [FA.FastaEntry Nucleotide]
inputTag = Conc.JobTagIn MyTag

resultTag :: Conc.JobTagOut MyTag [Text]
resultTag = Conc.JobTagOut MyTag

jobTags :: Conc.JobTags MyTag MyTag [FA.FastaEntry Nucleotide] [Text]
jobTags = Conc.JobTags (Conc.JobTagIn MyTag) (Conc.JobTagOut MyTag)


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

            asyncPrinter :: Conc.AsyncConsumer' [Text] (PS.SafeT IO) ()
            asyncPrinter = Conc.asyncFoldM (L.handlesM folded $ L.mapM_ (liftIO . T.putStrLn))

        fmap (getAlt . fst) $
          Conc.runAsyncEffect (nslaves+1) $
              Conc.duplicatingAsyncProducer (fmap Alt fastaProducer)
              >||>
              foldMap1 (\worker -> Conc.workerToPipe worker >-|> asyncPrinter)
                       workers
  where
    nslaves :: Num a => a
    nslaves = fromIntegral (length slaves)

    fastaProducer :: P.Producer [FA.FastaEntry Nucleotide] (PS.SafeT IO) (Maybe FA.ParseError)
    fastaProducer =
        fmap (either Just (\() -> Nothing)) $ Conc.bufferedChunks 10 fastaEntries

    fastaEntries :: P.Producer (FA.FastaEntry Nucleotide) (PS.SafeT IO) (Either FA.ParseError ())
    fastaEntries = FA.fastaFileReader (Tagged "../vpf-data/All_Viral_Contigs_4filters_final.fasta")


workerMain :: MPI.Rank -> MPI.Rank -> MPI.Comm -> IO ()
workerMain master _ comm = do
    PS.runSafeT $ Conc.makeProcessWorker master jobTags comm $ \chunk -> do
        return (map FA.entryName chunk)
