{-# options_ghc -Wno-partial-type-signatures #-}
{-# language BlockArguments #-}
{-# language NumericUnderscores #-}
{-# language PartialTypeSignatures #-}
{-# language StaticPointers #-}
module Main where

import Control.Concurrent (threadDelay)

import Data.Function ((&))

import Control.Carrier.Distributed.MPI
import Control.Distributed.SClosure
import Control.Effect.Distributed

import qualified Control.Foldl as L
import Control.Monad.IO.Class (MonadIO(liftIO))

import qualified Pipes      as P
import qualified Pipes.Safe as PS

import qualified Pipes.Prelude as P
import Pipes.Concurrent.Async ((>||>), (>-|>))
import qualified Pipes.Concurrent.Async       as PA
import qualified Pipes.Concurrent.Synchronize as PA

import VPF.Formats
import qualified VPF.Util.Fasta as FA


main :: IO ()
main =
    runMpiWorld finalizeOnError id $ do
        liftIO . print =<< helloWorld
        liftIO . print =<< delays
        liftIO . print =<< fastaEntryCounter


helloWorld :: _ ()
helloWorld = do
    let hello w = putStrLn $ "Hello, from " ++ show w ++ "!"

    withWorkers_ $ \w -> do
        runInWorker_ w (static Dict) $ static hello <:*> spureWith (static Dict) w


delays :: _ ()
delays = do
    nslaves <- getNumWorkers_

    producer <- liftIO $ PA.stealingBufferedProducer (nslaves+1) (P.each [0..10])

    let delay i = liftIO do
          threadDelay 10_000_000
          print i

    withWorkers_ $ \w ->
        PS.runSafeT $ P.runEffect $ producer P.>-> P.mapM_ \(i :: Int) ->
            P.lift $ runInWorker_ w (static Dict) $ static delay <:*> spureWith (static Dict) i


fastaEntryCounter :: _ (Either FA.ParseError ())
fastaEntryCounter = do
    let workFn :: SClosure ([FA.FastaEntry Nucleotide] -> IO Int)
        workFn = static (\chunk -> threadDelay 1000000 >> return (length chunk))

    nslaves <- getNumWorkers_

    aproducer :: PA.AsyncProducer [_] (PS.SafeT IO) () _ (Either _ ()) <- liftIO $
        fastaEntries
          & PA.bufferedChunks 500
          & PA.stealingAsyncProducer (nslaves+1)
          & fmap \ap -> ap
          & PA.cmapOutput (Right () <$)
          & PA.hoist (liftIO . PS.runSafeT)

    let aconsumer :: MonadIO m => PA.AsyncConsumer' Int m ()
        aconsumer = PA.asyncFoldM (L.hoists liftIO $ L.mapM_ print)

    withWorkers_ $ \w -> do
        (res, ()) <- PA.runAsyncEffect (nslaves+1) $
            aproducer
            >||>
            P.mapM (runInWorker_ w (static Dict) . smap workFn . spureWith (static Dict))
            >-|>
            aconsumer

        runInWorker_ w (static Dict) $
            static (\w -> putStrLn $ show w ++ " done counting")
                <:*> spureWith (static Dict) w

        return res
  where
    fastaEntries :: P.Producer (FA.FastaEntry Nucleotide) (PS.SafeT IO) (Either FA.ParseError ())
    fastaEntries = FA.fastaFileReader (Tagged "../vpf-data/All_Viral_Contigs_4filters_final.fasta")
