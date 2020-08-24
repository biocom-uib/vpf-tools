{-# options_ghc -Wno-partial-type-signatures #-}
{-# language BlockArguments #-}
{-# language NumericUnderscores #-}
{-# language PartialTypeSignatures #-}
{-# language StaticPointers #-}
module Main where

import Control.Effect.Distributed
import Control.Carrier.Distributed.SingleProcess
import Control.Concurrent (getNumCapabilities, myThreadId, threadDelay)

import qualified Control.Foldl as L
import Control.Monad.IO.Class (MonadIO(liftIO))

import Control.Distributed.SClosure
import Data.Semigroup (stimes)

import Numeric.Natural (Natural)

import qualified Pipes      as P
import qualified Pipes.Safe as PS

import qualified Pipes.Prelude as P
import Pipes.Concurrent.Async ((>||>), (>-|>))
import qualified Pipes.Concurrent.Async       as PA
import qualified Pipes.Concurrent.Synchronize as PA

import VPF.Formats
import qualified VPF.Util.Fasta as FA


type DistributedM = SingleProcessT IO


main :: IO ()
main = do
    nthreads <- getNumCapabilities
    helloWorld (fromIntegral nthreads)


helloWorld :: Natural -> IO ()
helloWorld nthreads = do

    let hello i = do
          tid <- myThreadId
          putStrLn $ "Hello, consuming " ++ show i ++ " from " ++ show tid ++ "!"

    ((), ()) <- PA.runAsyncEffect (nthreads+1) $
        PA.duplicatingAsyncProducer (P.each [1..nthreads])
        >||> P.mapM hello
        >-|> stimes nthreads (PA.asyncFold L.mconcat)

    return ()


delayConsumer :: DistributedM ()
delayConsumer = do
    producer <- liftIO $ PA.stealingEach [0..10]

    let delay i = do
          threadDelay 10_000_000
          print i

    withWorkers_ $ \w ->
        PS.runSafeT $ P.runEffect $
            producer
            P.>->
            P.mapM_ \(i :: Int) ->
                P.lift $ runInWorker_ w (static Dict) $
                    static delay <:*> spureWith (static Dict) i


fastaEntryCounter :: DistributedM (Either FA.ParseError ())
fastaEntryCounter = do
    let workFn :: SClosure ([FA.FastaEntry Nucleotide] -> IO Int)
        workFn = static (\chunk -> threadDelay 1000000 >> return (length chunk))

    nslaves <- getNumWorkers_

    asyncFastaChunks <- liftIO $ PA.stealingAsyncProducer (nslaves+1) $
        PA.bufferedChunks 500 fastaEntries

    let aproducer :: PA.AsyncProducer [_] (PS.SafeT IO) () _ (Either _ ())
        aproducer = PA.hoist (liftIO . PS.runSafeT) $ PA.defaultOutput (Right ()) asyncFastaChunks

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
    fastaEntries = FA.fastaFileReader (Tagged "../input-seqs/GOV_all_contigs_1k5.fna")
