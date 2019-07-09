{-# language ApplicativeDo #-}
{-# language OverloadedStrings #-}
module VPF.Model.VirusClass where

import Control.Eff
import Control.Eff.Reader.Strict
import Control.Eff.Exception
import Control.Lens (view, toListOf)
import Control.Monad.Trans.Except (ExceptT, runExceptT)
import qualified Control.Foldl as L

import Data.Function (on)
import Data.Map (Map)
import qualified Data.Map as Map
import Data.Maybe (fromJust)
import Data.Text (Text)
import qualified Data.Text as T
import Data.Vinyl (Rec(..))
import Data.Vinyl.TypeLevel (type (++))

import Frames (Frame, FrameRec, Record, RecordColumns, toFrame)
import Frames.Joins (innerJoin)
import Frames.Melt (RDeleteAll)

import Pipes (Producer, (>->))
import Pipes.Safe (SafeT, runSafeT)
import qualified Pipes.Prelude as P

import VPF.Eff.Cmd (Cmd)
import VPF.Ext.Prodigal (Prodigal, prodigal)
import VPF.Ext.HMMER.Search (HMMSearch, hmmsearch, ProtSearchHit, ProtSearchHitCols)
import qualified VPF.Ext.HMMER.Search.Cols as HMM
import qualified VPF.Ext.HMMER.TableFormat as Tbl

import VPF.Formats
import VPF.Model.Class (Classification)
import qualified VPF.Model.Cols as M

import VPF.Util.Tagged
import VPF.Util.Vinyl
import VPF.Util.FS (emptyTmpFile)


data ModelConfig = ModelConfig
    { modelWorkDir         :: Path Directory
    , modelEValueThreshold :: Double
    }

newtype ModelHits = ModelHits
    { getModelHits ::  Map (Field M.ModelName) (Field M.NumHits)
    }
  deriving (Eq, Ord, Show)

data MetagenomeHits = MetagenomeHits
    { metagenomeModelHits  :: !ModelHits
    , metagenomeBestEValue :: !(Field HMM.SequenceEValue)
    }
  deriving (Eq, Ord, Show)

type MetagenomeHitsRec = Record
    '[ M.VirusName
     , M.ModelName
     , M.NumHits
     ]


produceHits :: ( Lifted IO r
               , '[ Reader ModelConfig
                  , Cmd HMMSearch
                  , Cmd Prodigal
                  ] <:: r
               )
         => Path HMMERModel
         -> Path (FASTA Nucleotide)
         -> Eff r (Producer ProtSearchHit
                            (SafeT (ExceptT Tbl.RowParseError IO))
                            ())
produceHits vpfModel metagenomes = do
    wd <- reader modelWorkDir

    aminoacidFile <- emptyTmpFile @(FASTA Aminoacid) wd "temp_proteins.faa"
    prodigal metagenomes aminoacidFile Nothing

    hitsFile <- emptyTmpFile @(WithComments (HMMERTable ProtSearchHitCols)) wd "temp_hits.txt"
    --let hitsFile = "../data/test.tblout"
    hmmsearch vpfModel aminoacidFile hitsFile

    return (Tbl.produceTableRows hitsFile)




runModel :: ( Lifted IO r
            , '[ Reader ModelConfig
               , Exc Tbl.RowParseError
               , Cmd HMMSearch
               , Cmd Prodigal
               ] <:: r
            )
         => Path HMMERModel
         -> Path (FASTA Nucleotide)
         -> Eff r (Map (Field M.VirusName) MetagenomeHits)
runModel vpfModel metagenomes = do
    hitRows <- produceHits vpfModel metagenomes

    thr <- reader modelEValueThreshold

    er <- lift $
          runExceptT $
          runSafeT $
          L.purely P.fold
              (L.prefilter (\row -> view HMM.sequenceEValue row < thr)
                           gatherHits)
              hitRows

    liftEither er
  where
    gatherHits :: L.Fold ProtSearchHit (Map (Field M.VirusName) MetagenomeHits)
    gatherHits =
        L.groupBy (metagenomeName . rgetTagged)
                  metagenomeHits

    metagenomeHits :: L.Fold ProtSearchHit MetagenomeHits
    metagenomeHits = do
      bestHits <- L.groupBy (view HMM.targetName) proteinBestHit

      pure $ flip L.fold bestHits $ do
        hits       <- L.groupBy (retagFieldTo @M.ModelName . rgetTagged @HMM.QueryName)
                                (Tagged <$> L.length)

        bestEValue <- fromJust <$> L.premap rgetTagged
                                            L.minimum

        pure (MetagenomeHits (ModelHits hits) bestEValue)

    proteinBestHit :: L.Fold ProtSearchHit ProtSearchHit
    proteinBestHit =
        fromJust <$>
          L.minimumBy (compare `on` view HMM.sequenceEValue)

    metagenomeName :: Field HMM.TargetName -> Field M.VirusName
    metagenomeName (Tagged t) =
        let (metagenome_, protein) = T.breakOnEnd "_" t
        in
          case T.stripSuffix "_" metagenome_ of
            Just metagenome -> Tagged metagenome
            Nothing -> error (
                "unrecognized protein naming scheme in hmmsearch result "
                ++ T.unpack t)


hitCountsToFrame :: Map (Field M.VirusName) MetagenomeHits
                 -> Frame MetagenomeHitsRec
hitCountsToFrame =
    mconcat
    . map (\(name, hits) ->
        let fname = elfield name
        in  fmap (fname :&) (hitsToFrame hits))
    . Map.toAscList
  where
    hitsToFrame :: MetagenomeHits -> FrameRec '[M.ModelName, M.NumHits]
    hitsToFrame =
        toFrame
        . map (\(query, cnt) -> elfield query :& elfield cnt :& RNil)
        . Map.toAscList
        . getModelHits
        . metagenomeModelHits


type PredictedClassificationCols =
     '[ M.VirusName
      , M.ModelName
      , M.NumHits
      ]
     ++
     RDeleteAll '[M.ModelName]
                (RecordColumns Classification)

predictClassification :: Frame MetagenomeHitsRec
                      -> Frame Classification
                      -> FrameRec PredictedClassificationCols
predictClassification hits cls =
    innerJoin @'[M.ModelName] hits cls
