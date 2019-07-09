{-# language OverloadedStrings #-}
{-# language TemplateHaskell #-}
module VPF.Ext.HMMER.Search.Cols where

import Data.Text (Text)

import Frames (Record)
import Frames.TH (declareColumn)

import VPF.Ext.HMMER.TableFormat (Accession)


declareColumn "target name"      ''Text
declareColumn "target accession" ''Accession
declareColumn "query name"       ''Text
declareColumn "query accession"  ''Accession

declareColumn "sequence E-value" ''Double
declareColumn "sequence score"   ''Double
declareColumn "sequence bias"    ''Double

declareColumn "domain E-value" ''Double
declareColumn "domain score"   ''Double
declareColumn "domain bias"    ''Double

declareColumn "exp estimation" ''Double
declareColumn "reg estimation" ''Int
declareColumn "clu estimation" ''Int
declareColumn "ov estimation"  ''Int
declareColumn "env estimation" ''Int
declareColumn "dom estimation" ''Int
declareColumn "rep estimation" ''Int
declareColumn "inc estimation" ''Int


type ProtSearchHitCols =
    '[ TargetName
     , TargetAccession
     , QueryName
     , QueryAccession

     , SequenceEValue
     , SequenceScore
     , SequenceBias

     , DomainEValue
     , DomainScore
     , DomainBias

     , ExpEstimation
     , RegEstimation
     , CluEstimation
     , OvEstimation
     , EnvEstimation
     , DomEstimation
     , RepEstimation
     , IncEstimation
     ]

type ProtSearchHit = Record ProtSearchHitCols
