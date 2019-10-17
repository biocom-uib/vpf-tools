{-# language OverloadedStrings #-}
{-# language Strict #-}
{-# language TemplateHaskell #-}
module VPF.Ext.HMMER.Search.Cols where

import Data.Text (Text)

import Frames (Record)
import Frames.TH (declareColumn)

import VPF.Ext.HMMER.TableFormat (Accession)


declareColumn "target_name"      ''Text
declareColumn "target_accession" ''Accession
declareColumn "query_name"       ''Text
declareColumn "query_accession"  ''Accession

declareColumn "sequence_e_value" ''Double
declareColumn "sequence_score"   ''Double
declareColumn "sequence_bias"    ''Double

declareColumn "domain_e_value" ''Double
declareColumn "domain_score"   ''Double
declareColumn "domain_bias"    ''Double

declareColumn "exp_estimation" ''Double
declareColumn "reg_estimation" ''Int
declareColumn "clu_estimation" ''Int
declareColumn "ov_estimation"  ''Int
declareColumn "env_estimation" ''Int
declareColumn "dom_estimation" ''Int
declareColumn "rep_estimation" ''Int
declareColumn "inc_estimation" ''Int

declareColumn "description" ''Text


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

     , Description
     ]

type ProtSearchHit = Record ProtSearchHitCols
