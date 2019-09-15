{-# language OverloadedStrings #-}
{-# language TemplateHaskell #-}
module VPF.Model.Cols where

import Data.Text (Text)

import Frames.TH (declareColumn)

declareColumn "virus_name"   ''Text
declareColumn "protein_name" ''Text
declareColumn "model_name"   ''Text
declareColumn "num_hits"     ''Int

declareColumn "k_base_size"       ''Double
declareColumn "protein_hit_score" ''Double
declareColumn "virus_hit_score"   ''Double
declareColumn "membership_ratio"  ''Double
declareColumn "confidence_score"  ''Double
