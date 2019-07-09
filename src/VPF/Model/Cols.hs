{-# language OverloadedStrings #-}
{-# language TemplateHaskell #-}
module VPF.Model.Cols where

import Data.Text (Text)

import Frames.TH (declareColumn)

declareColumn "virus_name"   ''Text
declareColumn "protein_name" ''Text
declareColumn "model_name"   ''Text
declareColumn "num_hits"     ''Int
