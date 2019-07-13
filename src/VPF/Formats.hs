module VPF.Formats
  ( Tagged(..)
  , untag
  , Path
  , Directory
  , FASTA
  , SeqType(..)
  , GenBank
  , HMMERModel
  , HMMERTable
  , DSV
  , CSV
  , TSV
  ) where

import GHC.TypeLits (Symbol)

import Data.Kind (Type)
import Data.Tagged


type Path tag = Tagged tag FilePath

data Directory

data FASTA (t :: SeqType)
data SeqType = Nucleotide | Aminoacid
data GenBank

data HMMERModel
data HMMERTable (cols :: [(Symbol, Type)])


data DSV (sep :: Symbol) (cols :: [(Symbol, Type)])

type CSV cols = DSV "," cols
type TSV cols = DSV "\t" cols
