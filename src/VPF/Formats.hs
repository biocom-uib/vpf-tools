module VPF.Formats
  ( Tagged(..)
  , untag
  , Path
  , Directory
  , Executable
  , FASTA
  , SeqType(..)
  , GenBank
  , HMMERModel
  , HMMERTable
  , DSV
  , CSV
  , TSV
  , JSON
  , YAML
  ) where

import GHC.TypeLits (Symbol)

import Data.Kind (Type)
import Data.Store (Store)
import Data.Tagged


type Path tag = Tagged tag FilePath

data Directory
data Executable

data FASTA (t :: SeqType)
data SeqType = Nucleotide | Aminoacid
data GenBank

data HMMERModel
data HMMERTable (cols :: [(Symbol, Type)])


data DSV (sep :: Symbol) (cols :: [(Symbol, Type)])

type CSV cols = DSV "," cols
type TSV cols = DSV "\t" cols

data JSON (a :: Type)
data YAML (a :: Type)


instance Store a => Store (Tagged tag a)
