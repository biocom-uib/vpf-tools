{-# language OverloadedLabels #-}
{-# language OverloadedStrings #-}
{-# language TemplateHaskell #-}
module VPF.Model.Class.Cols where

import GHC.TypeLits (KnownSymbol)

import Control.Lens (Iso', iso, (^.))
import Control.Monad ((<=<))

import Data.Text (Text)
import Data.Vector (Vector)

import Data.Vinyl (ElField(..), Rec(..), (=:))
import Data.Vinyl.Lens (rlens)

import Frames (Record, type (:->))
import Frames.InCore (VectorFor)
import Frames.TH (declareColumn, RowGen(..), rowGen, tableTypes')


declareColumn "model_name" ''Text

declareColumn "balt_class"     ''Text
declareColumn "balt_cat"       ''Int
declareColumn "fam_class"      ''Text
declareColumn "fam_cat"        ''Int
declareColumn "host_class"     ''Text
declareColumn "host_cat"       ''Int
declareColumn "host_fam_class" ''Text
declareColumn "host_fam_cat"   ''Int

type RawClassificationCols =
    '[ ModelName
     , BaltClass
     , BaltCat
     , FamClass
     , FamCat
     , HostClass
     , HostCat
     , HostFamClass
     , HostFamCat
     ]

type RawClassification = Record RawClassificationCols


data Class = HomogClass !Text !Int
           | NonHomogClass
           | UndecidedClass
  deriving (Eq, Ord, Show)

type instance VectorFor Class = Vector


declareColumn "balt"     ''Class
declareColumn "fam"      ''Class
declareColumn "host"     ''Class
declareColumn "host_fam" ''Class

type ClassificationCols =
    '[ ModelName
     , Balt
     , Fam
     , Host
     , HostFam
     ]

type Classification = Record ClassificationCols


className :: Class -> Text
className (HomogClass cn _) = cn
className _                = ""

classCat :: Class -> Int
classCat (HomogClass _ cc) = cc
classCat NonHomogClass     = -1
classCat UndecidedClass    = 0


mkClass :: Text -> Int -> Class
mkClass cls cat
  | cat < 0  = NonHomogClass
  | cat == 0 = UndecidedClass
  | cat > 0  = HomogClass cls cat


asClass :: (KnownSymbol s1, KnownSymbol s2, KnownSymbol s3)
            => Iso' (Record '[s1 :-> Text, s2 :-> Int])
                    (Record '[s3 :-> Class])
asClass =
    iso (\(Field cls :& Field cat :& RNil) -> Field (mkClass cls cat)
                                           :& RNil)
        (\(Field cls :& RNil)              -> Field (className cls)
                                           :& Field (classCat cls)
                                           :& RNil)

fromRawClassification :: RawClassification -> Classification
fromRawClassification rc
    =  rc ^. rlens @ModelName
    :& #balt     =: mkClass (rc^.baltClass)    (rc^.baltCat)
    :& #fam      =: mkClass (rc^.famClass)     (rc^.famCat)
    :& #host     =: mkClass (rc^.hostClass)    (rc^.hostCat)
    :& #host_fam =: mkClass (rc^.hostFamClass) (rc^.hostFamCat)
    :& RNil

toRawClassification :: Classification -> RawClassification
toRawClassification cls
    =  cls ^. rlens @ModelName
    :& #balt_class     =: className (cls^.balt)
    :& #balt_cat       =: classCat  (cls^.balt)
    :& #fam_class      =: className (cls^.fam)
    :& #fam_cat        =: classCat  (cls^.fam)
    :& #host_class     =: className (cls^.host)
    :& #host_cat       =: classCat  (cls^.host)
    :& #host_fam_class =: className (cls^.hostFam)
    :& #host_fam_cat   =: classCat  (cls^.hostFam)
    :& RNil

