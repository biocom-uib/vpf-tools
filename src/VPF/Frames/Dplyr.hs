{-# language AllowAmbiguousTypes #-}
{-# language DeriveGeneric #-}
{-# language GeneralizedNewtypeDeriving #-}
{-# language UndecidableInstances #-}
module VPF.Frames.Dplyr
  ( module Exports
  ) where

import VPF.Frames.Dplyr.Basic as Exports
import VPF.Frames.Dplyr.Group as Exports
import VPF.Frames.Dplyr.Index as Exports
import VPF.Frames.Dplyr.Row   as Exports
import VPF.Frames.VinylExts   as Exports (RMonoid(..), RSingleton(..))
