{-# language TemplateHaskell #-}
module Control.Effect.MTL.TH where

import Data.Coerce (coerce)
import Data.List (foldl')

import qualified Control.Monad.IO.Class      as MT
import qualified Control.Monad.Morph         as MT
import qualified Control.Monad.Base          as MT
import qualified Control.Monad.Trans.Control as MT

import qualified Control.Lens as L
import Language.Haskell.TH
import Language.Haskell.TH.Lens


deriveMonadTrans :: Name -> Q [Dec]
deriveMonadTrans decName = do
    TyConI (NewtypeD [] _ tyvars Nothing con _) <- reify decName

    let ntyvars = length tyvars
        tyvarNames = map (L.view name) tyvars

    Just conName <- return $ L.preview name con

    Just (AppT (AppT innerT innerM) (VarT _a)) <- return $ L.preview types con

    let t :: Q Type
        t = return $ foldl' AppT (ConT decName) (map VarT (take (ntyvars-2) tyvarNames))

    case innerM of
        VarT _m -> [d|
            instance MT.MonadIO m => MT.MonadIO ($t m) where
                liftIO = $(conE conName) . MT.liftIO

            instance MT.MFunctor $t where
                hoist f ($(conP conName [varP (mkName "m")])) = $(conE conName) (MT.hoist f m)

            instance MT.MonadTrans $t where
                lift = $(conE conName) . MT.lift

            instance MT.MonadTransControl $t where
                type StT $t a = MT.StT $(return innerT) a
                liftWith = MT.defaultLiftWith $(conE conName) coerce
                restoreT = MT.defaultRestoreT $(conE conName)

            instance MT.MonadBase b m => MT.MonadBase b ($t m) where
                liftBase = $(conE conName) . MT.liftBase

            instance MT.MonadBaseControl b m => MT.MonadBaseControl b ($t m) where
                type StM ($t m) a = MT.ComposeSt $t m a
                liftBaseWith = MT.defaultLiftBaseWith
                restoreM = MT.defaultRestoreM
           |]

        AppT innerT2 (VarT _m) -> [d|
            instance MT.MonadIO m => MT.MonadIO ($t m) where
                liftIO = $(conE conName) . MT.liftIO

            instance MT.MFunctor $t where
                hoist f ($(conP conName [varP (mkName "m")])) = $(conE conName) (MT.hoist (MT.hoist f) m)

            instance MT.MonadTrans $t where
                lift = $(conE conName) . MT.lift . MT.lift

            instance MT.MonadTransControl $t where
                type StT $t a = MT.StT $(return innerT) (MT.StT $(return innerT2) a)
                liftWith = MT.defaultLiftWith2 $(conE conName) coerce
                restoreT = MT.defaultRestoreT2 $(conE conName)

            instance MT.MonadBase b m => MT.MonadBase b ($t m) where
                liftBase = $(conE conName) . MT.liftBase

            instance MT.MonadBaseControl b m => MT.MonadBaseControl b ($t m) where
                type StM ($t m) a = MT.ComposeSt $t m a
                liftBaseWith = MT.defaultLiftBaseWith
                restoreM = MT.defaultRestoreM
          |]
