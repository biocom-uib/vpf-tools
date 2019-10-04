{-# language TemplateHaskell #-}
module Control.Effect.MTL.TH where

import Data.Coerce (coerce)
import Data.List (foldl')

import Control.Effect.Carrier
import Control.Effect.MTL

import qualified Control.Monad.IO.Class      as MT
import qualified Control.Monad.Morph         as MT
import qualified Control.Monad.Base          as MT
import qualified Control.Monad.Trans.Control as MT

import qualified Control.Lens as L
import Language.Haskell.TH
import Language.Haskell.TH.Lens


deriveMonadTrans :: Name -> Q [Dec]
deriveMonadTrans carrierName = do
    TyConI (NewtypeD [] _ tyvars Nothing con _) <- reify carrierName

    let tyvarNames :: [Name]
        tyvarNames = map (L.view name) tyvars

        carrierT :: TypeQ
        carrierT = return $ foldl' AppT (ConT carrierName)
                        (map VarT (take (length tyvars -2) tyvarNames))

        conName :: Name
        conName =
          case L.preview name con of
            Just conName -> conName

        carrierConE :: ExpQ
        carrierConE = conE conName

        carrierConP :: String -> PatQ
        carrierConP varName = conP conName [varP (mkName varName)]

        innerM :: Type
        innerM =
          case L.preview types con of
            Just (AppT innerM (VarT _a)) -> innerM

    case innerM of
      VarT _m -> [d|
          instance MT.MonadIO m => MT.MonadIO ($carrierT m) where
              liftIO = $carrierConE . MT.liftIO

          instance MT.MFunctor $carrierT where
              hoist f $(conP conName [varP (mkName "m")]) = $carrierConE (f m)

          instance MT.MonadTrans $carrierT where
              lift = $carrierConE

          instance MT.MonadTransControl $carrierT where
              type StT $carrierT a = a
              liftWith f = coerce (f coerce)
              restoreT = MT.defaultRestoreT $carrierConE

          instance MT.MonadBase b m => MT.MonadBase b ($carrierT m) where
              liftBase = $carrierConE . MT.liftBase

          instance MT.MonadBaseControl b m => MT.MonadBaseControl b ($carrierT m) where
              type StM ($carrierT m) a = MT.ComposeSt $carrierT m a
              liftBaseWith = MT.defaultLiftBaseWith
              restoreM = MT.defaultRestoreM
         |]

      AppT innerTrans (VarT _m) -> [d|
          instance MT.MonadIO m => MT.MonadIO ($carrierT m) where
              liftIO = $carrierConE . MT.liftIO

          instance MT.MFunctor $carrierT where
              hoist f $(carrierConP "m") = $carrierConE (MT.hoist f m)

          instance MT.MonadTrans $carrierT where
              lift = $carrierConE . MT.lift

          instance MT.MonadTransControl $carrierT where
              type StT $carrierT a = MT.StT $(return innerTrans) a
              liftWith = MT.defaultLiftWith $carrierConE coerce
              restoreT = MT.defaultRestoreT $carrierConE

          instance MT.MonadBase b m => MT.MonadBase b ($carrierT m) where
              liftBase = $carrierConE . MT.liftBase

          instance MT.MonadBaseControl b m => MT.MonadBaseControl b ($carrierT m) where
              type StM ($carrierT m) a = MT.ComposeSt $carrierT m a
              liftBaseWith = MT.defaultLiftBaseWith
              restoreM = MT.defaultRestoreM
         |]

      AppT innerT1 (AppT innerT2 (VarT _m)) -> [d|
          instance MT.MonadIO m => MT.MonadIO ($carrierT m) where
              liftIO = $carrierConE . MT.liftIO

          instance MT.MFunctor $carrierT where
              hoist f $(carrierConP "m") = $carrierConE (MT.hoist (MT.hoist f) m)

          instance MT.MonadTrans $carrierT where
              lift = $carrierConE . MT.lift . MT.lift

          instance MT.MonadTransControl $carrierT where
              type StT $carrierT a = MT.StT $(return innerT1) (MT.StT $(return innerT2) a)
              liftWith = MT.defaultLiftWith2 $carrierConE coerce
              restoreT = MT.defaultRestoreT2 $carrierConE

          instance MT.MonadBase b m => MT.MonadBase b ($carrierT m) where
              liftBase = $carrierConE . MT.liftBase

          instance MT.MonadBaseControl b m => MT.MonadBaseControl b ($carrierT m) where
              type StM ($carrierT m) a = MT.ComposeSt $carrierT m a
              liftBaseWith = MT.defaultLiftBaseWith
              restoreM = MT.defaultRestoreM
         |]


deriveCarrier :: Name -> Name -> Q [Dec]
deriveCarrier carrierName interpName = do
    TyConI (NewtypeD [] _ tyvars Nothing con _) <- reify carrierName

    let conName :: Name
        conName =
          case L.preview name con of
            Just conName -> conName

        carrierConE :: ExpQ
        carrierConE = conE conName

        carrierConP :: String -> PatQ
        carrierConP varName = conP conName [varP (mkName varName)]

    VarI _ interpType _ <- reify interpName

    let (interpCxt, algT, carrierT, m, a) =
          case interpType of
            ForallT _ interpCxt (AppT (AppT ArrowT tyAlgMA) tyCarrierMA) ->
                case tyAlgMA of
                  AppT (AppT algT carrierM) (VarT a) ->
                      case tyCarrierMA of
                        AppT carrierM'@(AppT carrierT (VarT m)) (VarT a')
                          | carrierM == carrierM' && a == a' ->
                              (interpCxt, return algT, return carrierT, varT m, varT a)

                        _ -> error $ "the given interpreter does not return the carrier type: " ++ show carrierM
                  _ -> error $ "don't know how to read algebra type in the interpreter type signature: " ++ show tyAlgMA
            _ -> error $ "don't know how to read interpreter Type: " ++ show interpType

    let innerM :: TypeQ
        innerM =
          case L.preview types con of
            Just (AppT innerM (VarT _a)) -> replaceUnderlyingMonad innerM m

    let classCxtT = return $ foldl' AppT (TupleT (length interpCxt)) interpCxt
        interpE = varE interpName

    algName <- newName "alg"
    let algP = varP algName
        algE = varE algName

    innerSigName <- newName "innerSig"
    let innerSigT = varT innerSigName

    sigVarName <- newName "sig"
    let sigT = varT sigVarName

    [d|
        instance
            ( Effect $sigT
            , Carrier $sigT $m
            , Carrier $innerSigT $innerM
            , InjR $sigT $innerSigT
            , $classCxtT
            )
            => Carrier ($algT :+: $sigT) ($carrierT $m) where

            eff (L $algP) = $interpE $algE
            eff (R other) = relayCoerceInner $carrierConE other
      |]
  where
    replaceUnderlyingMonad :: Type -> TypeQ -> TypeQ
    replaceUnderlyingMonad innerM m =
        case innerM of
          VarT _m             -> m
          AppT innerT innerM' -> AppT innerT <$> replaceUnderlyingMonad innerM' m

    transformerDepth :: Type -> Int
    transformerDepth (VarT _m)              = 0
    transformerDepth (AppT _innerT innerM') = 1 + transformerDepth innerM'

    inj :: Int -> ExpQ
    inj n = iterate (\f -> [e| $f . R |]) [e| id |] !! n

 --VarT _m -> [d|

      -- AppT innerTrans (VarT _m) -> do
      --       innerAlg <- newName "malg"
      --       let innerAlgT = varT innerAlg
      --       [d|
      --           instance (Effect $innerAlgT, Carrier ($innerAlgT :+: $sigT) ($(return innerTrans) $m), $classCxtT)
      --               => Carrier ($algT $m $a :+: $sigT) ($carrierT $m) where

      --               eff (L $algP) = $interpE $algE
      --               eff (R other) = relayCarrier (\ $(carrierConP "m1") -> m1) $carrierConE other
      --           |]


      -- AppT innerTrans1 (AppT innerTrans2 (VarT _m)) -> do
      --       innerAlg1 <- newName "malg"
      --       let innerAlg1T = varT innerAlg1

      --       innerAlg2 <- newName "malg"
      --       let innerAlg2T = varT innerAlg2
      --       [d|
      --           instance (Effect $innerAlgT, Carrier ($innerAlgT :+: $sigT) ($(return innerTrans) $m), $classCxtT)
      --               => Carrier ($algT $m $a :+: $sigT) ($carrierT $m) where

      --               eff (L $algP) = $interpE $algE
      --               eff (R other) = relayCarrier (\ $(carrierConP "m1") -> m1) $carrierConE other
      --           |]
