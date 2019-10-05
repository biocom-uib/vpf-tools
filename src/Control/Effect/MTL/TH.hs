{-# language TemplateHaskell #-}
module Control.Effect.MTL.TH where

import Data.Bifunctor (first)
import Data.Coerce (coerce)
import Data.List ((\\), foldl')
import qualified Data.Map as Map
import Data.Maybe (fromMaybe)

import Control.Effect.Carrier
import Control.Effect.MTL

import qualified Control.Monad.IO.Class      as MT
import qualified Control.Monad.Morph         as MT
import qualified Control.Monad.Base          as MT
import qualified Control.Monad.Trans.Control as MT

import qualified Control.Lens as L
import Language.Haskell.TH
import Language.Haskell.TH.Lens



tyConName :: Type -> Q Name
tyConName (ConT n)    = return n
tyConName (AppT ty _) = tyConName ty
tyConName ty          = fail $ "cannot find concrete type constructor of " ++ show ty


replaceTyVars :: Type -> Type -> Type
replaceTyVars (AppT t1 t2)         (AppT t1' t2')          = AppT (replaceTyVars t1 t1') (replaceTyVars t2 t2')
-- replaceTyVars (AppKindT t k)       (AppKindT t' k')        = AppKindT (replaceTyVars t t') (replaceTyVars k k')
replaceTyVars (SigT t k)           (SigT t' k')            = SigT (replaceTyVars t t') (replaceTyVars k k')
replaceTyVars (VarT _n)            (VarT n')               = VarT n'
replaceTyVars (ConT n)             (ConT _n')              = ConT n
replaceTyVars (PromotedT n)        (PromotedT _n)          = PromotedT n
replaceTyVars (InfixT t1 _n t2)    (InfixT t1' n' t2')     = InfixT (replaceTyVars t1 t1') n' (replaceTyVars t2 t2')
replaceTyVars (UInfixT t1 _n t2)   (UInfixT t1' n' t2')    = UInfixT (replaceTyVars t1 t1') n' (replaceTyVars t2 t2')
replaceTyVars (ParensT t)          (ParensT t')            = ParensT (replaceTyVars t t')
replaceTyVars (TupleT i)           (TupleT _i')            = TupleT i
replaceTyVars (UnboxedTupleT i)    (UnboxedTupleT _i')     = UnboxedTupleT i
replaceTyVars (UnboxedSumT a)      (UnboxedSumT _a')       = UnboxedSumT a
replaceTyVars ArrowT               ArrowT                  = ArrowT
replaceTyVars EqualityT            EqualityT               = EqualityT
replaceTyVars ListT                ListT                   = ListT
replaceTyVars (PromotedTupleT i)   (PromotedTupleT _i')    = PromotedTupleT i
replaceTyVars PromotedNilT         PromotedNilT            = PromotedNilT
replaceTyVars PromotedConsT        PromotedConsT           = PromotedConsT
replaceTyVars StarT                StarT                   = StarT
replaceTyVars ConstraintT          ConstraintT             = ConstraintT
replaceTyVars (LitT lit)           (LitT _lit')            = LitT lit
replaceTyVars WildCardT            WildCardT               = WildCardT
-- replaceTyVars (ImplicitParamT s t) (ImplicitParamT _s' t') = ImplicitParamT s (replaceTyVars t t')


decomposeTransStack :: Type -> ([Type], Type)
decomposeTransStack = go
  where
    go :: Type -> ([Type], Type)
    go (AppT t m) = first (t:) (go m)
    go m          = ([], m)


applyTransStack :: [Type] -> Type -> Type
applyTransStack ts m = foldr AppT m ts


cxtType :: Cxt -> Type
cxtType cxt = foldl' AppT (TupleT (length cxt)) cxt


makeRenamer :: [Name] -> [Name] -> Map.Map Name Type
makeRenamer ns1 ns2 = Map.fromList $ zipWith (\n1 n2 -> (n1, VarT n2)) ns1 ns2


data CarrierInfo = CarrierInfo
    { carrierTypeName    :: Name
    , carrierConName     :: Name
    , carrierExtraTyVars :: [Name]
    , carrierInnerStack  :: [Type]
    , carrierBaseMonad   :: Name
    , carrierBaseValue   :: Name
    , carrierDerives     :: [DerivClause]
    }


getCarrierInfo :: Maybe [Name] -> Name -> Q CarrierInfo
getCarrierInfo mayTyArgs carrierName = do
    TyConI carrierDec <- reify carrierName

    (tyVars, carrierConName, innerCarrierM, a, derives) <-
        case carrierDec of
          NewtypeD [] _name conTyVars _kind con derives
            | [satInnerCarrierType] <- L.toListOf types con -> do

                let conTyVarNames = L.toListOf typeVars conTyVars
                    tyVars        = fromMaybe conTyVarNames mayTyArgs
                    conName       = L.view name con

                case doRenaming mayTyArgs satInnerCarrierType of
                  AppT innerCarrierM (VarT a) ->
                      return (tyVars, conName, innerCarrierM, a, derives)

                  t -> fail $ "error: the inner carrier type is not of the form M a: " ++ show t

          _ -> fail $ "invalid carrier declaration: " ++ show carrierDec

    (stack, m) <-
        case decomposeTransStack innerCarrierM of
          (ts, VarT m) -> return (ts, m)
          _ -> fail "the carrier stack does not have a free type variable as base monad"

    return CarrierInfo
        { carrierTypeName    = carrierName
        , carrierConName     = carrierConName
        , carrierExtraTyVars = tyVars \\ [m, a]
        , carrierInnerStack  = stack
        , carrierBaseMonad   = m
        , carrierBaseValue   = a
        , carrierDerives     = derives
        }
  where
    doRenaming =
        case mayTyArgs of
          Nothing      -> const id
          Just renames -> \tyVars ->
            let renamer = makeRenamer (L.toListOf typeVars tyVars) renames
            in  substType renamer


carrierTrans :: CarrierInfo -> Type
carrierTrans c = foldl' AppT (ConT (carrierTypeName c)) (map VarT (carrierExtraTyVars c))


data InterpInfo = InterpInfo
    { interpCxt       :: [Pred]
    , interpEffType   :: Type
    , interpCarrierT  :: Type
    , interpBaseM     :: Name
    , interpBaseValue :: Name
    }


getInterpInfo :: Name -> Q InterpInfo
getInterpInfo interpName = do
    VarI _name interpType _rhs <- reify interpName

    (interpCxt, satEffType, satCarrierType) <-
        case interpType of
          ForallT _tyVars cxt (AppT (AppT ArrowT effType) cType) ->
              return (cxt, effType, cType)

          _ -> fail $ "could not match interpreter type with forall <vars>. Cxt => Eff M a -> M a: "
                        ++ show interpType

    (effType, carrierM, a) <-
        case satEffType of
          AppT (AppT effType carrierM) (VarT a) ->
              return (effType, carrierM, a)

          _ -> fail $ "could not match saturated effect type with Eff M a: " ++ show satEffType

    (carrierT, m) <-
        case satCarrierType of
          AppT carrierM' (VarT a')
            | AppT carrierT (VarT m) <- carrierM'
            , carrierM == carrierM' && a == a' ->
                 return (carrierT, m)
          _ -> fail "error inspecting carrier type"

    return InterpInfo
        { interpCxt       = interpCxt
        , interpEffType   = effType
        , interpCarrierT  = carrierT
        , interpBaseM     = m
        , interpBaseValue = a
        }


interpSatCarrier :: InterpInfo -> Type
interpSatCarrier i =
    interpCarrierT i
        `AppT` VarT (interpBaseM i)
        `AppT` VarT (interpBaseValue i)


interpCarrierTyVars :: InterpInfo -> [Name]
interpCarrierTyVars = L.toListOf typeVars . interpSatCarrier


deriveCarrier :: Name -> Q [Dec]
deriveCarrier interpName = do
    interp <- getInterpInfo interpName

    carrierName <- tyConName (interpCarrierT interp)

    carrier <- getCarrierInfo (Just (interpCarrierTyVars interp))
                            carrierName

    sigName <- newName "sig"
    innerSigName <- newName "innerSig"
    effName <- newName "eff"

    let sigQ = varT sigName
        carrierTQ = return (interpCarrierT interp)
        carrierConQ = conE (carrierConName carrier)
        cxtQ = return (cxtType (interpCxt interp))

        m = VarT (interpBaseM interp)

        innerSigQ = varT innerSigName
        innerMQ = return (applyTransStack (carrierInnerStack carrier) m)
        mQ = return m

        interpQ = varE interpName
        effTQ = return (interpEffType interp)
        effPQ = varP effName
        effEQ = varE effName

    [d|
        instance
            ( Effect $sigQ
            , Carrier $sigQ $mQ
            , Carrier $innerSigQ $innerMQ
            , InjR $sigQ $innerSigQ
            , $cxtQ
            )
            => Carrier ($effTQ :+: $sigQ) ($carrierTQ $mQ) where

            eff (L $effPQ) = $interpQ $effEQ
            eff (R other)  = relayCoerceInner $carrierConQ other
      |]


deriveMonadTrans :: Name -> Q [Dec]
deriveMonadTrans carrierName = do
    carrier <- getCarrierInfo Nothing carrierName

    let carrierConE :: ExpQ
        carrierConE = conE (carrierConName carrier)

        carrierConP :: String -> PatQ
        carrierConP varName = conP (carrierConName carrier) [varP (mkName varName)]

        carrierQ :: TypeQ
        carrierQ = return (carrierTrans carrier)

        ts :: [Type]
        ts = carrierInnerStack carrier

        innerT :: TypeQ -> TypeQ
        innerT = fmap (applyTransStack ts)

        mapConstraint :: TypeQ -> [Type] -> TypeQ
        mapConstraint cq tys = do
            c <- cq
            return $ foldl' (\cxt t -> AppT cxt (AppT c t)) (TupleT (length tys)) ts

    let liftWithLayer :: Type -> (ExpQ -> ExpQ) -> ExpQ -> ExpQ
        liftWithLayer _t innerExp runs = do
            runName <- newName "run"
            let runP = varP runName
                runE = varE runName

            [e| MT.liftWith (\ $runP -> $(innerExp [e| $runE . $runs |])) |]

        mkLiftWith :: ExpQ -> ExpQ
        mkLiftWith f = foldr liftWithLayer
                            (\runs -> [e| $f $runs |])
                            ts
                            [e| coerce |]

    [d|
        instance (Monad ($carrierQ m), MT.MonadIO $(innerT [t| m |]))
            => MT.MonadIO ($carrierQ m) where

            liftIO io = $carrierConE (MT.liftIO io)

        instance $(mapConstraint [t| MT.MFunctor |] ts)
            => MT.MFunctor $carrierQ where

            hoist f $(carrierConP "m") =
                $carrierConE $
                    $(foldr (\_t f' -> [e| MT.hoist $f' |]) [e| f |] ts)
                    m

        instance $(mapConstraint [t| MT.MonadTrans |] ts)
            => MT.MonadTrans $carrierQ where

            lift m =
                $carrierConE
                    $(foldr (\_t m' -> [e| MT.lift $m' |]) [e| m |] ts)

        instance $(mapConstraint [t| MT.MonadTransControl |] ts)
            => MT.MonadTransControl $carrierQ where

            type StT $carrierQ a =
              $(foldr (\t st -> [t| MT.StT $(return t) $st |]) [t| a |] ts)

            liftWith f = $carrierConE $(mkLiftWith [e| f |])

            restoreT st =
                $carrierConE
                    $(foldr (\_t st' -> [e| MT.restoreT $st' |]) [e| st |] ts)

        instance (Monad b, Monad ($carrierQ m), MT.MonadBase b $(innerT [t| m |]))
            => MT.MonadBase b ($carrierQ m) where

            liftBase b = $carrierConE (MT.liftBase b)

        instance (Monad b, Monad ($carrierQ m), MT.MonadBaseControl b $(innerT [t| m |]))
            => MT.MonadBaseControl b ($carrierQ m) where

            type StM ($carrierQ m) a = MT.StM $(innerT [t| m |]) a

            liftBaseWith f = $carrierConE (MT.liftBaseWith (\run -> f (run . unwrap)))
              where
                unwrap :: $carrierQ m a -> $(innerT [t| m |]) a
                unwrap $(carrierConP "m") = m

            restoreM st = $carrierConE (MT.restoreM st)
      |]
