{-# language TemplateHaskell #-}
module Control.Carrier.MTL.TH
  ( deriveMonadTrans
  , deriveMonadTransExcept
  --, deriveAlgebra
  ) where

import Data.Bifunctor (first)
import Data.Profunctor.Unsafe ((#.), (.#))
import Data.Coerce (Coercible, coerce)
import Data.List ((\\), foldl')
import qualified Data.Map as Map
import Data.Maybe (fromMaybe)
import Data.Monoid (Ap(..))

import qualified Control.Monad.Catch         as MT
import qualified Control.Monad.IO.Class      as MT
import qualified Control.Monad.Morph         as MT
import qualified Control.Monad.Base          as MT
import qualified Control.Monad.Trans.Control as MT
import qualified Control.Monad.Trans.Writer  as MT

import qualified Control.Lens as L
import Language.Haskell.TH
import Language.Haskell.TH.Lens



decomposeTransStack :: Type -> ([Type], Type)
decomposeTransStack = go
  where
    go :: Type -> ([Type], Type)
    go (AppT t m) = first (t:) (go m)
    go m          = ([], m)


applyTransStack :: [Type] -> Type -> Type
applyTransStack ts m = foldr AppT m ts


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

carrierTrans :: CarrierInfo -> Type
carrierTrans c = foldl' AppT (ConT (carrierTypeName c)) (map VarT (carrierExtraTyVars c))



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

                case doRenaming conTyVarNames satInnerCarrierType of
                  AppT innerCarrierM (VarT a) -> do

                      return (tyVars, conName, innerCarrierM, a, derives)

                  t -> fail $ "error: the inner carrier type is not of the form M a: " ++ pprint t

          _ -> fail $ "invalid carrier declaration: " ++ pprint carrierDec

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


unwrap1 :: Coercible f f' => (f' a -> f a) -> f a -> f' a
unwrap1 _ = coerce
{-# inline unwrap1 #-}


deriveMonadTransExcept :: Name -> [Name] -> Q [Dec]
deriveMonadTransExcept carrierName excluded = do
    carrier <- getCarrierInfo Nothing carrierName

    let carrierConE :: ExpQ
        carrierConE = conE (carrierConName carrier)

        carrierConP :: PatQ -> PatQ
        carrierConP binding = conP (carrierConName carrier) [binding]

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
                            [e| unwrap1 $carrierConE |]

    getAp $ MT.execWriter $ do
        ifWanted ''Functor $ [d|
            instance Functor $(innerT [t| m |]) => Functor ($carrierQ m) where
                fmap f $(carrierConP [p| m |]) = $carrierConE (fmap f m)
                {-# inline fmap #-}
          |]

        ifWanted ''Applicative [d|
            instance Applicative $(innerT [t| m |]) => Applicative ($carrierQ m) where
                pure a = $carrierConE (pure a)
                {-# inline pure #-}

                $(carrierConP [p| fab |]) <*> $(carrierConP [p| fa |]) =
                    $carrierConE (fab <*> fa)
                {-# inline (<*>) #-}
          |]

        ifWanted ''Monad [d|
            instance Monad $(innerT [t| m |]) => Monad ($carrierQ m) where
                return a = $carrierConE (return a)
                {-# inline return #-}

                $(carrierConP [p| m |]) >>= f = $carrierConE (m >>= unwrap1 $carrierConE #. f)
                {-# inline (>>=) #-}
          |]

        ifWanted ''MT.MonadIO [d|
            instance (Monad ($carrierQ m), MT.MonadIO $(innerT [t| m |]))
                => MT.MonadIO ($carrierQ m) where

                liftIO io = $carrierConE (MT.liftIO io)
                {-# inline liftIO #-}
          |]


        ifWanted ''MT.MonadThrow [d|
            instance MT.MonadThrow $(innerT [t| m |])
                => MT.MonadThrow ($carrierQ m) where

                throwM e = $carrierConE (MT.throwM e)
                {-# inline throwM #-}
          |]


        ifWanted ''MT.MonadCatch [d|
            instance MT.MonadCatch $(innerT [t| m |])
                => MT.MonadCatch ($carrierQ m) where

                catch $(carrierConP [p| m |]) h = $carrierConE (MT.catch m (unwrap1 $carrierConE #. h))
                {-# inline catch #-}
          |]


        ifWanted ''MT.MonadMask [d|
            instance MT.MonadMask $(innerT [t| m |])
                => MT.MonadMask ($carrierQ m) where

                mask action =
                    $carrierConE $
                        MT.mask $ \restore ->
                            case action ($carrierConE #. restore .# coerce) of
                              $(carrierConP [p| m |]) -> m
                {-# inline mask #-}

                uninterruptibleMask action =
                    $carrierConE $
                        MT.uninterruptibleMask $ \restore ->
                            case action ($carrierConE #. restore .# coerce) of
                              $(carrierConP [p| m |]) -> m
                {-# inline uninterruptibleMask #-}

                generalBracket acquire release action =
                    $carrierConE
                        (MT.generalBracket
                            (unwrap1 $carrierConE acquire)
                            (coerce release)
                            (unwrap1 $carrierConE #. action))
                {-# inline generalBracket #-}
          |]


        ifWanted ''MT.MFunctor [d|
            instance $(mapConstraint [t| MT.MFunctor |] ts)
                => MT.MFunctor $carrierQ where

                hoist f $(carrierConP [p| m |]) =
                    $carrierConE $
                        $(foldr (\_t f' -> [e| MT.hoist $f' |]) [e| f |] ts)
                        m
                {-# inline hoist #-}
          |]


        ifWanted ''MT.MonadTrans [d|
            instance $(mapConstraint [t| MT.MonadTrans |] ts)
                => MT.MonadTrans $carrierQ where

                lift m =
                    $carrierConE
                        $(foldr (\_t m' -> [e| MT.lift $m' |]) [e| m |] ts)
                {-# inline lift #-}
          |]


        ifWanted ''MT.MonadTransControl [d|
            instance $(mapConstraint [t| MT.MonadTransControl |] ts)
                => MT.MonadTransControl $carrierQ where

                type StT $carrierQ a =
                  $(foldr (\t st -> [t| MT.StT $(return t) $st |]) [t| a |] ts)

                liftWith f = $carrierConE $(mkLiftWith [e| f |])
                {-# inline liftWith #-}

                restoreT st =
                    $carrierConE
                        $(foldr (\_t st' -> [e| MT.restoreT $st' |]) [e| st |] ts)
                {-# inline restoreT #-}
          |]


        ifWanted ''MT.MonadBase [d|
            instance (Monad b, Monad ($carrierQ m), MT.MonadBase b $(innerT [t| m |]))
                => MT.MonadBase b ($carrierQ m) where

                liftBase b = $carrierConE (MT.liftBase b)
                {-# inline liftBase #-}
          |]


        ifWanted ''MT.MonadBaseControl [d|
            instance (Monad b, Monad ($carrierQ m), MT.MonadBaseControl b $(innerT [t| m |]))
                => MT.MonadBaseControl b ($carrierQ m) where

                type StM ($carrierQ m) a = MT.StM $(innerT [t| m |]) a

                liftBaseWith f = $carrierConE (MT.liftBaseWith (\run -> f (run .# unwrap1 $carrierConE)))
                {-# inline liftBaseWith #-}

                restoreM st = $carrierConE (MT.restoreM st)
                {-# inline restoreM #-}
          |]

  where
    ifWanted :: Name -> DecsQ -> MT.Writer (Ap Q [Dec]) ()
    ifWanted className decs
      | className `elem` excluded = return ()
      | otherwise                 = MT.tell (Ap decs)


deriveMonadTrans :: Name -> Q [Dec]
deriveMonadTrans carrierName = deriveMonadTransExcept carrierName []

