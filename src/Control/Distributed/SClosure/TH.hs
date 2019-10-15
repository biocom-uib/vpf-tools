{-# language StaticPointers #-}
{-# language TemplateHaskell #-}
module Control.Distributed.SClosure.TH
  ( sinstanceFor
  , sdictTypeOf
  , sliftE
  , sencode
  ) where

import Control.Distributed.SClosure
import qualified Control.Lens as L

import Data.List (foldl')
import qualified Data.Map as Map

import Language.Haskell.TH
import qualified Language.Haskell.TH.Lens as L


splitArrows :: Type -> ([Type], Type)
splitArrows (AppT (AppT ArrowT t1) t2) = let (tys, r) = splitArrows t2 in (t1 : tys, r)
splitArrows r                          = ([], r)


contextOf :: Name -> [Type] -> TypeQ
contextOf name tyVars = do
    VarI _ (ForallT decTyBndrs cxt _ty) _dec <- reify name

    let decTyVars = L.toListOf L.typeVars decTyBndrs
        rename = L.substType $ Map.fromAscList (zip decTyVars tyVars)

    let cxtType = foldl' AppT (TupleT (length cxt)) (map rename cxt)

    return cxtType


unapplyTypes :: Exp -> Q (Name, [Type])
unapplyTypes = go []
  where
    go tys (AppTypeE exp ty) = go (ty:tys) exp
    go tys (VarE name)       = return (name, tys)
    go _   exp               = fail $ "unapplyTypes: the provided expression is not of the form f @a @b ...: " ++ show exp


sinstanceFor :: ExpQ -> TypeQ
sinstanceFor expQ = [t| SInstance $(uncurry contextOf =<< unapplyTypes =<< expQ) |]


sdictTypeOf :: ExpQ -> TypeQ
sdictTypeOf expQ = [t| SDict $(uncurry contextOf =<< unapplyTypes =<< expQ) |]


sliftE :: ExpQ -> ExpQ
sliftE expQ = do
    exp <- expQ
    (fname, tyVars) <- unapplyTypes exp

    VarI _fname (ForallT decTyBndrs _ decTy) _dec <- reify fname

    let decTyVars = L.toListOf L.typeVars decTyBndrs
        rename = L.substType $ Map.fromAscList (zip decTyVars tyVars)

    let (tys, _) = splitArrows (rename decTy)

    let mkArg ty = do
          a <- newName "a"
          return (return ty, a)

    tyArgs <- mapM mkArg tys

    let lambda :: ExpQ
        lambda = foldr (\(ty, arg) lam -> [| \($(varP arg) :: $ty) -> $lam |]) lambdaBody tyArgs

        lambdaBody :: ExpQ
        lambdaBody = foldl' (\exp arg -> [| $exp $(varE arg) |]) (return exp) (map (\(_, arg) -> arg) tyArgs)

    [e| static (\(Dict :: Dict $(contextOf fname tyVars)) -> $lambda) |]


-- slift :: Name -> [TypeQ] -> ExpQ
-- slift fname tyVarsQ = do
--     VarI _fname (ForallT _ _ decTy) _dec <- reify fname
--
--     let (tys, r) = splitArrows decTy
--
--     tyArgs <- mapM (\ty -> (,) ty <$> newName "a") tys
--
--     [e| static (\(cxt :: $(sdictFor fname tyVarsQ)) -> withSDict cxt $(go tyArgs []) :: $(return r)) |]
--   where
--     go = buildLambda
--
--     buildLambda :: [(Type, Name)] -> [Name] -> ExpQ
--     buildLambda ((ty, arg) : tyArgs) args = [e| \ ($(varP arg) :: $(return ty)) -> $(buildLambda tyArgs (arg:args)) |]
--     buildLambda []                   args = applyArgs args
--
--     applyArgs :: [Name] -> ExpQ
--     applyArgs = foldl' (\exp arg -> appE exp (varE arg)) (varE fname)
--
--
sencode :: ExpQ -> ExpQ
sencode exp = [e| spureWith (static Dict) $exp |]
