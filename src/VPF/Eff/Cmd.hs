{-# language ConstraintKinds #-}
{-# language UndecidableInstances #-}
module VPF.Eff.Cmd where

import Data.Kind (Constraint, Type)
import Data.Function (fix)

import Control.Eff
import Control.Eff.Extend

import Control.Monad.Base (MonadBase(..))
import Control.Monad.Trans.Control (MonadBaseControl(..), RunInBase)

data Cmd cmd a where
  Cmd :: cmd a -> Cmd cmd a
  GetImpl :: Cmd cmd (Impl cmd)


type family CmdEff (cmd :: Type -> Type) (r :: [Type -> Type]) :: Constraint

data Impl cmd = Impl { runImpl :: forall r a. CmdEff cmd r => cmd a -> Eff r a }


exec :: Member (Cmd cmd) r => cmd a -> Eff r a
exec = send . Cmd

getImpl :: Member (Cmd cmd) r => Eff r (Impl cmd)
getImpl = send GetImpl


withCmd :: a -> Impl cmd -> Eff r a
withCmd a _ = return a

runCmd :: forall cmd r a. (CmdEff cmd r)
       => (forall r' a'. (CmdEff cmd r') => cmd a' -> Eff r' a')
       -> Eff (Cmd cmd ': r) a
       -> Eff r a
runCmd run e = fix (handle_relay withCmd) e (Impl run :: Impl cmd)

-- handle :: CmdEff cmd r'
--        => (Eff r a -> Impl cmd -> Eff r' a)
--        -> Arrs r v a
--        -> Cmd cmd a
--        -> Impl cmd
--        -> Eff r' a
--
-- handle_relay :: (CmdEff cmd r')
--              => (a -> Impl cmd -> Eff r' a)
--              -> (Eff (Cmd cmd ': r) a -> Impl r' k -> Eff r' a)
--              -> Eff (Cmd cmd ': r) a -> Impl r' k -> Eff r' a


instance (CmdEff cmd r') => Handle (Cmd cmd) r a (Impl cmd -> Eff r' a) where
  handle step q GetImpl impl = step (q ^$ impl) impl

  handle step q (Cmd cmd) impl = do
    a <- runImpl impl cmd
    step (q ^$ a) impl


instance (MonadBase m m, LiftedBase m r, CmdEff cmd r) => MonadBaseControl m (Eff (Cmd cmd ': r)) where
    type StM (Eff (Cmd cmd ': r)) a = StM (Eff r) a

    liftBaseWith f = do
        impl <- getImpl
        raise $ liftBaseWith (\runInBase -> f (liftRunInBase impl runInBase))
      where
        -- liftRunInBase :: RunInBase (Eff r) m -> RunInBase (Eff (Cmd cmd ':r )) m
        liftRunInBase :: Impl cmd -> RunInBase (Eff r) m -> forall a. Eff (Cmd cmd ': r) a -> m (StM (Eff r) a)
        liftRunInBase impl runInBase e = runInBase (fix (handle_relay withCmd) e impl)

    restoreM = raise . restoreM

