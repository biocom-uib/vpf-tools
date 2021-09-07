module VPF.Util.Lift where

import Control.Monad ((<=<))
import Control.Monad.IO.Class (MonadIO (liftIO))
import Control.Effect.Throw


liftEitherIO :: (MonadIO m, Has (Throw e) m) => IO (Either e a) -> m a
liftEitherIO = liftEither <=< liftIO
