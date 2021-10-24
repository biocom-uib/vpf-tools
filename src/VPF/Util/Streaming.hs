module VPF.Util.Streaming where

import Control.Exception (Exception)
import Control.Monad.Catch (MonadThrow, throwM)

import Streaming (Stream, Of ((:>)))
import Streaming qualified as S
import Streaming.Prelude qualified as S


stopStreamOnFirstError ::
    Monad m
    => Stream (Of (Either e a)) m r
    -> Stream (Of a) m (Either e r)
stopStreamOnFirstError =
    S.streamFold (return . Right) S.effect \case
        Left e  :> _ -> return (Left e)
        Right a :> s -> S.wrap (a :> s)


throwStreamLefts ::
    (Exception e, MonadThrow m)
    => Stream (Of (Either e a)) m ()
    -> Stream (Of a) m ()
throwStreamLefts = S.mapM (either throwM return)


wrapEitherOf :: Of a (Either e ()) -> Either e a
wrapEitherOf = fmap S.fst' . sequence
