module VPF.Util.Frames where

import Data.Foldable (forM_)
import Data.Proxy (Proxy(..))
import Data.Text (Text)
import qualified Data.Text    as T
import qualified Data.Text.IO as T

import Data.Vinyl (ElField, RecMapMethod)
import Data.Vinyl.Core (RecordToList)

import Frames (Record, ColumnHeaders(..))
import Frames.CSV (showFieldsCSV)
import Frames.ShowCSV (ShowCSV)

import Pipes (Producer, Consumer, yield)
import qualified Pipes.Prelude as P

import System.IO (Handle)


produceDSV :: forall ts f m.
           ( RecMapMethod ShowCSV ElField ts
           , RecordToList ts
           , ColumnHeaders ts
           , Foldable f
           , Monad m
           )
           => Text -> f (Record ts) -> Producer Text m ()
produceDSV sep records = do
    let headers = map T.pack (columnHeaders (Proxy @(Record ts)))

    yield $ T.intercalate sep headers

    forM_ records (yield . T.intercalate sep . showFieldsCSV )


