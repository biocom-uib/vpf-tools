{-# language DeriveGeneric #-}
{-# language ImplicitParams #-}
{-# language OverloadedStrings #-}
{-# language Strict #-}
module VPF.Frames.DSV
  ( RowTokenizer
  , ParserOptions(..)
  , ParseCtx(..)
  , ParseError(..)
  , defRowTokenizer
  , defParserOptions
  , parseEitherRow
  , pipeEitherRows
  , produceEitherRows
  , throwLeftsM
  , inCoreAoSExc
  , readFrame

  , WriterOptions(..)
  , defWriterOptions
  , headerToDSV
  , rowToDSV
  , produceFromFrame
  , pipeDSVLines
  , produceDSVLinesFromFrame
  , writeDSV
  ) where

import GHC.Generics (Generic)
import GHC.TypeLits (KnownSymbol, symbolVal)

import Control.Algebra (Has)
import Control.Effect.Throw (Throw, throwError)
import Control.Monad (when, (>=>))
import Control.Exception (try)
import Control.Monad.Catch (Exception, MonadThrow(throwM))
import Control.Monad.IO.Class (MonadIO(liftIO))

import Data.List (intercalate)
import Data.Proxy (Proxy(..))
import Data.Store (Store)
import Data.Tagged (untag)
import Data.Text (Text)
import qualified Data.Text    as T
import Data.Typeable (Typeable)

import Data.Vinyl (ElField, RecMapMethod, RecordToList, rtraverse)
import Data.Vinyl.Functor (Compose(..))

import Frames (ColumnHeaders(..), FrameRec, Record, inCoreAoS)
import Frames.InCore (RecVec)
import qualified Frames.CSV     as CSV
import qualified Frames.ShowCSV as CSV

import Pipes (Consumer, Pipe, Producer, (>->), runEffect)
import qualified Pipes         as P
import qualified Pipes.Prelude as P
import Pipes.Safe (MonadSafe, SafeT)

import VPF.Formats (Path, DSV)


type RowTokenizer = Text -> [Text]

data ParserOptions = ParserOptions
    { hasHeader    :: Bool
    , isComment    :: Text -> Bool
    , rowTokenizer :: RowTokenizer
    }

data ParseCtx = ParseCtx { ctxPath :: FilePath, ctxSep :: String, ctxColNames :: [String] }
  deriving (Eq, Generic)

instance Show ParseCtx where
    show (ParseCtx fp sep colNames) =
        let -- sep      = symbolVal (Proxy @sep)
            -- colNames = columnHeaders (Proxy @(Record cols))
            colsDesc = intercalate ", " colNames
        in
            "file " ++ show fp ++ " with expected columns " ++ colsDesc ++
            " (separated by " ++ show sep ++ ")"

instance Store ParseCtx


type HasParseCtx = (?parseRowCtx :: ParseCtx)


data ParseError = ParseError { parseErrorCtx :: ParseCtx, parseErrorRow :: Text }
  deriving (Eq, Show, Typeable, Generic)

instance Exception ParseError
instance Store ParseError


defRowTokenizer :: Char -> RowTokenizer
defRowTokenizer sep =
    CSV.tokenizeRow CSV.defaultParser { CSV.columnSeparator = T.singleton sep }


defParserOptions :: Char -> ParserOptions
defParserOptions sep = ParserOptions
    { hasHeader = True
    , isComment = const False -- T.all isSpace
    , rowTokenizer = defRowTokenizer sep
    }


parseEitherRow :: (HasParseCtx, CSV.ReadRec cols)
               => RowTokenizer
               -> Text
               -> Either ParseError (Record cols)
parseEitherRow tokenize row =
    case rtraverse getCompose (CSV.readRec (tokenize row)) of
      Left _    -> Left ParseError { parseErrorCtx = ?parseRowCtx, parseErrorRow = row }
      Right rec -> Right rec


pipeEitherRows :: (HasParseCtx, Monad m, CSV.ReadRec cols)
               => ParserOptions
               -> Pipe Text (Either ParseError (Record cols)) m ()
pipeEitherRows opts =
    (if hasHeader opts then P.drop 1 else P.cat)
    >-> P.filter (not . isComment opts)
    >-> P.map (parseEitherRow (rowTokenizer opts))


produceEitherRows :: forall m sep cols.
    ( KnownSymbol sep
    , ColumnHeaders cols
    , CSV.ReadRec cols
    , MonadSafe m
    )
    => ParserOptions
    -> Path (DSV sep cols)
    -> Producer (Either ParseError (Record cols)) m ()
produceEitherRows opts fp =
    CSV.produceTextLines (untag fp) >-> pipeEitherRows opts
  where
    ?parseRowCtx =
        let sep      = symbolVal (Proxy @sep)
            colNames = columnHeaders (Proxy @(Record cols))
        in  ParseCtx (untag fp) sep colNames


throwLeftsM :: (Exception e, MonadThrow m) => Pipe (Either e a) a m ()
throwLeftsM = P.mapM (either throwM return)


inCoreAoSExc ::
    ( RecVec cols
    , MonadIO m
    , Has (Throw ParseError) m
    )
    => Producer (Record cols) (SafeT IO) ()
    -> m (FrameRec cols)
inCoreAoSExc =
    liftIO . try @ParseError . inCoreAoS >=> either throwError return


readFrame ::
    ( KnownSymbol sep, ColumnHeaders cols
    , CSV.ReadRec cols, RecVec cols
    , MonadIO m
    , Has (Throw ParseError) m
    )
    => ParserOptions
    -> Path (DSV sep cols)
    -> m (FrameRec cols)
readFrame opts fp =
    inCoreAoSExc (produceEitherRows opts fp >-> throwLeftsM)


data WriterOptions = WriterOptions
    { writeHeader    :: Bool
    , writeSeparator :: Char
    }
  deriving (Eq, Ord, Show)


defWriterOptions :: Char -> WriterOptions
defWriterOptions sep = WriterOptions
    { writeHeader    = True
    , writeSeparator = sep
    }


headerToDSV :: ColumnHeaders cols => proxy (Record cols) -> Text -> Text
headerToDSV proxy sep =
    T.intercalate sep $ map T.pack (columnHeaders proxy)


rowToDSV ::
    ( RecMapMethod CSV.ShowCSV ElField cols
    , RecordToList cols
    )
    => Text
    -> Record cols
    -> Text
rowToDSV sep = T.intercalate sep . CSV.showFieldsCSV


produceFromFrame :: (Foldable f, Monad m) => f (Record cols) -> Producer (Record cols) m ()
produceFromFrame = P.each


pipeDSVLines :: forall cols m.
    ( RecMapMethod CSV.ShowCSV ElField cols
    , RecordToList cols
    , ColumnHeaders cols
    , Monad m
    )
    => WriterOptions
    -> Pipe (Record cols) Text m ()
pipeDSVLines opts = do
    let sep = T.singleton (writeSeparator opts)

    when (writeHeader opts) $
      P.yield (headerToDSV @cols Proxy sep)

    P.map (rowToDSV sep)


produceDSVLinesFromFrame ::
    ( RecMapMethod CSV.ShowCSV ElField cols
    , RecordToList cols
    , ColumnHeaders cols
    , Monad m
    , Foldable f
    )
    => WriterOptions
    -> f (Record cols)
    -> Producer Text m ()
produceDSVLinesFromFrame opts frame =
    produceFromFrame frame >-> pipeDSVLines opts


writeDSV ::
    ( RecMapMethod CSV.ShowCSV ElField cols
    , RecordToList cols
    , ColumnHeaders cols
    , Foldable f
    , Monad m
    )
    => WriterOptions
    -> Consumer Text m ()
    -> f (Record cols)
    -> m ()
writeDSV opts writer frame =
    runEffect $
        produceFromFrame frame
        >-> pipeDSVLines opts
        >-> writer
