{-# language ImplicitParams #-}
{-# language OverloadedStrings #-}
{-# language StrictData #-}
module VPF.Util.DSV
  ( RowTokenizer
  , ParserOptions(..)
  , ParseCtx(..)
  , RowParseError(..)
  , defRowTokenizer
  , defParserOptions
  , parseEitherRow
  , pipeEitherRows
  , produceEitherRows
  , throwLeftsM
  , inCoreAoSExc
  , readFrameExc

  , WriterOptions(..)
  , defWriterOptions
  , produceFromFrame
  , pipeDSVLines
  , stdoutWriter
  , fileWriter
  , writeDSV
  ) where

import GHC.TypeLits (Symbol, KnownSymbol, symbolVal)

import Control.Eff (Member, Lifted, lift, Eff)
import Control.Eff.Exception (Exc, liftEither)
import Control.Monad (when, (>=>))
import Control.Exception (try)
import Control.Monad.Catch (Exception, MonadThrow(throwM))
import Control.Monad.IO.Class (MonadIO(liftIO))

import Data.Char (isSpace)
import Data.Kind (Type)
import Data.List (intercalate)
import Data.Proxy (Proxy(..))
import Data.Tagged (Tagged(..), untag)
import Data.Text (Text)
import qualified Data.Text    as T
import qualified Data.Text.IO as T
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

data ParseCtx where
    ParseCtx :: forall (sep :: Symbol) (cols :: [(Symbol, Type)]).
             (KnownSymbol sep, ColumnHeaders cols)
             => Path (DSV sep cols)
             -> ParseCtx

instance Eq ParseCtx where
    ParseCtx fp1 == ParseCtx fp2 = untag fp1 == untag fp2

instance Show ParseCtx where
    show (ParseCtx (fp :: Path (DSV sep cols))) =
        let sep      = symbolVal (Proxy @sep)
            colNames = columnHeaders (Proxy @(Record cols))
            colsDesc = intercalate ", " colNames
        in
            "file " ++ show (untag fp) ++ " with columns " ++ colsDesc ++
            " (separated by " ++ show sep ++ ")"


type HasParseCtx = (?parseRowCtx :: ParseCtx)


data RowParseError = RowParseError { parseErrorCtx :: ParseCtx, parseErrorRow :: Text }
  deriving (Eq, Show, Typeable)

instance Exception RowParseError


defRowTokenizer :: CSV.Separator -> RowTokenizer
defRowTokenizer sep =
    CSV.tokenizeRow CSV.defaultParser { CSV.columnSeparator = sep }


defParserOptions :: CSV.Separator -> ParserOptions
defParserOptions sep = ParserOptions
    { hasHeader = True
    , isComment = const False -- T.all isSpace
    , rowTokenizer = defRowTokenizer sep
    }


parseEitherRow :: (HasParseCtx, CSV.ReadRec cols)
               => RowTokenizer
               -> Text
               -> Either RowParseError (Record cols)
parseEitherRow tokenize row =
    case rtraverse getCompose (CSV.readRec (tokenize row)) of
      Left _    -> Left RowParseError { parseErrorCtx = ?parseRowCtx, parseErrorRow = row }
      Right rec -> Right rec


pipeEitherRows :: forall m cols. (HasParseCtx, Monad m, CSV.ReadRec cols)
               => ParserOptions
               -> Pipe Text (Either RowParseError (Record cols)) m ()
pipeEitherRows opts =
    (if hasHeader opts then P.drop 1 else P.cat)
    >-> P.filter (not . isComment opts)
    >-> P.map (parseEitherRow (rowTokenizer opts))


produceEitherRows ::
                  ( MonadSafe m, MonadIO m
                  , KnownSymbol sep, ColumnHeaders cols, CSV.ReadRec cols
                  )
                  => Path (DSV sep cols)
                  -> ParserOptions
                  -> Producer (Either RowParseError (Record cols)) m ()
produceEitherRows fp opts =
    CSV.produceTextLines (untag fp) >-> pipeEitherRows opts
  where
    ?parseRowCtx = ParseCtx fp



throwLeftsM :: (Exception e, MonadThrow m) => Pipe (Either e a) a m ()
throwLeftsM = P.mapM (either throwM return)


inCoreAoSExc ::
             ( RecVec cols
             , Lifted IO r
             , Member (Exc RowParseError) r
             )
             => Producer (Record cols) (SafeT IO) ()
             -> Eff r (FrameRec cols)
inCoreAoSExc =
    lift . try @RowParseError . inCoreAoS >=> liftEither


readFrameExc ::
             ( KnownSymbol sep, ColumnHeaders cols
             , CSV.ReadRec cols, RecVec cols
             , Lifted IO r
             , Member (Exc RowParseError) r
             )
             => Path (DSV sep cols)
             -> ParserOptions
             -> Eff r (FrameRec cols)
readFrameExc fp opts =
    inCoreAoSExc (produceEitherRows fp opts >-> throwLeftsM)



data WriterOptions = WriterOptions
    { writeHeader    :: Bool
    , writeSeparator :: CSV.Separator
    }
  deriving (Eq, Ord, Show)


defWriterOptions :: CSV.Separator -> WriterOptions
defWriterOptions sep = WriterOptions
    { writeHeader    = True
    , writeSeparator = sep
    }


produceFromFrame :: (Foldable f, Monad m)
                 => f (Record cols)
                 -> Producer (Record cols) m ()
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
    let headers = map T.pack (columnHeaders (Proxy @(Record cols)))
        sep     = writeSeparator opts

    when (writeHeader opts) $
      P.yield (T.intercalate sep headers)

    P.map (T.intercalate sep . CSV.showFieldsCSV)


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


stdoutWriter :: MonadIO m => Consumer Text m ()
stdoutWriter = P.mapM_ (liftIO . T.putStrLn)

fileWriter :: (MonadIO m, MonadSafe m) => FilePath -> Consumer Text m ()
fileWriter = CSV.consumeTextLines


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
