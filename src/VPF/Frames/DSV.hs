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
  , parseEitherRows
  , streamEitherRows
  , throwLeftsM
  , inCoreAoSExc
  , readFrame

  , WriterOptions(..)
  , defWriterOptions
  , headerToDSV
  , rowToDSV
  , streamFromFrame
  , streamDSVLines
  , streamDSVLinesFromFrame
  , writeDSV
  ) where

import GHC.Generics (Generic)
import GHC.TypeLits (KnownSymbol, symbolVal)

import Control.Algebra (Has)
import Control.Category ((>>>))
import Control.Effect.Throw (Throw, throwError)
import Control.Monad ((>=>))
import Control.Exception (try)
import Control.Monad.Catch (Exception, MonadThrow(throwM))
import Control.Monad.IO.Class (MonadIO(liftIO))
import Control.Monad.Trans.Resource (ResourceT, MonadResource, runResourceT)

import Data.List (intercalate)
import Data.Proxy (Proxy(..))
import Data.Store (Store)
import Data.Tagged (untag)
import Data.Text (Text)
import Data.Text qualified as T
import Data.Typeable (Typeable)
import Data.Vector (Vector)

import Data.Vinyl (ElField, RecMapMethod, RecordToList, rtraverse)
import Data.Vinyl.Functor (Compose(..))

import Frames (ColumnHeaders(..), FrameRec, Record)
import Frames.InCore (RecVec)
import qualified Frames.CSV     as CSV
import qualified Frames.ShowCSV as CSV

import Streaming (Stream, Of)
import Streaming.Prelude qualified as S

import VPF.Frames.InCore (fromRowStreamAoS)
import VPF.Formats (Path, DSV)
import VPF.Util.FS (streamTextLines)


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


parseEitherRows ::
    (HasParseCtx, Monad m, CSV.ReadRec cols)
    => ParserOptions
    -> Stream (Of Text) m ()
    -> Stream (Of (Either ParseError (Record cols))) m ()
parseEitherRows opts =
    (if hasHeader opts then S.drop 1 else id)
    >>> S.filter (not . isComment opts)
    >>> S.map (parseEitherRow (rowTokenizer opts))


streamEitherRows :: forall m sep cols.
    ( KnownSymbol sep
    , ColumnHeaders cols
    , CSV.ReadRec cols
    , MonadResource m
    )
    => ParserOptions
    -> Path (DSV sep cols)
    -> Stream (Of (Either ParseError (Record cols))) m ()
streamEitherRows opts fp =
    parseEitherRows opts $ streamTextLines fp
  where
    ?parseRowCtx =
        let sep      = symbolVal (Proxy @sep)
            colNames = columnHeaders (Proxy @(Record cols))
        in  ParseCtx (untag fp) sep colNames


throwLeftsM ::
    (Exception e, MonadThrow m)
    => Stream (Of (Either e a)) m ()
    -> Stream (Of a) m ()
throwLeftsM = S.mapM (either throwM return)


inCoreAoSExc :: forall m cols.
    ( RecVec cols
    , MonadIO m
    , Has (Throw ParseError) m
    )
    => Stream (Of (Record cols)) (ResourceT IO) ()
    -> m (FrameRec cols)
inCoreAoSExc =
    liftIO . try @ParseError . runResourceT . fromRowStreamAoS
        >=> either throwError return


readColumnMap :: forall sep cols m.
    ( KnownSymbol sep
    , MonadIO m
    , Has (Throw ParseError) m
    )
    => ParserOptions
    -> Path (DSV sep cols)
    -> m [(Text, Vector Text)]
readColumnMap opts fp = liftIO $ runResourceT do
    er <- S.next $ streamTextLines fp

    case er of
      Left () -> return []
      Right (headerRow, stream) -> do
          let header = rowTokenizer opts headerRow

          mmvecs <- MVector.replicateM (length header) (MVector.new 0)
          res <- go mmvecs 0 0 stream

          case res of
              Nothing -> _
              Just badRow -> _
  where
    go ::
        MVector.IOVector (MVector.IOVector Text)
        -> Int
        -> Int
        -> Stream (Of Text) IO ()
        -> IO (Maybe Text)
    go !mvecs = loop
      where
        loop :: Int -> Int -> Stream (Of Text) IO () -> IO (Maybe Text)
        loop !capacity !n stream =
            enext <- S.next stream

            case enext of
                Left () ->
                    return Nothing

                Right (row, stream') -> do
                    let tokenized = rowTokenizer opts row

                    let needsGrow = capacity >= n

                        capacity'
                          | needsGrow = capacity*2
                          | otherwise = capacity

                        updatedMVec
                          | needsGrow = \i -> do
                              mvec <- MVector.unsafeRead mvecs i
                              mvec' <- MVector.unsafeGrow mvecs capacity
                              MVector.unsafeWrite mvecs i mvec'
                              return mvec'
                          | otherwise = MVector.unsafeRead mvecs

                    status <- buildZipWith
                        (>>)
                        (\case
                            Nothing -> return Nothing
                            Left  _ -> return (Just row)
                            Right _ -> return (Just row))
                        (\i value ->
                            mvec <- updatedMVec i
                            MVector.unsafeWrite mvec n value)
                        [0 .. MVector.length mmvecs - 1]
                        tokenized

                    case status of
                        Nothing -> loop (capacity*2) (n+1) stream
                        Just _  -> return status


readFrame ::
    ( KnownSymbol sep, ColumnHeaders cols
    , CSV.ReadRec cols, RecVec cols
    , MonadIO m
    , Has (Throw ParseError) m
    )
    => ParserOptions
    -> Path (DSV sep cols)
    -> m (FrameRec cols)
readFrame opts =
    inCoreAoSExc . throwLeftsM . streamEitherRows opts


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


streamFromFrame :: (Foldable f, Monad m) => f (Record cols) -> Stream (Of (Record cols)) m ()
streamFromFrame = S.each


streamDSVLines :: forall cols m.
    ( RecMapMethod CSV.ShowCSV ElField cols
    , RecordToList cols
    , ColumnHeaders cols
    , Monad m
    )
    => WriterOptions
    -> Stream (Of (Record cols)) m ()
    -> Stream (Of Text) m ()
streamDSVLines opts records =
    if writeHeader opts then
        S.cons (headerToDSV @cols Proxy sep) encodedRows
    else
        encodedRows
  where
    encodedRows = S.map (rowToDSV sep) records

    sep = T.singleton (writeSeparator opts)


streamDSVLinesFromFrame ::
    ( RecMapMethod CSV.ShowCSV ElField cols
    , RecordToList cols
    , ColumnHeaders cols
    , Monad m
    , Foldable f
    )
    => WriterOptions
    -> f (Record cols)
    -> Stream (Of Text) m ()
streamDSVLinesFromFrame opts =
    streamDSVLines opts .  streamFromFrame


writeDSV ::
    ( RecMapMethod CSV.ShowCSV ElField cols
    , RecordToList cols
    , ColumnHeaders cols
    , Foldable f
    , Monad m
    )
    => WriterOptions
    -> (Stream (Of Text) m () -> r)
    -> f (Record cols)
    -> r
writeDSV opts writer =
    writer . streamDSVLines opts . streamFromFrame
