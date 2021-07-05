{-# language AllowAmbiguousTypes #-}
{-# language DeriveFunctor #-}
{-# language DeriveGeneric #-}
{-# language OverloadedStrings #-}
{-# language Strict #-}
module VPF.Util.Fasta
  ( FastaEntry
  , ParseError(..)
  , entryName
  , removeNameComments
  , entrySeqNumBases
  , readFastaFile
  , writeFastaFile
  ) where

import GHC.Generics (Generic)
import GHC.Stack (HasCallStack)

import Control.Category ((>>>))
import Control.Monad.Trans.Resource (MonadResource)

import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as C8

import Data.Char (isAlpha)
import Data.Function ((&))
import Data.Store (Store)

import Streaming
import Streaming.Internal qualified as S
import Streaming.Prelude qualified as S
import Streaming.ByteString qualified as BSS
import Streaming.ByteString.Char8 qualified as BSS8


import VPF.Formats


data FastaEntry acid = FastaEntry ByteString [ByteString]
  deriving (Eq, Ord, Show, Generic)

instance Store (FastaEntry acid)

data ParseError
    = ExpectedNameLine     FilePath Int {- what was found instead -}  ByteString
    | MissingSequenceLines FilePath Int {- corresponding name line -} ByteString
    deriving (Show, Generic)

instance Store ParseError


entryName :: HasCallStack => FastaEntry acid -> ByteString
entryName (FastaEntry n _)
  | C8.isPrefixOf (C8.pack ">") n = C8.tail n
  | otherwise                     = error "entryName: badly constructed FastaEntry"


breakNameAtComment :: ByteString -> (ByteString, ByteString)
breakNameAtComment = C8.breakSubstring (C8.pack " #")


removeNameComments :: HasCallStack => ByteString -> ByteString
removeNameComments = C8.strip . fst . breakNameAtComment


class AcidBaseLength acid where
    seqNumBases :: ByteString -> Int

instance AcidBaseLength Aminoacid where
    seqNumBases =
        C8.foldl' (\acc c -> if knownAcid c then acc+1 else acc) 0
      where
        knownAcid 'X' = False
        knownAcid c
          | not (isAlpha c) = False
          | otherwise       = True


entrySeqNumBases :: forall acid. AcidBaseLength acid => FastaEntry acid -> Int
entrySeqNumBases (FastaEntry _ seq) = sum (map (seqNumBases @acid) seq)


readFastaFile :: forall acid m.
    MonadResource m
    => Path (FASTA acid)
    -> Stream  (Of (FastaEntry acid)) m (Either ParseError ())
readFastaFile p =
    BSS.readFile (untag p)
        & fastaEntries p


writeFastaFile :: MonadResource m => Path (FASTA acid) -> Stream (Of (FastaEntry acid)) m r -> m r
writeFastaFile p = BSS.writeFile (untag p) . fastaBytes


fastaBytes :: Monad m => Stream (Of (FastaEntry acid)) m r -> BSS.ByteStream m r
fastaBytes entries =
    S.for entries (\(FastaEntry name seq) -> S.yield name >> S.each seq) -- Stream (Of ByteString)
        & S.maps (\(a :> x) -> x <$ BSS.fromStrict a)
        & BSS8.unlines                                                   -- ByteStream m r


fastaEntries :: forall m acid r.
    Monad m
    => Path (FASTA acid)
    -> BSS.ByteStream m r
    -> Stream (Of (FastaEntry acid)) m (Either ParseError r)
fastaEntries p =
    BSS8.lines
    >>> S.mapped BSS.toStrict
    >>> S.map C8.strip
    >>> S.filter (not . C8.null)
    >>> S.zip (S.enumFrom 1)
    >>> S.unfoldr peekEntry
  where
    peekEntry ::
        Stream (Of (Int, ByteString)) m r
        -> m (Either
                (Either ParseError r)
                (FastaEntry acid, Stream (Of (Int, ByteString)) m r))
    peekEntry stream = do
        e <- S.next stream

        case e of
            Left r -> return (Left (Right r))
            Right ((linenum, nameLine), rest)
              | not (isNameLine nameLine) ->
                  return $ Left (Left (ExpectedNameLine (untag p) linenum nameLine))
              | otherwise -> do
                  seqLines :> rest' <- S.toList $ S.map snd $ S.span (isSequenceLine . snd) rest

                  case seqLines of
                      [] -> return $ Left (Left (MissingSequenceLines (untag p) linenum nameLine))
                      _  -> return $ Right (FastaEntry nameLine seqLines, rest')


    isNameLine :: ByteString -> Bool
    isNameLine = C8.isPrefixOf (C8.singleton '>')

    isSequenceLine :: ByteString -> Bool
    isSequenceLine = not . isNameLine

