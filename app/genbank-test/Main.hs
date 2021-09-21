import Control.Applicative (liftA2)
import Control.Category ((>>>))
import Control.Concurrent.Async.Lifted qualified as Async
import Control.Foldl qualified as Fold
import Control.Monad.Trans.Resource (runResourceT)

import Data.Functor.Identity (Identity(..))
import Data.HashSet (HashSet)
import Streaming (Stream, Of(..))
import Streaming.ByteString qualified as BSS
import Streaming.Prelude qualified as S

import VPF.DataSource.NCBI.GenBank qualified as GenBank
import VPF.DataSource.NCBI.RefSeq qualified as RefSeq
import VPF.Formats
import VPF.Util.GBFF qualified as GBFF


collectAccessions :: forall m.
    Monad m
    => Stream (Of [GBFF.GenBankField]) m (Either (GBFF.ParseError m ()) ())
    -> m (Either (GBFF.ParseError Identity ()) (HashSet GBFF.Accession))
collectAccessions =
    S.concat
    >>> S.map GBFF.copyGenBankField
    >>> S.mapMaybe \case
            GBFF.AccessionField accs -> Just (head accs)
            _                        -> Nothing
    >>> Fold.purely S.fold Fold.hashSet
    >>> (>>= wrapError)
  where
    wrapError ::
        Of (HashSet GBFF.Accession) (Either (GBFF.ParseError m ()) ())
        -> m (Either (GBFF.ParseError Identity ()) (HashSet GBFF.Accession))
    wrapError (accs :> e) =
        case e of
            Left err -> do
                leftoversPeek <- BSS.toStrict_ (BSS.take 1000 (GBFF.parseLeftovers err))
                return $ Left err { GBFF.parseLeftovers = BSS.fromStrict leftoversPeek }
            Right () -> return (Right accs)


main :: IO ()
main = do
    (gbRes, rsRes) <-
        Async.concurrently
            (runResourceT $
                collectAccessions $
                    GenBank.loadGenBankGbWith
                        GenBank.genBankViralConfig
                        (Tagged "./downloads/genbank")
                        GBFF.parseGenBankStream)
            (runResourceT $
                collectAccessions $
                    RefSeq.loadRefSeqGbWith
                        RefSeq.refSeqViralConfig
                        (Tagged "./downloads/refseq")
                        GBFF.parseGenBankStream)

    case liftA2 (,) gbRes rsRes of
        Left e   -> do
            print e
            putStrLn $ "Leftovers (peek): " ++ show (runIdentity (BSS.toStrict_ (GBFF.parseLeftovers e)))
        Right (gbAccessions :: t, refseqAccessions :: t) -> do
            putStrLn $ "Found " ++ show (length gbAccessions) ++ " accessions in GenBank"
            putStrLn $ "Found " ++ show (length refseqAccessions) ++ " accessions in RefSeq"
