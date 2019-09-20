module VPF.Util.Hash
  ( hashFile
  , hashFileSHA512t_256
  , digestToHex
  ) where

import Control.Monad (when)
import Data.Function (fix)
import qualified Data.ByteString as BS
import Data.ByteArray.Encoding as Enc

import Crypto.Hash    as Crypto
import Crypto.Hash.IO as Crypto

import System.IO


hashFile :: forall a. Crypto.HashAlgorithm a => FilePath -> IO (Crypto.Digest a)
hashFile fp = do
    ctx <- Crypto.hashMutableInit

    withBinaryFile fp ReadMode $ \h -> fix $ \loop -> do
        chunk <- BS.hGetSome h (1024*1024)

        when (not (BS.null chunk)) $ do
            Crypto.hashMutableUpdate ctx chunk
            loop

    Crypto.hashMutableFinalize ctx


hashFileSHA512t_256 :: FilePath -> IO (Crypto.Digest Crypto.SHA512t_256)
hashFileSHA512t_256 = hashFile


digestToHex :: Crypto.Digest a -> BS.ByteString
digestToHex = Enc.convertToBase Enc.Base16
