{-# language OverloadedStrings #-}
module VPF.Util.Hash
  ( hashFile
  , hashFileSHA512t_256
  , hashFileMD5
  , hashFileCRC32
  , digestToHex
  , Checksum(..)
  , checksumToFold
  , checksumToFoldM
  ) where

import Control.Foldl qualified as L
import Control.Monad (when)

import Crypto.Hash    as Crypto
import Crypto.Hash.IO as Crypto


import Data.Aeson qualified as Aeson
import Data.Aeson.Types qualified as Aeson
import Data.ByteArray.Encoding as Enc
import Data.ByteString (ByteString)
import Data.ByteString qualified as BS
import Data.ByteString.Char8 qualified as BS8
import Data.ByteString.Lazy qualified as LBS
import Data.Digest.CRC32 qualified as CRC32
import Data.Function (fix)
import Data.Text (Text)
import Data.Word (Word32)

import System.IO
import Data.Functor (($>))


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


hashFileMD5 :: FilePath -> IO (Crypto.Digest Crypto.MD5)
hashFileMD5 = hashFile


hashFileCRC32 :: FilePath -> IO Word32
hashFileCRC32 = fmap CRC32.crc32 . LBS.readFile


digestToHex :: Crypto.Digest a -> BS.ByteString
digestToHex = Enc.convertToBase Enc.Base16



data Checksum where
    ChecksumHashAlgorithm ::
        (Crypto.HashAlgorithm a, ImplementedParser a)
        => !a
        -> !ByteString
        -> Checksum

    ChecksumCRC32 :: !ByteString -> Checksum
    NoChecksum :: Checksum


class Show a => ImplementedParser a
instance ImplementedParser Crypto.MD5
instance ImplementedParser Crypto.SHA1
instance ImplementedParser Crypto.SHA224
instance ImplementedParser Crypto.SHA256
instance ImplementedParser Crypto.SHA3_224
instance ImplementedParser Crypto.SHA3_256
instance ImplementedParser Crypto.SHA3_384
instance ImplementedParser Crypto.SHA3_512
instance ImplementedParser Crypto.SHA384
instance ImplementedParser Crypto.SHA512
instance ImplementedParser Crypto.SHA512t_224
instance ImplementedParser Crypto.SHA512t_256


instance Aeson.FromJSON Checksum where
    parseJSON Aeson.Null = pure NoChecksum

    parseJSON (Aeson.Object o) = do
        algo <- o Aeson..: "algorithm"

        con <- case algo :: Text of
            "CRC32"       -> pure ChecksumCRC32
            "MD5"         -> pure (ChecksumHashAlgorithm Crypto.MD5)
            "SHA1"        -> pure (ChecksumHashAlgorithm Crypto.SHA1)
            "SHA224"      -> pure (ChecksumHashAlgorithm Crypto.SHA224)
            "SHA256"      -> pure (ChecksumHashAlgorithm Crypto.SHA256)
            "SHA3_224"    -> pure (ChecksumHashAlgorithm Crypto.SHA3_224)
            "SHA3_256"    -> pure (ChecksumHashAlgorithm Crypto.SHA3_256)
            "SHA3_384"    -> pure (ChecksumHashAlgorithm Crypto.SHA3_384)
            "SHA3_512"    -> pure (ChecksumHashAlgorithm Crypto.SHA3_512)
            "SHA384"      -> pure (ChecksumHashAlgorithm Crypto.SHA384)
            "SHA512"      -> pure (ChecksumHashAlgorithm Crypto.SHA512)
            "SHA512t_224" -> pure (ChecksumHashAlgorithm Crypto.SHA512t_224)
            "SHA512t_256" -> pure (ChecksumHashAlgorithm Crypto.SHA512t_256)
            _             -> fail "invalid checksum algorithm, expected CRC32, ..."

        digest <- o Aeson..: "digest"

        return (con (BS8.pack digest))

    parseJSON v =
        Aeson.prependFailure "parsing Checksum failed, " (Aeson.typeMismatch "Object or null" v)


checksumToFold :: Checksum -> L.Fold ByteString Bool
checksumToFold (ChecksumHashAlgorithm (_ :: algo) hex) =
    L.Fold Crypto.hashUpdate (Crypto.hashInit @algo) \cxt ->
      digestToHex (Crypto.hashFinalize cxt) == hex
checksumToFold (ChecksumCRC32 crc32)            = L.Fold CRC32.crc32Update 0 (\w32 -> BS8.pack (show w32) == crc32)
checksumToFold NoChecksum                       = pure True


checksumToFoldM :: Checksum -> L.FoldM IO ByteString Bool
checksumToFoldM (ChecksumHashAlgorithm (_ :: algo) hex) =
    L.FoldM (\ctx chunk -> Crypto.hashMutableUpdate ctx chunk $> ctx) (Crypto.hashMutableInit @algo) \cxt ->
        (== hex) . digestToHex <$> Crypto.hashMutableFinalize cxt
checksumToFoldM cksum = L.generalize (checksumToFold cksum)
