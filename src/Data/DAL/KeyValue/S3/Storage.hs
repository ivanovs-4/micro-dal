module Data.DAL.KeyValue.S3.Storage where

-- import Crypto.Hash (hashWith, Blake2b_256(..))
import Data.ByteString (ByteString)
import Data.ByteString.Base58
import Data.DAL.KeyValue.HashRef
import Data.Text (Text)
import Network.Minio
import System.IO (hClose)
import System.IO.Temp
import UnliftIO (throwIO, try)

import qualified Data.ByteArray as BA
import qualified Data.ByteString as BS
import qualified Data.Text.Encoding as TE

toHex :: B58 -> Text
toHex = TE.decodeUtf8 . encodeBase58 bitcoinAlphabet . unB58

ensureBucketExists :: Bucket -> Minio ()
ensureBucketExists bucket = do
    -- Make a bucket; catch bucket already exists exception if thrown.
    bErr <- try $ makeBucket bucket Nothing
    -- If the bucket already exists, we would get a specific
    -- `ServiceErr` exception thrown.
    case bErr of
        Left BucketAlreadyOwnedByYou -> pure ()
        Left e                       -> throwIO e
        Right _                      -> pure ()


newtype S3Key = S3Key Text
  deriving (Eq, Ord, Show)

unS3Key :: S3Key -> Text
unS3Key (S3Key a) = a

mkS3Key :: ByteString -> S3Key
mkS3Key = S3Key . toHex . B58 -- . hash
  -- where
  --   hash :: ByteString -> ByteString
  --   hash = BA.convert . hashWith Blake2b_256

-- data FileObj = FileObj
--   { objFilePath :: FilePath
--   , objFileHash :: S3Key
--   } deriving (Show)

-- withObjFromBS :: ByteString -> (FileObj -> IO a) -> IO a
-- withObjFromBS bs fioa = do
--     withSystemTempFile "micro-dal-s3" $ \objFilePath h -> do
--         BS.hPut h bs
--         hClose h
--         fioa FileObj{..}
--     where
--       objFileHash = mkS3Key bs

-- prepareObj :: FilePath -> IO FileObj
-- prepareObj objFilePath = do
--     objFileHash <- mkS3Key <$> BS.readFile objFilePath
--     pure FileObj{..}

-- uploadFileObj :: MinioConn -> Bucket -> FileObj -> IO ()
-- uploadFileObj conn bucket FileObj {..} = either throwIO pure =<< do
--     runMinioWith conn $ do
--         ensureBucketExists bucket
--         fPutObject bucket (unS3Key objFileHash) objFilePath defaultPutObjectOptions

-- removeKey :: MinioConn -> Bucket -> S3Key -> IO ()
-- removeKey conn bucket S3Key{..} = either throwIO pure =<< do
--     runMinioWith conn $ do
--         ensureBucketExists bucket
--         removeObject bucket unS3Key

-- uploadFile :: MinioConn -> Bucket -> FilePath -> IO S3Key
-- uploadFile conn bucket filepath = do
--     fileObj@FileObj{..} <- prepareObj filepath
--     uploadFileObj conn bucket fileObj
--     pure objFileHash

-- uploadBS :: MinioConn -> Bucket -> ByteString -> IO S3Key
-- uploadBS conn bucket bs = do
--     withObjFromBS bs $ \ fobj@FileObj{..} -> do
--         uploadFileObj conn bucket fobj
--         pure objFileHash

-- downloadBS :: MinioConn -> Bucket -> S3Key -> IO (Maybe ByteString)
-- downloadBS conn bucket S3Key {..} =
--     withSystemTempFile "micro-dal-s3" $ \filepath _ -> do
--         either (fmap (const Nothing) . handleLeft) (const $ Just <$> BS.readFile filepath) =<< do
--             runMinioWith conn $ do
--                 fGetObject bucket unS3Key filepath defaultGetObjectOptions
--     where
--       handleLeft = \case
--           MErrService NoSuchKey -> pure ()
--           e -> throwIO e
