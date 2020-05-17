{-# LANGUAGE QuasiQuotes, ExtendedDefaultRules #-}
{-# LANGUAGE RecordWildCards #-}
module Data.DAL.KeyValue.S3
( S3Engine
, S3EngineOpts(..)
, createEngine
-- , closeEngine
, withEngine
, s3EngineOptsDev
) where

import Control.Applicative ((<|>))
import Control.Exception
import Control.Monad
import Control.Monad.IO.Class
import Data.ByteString (ByteString)
import Data.ByteString.Base58
import Data.DAL.KeyValue.HashRef
import Data.Either
import Data.Function
import Data.Functor
import Data.Maybe
import Data.Monoid
import Data.Store
import Data.String (IsString(..))
import Data.String.Conversions
import Data.Text (Text)
import Network.HTTP.Client
import Network.HTTP.Client.TLS
import Network.Minio
import Safe
import System.FilePath
import System.IO (hClose)
import System.IO.Temp

import qualified Control.Monad.Catch as Catch
import qualified Data.ByteArray as BA
import qualified Data.ByteString as BS
import qualified Data.Conduit as Conduit
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Network.Minio as Minio
import qualified UnliftIO -- (bracket, throwIO, try)

import Data.DAL.Types

toHex :: B58 -> Text
toHex = TE.decodeUtf8 . encodeBase58 bitcoinAlphabet . unB58

ensureBucketExists :: Bucket -> Minio ()
ensureBucketExists b = do
    -- Make a bucket; catch bucket already exists exception if thrown.
    bErr <- UnliftIO.try $ makeBucket b Nothing
    -- If the bucket already exists, we would get a specific
    -- `ServiceErr` exception thrown.
    case bErr of
        Left BucketAlreadyOwnedByYou -> pure ()
        Left e                       -> UnliftIO.throwIO e
        Right _                      -> pure ()


newtype S3Key a = S3Key Text
  deriving (Eq, Ord, Show)

unS3Key :: S3Key a -> Text
unS3Key (S3Key k) = k

data S3Engine = S3Engine
                        { conn   :: Minio.MinioConn
                        , bucket :: Text
                        }

data S3EngineOpts = S3EngineOpts
                        { s3Addr         :: Text
                        , s3AccessKey    :: Text
                        , s3SecretKey    :: Text
                        , s3BucketPrefix :: Text
                        }

s3EngineOptsDev :: S3EngineOpts
s3EngineOptsDev = S3EngineOpts
                        { s3Addr         = "http://127.0.0.1:9001"
                        , s3AccessKey    = "s3-access-key"
                        , s3SecretKey    = "s3-secret-key"
                        , s3BucketPrefix = "dev"
                        }

createEngine :: S3EngineOpts -> IO S3Engine
createEngine S3EngineOpts {..} = do
  manager <- newManager tlsManagerSettings
  let minioConnectionInfo = toAddr s3Addr & Minio.setCreds (Minio.Credentials s3AccessKey s3SecretKey)
  minioConn <- Minio.mkMinioConn minioConnectionInfo manager
  pure $ S3Engine minioConn s3BucketPrefix
  where
    toAddr = fromString . T.unpack

withEngine :: S3EngineOpts -> (S3Engine -> IO a) -> IO a
withEngine opts = bracket (createEngine opts) closeEngine
  where
    closeEngine :: S3Engine -> IO ()
    closeEngine _ = pure ()

instance (Store a, HasKey a) => SourceListAll a IO S3Engine where
  listAll :: S3Engine -> IO [a]
  listAll e = do
      keys <- fmap (either throw id) $ runMinioWith (conn e) $ do
          obs <- Conduit.sourceToList $ listObjects (bucket e) (Just (cs $ nsUnpack (ns @a) </> "")) False
          pure $ catMaybes $ obs <&> \case
                  ListItemObject oi -> Just $ oiObject oi
                  _                 -> Nothing
      fmap catMaybes $ mapM (load' e . S3Key) keys

load' :: forall a. (Store a, HasKey a) => S3Engine -> S3Key a -> IO (Maybe a)
load' e s3key = do
    withSystemTempFile "micro-dal-s3" $ \filepath _ -> do
        either handleLeft (const $ (either (const Nothing) Just . decode @a) <$> BS.readFile filepath) =<< do
            runMinioWith (conn e) $ do
                fGetObject (bucket e) (unS3Key s3key) filepath defaultGetObjectOptions
  where
    handleLeft = \case
        MErrService NoSuchKey -> pure Nothing
        e -> throwIO e

mkS3Key :: forall a. (HasKey a, Store (KeyOf a)) => KeyOf a -> S3Key a
mkS3Key k = S3Key $ cs $ nsUnpack (ns @a) </> cs (toHex . B58 . encode $ k)

instance (Store a, Store (KeyOf a), HasKey a) => SourceStore a IO S3Engine where
  load :: S3Engine -> KeyOf a -> IO (Maybe a)
  load e k = load' e $ mkS3Key k

  store :: S3Engine -> a -> IO (KeyOf a)
  store e v = do
      withSystemTempFile "micro-dal-s3" $ \filepath h -> do
          BS.hPut h bval
          hClose h
          fmap (either throw id) $ runMinioWith (conn e) $ do
              UnliftIO.catch
                  (fPutObject (bucket e) bkey filepath defaultPutObjectOptions)
                  $ \ NoSuchBucket -> do
                      ensureBucketExists (bucket e)
                      (fPutObject (bucket e) bkey filepath defaultPutObjectOptions)
              pure keyv
    where
      keyv = key v
      bkey = unS3Key $ mkS3Key @a keyv
      bval = encode v

instance (Store a, Store (KeyOf a), HasKey a) => SourceDeleteByKey a IO S3Engine where
  delete :: S3Engine -> KeyOf a -> IO ()
  delete e k = do
    fmap (either throw id) $
      runMinioWith (conn e) $ do
          removeObject (bucket e) (unS3Key $ mkS3Key @a k)


-- instance SourceTransaction a m S3Engine where
--   withTransaction e = error "Is not available"  -- TODO try runMinioWith (conn e)
