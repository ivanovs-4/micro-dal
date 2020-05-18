{-# LANGUAGE QuasiQuotes, ExtendedDefaultRules #-}
{-# LANGUAGE RecordWildCards #-}
module Data.DAL.KeyValue.S3
( S3Engine
, S3EngineOpts(..)
, createEngine
-- , closeEngine
, withEngine
, cleanEngine
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

import qualified Control.Monad.Catch as Catch
import qualified Data.ByteArray as BA
import qualified Data.ByteString as BS
import qualified Data.Conduit as Conduit
import qualified Conduit as Conduit
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Network.Minio as Minio
import qualified UnliftIO -- (bracket, throwIO, try)

import Data.DAL.Types


newtype S3Key a = S3Key Text
  deriving (Eq, Ord, Show)

unS3Key :: S3Key a -> Text
unS3Key (S3Key k) = k

data S3Engine = S3Engine
                        { conn   :: Minio.MinioConn
                        , bucket :: Text
                        }

data S3EngineOpts = S3EngineOpts
                        { s3Addr      :: Text
                        , s3AccessKey :: Text
                        , s3SecretKey :: Text
                        , s3Bucket    :: Text
                        , s3Region    :: Text
                        }
    deriving (Eq, Ord, Show)

s3EngineOptsDev :: S3EngineOpts
s3EngineOptsDev = S3EngineOpts
                        { s3Addr      = "http://127.0.0.1:9001"
                        , s3AccessKey = "s3-access-key"
                        , s3SecretKey = "s3-secret-key"
                        , s3Bucket    = "dev"
                        , s3Region    = "ru-central1"
                        }

createEngine :: S3EngineOpts -> IO S3Engine
createEngine S3EngineOpts {..} = do
  manager <- newManager tlsManagerSettings
  let minioConnectionInfo =
          toAddr s3Addr
        & Minio.setCreds (Minio.Credentials s3AccessKey s3SecretKey)
        & Minio.setRegion s3Region
  minioConn <- Minio.mkMinioConn minioConnectionInfo manager
  pure $ S3Engine minioConn s3Bucket
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
        keys <- either throwIO pure =<< do
            runMinioWith (conn e) $ do
              let prefix = cs $ nsUnpack (ns @a) </> ""
              obs <- Conduit.sourceToList $ listObjects (bucket e) (Just prefix) True
              pure $ catMaybes $ obs <&> \case
                      ListItemObject oi -> Just $ S3Key (oiObject oi)
                      _                 -> Nothing
        catMaybes <$> mapM (load' e) keys

load' :: forall a. (Store a, HasKey a) => S3Engine -> S3Key a -> IO (Maybe a)
load' e s3key = do
    either handleLeft (pure . (decodeMay <=< headMay)) =<< do
        runMinioWith (conn e) $ do
            resp <- getObject (bucket e) (unS3Key s3key) defaultGetObjectOptions
            Conduit.sourceToList (gorObjectStream resp)
    where
      handleLeft = \case
          MErrService NoSuchKey -> pure Nothing
          e -> throwIO e
      decodeMay :: ByteString -> Maybe a
      decodeMay = either (const Nothing) Just . decode @a

mkS3Key :: forall a. (HasKey a, Store (KeyOf a)) => KeyOf a -> S3Key a
mkS3Key k = S3Key $ cs $ nsUnpack (ns @a) </> (cs . encodeBase58 bitcoinAlphabet . encode $ k)

instance (Store a, Store (KeyOf a), HasKey a) => SourceStore a IO S3Engine where
  load :: S3Engine -> KeyOf a -> IO (Maybe a)
  load e k = load' e $ mkS3Key k

  store :: S3Engine -> a -> IO (KeyOf a)
  store e v = do
      either throwIO pure =<< do
        runMinioWith (conn e) $ do
          UnliftIO.catch
              (putObject (bucket e) bkey (Conduit.yield bval) Nothing defaultPutObjectOptions)
              $ \ NoSuchBucket -> do
                  ensureBucketExists (bucket e)
                  (putObject (bucket e) bkey (Conduit.yield bval) Nothing defaultPutObjectOptions)
          pure keyv
    where
      keyv = key v
      bkey = unS3Key $ mkS3Key @a keyv
      bval = encode v

instance (Store a, Store (KeyOf a), HasKey a) => SourceDeleteByKey a IO S3Engine where
  delete :: S3Engine -> KeyOf a -> IO ()
  delete e k = do
    either throwIO pure =<< do
      runMinioWith (conn e) $ do
          removeObject (bucket e) (unS3Key $ mkS3Key @a k)

cleanEngine :: S3Engine -> IO ()
cleanEngine e = either throwIO pure =<< do
  runMinioWith (conn e) $ do
    UnliftIO.catch
        (removeBucket (bucket e))
      $ \case
          (ServiceErr "BucketNotEmpty" _) -> do
              obs <- Conduit.sourceToList $ listObjects (bucket e) Nothing True
              mapM_ (removeObject (bucket e))
                  $ catMaybes $ obs <&> \case
                      ListItemObject oi -> Just $ oiObject oi
                      _                 -> Nothing
              removeBucket (bucket e)
          NoSuchBucket -> pure ()
          e -> UnliftIO.throwIO e

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
