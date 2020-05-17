{-# LANGUAGE QuasiQuotes, ExtendedDefaultRules #-}
{-# LANGUAGE RecordWildCards #-}
module Data.DAL.KeyValue.S3
( S3Engine
, S3EngineOpts
, createEngine
-- , closeEngine
, withEngine
, s3EngineOptsDev
) where

-- import System.FilePath
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
ensureBucketExists bucket = do
    -- Make a bucket; catch bucket already exists exception if thrown.
    bErr <- UnliftIO.try $ makeBucket bucket Nothing
    -- If the bucket already exists, we would get a specific
    -- `ServiceErr` exception thrown.
    case bErr of
        Left BucketAlreadyOwnedByYou -> pure ()
        Left e                       -> UnliftIO.throwIO e
        Right _                      -> pure ()


newtype S3Key = S3Key Text
  deriving (Eq, Ord, Show)

unS3Key :: S3Key -> Text
unS3Key (S3Key a) = a

mkS3Key :: ByteString -> S3Key
mkS3Key = S3Key . toHex . B58 -- . hash




data S3Engine = S3Engine
                        { conn :: Minio.MinioConn
                        , pref :: String
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
  pure $ S3Engine minioConn (T.unpack s3BucketPrefix)
  where
    toAddr = fromString . T.unpack

(</>) :: (IsString c, Semigroup c, ConvertibleStrings a c, ConvertibleStrings b c) => a -> b -> c
(</>) a b = cs a <> "" <> cs b

withEngine :: S3EngineOpts -> (S3Engine -> IO a) -> IO a
withEngine opts = bracket (createEngine opts) closeEngine
  where
    closeEngine :: S3Engine -> IO ()
    closeEngine _ = pure ()

instance (Store a, HasKey a) => SourceListAll a IO S3Engine where
  listAll :: S3Engine -> IO [a]
  listAll e = do
      keys <- fmap (either throw id) $ runMinioWith (conn e) $ do
          obs <- Conduit.sourceToList $ listObjects bucket Nothing False
          pure $ catMaybes $ obs <&> \case
                  ListItemObject oi -> Just $ oiObject oi
                  _                 -> Nothing
      fmap catMaybes $ mapM (load' e . S3Key) keys
    where
      bucket = T.pack $ (pref e) </> nsUnpack (ns @a)

load' :: forall a. (Store a, HasKey a) => S3Engine -> S3Key -> IO (Maybe a)
load' e bkey = do
    withSystemTempFile "micro-dal-s3" $ \filepath _ -> do
        either handleLeft (const $ (either (const Nothing) Just . decode @a) <$> BS.readFile filepath) =<< do
            runMinioWith (conn e) $ do
                fGetObject bucket (unS3Key bkey) filepath defaultGetObjectOptions
  where
    bucket = T.pack $ (pref e) </> nsUnpack (ns @a)
    handleLeft = \case
        MErrService NoSuchKey -> pure Nothing
        e -> throwIO e

instance (Store a, Store (KeyOf a), HasKey a) => SourceStore a IO S3Engine where
  load :: S3Engine -> KeyOf a -> IO (Maybe a)
  load e k = load' e bkey
    where
      bkey  = mkS3Key $ encode k

  store :: S3Engine -> a -> IO (KeyOf a)
  store e v = do
      withSystemTempFile "micro-dal-s3" $ \filepath h -> do
          BS.hPut h bval
          hClose h
          fmap (either throw id) $ runMinioWith (conn e) $ do
              UnliftIO.catch
                  (fPutObject bucket (unS3Key bkey) filepath defaultPutObjectOptions)
                  $ \ NoSuchBucket -> do
                      ensureBucketExists bucket
                      (fPutObject bucket (unS3Key bkey) filepath defaultPutObjectOptions)
              pure keyv
    where
      bucket = T.pack $ (pref e) </> nsUnpack (ns @a)
      keyv = key v
      bkey  = mkS3Key $ encode keyv
      bval  = encode v

instance (Store a, Store (KeyOf a), HasKey a) => SourceDeleteByKey a IO S3Engine where
  delete :: S3Engine -> KeyOf a -> IO ()
  delete e k = do
    fmap (either throw id) $
      runMinioWith (conn e) $ do
          -- ensureBucketExists bucket
          removeObject bucket (unS3Key bkey </> "as")
    where
      bucket = T.pack $ (pref e) </> nsUnpack (ns @a)
      bkey  = mkS3Key $ encode k


-- mkBucket :: forall a. HasKey a => Proxy a -> S3Engine -> Text
-- mkBucket _ e = T.pack $ (pref e) </> nsUnpack (ns @a)


-- instance SourceTransaction a m S3Engine where
--   withTransaction e = error "Is not available"  -- TODO try runMinioWith (conn e)