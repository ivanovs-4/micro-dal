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

import Control.Applicative ((<|>))
import Control.Exception
import Data.ByteString (ByteString)
import Data.Either
import Data.Function
import Data.Maybe
import Data.Monoid
import Data.Store
import Data.String (IsString(..))
import Data.Text (Text)
import Network.HTTP.Client
import Network.HTTP.Client.TLS
import Safe
import Text.InterpolatedString.Perl6 (qq,qc)

import qualified Network.Minio as Minio
import qualified Data.Text as T

import Data.DAL.Types

data S3Engine = S3Engine
                        { conn :: Minio.MinioConn
                        , pref :: Text
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
                        , s3BucketPrefix = "dev/"
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
    undefined
    -- execute_ (conn e) [qc|create table if not exists {table} (k blob primary key, v blob)|]
    -- rows <- query_ (conn e) [qc|select v from {table}|] :: IO [Only ByteString]
    -- pure $ rights $ fmap (\(Only x) -> decode @a x) rows
    -- where
    --   table = nsUnpack (ns @a)

instance (Store a, Store (KeyOf a), HasKey a) => SourceStore a IO S3Engine where
  load :: S3Engine -> KeyOf a -> IO (Maybe a)
  load e k = do
    undefined
    -- execute_ (conn e) [qc|create table if not exists {table} (k blob primary key, v blob)|]
    -- bs <- query (conn e) [qc|select v from {table} where k = ?|] (Only (encode k)) :: IO [Only ByteString]
    -- case headMay bs of
    --   Just (Only v) -> pure $ either (const Nothing) (Just) (decode @a v)
    --   _             -> pure Nothing
    -- where
    --   table = nsUnpack (ns @a)

  store :: S3Engine -> a -> IO (KeyOf a)
  store e v = do
    undefined
    -- execute_ (conn e) [qc|create table if not exists {table} (k blob primary key, v blob)|]
    -- execute (conn e)  [qc|insert or replace into {table} (k,v) values(?,?)|] (bkey,bval)
    -- pure (key v)
    -- where
    --   table = nsUnpack (ns @a)
    --   bkey  = encode (key v)
    --   bval  = encode v

instance (Store a, Store (KeyOf a), HasKey a) => SourceDeleteByKey a IO S3Engine where
  delete :: S3Engine -> KeyOf a -> IO ()
  delete e k = do
    undefined
    -- execute (conn e) [qc|delete from {table} where k  = ?|] (Only (encode k))
    -- where
    --   table = nsUnpack (ns @a)

instance SourceTransaction a IO S3Engine where
  withTransaction e = error "Is ot available"  -- FIXME implement as void or smth
