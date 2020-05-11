{-# LANGUAGE QuasiQuotes, ExtendedDefaultRules #-}
module Data.DAL.KeyValue.S3
( S3Engine
, S3EngineOpts
, createEngine
, closeEngine
, withEngine
, optInMemory
) where

import Control.Applicative ((<|>))
import Control.Exception
import Database.S3.Simple hiding (withTransaction)
import Data.ByteString (ByteString)
import Data.Either
import Data.Maybe
import Data.Monoid
import Data.Store
import Data.String (IsString(..))
import qualified Database.S3.Simple as S3
import Safe
import Text.InterpolatedString.Perl6 (qq,qc)

import Data.DAL.Types

newtype S3Engine = S3Engine { conn :: Connection }

data S3EngineOpts = S3EngineOpts
                        { dbName :: Maybe FilePath
                        }

instance Monoid S3EngineOpts where
  mempty = S3EngineOpts { dbName = Nothing
                            }

instance Semigroup S3EngineOpts where
  (<>) a b = S3EngineOpts { dbName = dbName b <|> dbName a }

instance IsString S3EngineOpts where
  fromString s = S3EngineOpts { dbName = pure s }

optInMemory :: S3EngineOpts
optInMemory = mempty

createEngine :: S3EngineOpts -> IO S3Engine
createEngine opts = do
  conn <- open (fromMaybe ":memory:" $ dbName opts)
  -- FIXME: move this stuff to options
  execute_ conn "PRAGMA journal_mode = WAL"
  execute_ conn "PRAGMA synchronous = NORMAL"
  pure $ S3Engine conn

closeEngine :: S3Engine -> IO ()
closeEngine e = close (conn e)

withEngine :: S3EngineOpts -> (S3Engine -> IO a) -> IO a
withEngine opts = bracket (createEngine opts) closeEngine

instance (Store a, HasKey a) => SourceListAll a IO S3Engine where
  listAll :: S3Engine -> IO [a]
  listAll e = do
    execute_ (conn e) [qc|create table if not exists {table} (k blob primary key, v blob)|]
    rows <- query_ (conn e) [qc|select v from {table}|] :: IO [Only ByteString]
    pure $ rights $ fmap (\(Only x) -> decode @a x) rows
    where
      table = nsUnpack (ns @a)

instance (Store a, Store (KeyOf a), HasKey a) => SourceStore a IO S3Engine where
  load :: S3Engine -> KeyOf a -> IO (Maybe a)
  load e k = do
    execute_ (conn e) [qc|create table if not exists {table} (k blob primary key, v blob)|]
    bs <- query (conn e) [qc|select v from {table} where k = ?|] (Only (encode k)) :: IO [Only ByteString]
    case headMay bs of
      Just (Only v) -> pure $ either (const Nothing) (Just) (decode @a v)
      _             -> pure Nothing
    where
      table = nsUnpack (ns @a)

  store :: S3Engine -> a -> IO (KeyOf a)
  store e v = do
    execute_ (conn e) [qc|create table if not exists {table} (k blob primary key, v blob)|]
    execute (conn e)  [qc|insert or replace into {table} (k,v) values(?,?)|] (bkey,bval)
    pure (key v)
    where
      table = nsUnpack (ns @a)
      bkey  = encode (key v)
      bval  = encode v

instance (Store a, Store (KeyOf a), HasKey a) => SourceDeleteByKey a IO S3Engine where
  delete :: S3Engine -> KeyOf a -> IO ()
  delete e k = do
    execute (conn e) [qc|delete from {table} where k  = ?|] (Only (encode k))
    where
      table = nsUnpack (ns @a)

instance SourceTransaction a IO S3Engine where
  withTransaction e = S3.withTransaction (conn e)

