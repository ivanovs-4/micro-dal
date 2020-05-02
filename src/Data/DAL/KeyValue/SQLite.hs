{-# LANGUAGE QuasiQuotes, ExtendedDefaultRules #-}
module Data.DAL.KeyValue.SQLite
( SQLiteEngine
, SQLiteEngineOpts
, createEngine
, closeEngine
, withEngine
, optInMemory
) where

import Control.Applicative ((<|>))
import Control.Exception
import Database.SQLite.Simple hiding (withTransaction)
import Data.ByteString (ByteString)
import Data.Either
import Data.Maybe
import Data.Monoid
import Data.Store
import Data.String (IsString(..))
import qualified Database.SQLite.Simple as SQLite
import Safe
import Text.InterpolatedString.Perl6 (qq,qc)

import Data.DAL.Types

newtype SQLiteEngine = SQLiteEngine { conn :: Connection }

data SQLiteEngineOpts = SQLiteEngineOpts
                        { dbName :: Maybe FilePath
                        }

instance Monoid SQLiteEngineOpts where
  mempty = SQLiteEngineOpts { dbName = Nothing
                            }

instance Semigroup SQLiteEngineOpts where
  (<>) a b = SQLiteEngineOpts { dbName = dbName b <|> dbName a }

instance IsString SQLiteEngineOpts where
  fromString s = SQLiteEngineOpts { dbName = pure s }

optInMemory :: SQLiteEngineOpts
optInMemory = mempty

createEngine :: SQLiteEngineOpts -> IO SQLiteEngine
createEngine opts = do
  conn <- open (fromMaybe ":memory:" $ dbName opts)
  -- FIXME: move this stuff to options
  execute_ conn "PRAGMA journal_mode = WAL"
  execute_ conn "PRAGMA synchronous = NORMAL"
  pure $ SQLiteEngine conn

closeEngine :: SQLiteEngine -> IO ()
closeEngine e = close (conn e)

withEngine :: SQLiteEngineOpts -> (SQLiteEngine -> IO a) -> IO a
withEngine opts = bracket (createEngine opts) closeEngine

instance (Store a, HasKey a) => SourceListAll a IO SQLiteEngine where
  listAll :: SQLiteEngine -> IO [a]
  listAll e = do
    execute_ (conn e) [qc|create table if not exists {table} (k blob primary key, v blob)|]
    rows <- query_ (conn e) [qc|select v from {table}|] :: IO [Only ByteString]
    pure $ rights $ fmap (\(Only x) -> decode @a x) rows
    where
      table = nsUnpack (ns @a)

instance (Store a, Store (KeyOf a), HasKey a) => SourceStore a IO SQLiteEngine where
  load :: SQLiteEngine -> KeyOf a -> IO (Maybe a)
  load e k = do
    execute_ (conn e) [qc|create table if not exists {table} (k blob primary key, v blob)|]
    bs <- query (conn e) [qc|select v from {table} where k = ?|] (Only (encode k)) :: IO [Only ByteString]
    case headMay bs of
      Just (Only v) -> pure $ either (const Nothing) (Just) (decode @a v)
      _             -> pure Nothing
    where
      table = nsUnpack (ns @a)

  store :: SQLiteEngine -> a -> IO (KeyOf a)
  store e v = do
    execute_ (conn e) [qc|create table if not exists {table} (k blob primary key, v blob)|]
    execute (conn e)  [qc|insert or replace into {table} (k,v) values(?,?)|] (bkey,bval)
    pure (key v)
    where
      table = nsUnpack (ns @a)
      bkey  = encode (key v)
      bval  = encode v

instance (Store a, Store (KeyOf a), HasKey a) => SourceDeleteByKey a IO SQLiteEngine where
  delete :: SQLiteEngine -> KeyOf a -> IO ()
  delete e k = do
    execute (conn e) [qc|delete from {table} where k  = ?|] (Only (encode k))
    where
      table = nsUnpack (ns @a)

instance SourceTransaction a IO SQLiteEngine where
  withTransaction e = SQLite.withTransaction (conn e)

