{-# LANGUAGE QuasiQuotes, ExtendedDefaultRules #-}
module Data.DAL.KeyValue.SQLite where

import Database.SQLite.Simple hiding (withTransaction)
import Data.ByteString (ByteString)
import Data.Either
import Data.Store
import qualified Database.SQLite.Simple as SQLite
import Safe
import Text.InterpolatedString.Perl6 (qq,qc)

import Data.DAL.Types

newtype SQLiteEngine = SQLiteEngine { conn :: Connection }

instance (Store a, HasKey a) => SourceListAll a IO SQLiteEngine where
  listAll :: SQLiteEngine -> IO [a]
  listAll e = do
    rows <- query_ (conn e) [qc|select v from {table}|] :: IO [Only ByteString]
    pure $ rights $ fmap (\(Only x) -> decode @a x) rows
    where
      table = nsUnpack (ns @a)

instance (Store a, Store (KeyOf a), HasKey a) => SourceStore a IO SQLiteEngine where
  load :: SQLiteEngine -> KeyOf a -> IO (Maybe a)
  load e k = do
    bs <- query (conn e) [qc|select v from {table} where k = ?|] (Only (encode k)) :: IO [Only ByteString]
    case headMay bs of
      Just (Only v) -> pure $ either (const Nothing) (Just) (decode @a v)
      _             -> pure Nothing
    where
      table = nsUnpack (ns @a)

  store :: SQLiteEngine -> a -> IO (KeyOf a)
  store e v = do
    execute_ (conn e) [qc|create table if not exists {table} (k blob primary key, v blob not null)|]
    execute (conn e)  [qc|insert or replace into {table} (k,v) values(?,?)|] (bkey,bval)
    pure (key v)
    where
      table = nsUnpack (ns @a)
      bkey  = encode (key v)
      bval  = encode v

instance SourceTransaction a IO SQLiteEngine where
  withTransaction e = SQLite.withTransaction (conn e)

