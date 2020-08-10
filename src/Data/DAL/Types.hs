module Data.DAL.Types where

import Data.Int
import Data.Proxy
import Data.String(IsString(..))

newtype NS a = NS { nsUnpack :: String }
               deriving(Show,Eq,Ord,IsString)

instance Monoid (NS a) where
  mempty = NS mempty

instance Semigroup (NS a) where
  (<>) (NS a) (NS b) = NS (a<>b)

class HasKey a where
  data KeyOf a :: *
  key   :: a -> KeyOf a
  ns    :: NS a

class Monad m => SourceListAll a m e where
  listAll :: e -> m [a]

class Monad m => SourceListOffsetLimit a m e where
  listOffsetLimit :: e -> Int -> Int -> m [a]

class Monad m => SourceQueryAll a q m e where
  queryAll :: e -> q -> m [a]

class (Monad m, HasKey a) => SourceStore a m e where
  store   :: e -> a -> m (KeyOf a)
  load    :: e -> KeyOf a -> m (Maybe a)

class (Monad m, HasKey a) => SourceDeleteByKey a m e where
  delete :: e -> KeyOf a -> m ()

class (Monad m, HasKey a) => SourceDeleteAll a m e where
  deleteAll :: Proxy a -> e -> m ()

class (Monad m, HasKey a) => SourceCountAll a m e where
  countAll :: Proxy a -> e -> m Int64

class Monad m => SourceTransaction a m e where
  withTransaction :: e -> m a -> m a

data KV k v = KV k v
  deriving(Eq,Ord,Show)

class KeyValNS k v where
  keyValNS :: NS (KV k v)

class (Monad m) => SourceKVStore k v m e where
  storeKV  :: e -> k -> v -> m ()
  loadK    :: e -> k -> m (Maybe v)
  listKeys :: e -> v -> m [k]
