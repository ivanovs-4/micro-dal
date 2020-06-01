module Data.DAL.Types where

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

class (Monad m, HasKey a) => SourceStore a m e where
  store   :: e -> a -> m (KeyOf a)
  load    :: e -> KeyOf a -> m (Maybe a)

class (Monad m, HasKey a) => SourceDeleteByKey a m e where
  delete :: e -> KeyOf a -> m ()

class (Monad m, HasKey a) => SourceDeleteAll a m e where
  deleteAll :: Proxy a -> e -> m ()

class Monad m => SourceTransaction a m e where
  withTransaction :: e -> m a -> m a

