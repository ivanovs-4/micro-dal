module Data.DAL.Types where

import Data.String(IsString(..))

newtype NS a = NS { nsUnpack :: String }
               deriving(Show,Eq,Ord,IsString)

class HasKey a where
  data KeyOf a :: *
  key   :: a -> KeyOf a
  ns    :: NS a

class Monad m => SourceListAll a m e where
  listAll :: e -> m [a]

class (Monad m, HasKey a) => SourceStore a m e where
  store :: e -> a -> m (KeyOf a)
  load :: e -> KeyOf a -> m (Maybe a)

class Monad m => SourceTransaction a m e where
  withTransaction :: e -> m a -> m a

