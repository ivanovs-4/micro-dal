{-# LANGUAGE UndecidableInstances #-}
module Data.DAL.KeyValue.HashRef where

import Crypto.Hash
import Data.ByteString.Base58
import Data.ByteString (ByteString)
import Data.Data
import Data.Maybe(fromJust)
import Data.Store
import Data.String
import GHC.Generics
import GHC.TypeLits
import qualified Data.ByteArray as BA
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BS8

import Data.DAL.Types

newtype B58 = B58 ByteString
              deriving(Eq,Ord,Data,Generic)

instance Store B58

instance Show B58 where
  show (B58 s) = show (encodeBase58 bitcoinAlphabet s)

instance IsString B58 where
  fromString s = fromJust $ B58 <$> decodeBase58 bitcoinAlphabet (BS8.pack s)

newtype HashRef a = HashRef (Either B58 a)
                    deriving(Eq,Ord,Data,Generic)

hashRefValue :: Store a => a -> HashRef a
hashRefValue x = HashRef (Right x)

hashRefRef :: Store a => a -> HashRef a
hashRefRef x = HashRef (Left (B58 q))
  where
    q = hashRefKey (HashRef (Right x))

hashRefKey :: forall a . Store a => HashRef a -> ByteString
hashRefKey (HashRef (Left (B58 s))) = s
hashRefKey (HashRef (Right a)) = sha256
  where
    sha256 = BS.pack (BA.unpack (hash (encode a) :: Digest SHA256))


class (KnownSymbol (CASName a), Store a) => HasHashKey a where
  type CASName a :: Symbol

instance (Store a, HasHashKey a) => HasKey a where
  newtype KeyOf a = HashRefKey ByteString
  key a = HashRefKey (hashRefKey (hashRefValue a))
  ns = fromString (symbolVal (Proxy :: Proxy (CASName a)))

