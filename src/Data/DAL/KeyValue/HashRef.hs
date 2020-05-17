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

unB58 :: B58 -> ByteString
unB58 (B58 s) = s

instance Store B58

instance Show B58 where
  show (B58 s) = show (encodeBase58 bitcoinAlphabet s)

instance IsString B58 where
  fromString s = fromJust $ B58 <$> decodeBase58 bitcoinAlphabet (BS8.pack s)

newtype HashRef (ns :: Symbol) a = HashRef (Either B58 a)
                                   deriving(Eq,Ord,Show,Data,Generic)

instance (KnownSymbol n, Store a) => HasKey (HashRef n a) where
  data KeyOf (HashRef n a) = HashRefKey ByteString deriving(Eq,Ord,Show,Generic)
  key = HashRefKey . hashRefKey
  ns = fromString (symbolVal (Proxy :: Proxy n))

instance (KnownSymbol n, Store a) => Store (KeyOf (HashRef n a))

hashRefUnpack :: HashRef n a -> Maybe a
hashRefUnpack (HashRef (Left _)) = Nothing
hashRefUnpack (HashRef (Right v)) = pure v

hashRefPack :: Store a => a -> HashRef n a
hashRefPack x = hashRefValue x

hashRefValue :: Store a => a -> HashRef n a
hashRefValue x = HashRef (Right x)

hashRefRef :: Store a => a -> HashRef n a
hashRefRef x = HashRef (Left (B58 q))
  where
    q = hashRefKey (HashRef (Right x))

hashRefKey :: Store a => HashRef n a -> ByteString
hashRefKey (HashRef (Left (B58 s))) = s
hashRefKey (HashRef (Right a)) = sha256
  where
    sha256 = BS.pack (BA.unpack (hash (encode a) :: Digest SHA256))

