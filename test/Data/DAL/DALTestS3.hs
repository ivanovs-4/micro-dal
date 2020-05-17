module Main where

import Control.Monad
import Data.Data
import Data.List (sort)
import Data.Map (Map)
import Data.Maybe
import Data.Set (Set)
import Data.Store
import Data.Text (Text)
import Data.Word
import GHC.Generics
import qualified Data.Map as Map
import qualified Data.Set as Set

import Test.Hspec
import Test.Hspec.Expectations
import Test.QuickCheck
import Test.QuickCheck.Arbitrary

import Data.DAL
import Data.DAL.KeyValue.HashRef

import Data.DAL.KeyValue.S3

data SomeData = SomeData Word32 Word32
                deriving (Eq,Ord,Show,Data,Generic)

instance Store SomeData

instance HasKey SomeData where

  newtype KeyOf SomeData = SomeDataKey Word32
                           deriving (Eq,Ord,Show,Generic,Store)

  key (SomeData a _) = SomeDataKey a
  ns = "somedata"

newtype SomeOtherData = SomeOtherData String
                        deriving (Eq,Ord,Show,Generic,Store)

type HashedInt = HashRef "ints" Int
instance Store HashedInt

instance Arbitrary SomeData where
  arbitrary = SomeData <$> arbitrary <*> arbitrary

engOps = s3EngineOptsDev { s3BucketPrefix = "test" }

main :: IO ()
main = do
  -- describe "DAL simple load/store test" $ do
  --   it "stores some random SomeData values and restores them" $ do

      withEngine engOps $ \eng -> do

        replicateM_ 5 $ do

          v1 <- generate arbitrary :: IO SomeData
          k1 <- store eng v1
          v2 <- load eng k1

          v2 `shouldBe` Just v1


  -- describe "DAL simple store/loadAll test" $ do
  --   it "stores some random SomeData values and restores them" $ do

      replicateM_ 3 $ do
        withEngine engOps $ \eng -> do
          cleanEngine eng

          els  <- Map.fromList <$> generate arbitrary :: IO (Map Word32 Word32)
          let vals  = [ SomeData k v | (k,v) <- Map.toList els ]
          mapM (store eng) vals
          vals2 <- listAll @SomeData eng

          (sort vals2) `shouldMatchList` (sort vals)


  -- describe "DAL HashRef test" $ do
  --   it "stores and restores some random values using HashRef" $ do

      replicateM_ 10 $ do
        withEngine engOps $ \eng -> do
          cleanEngine eng

          ivalues <- generate arbitrary :: IO [Int]
          forM_ ivalues $ \i -> do
            k <- store @HashedInt eng (hashRefPack i)
            ii <- load @HashedInt eng k
            Just i `shouldBe` (fromJust $ hashRefUnpack <$> ii)


  -- describe "DAL delete test" $ do
  --   it "stores and restores some random values using HashRef and deletes odds" $ do

      replicateM_ 3 $ do
        withEngine engOps $ \eng -> do
          cleanEngine eng

          let ivalues = [1..100]

          forM_ ivalues $ \i -> do
            store @HashedInt eng (hashRefPack i)

          hvals <- listAll @HashedInt eng
          ivals <- (catMaybes . fmap hashRefUnpack) <$> pure hvals

          ivalues `shouldMatchList` ivals

          forM_ (filter odd ivalues) $ \v -> do
            delete @HashedInt eng (key (hashRefPack v))

          ivals2 <- fmap sort $ (catMaybes . fmap hashRefUnpack) <$> listAll @HashedInt eng
          ivals2 `shouldMatchList` (filter even ivalues)


      withEngine engOps cleanEngine
