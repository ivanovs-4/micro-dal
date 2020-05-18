module Data.DAL.KeyValue.S3Spec (spec) where

import Control.Monad
import Control.Monad.IO.Class
import Data.Data
import Data.List (sort)
import Data.Map (Map)
import Data.Maybe
import Data.Set (Set)
import Data.Store
import Data.String.Conversions (cs)
import Data.Text (Text)
import Data.Word
import GHC.Generics
import qualified Data.Map as Map
import qualified Data.Set as Set

import Test.Hspec
import Test.Hspec.Expectations
import Test.QuickCheck
import Test.QuickCheck.Arbitrary

import Test.Hspec (Spec, SpecWith, hspec, before, describe, it, shouldBe)
import Test.Hspec.NeedEnv (EnvMode(Want), needEnv, needEnvRead)

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

getEnvs :: IO S3EngineOpts
getEnvs = do
    s3Address   <- cs <$> needEnv mode "TEST_S3_ADDR"
    s3AccessKey <- cs <$> needEnv mode "TEST_S3_ACCESS_KEY"
    s3SecretKey <- cs <$> needEnv mode "TEST_S3_SECRET_KEY"
    s3Bucket    <- cs <$> needEnv mode "TEST_S3_BUCKET"
    s3Region    <- cs <$> needEnv mode "TEST_S3_REGION"
    pure S3EngineOpts {..}
    where
      mode = Want

spec :: Spec
spec = before (createEngine =<< getEnvs) specWithS3

specWithS3 :: SpecWith S3Engine
specWithS3 = do

  describe "DAL S3 simple load/store test" $ do
    it "stores some random SomeData values and restores them" $ \eng -> do

      replicateM_ 5 $ do

          v1 <- generate arbitrary :: IO SomeData
          k1 <- store eng v1
          v2 <- load eng k1

          v2 `shouldBe` Just v1


  describe "DAL S3 simple store/loadAll test" $ do
    it "stores some random SomeData values and restores them" $ \eng -> do

      replicateM_ 1 $ do
          cleanEngine eng

          els  <- Map.fromList <$> generate arbitrary :: IO (Map Word32 Word32)
          let vals  = [ SomeData k v | (k,v) <- Map.toList els ]
          mapM (store eng) vals
          vals2 <- listAll @SomeData eng

          (sort vals2) `shouldMatchList` (sort vals)


  describe "DAL S3 HashRef test" $ do
    it "stores and restores some random values using HashRef" $ \eng -> do

      replicateM_ 2 $ do
          cleanEngine eng

          ivalues <- generate arbitrary :: IO [Int]
          forM_ ivalues $ \i -> do
            k <- store @HashedInt eng (hashRefPack i)
            ii <- load @HashedInt eng k
            Just i `shouldBe` (fromJust $ hashRefUnpack <$> ii)


  describe "DAL S3 delete test" $ do
    it "stores and restores some random values using HashRef and deletes odds" $ \eng -> do

      replicateM_ 2 $ do
          cleanEngine eng

          let ivalues = [1..10]

          forM_ ivalues $ \i -> do
            store @HashedInt eng (hashRefPack i)

          hvals <- listAll @HashedInt eng
          ivals <- (catMaybes . fmap hashRefUnpack) <$> pure hvals

          ivalues `shouldMatchList` ivals

          forM_ (filter odd ivalues) $ \v -> do
            delete @HashedInt eng (key (hashRefPack v))

          ivals2 <- fmap sort $ (catMaybes . fmap hashRefUnpack) <$> listAll @HashedInt eng
          ivals2 `shouldMatchList` (filter even ivalues)


      cleanEngine eng
