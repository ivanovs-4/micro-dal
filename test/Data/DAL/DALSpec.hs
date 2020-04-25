module Data.DAL.DALSpec (spec) where

import Data.DAL
import Data.DAL.KeyValue.SQLite

import Control.Monad
import Data.Data
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

data SomeData = SomeData Word32 Word32
                deriving (Eq,Ord,Show,Data,Generic)

instance Store SomeData

instance HasKey SomeData where

  newtype KeyOf SomeData = SomeDataKey Word32
                           deriving (Eq,Ord,Show,Generic,Store)

  key (SomeData a _) = SomeDataKey a
  ns = "somedata"

instance Arbitrary SomeData where
  arbitrary = SomeData <$> arbitrary <*> arbitrary

spec :: Spec
spec = do
  describe "DAL simple load/store test" $ do
    it "stores some random SomeData values and restores them" $ do

      withEngine optInMemory $ \eng -> do

        replicateM_ 1000 $ do

          v1 <- generate arbitrary :: IO SomeData
          k1 <- store eng v1
          v2 <- load eng k1

          v2 `shouldBe` Just v1

        pure ()

      pure ()

  describe "DAL simple store/loadAll test" $ do
    it "stores some random SomeData values and restores them" $ do

      replicateM 1000 $ do
        withEngine optInMemory $ \eng -> do

          els  <- Map.fromList <$> generate arbitrary :: IO (Map Word32 Word32)
          let vals  = [ SomeData k v | (k,v) <- Map.toList els ]
          mapM (store eng) vals
          vals2 <- listAll @SomeData eng

          vals2 `shouldMatchList` vals

          pure ()

      pure ()
