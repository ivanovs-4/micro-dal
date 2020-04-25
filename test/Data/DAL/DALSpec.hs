module Data.DAL.DALSpec (spec) where

import Data.DAL
import Data.DAL.KeyValue.SQLite

import Test.Hspec

spec :: Spec
spec = do
  describe "DAL test 2" $ do
    it "jerks around" $ do
      0 `shouldBe` 0
      pure ()
