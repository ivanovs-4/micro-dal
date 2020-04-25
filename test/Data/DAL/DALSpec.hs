module Data.DAL.DALSpec (spec) where

import Data.DAL

import Test.Hspec

spec :: Spec
spec = do
  describe "DAL test 1" $ do
    it "jerks around" $ do
      1 `shouldBe` 0
      pure ()
