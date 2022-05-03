{-# language DeriveGeneric #-}
{-# language OverloadedStrings #-}
{-# options_ghc -Wno-unused-imports #-}
module JSONL.ConduitSpec where

import Data.Functor.Identity (Identity(..))
import GHC.Generics (Generic)

-- aeson
import qualified Data.Aeson as A (FromJSON(..), ToJSON(..))
-- bytestring
import qualified Data.ByteString as BS (ByteString)
import qualified Data.ByteString.Builder as BBS (toLazyByteString, Builder)
import qualified Data.ByteString.Internal as BS (c2w)
import qualified Data.ByteString.Char8 as BS8 (span)
import qualified Data.ByteString.Lazy as LBS (ByteString, span, toStrict, fromStrict)
-- conduit
import qualified Conduit as C (ConduitT, runConduit, runConduitRes, sourceFile, sinkFile, await, yield, yieldMany, mapC, unfoldC, foldMapC, foldlC, sinkList)
import Conduit ( (.|) , MonadResource)
-- hspec
import Test.Hspec (Spec, describe, it, shouldBe)

import JSONL.Conduit (jsonToLBSC, jsonFromLBSC, sourceFileC, sinkFileC)

spec :: Spec
spec =
  describe "JSONL.Conduit" $ do
    it "encodes a list of JSONL records" $ do
      datsC `shouldBe` datsLBS
    it "reads a JSONL file and decodes its contents" $ do
      rs <- readRecords
      rs `shouldBe` dats

data Record = R String String Int Bool deriving (Eq, Show, Generic)
instance A.ToJSON Record
instance A.FromJSON Record

datsC :: LBS.ByteString
datsC = runIdentity $ C.runConduit $ C.yieldMany dats .| jsonToLBSC

dats :: [Record]
dats = [R "Gilbert" "2013" 24 True,R "Alexa" "2013" 29 True, R "May" "2012B" 14 False, R "Deloise" "2012A" 19 True]

datsLBS :: LBS.ByteString
datsLBS = "[\"Gilbert\",\"2013\",24,true]\n[\"Alexa\",\"2013\",29,true]\n[\"May\",\"2012B\",14,false]\n[\"Deloise\",\"2012A\",19,true]\n"


-- data asset

readRecords :: IO [Record]
readRecords = C.runConduitRes $ sourceFileC "records" .| C.sinkList

dump :: IO ()
dump = C.runConduitRes $ C.yieldMany dats .| sinkFileC "records"
