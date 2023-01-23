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
import Test.Hspec (Spec, describe, it, shouldBe, shouldSatisfy)

import JSONL.Conduit (jsonToLBSC, jsonFromLBSC, jsonFromLBSCE, sourceFileC, sinkFileC, sourceFileCE)

spec :: Spec
spec =
  describe "JSONL.Conduit" $ do
    it "encodes a list of JSONL records" $ do
      datsC `shouldBe` datsLBS
    it "sourceFileC : reads a JSONL file and decodes its contents" $ do
      rs <- readRecords
      rs `shouldBe` dats
    it "sourceFileCE : reads a JSONL file and decodes its contents" $ do
      rs <- readRecordsE
      rs `shouldBe` datsE
    it "jsonFromLBSCE : decode a JSONL string" $ do
      rs <- C.runConduit (jsonFromLBSCE datsLBS .| C.sinkList)
      rs `shouldBe` datsE
    it "jsonFromLBSCE : decode a JSONL string - one failing entry" $ do
      rs <- C.runConduit (jsonFromLBSCE datsLBS_ng .| C.sinkList)
      rs `shouldBe` datsE_ng

data Record = R String String Int Bool deriving (Eq, Show, Generic)
instance A.ToJSON Record
instance A.FromJSON Record

datsC :: LBS.ByteString
datsC = runIdentity $ C.runConduit $ C.yieldMany dats .| jsonToLBSC

datsE, datsE_ng :: [Either String Record]
datsE = map Right dats
datsE_ng = [Right (R "Gilbert" "2013" 24 True),Left "Error in $[2]: parsing Int failed, expected Number, but encountered Boolean"]

dats :: [Record]
dats = [R "Gilbert" "2013" 24 True,R "Alexa" "2013" 29 True, R "May" "2012B" 14 False, R "Deloise" "2012A" 19 True]

datsLBS, datsLBS_ng :: LBS.ByteString
datsLBS = "[\"Gilbert\",\"2013\",24,true]\n[\"Alexa\",\"2013\",29,true]\n[\"May\",\"2012B\",14,false]\n[\"Deloise\",\"2012A\",19,true]\n"
datsLBS_ng = "[\"Gilbert\",\"2013\",24,true]\n[\"Alexa\",\"2013\",false,true]\n"

-- data asset
readRecordsE :: IO [Either String Record]
readRecordsE = C.runConduitRes $ sourceFileCE "records" .| C.sinkList

readRecords :: IO [Record]
readRecords = C.runConduitRes $ sourceFileC "records" .| C.sinkList

dump :: IO ()
dump = C.runConduitRes $ C.yieldMany dats .| sinkFileC "records"
