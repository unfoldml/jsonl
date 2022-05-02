module JSONL.Conduit (
  -- * Encode
  jsonToLBSC
  , sinkFile
  -- * Decode
  , jsonFromLBSC
  ) where

import Data.Void (Void)

import qualified Conduit as C (ConduitT, runConduit, sourceFile, sinkFile, yield, mapC, unfoldC, foldMapC)
import Conduit ( (.|) , MonadResource)

  -- aeson
import Data.Aeson (ToJSON(..), FromJSON(..), eitherDecode' )
-- bytestring
import qualified Data.ByteString.Builder as BBS (toLazyByteString, Builder, lazyByteString, string7)

import qualified Data.ByteString.Internal as BS (c2w)
import qualified Data.ByteString.Lazy as LBS (ByteString, span, toStrict)
-- jsonl
import JSONL (jsonLine)

jsonToLBSC :: (ToJSON a, Monad m) => C.ConduitT a o m LBS.ByteString
jsonToLBSC = BBS.toLazyByteString <$> jsonToBuilderC

-- | Render a stream of JSON-encodable objects into a `BSB.Builder`
jsonToBuilderC :: (ToJSON a, Monad m) => C.ConduitT a o m BBS.Builder
jsonToBuilderC = C.foldMapC jsonLine

-- | Render a stream of JSON-encodable objects into a JSONL file
sinkFile :: (ToJSON a, MonadResource m) =>
            FilePath -- ^ path of JSONL file to be created
         -> C.ConduitT a () m ()
sinkFile fpath = C.mapC (LBS.toStrict . BBS.toLazyByteString . jsonLine) .|
                 C.sinkFile fpath 

-- sourceFile fpath = C.sourceFile fpath .|
--                    go
--   where
--     go = 

-- | Source a `LBS.ByteString` for JSONL records
jsonFromLBSC :: (FromJSON a, Monad m) => LBS.ByteString -> C.ConduitT Void a m ()
jsonFromLBSC = C.unfoldC mk
  where
    mk lbs = case eitherDecode' s of
      Right x -> Just (x, srest)
      Left _ -> Nothing
      where
        (s, srest) = LBS.span (== BS.c2w '\n') lbs


