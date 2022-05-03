{-# options_ghc -Wno-unused-imports #-}
module JSONL.Conduit (
  -- * Encode
  jsonToLBSC
  -- ** I/O
  , sinkFileC
  -- * Decode
  , jsonFromLBSC
  -- ** I/O
  , sourceFileC
  ) where

import Data.Void (Void)



  -- aeson
import Data.Aeson (ToJSON(..), FromJSON(..), eitherDecode' )
-- bytestring
import qualified Data.ByteString as BS (ByteString)
import qualified Data.ByteString.Builder as BBS (toLazyByteString, Builder)
import qualified Data.ByteString.Internal as BS (c2w)
import qualified Data.ByteString.Char8 as BS8 (span)
import qualified Data.ByteString.Lazy as LBS (ByteString, span, toStrict, fromStrict)
-- conduit
import qualified Conduit as C (ConduitT, runConduit, sourceFile, sinkFile, await, yield, mapC, unfoldC, foldMapC, foldlC)
import Conduit ( (.|) , MonadResource)
-- jsonl
import JSONL (jsonLine)

-- | Render a stream of JSON-encodable objects into a `LBS.ByteString`
jsonToLBSC :: (ToJSON a, Monad m) => C.ConduitT a o m LBS.ByteString
jsonToLBSC = BBS.toLazyByteString <$> jsonToBuilderC

-- | Render a stream of JSON-encodable objects into a `BSB.Builder`
jsonToBuilderC :: (ToJSON a, Monad m) => C.ConduitT a o m BBS.Builder
jsonToBuilderC = C.foldMapC jsonLine

-- | Render a stream of JSON-encodable objects into a JSONL file
sinkFileC :: (ToJSON a, MonadResource m) =>
             FilePath -- ^ path of JSONL file to be created
          -> C.ConduitT a () m ()
sinkFileC fpath = C.mapC (LBS.toStrict . BBS.toLazyByteString . jsonLine) .|
                  C.sinkFile fpath

-- | Read a JSONL file and stream the decoded records
sourceFileC :: (MonadResource m, FromJSON a) =>
               FilePath -- ^ path of JSONL file to be read
            -> C.ConduitT Void a m ()
sourceFileC fpath = C.sourceFile fpath .|
                    parseChunk

parseChunk :: (Monad m, FromJSON a) => C.ConduitT BS.ByteString a m ()
parseChunk = go mempty
  where
    go acc = do
      mc <- C.await
      case mc of
        Nothing -> pure ()
        Just x -> do
          let
            acc' = x <> acc
            (s, srest) = BS8.span (== '\n') acc'
          case eitherDecode' $ LBS.fromStrict s of
            Left _ -> pure ()
            Right y -> do
              C.yield y
              go srest


-- parseChunk = C.foldlC mk mempty
--   where
--     mk acc x = undefined

-- | Source a `LBS.ByteString` for JSONL records
jsonFromLBSC :: (FromJSON a, Monad m) => LBS.ByteString -> C.ConduitT Void a m ()
jsonFromLBSC = C.unfoldC mk
  where
    mk lbs = case eitherDecode' s of
      Right x -> Just (x, srest)
      Left _ -> Nothing
      where
        (s, srest) = LBS.span (== BS.c2w '\n') lbs


