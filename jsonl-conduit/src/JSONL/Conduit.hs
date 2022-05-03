{-# LANGUAGE TupleSections #-}
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

import Control.Monad.IO.Class (MonadIO(..))

  -- aeson
import Data.Aeson (ToJSON(..), FromJSON(..), eitherDecode' )
-- bytestring
import qualified Data.ByteString as BS (ByteString, null)
import qualified Data.ByteString.Builder as BBS (toLazyByteString, Builder)
import qualified Data.ByteString.Internal as BS (c2w)
import qualified Data.ByteString.Char8 as BS8 (span, drop, putStrLn, putStr)
import qualified Data.ByteString.Lazy as LBS (ByteString, drop, span, toStrict, fromStrict)
-- conduit
import qualified Conduit as C (ConduitT, runConduit, sourceFile, sinkFile, await, yield, mapC, unfoldC, foldMapC, foldlC, printC)
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
          -> C.ConduitT a o m ()
sinkFileC fpath = C.mapC (LBS.toStrict . BBS.toLazyByteString . jsonLine) .|
                  C.sinkFile fpath

-- | Read a JSONL file and stream the decoded records
--
-- NB : ignores any parsing errors and returns 
sourceFileC :: (MonadResource m, FromJSON a) =>
               FilePath -- ^ path of JSONL file to be read
            -> C.ConduitT () a m ()
sourceFileC fpath = C.sourceFile fpath .|
                    parseChunk

parseChunk :: (Monad m, FromJSON a) => C.ConduitT BS.ByteString a m ()
parseChunk = go mempty
  where
    go acc =
      if not (BS.null acc) -- buffer is non empty
      then
        case chopDecode acc of
          Left _ -> pure ()
          Right (y, srest) -> do
            C.yield y
            go srest
      else do
        mc <- C.await -- get data from upstream
        case mc of
          Nothing -> pure ()
          Just x -> do
            let
              acc' = acc <> x
            case chopDecode acc' of
              Left _ -> pure ()
              Right (y, srest) -> do
                C.yield y
                go srest

chopDecode :: FromJSON a =>
              BS.ByteString -> Either String (a, BS.ByteString)
chopDecode acc = (, srest) <$> eitherDecode' (LBS.fromStrict s)
  where
    (s, srest) = chopBS8 acc



-- | Source a `LBS.ByteString` for JSONL records
jsonFromLBSC :: (FromJSON a, Monad m) => LBS.ByteString -> C.ConduitT Void a m ()
jsonFromLBSC = C.unfoldC mk
  where
    mk lbs = case eitherDecode' s of
      Right x -> Just (x, srest)
      Left _ -> Nothing
      where
        (s, srest) = chopLBS lbs -- LBS.span (== BS.c2w '\n') lbs


-- * utilities

-- | 'span' for a strict bytestring encoding a JSONL record
chopBS8 :: BS.ByteString -> (BS.ByteString, BS.ByteString)
chopBS8 lbs = (s, BS8.drop 1 srest)
  where (s, srest) = BS8.span (/= '\n') lbs

-- | 'span' for a lazy bytestring encoding a JSONL record
chopLBS :: LBS.ByteString -> (LBS.ByteString, LBS.ByteString)
chopLBS lbs = (s, LBS.drop 1 srest)
  where (s, srest) = LBS.span (/= BS.c2w '\n') lbs

