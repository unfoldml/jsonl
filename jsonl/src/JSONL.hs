{-# LANGUAGE RankNTypes #-}
{-# language DeriveGeneric #-}
{-# language OverloadedStrings #-}
-- | JSON Lines https://jsonlines.org/
module JSONL (
  -- * Encode
  jsonlWriteFile, jsonlToLBS
  , jsonlBuilder
  , jsonLine
  -- * Decode
 , jsonlFromLBS
  ) where

import System.IO (Handle, IOMode(..), withBinaryFile)

-- aeson
import Data.Aeson (ToJSON(..), FromJSON(..), encode, eitherDecode' )
-- bytestring
import qualified Data.ByteString.Builder as BBS (toLazyByteString, Builder, lazyByteString, string7)
import qualified Data.ByteString.Builder.Internal as BBS (hPut, putBuilder)
import qualified Data.ByteString.Internal as BS (c2w)
import qualified Data.ByteString.Lazy as LBS (ByteString, span)

import Prelude hiding (writeFile)

-- | Parse a JSONL-encoded collection of objects from a `LBS.ByteString`
--
-- If parsing fails, returns the first parsing error in a Left
jsonlFromLBS :: FromJSON a => LBS.ByteString -> Either String [a]
jsonlFromLBS = sequence . jsonlFromLBS_

jsonlFromLBS_ :: FromJSON a => LBS.ByteString -> [Either String a]
jsonlFromLBS_ = go mempty
  where
    go :: FromJSON a => [Either String a] -> LBS.ByteString -> [Either String a]
    go acc lbs = let
      (s, srest) = LBS.span (== BS.c2w '\n') lbs
      in go (eitherDecode' s : acc) srest


-- | Write a collection of objects to a JSONL-encoded file
jsonlWriteFile :: (Foldable t, ToJSON a) => FilePath -> t a -> IO ()
jsonlWriteFile fpath xs = writeFile fpath (jsonlBuilder xs)

-- | Render a collection of objects to a JSONL-encoded `LBS.ByteString`
jsonlToLBS :: (Foldable t, ToJSON a) => t a -> LBS.ByteString
jsonlToLBS xs = BBS.toLazyByteString $ jsonlBuilder xs 

jsonlBuilder :: (Foldable t, ToJSON a) => t a -> BBS.Builder
jsonlBuilder = foldMap jsonLine

-- | Render a single JSONL line (together with its newline)
--
-- @since 0.2
jsonLine :: ToJSON a => a -> BBS.Builder
jsonLine x = jsonToBuilder x <> BBS.string7 "\n"

jsonToBuilder :: ToJSON a => a -> BBS.Builder
jsonToBuilder = BBS.lazyByteString . encode


-- vendored from bytestring < 0.11.2

hPutBuilder :: Handle -> BBS.Builder -> IO ()
hPutBuilder h = BBS.hPut h . BBS.putBuilder

modifyFile :: IOMode -> FilePath -> BBS.Builder -> IO ()
modifyFile mode f bld = withBinaryFile f mode (`hPutBuilder` bld)

writeFile :: FilePath -> BBS.Builder -> IO ()
writeFile = modifyFile WriteMode

