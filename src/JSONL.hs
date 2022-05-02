{-# language DeriveGeneric #-}
{-# language OverloadedStrings #-}
-- | JSON Lines https://jsonlines.org/
module JSONL (jsonlWriteFile, jsonlToLBS, jsonlBuilder) where

import System.IO (Handle, IOMode(..), withBinaryFile)

-- aeson
import Data.Aeson (ToJSON(..), encode )
-- bytestring
import qualified Data.ByteString.Builder as BBS (toLazyByteString, Builder, lazyByteString, string7)
import qualified Data.ByteString.Builder.Internal as BBS (hPut, putBuilder)
import qualified Data.ByteString.Lazy as LBS (ByteString)

import Prelude hiding (writeFile)


-- | Write a collection of objects to a JSONL-encoded file
jsonlWriteFile :: (Foldable t, ToJSON a) => FilePath -> t a -> IO ()
jsonlWriteFile fpath xs = writeFile fpath (jsonlBuilder xs)

-- | Render a collection of objects to a JSONL-encoded `LBS.ByteString`
jsonlToLBS :: (Foldable t, ToJSON a) => t a -> LBS.ByteString
jsonlToLBS xs = BBS.toLazyByteString $ jsonlBuilder xs 

jsonlBuilder :: (Foldable t, ToJSON a) => t a -> BBS.Builder
jsonlBuilder = foldMap jsonLine

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

