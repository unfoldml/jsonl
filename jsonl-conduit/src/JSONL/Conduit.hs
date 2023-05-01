{-# LANGUAGE TupleSections #-}
{-# options_ghc -Wno-unused-imports #-}
-- | Streaming interface for JSONL-encoded files, based on @conduit@
--
-- The JSONL (JSON Lines) format : https://jsonlines.org/
module JSONL.Conduit (
  -- * Encode
  jsonToLBSC
  -- ** I/O
  , sinkFileC
  , appendFileC
  -- * Decode
  , jsonFromLBSC
  , jsonFromLBSCE
  -- ** I/O
  , sourceFileC
  , sourceFileCLen
  , sourceFileCE
  -- * Tokenize only
  , sourceFileC_
  ) where

import Data.Void (Void)

import Control.Monad.IO.Class (MonadIO(..))
import System.IO (IOMode(..), Handle, openBinaryFile)

  -- aeson
import Data.Aeson (ToJSON(..), FromJSON(..), eitherDecode' )
-- bytestring
import qualified Data.ByteString as BS (ByteString, null)
import qualified Data.ByteString.Builder as BBS (toLazyByteString, Builder)
import qualified Data.ByteString.Internal as BS (c2w)
import qualified Data.ByteString.Char8 as BS8 (span, drop, length, putStrLn, putStr)
import qualified Data.ByteString.Lazy as LBS (ByteString, null, drop, span, toStrict, fromStrict)
-- conduit
import qualified Conduit as C (ConduitT, runConduit, sourceFile, sinkFile, await, yield, mapC, unfoldC, foldMapC, foldlC, printC, sinkIOHandle)
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
sinkFileC fpath = C.mapC encodeJSONL .|
                  C.sinkFile fpath

-- | Like `sinkFileC` but in `AppendMode`, which means that the handle is positioned at the
-- end of the file.
--
-- @since 0.1.1
appendFileC :: (ToJSON a, MonadResource m) =>
               FilePath
            -> C.ConduitT a o m ()
appendFileC fpath = C.mapC encodeJSONL .|
                    C.sinkIOHandle (openBinaryFile fpath AppendMode)

encodeJSONL :: ToJSON a => a -> BS.ByteString
encodeJSONL = LBS.toStrict . BBS.toLazyByteString . jsonLine

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
    progress acc = case chopDecode acc of
                     Left _ -> pure ()
                     Right (y, srest) -> do
                       C.yield y
                       go srest
    go acc =
      if not (BS.null acc) -- buffer is non empty
      then progress acc
      else do
        mc <- C.await -- get data from upstream
        case mc of
          Nothing -> pure ()
          Just x -> progress (acc <> x)

-- | Like 'sourceFileC' but streams out the length in characters of the bytestring from which the JSON object was decoded (including the terminating newline), in addition to the object itself
--
-- This can be handy for later memory mapping into the JSONL file
sourceFileCLen :: (MonadResource m, FromJSON a) =>
                  FilePath -- ^ path of JSONL file to be read
               -> C.ConduitT () (a, Int) m ()
sourceFileCLen fpath = C.sourceFile fpath .|
                       parseChunkLen

parseChunkLen :: (Monad m, FromJSON a) => C.ConduitT BS.ByteString (a, Int) m ()
parseChunkLen = go mempty
  where
    progress acc = case chopDecodeLen acc of
                     Left _ -> pure ()
                     Right (y, l, srest) -> do
                       C.yield (y, l)
                       go srest
    go acc =
      if not (BS.null acc) -- buffer is non empty
      then progress acc
      else do
        mc <- C.await -- get data from upstream
        case mc of
          Nothing -> pure ()
          Just x -> progress (acc <> x)




-- | Read a JSONL file and stream the decoded records
--
-- NB : decoding error messages are in 'Left' values
sourceFileCE :: (MonadResource m, FromJSON a) =>
                FilePath -- ^ path of JSONL file to be read
             -> C.ConduitT () (Either String a) m ()
sourceFileCE fpath = C.sourceFile fpath .|
                     parseChunkE

parseChunkE :: (Monad m, FromJSON a) => C.ConduitT BS.ByteString (Either String a) m ()
parseChunkE = go mempty
  where
    progress acc = case chopDecode acc of
                     Left e -> C.yield (Left e)
                     Right (y, srest) -> do
                       C.yield $ Right y
                       go srest
    go acc =
      if not (BS.null acc) -- buffer is non empty
      then progress acc
      else do
        mc <- C.await -- get data from upstream
        case mc of
          Nothing -> pure ()
          Just x -> progress (acc <> x)


-- | The outgoing stream elements are the lines of the file, i.e guaranteed not to contain newline characters
--
-- NB : In case it wasn't clear, no JSON parsing is done, only string copies
sourceFileC_ :: MonadResource m =>
                FilePath -- ^ path of JSONL file to be read
             -> C.ConduitT () LBS.ByteString m ()
sourceFileC_ fpath = C.sourceFile fpath .|
                     toLazyLines

toLazyLines :: (Monad m) => C.ConduitT BS.ByteString LBS.ByteString m ()
toLazyLines = go mempty
  where
    go acc =
      if not (BS.null acc)
      then
        do
          let
            (y, srest) = chop acc
          C.yield y
          go srest
      else
        do
          mc <- C.await
          case mc of
            Nothing -> pure ()
            Just x -> do
              let
                acc' = acc <> x
                (y, srest) = chop acc'
              C.yield y
              go srest


chop :: BS.ByteString -> (LBS.ByteString, BS.ByteString)
chop acc = (LBS.fromStrict s, srest)
  where
    (s, srest) = chopBS8 acc

chopDecode :: FromJSON a =>
              BS.ByteString -> Either String (a, BS.ByteString)
chopDecode acc = (, srest) <$> eitherDecode' (LBS.fromStrict s)
  where
    (s, srest) = chopBS8 acc

chopDecodeLen :: FromJSON a =>
                 BS.ByteString -> Either String (a, Int, BS.ByteString)
chopDecodeLen acc = (, l, srest) <$> eitherDecode' (LBS.fromStrict s)
  where
    (s, l, srest) = chopBS8Len acc


-- | Source a `LBS.ByteString` for JSONL records
--
-- NB in case of a decoding error the stream is stopped
jsonFromLBSC :: (FromJSON a, Monad m) => LBS.ByteString -> C.ConduitT Void a m ()
jsonFromLBSC = C.unfoldC mk
  where
    mk lbs = case eitherDecode' s of
      Right x -> Just (x, srest)
      Left _ -> Nothing
      where
        (s, srest) = chopLBS lbs

-- | Like 'jsonFromLBSC' but all decoding errors are passed in Left values
jsonFromLBSCE :: (FromJSON a, Monad m) => LBS.ByteString -> C.ConduitT i (Either String a) m ()
jsonFromLBSCE = C.unfoldC mk
  where
    mk lbs =
      let (s, srest) = chopLBS lbs
      in
        if LBS.null s
        then Nothing
        else case eitherDecode' s of
          Right x -> Just (Right x, srest)
          Left e -> Just (Left e, srest)


-- * utilities



-- | 'span' for a strict bytestring encoding a JSONL record
chopBS8 :: BS.ByteString
        -> (BS.ByteString, BS.ByteString) -- ^ (chars without newline, rest of string)
chopBS8 bs = (s, srest)
  where
    (s, _, srest) = chopBS8Len bs

chopBS8Len :: BS.ByteString
           -> (BS.ByteString, Int, BS.ByteString) -- ^ (chars without newline, length /including/ newline, rest of string)
chopBS8Len bs = (s, l, BS8.drop 1 srest)
  where
    (s, srest) = BS8.span (/= '\n') bs
    l = BS8.length s + 1 -- length in characters of the JSONL object, including newline

-- chopBS8 lbs = (s, BS8.drop 1 srest)
--   where (s, srest) = BS8.span (/= '\n') lbs

-- | 'span' for a lazy bytestring encoding a JSONL record
chopLBS :: LBS.ByteString -> (LBS.ByteString, LBS.ByteString)
chopLBS lbs = (s, LBS.drop 1 srest)
  where (s, srest) = LBS.span (/= BS.c2w '\n') lbs

