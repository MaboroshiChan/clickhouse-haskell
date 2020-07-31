{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}

module ClickHouseDriver.Core.Defines where

import Data.ByteString.Internal
import Data.Word
import Network.Simple.TCP
import Network.Socket (SockAddr, Socket)

_DEFAULT_PORT = "9000"

_DEFAULT_SECURE_PORT = "9440"

_DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES = 50264 :: Word

_DBMS_MIN_REVISION_WITH_TOTAL_ROWS_IN_PROGRESS = 51554 :: Word

_DBMS_MIN_REVISION_WITH_BLOCK_INFO = 51903

-- Legacy above.
_DBMS_MIN_REVISION_WITH_CLIENT_INFO = 54032 :: Word

_DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE = 54058 :: Word

_DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO = 54060 :: Word

_DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME = 54372 :: Word

_DBMS_MIN_REVISION_WITH_VERSION_PATCH = 54401 :: Word

_DBMS_MIN_REVISION_WITH_SERVER_LOGS = 54406 :: Word

_DBMS_MIN_REVISION_WITH_COLUMN_DEFAULTS_METADATA = 54410 :: Word

_DBMS_MIN_REVISION_WITH_CLIENT_WRITE_INFO = 54420 :: Word

_DBMS_MIN_REVISION_WITH_SETTINGS_SERIALIZED_AS_STRINGS = 54429 :: Word

-- Timeouts
_DBMS_DEFAULT_CONNECT_TIMEOUT_SEC = 10

_DBMS_DEFAULT_TIMEOUT_SEC = 300

_DBMS_DEFAULT_SYNC_REQUEST_TIMEOUT_SEC = 5

_DEFAULT_COMPRESS_BLOCK_SIZE = 1048576

_DEFAULT_INSERT_BLOCK_SIZE = 1048576

_DBMS_NAME = "ClickHouse" :: ByteString

_CLIENT_NAME = "haskell-driver" :: ByteString

_CLIENT_VERSION_MAJOR = 18 :: Word

_CLIENT_VERSION_MINOR = 10 :: Word

_CLIENT_VERSION_PATCH = 3 :: Word

_CLIENT_REVISION = 54429 :: Word

_STRINGS_ENCODING = "utf-8" :: ByteString

_DEFAULT_HTTP_PORT = 8123

_BUFFER_SIZE = 1048576 :: Int

_DEFAULT_HOST = "localhost"