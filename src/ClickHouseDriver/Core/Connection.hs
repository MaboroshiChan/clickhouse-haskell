-- Copyright (c) 2014-present, EMQX, Inc.
-- All rights reserved.
--
-- This source code is distributed under the terms of a MIT license,
-- found in the LICENSE file.

{-# LANGUAGE CPP #-}
{-# LANGUAGE FlexibleContexts #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards #-}
{-# LANGUAGE LambdaCase #-}
 {-# LANGUAGE MultiParamTypeClasses #-}

module ClickHouseDriver.Core.Connection
  ( tcpConnect,
    sendQuery,
    receiveData,
    sendData,
    receiveResult,
    closeConnection,
    processInsertQuery,
    ping',
    versionTuple,
    sendCancel
  )
where

import qualified ClickHouseDriver.Core.Block                as Block
import qualified ClickHouseDriver.Core.ClientProtocol       as Client
import ClickHouseDriver.Core.Column ( ClickhouseType, transpose )
import ClickHouseDriver.Core.Defines
    ( _DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES,
      _DBMS_MIN_REVISION_WITH_CLIENT_INFO,
      _DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE,
      _DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO,
      _DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME,
      _DBMS_MIN_REVISION_WITH_VERSION_PATCH,
      _DEFAULT_INSERT_BLOCK_SIZE,
      _DBMS_NAME,
      _CLIENT_NAME,
      _CLIENT_VERSION_MAJOR,
      _CLIENT_VERSION_MINOR,
      _CLIENT_REVISION,
      _STRINGS_ENCODING,
      _BUFFER_SIZE )
import qualified ClickHouseDriver.Core.Error                as Error
import qualified ClickHouseDriver.Core.QueryProcessingStage as Stage
import qualified ClickHouseDriver.Core.ServerProtocol       as Server
import ClickHouseDriver.Core.Types
    ( CKResult(CKResult),
      QueryInfo,
      Packet(EndOfStream, StreamProfileInfo, Progress, Hello,
             MultiString, Block, queryData, ErrorMessage),
      Context(Context, client_setting, client_info, server_info),
      Interface(HTTP),
      ClientSetting(ClientSetting, strings_encoding, strings_as_bytes,
                    insert_block_size),
      ClientInfo(ClientInfo),
      TCPConnection(..),
      ServerInfo(..),
      Block(ColumnOrientedBlock, cdata),
      getServerInfo,
      getDefaultClientInfo,
      readProgress,
      readBlockStreamProfileInfo,
      storeProfile,
      storeProgress )
import ClickHouseDriver.IO.BufferedReader
    ( Reader,
      Buffer(socket),
      createBuffer,
      refill,
      readVarInt,
      readBinaryStr )
import ClickHouseDriver.IO.BufferedWriter
    ( Writer, MonoidMap, writeBinaryStr, writeVarUInt )
import           Control.Monad.Loops                        (iterateWhile)
import           Control.Monad.State.Lazy                   (get, runStateT)
import Control.Monad.Writer ( execWriterT, WriterT(runWriterT) )
import Data.ByteString ( ByteString )
import           Data.ByteString.Builder                    (Builder,
                                                             toLazyByteString)
import           Data.ByteString.Char8                      (unpack)
import qualified Data.List                                  as List (transpose)
import           Data.List.Split                            (chunksOf)
import           Data.Vector                                ((!))
import qualified Data.Vector                                as V (concat,
                                                                  fromList,
                                                                  length, map)
import qualified Network.Simple.TCP                         as TCP (closeSock,
                                                                    connectSock,
                                                                    sendLazy)
import           Network.Socket                             (Socket)
import           System.Timeout                             (timeout)
--Debug

import Debug.Trace (trace)
-- | This module mainly focuses how to make connection
-- | to clickhouse database and protocols to send and receive data

versionTuple :: ServerInfo -> (Word, Word, Word)
versionTuple (ServerInfo _ major minor patch _ _ _) = (major, minor, patch)

-- | set timeout to 10 seconds
ping' :: Int->TCPConnection->IO(Maybe String)
ping' timeLimit TCPConnection{tcpHost=host,tcpPort=port,tcpSocket=sock}
  = timeout timeLimit $ do
      r <- execWriterT $ writeVarUInt Client._PING
      TCP.sendLazy sock (toLazyByteString r)
      buf <- createBuffer 1024 sock
      (packet_type,_) <- runStateT (iterateWhile ( == Server._PROGRESS) readVarInt) buf
      if packet_type /= Server._PONG
        then do
          let p_type = Server.toString $ fromIntegral packet_type
          let report = "Unexpected packet from server " <> show host <> ":" 
                      <> show port <> ", expected " <> "Pong!" <> ", got " 
                      <> show p_type
          return $ show Error.ServerException{
                      code = Error._UNEXPECTED_PACKET_FROM_SERVER,
                      message = report,
                      nested = Nothing
                  }
        else return "PONG!"

-- | send hello has to make for every new connection environment. 
sendHello :: (ByteString, ByteString, ByteString) -> Socket -> IO ()
sendHello (database, username, password) sock = do
  (_, w) <- runWriterT writeHello
  TCP.sendLazy sock (toLazyByteString w)
  where
    writeHello :: Writer Builder
    writeHello = do
      writeVarUInt Client._HELLO
      writeBinaryStr ("ClickHouse " <> _CLIENT_NAME)
      writeVarUInt _CLIENT_VERSION_MAJOR
      writeVarUInt _CLIENT_VERSION_MINOR
      writeVarUInt _CLIENT_REVISION
      writeBinaryStr database
      writeBinaryStr username
      writeBinaryStr password

-- | receive hello
receiveHello :: Buffer -> IO (Either ByteString ServerInfo)
receiveHello buf = do
  (res, _) <- runStateT receiveHello' buf
  return res
  where
    receiveHello' :: Reader (Either ByteString ServerInfo)
    receiveHello' = do
      packet_type <- readVarInt
      if packet_type == Server._HELLO
        then do
          server_name <- readBinaryStr
          server_version_major <- readVarInt
          server_version_minor <- readVarInt
          server_revision <- readVarInt
          server_timezone <-
            if server_revision >= _DBMS_MIN_REVISION_WITH_SERVER_TIMEZONE
              then do
                s <- readBinaryStr
                return $ Just s
              else return Nothing
          server_displayname <-
            if server_revision >= _DBMS_MIN_REVISION_WITH_SERVER_DISPLAY_NAME
              then do
                s <- readBinaryStr
                return s
              else return ""
          server_version_dispatch <-
            if server_revision >= _DBMS_MIN_REVISION_WITH_VERSION_PATCH
              then do
                s <- readVarInt
                return s
              else return server_revision
          return $
            Right
              ServerInfo
                { name = server_name,
                  version_major = server_version_major,
                  version_minor = server_version_minor,
                  version_patch = server_version_dispatch,
                  revision = server_revision,
                  timezone = server_timezone,
                  display_name = server_displayname
                }
        else
          if packet_type == Server._EXCEPTION
            then do
              e <- readBinaryStr
              e2 <- readBinaryStr
              e3 <- readBinaryStr
              return $ Left ("exception" <> e <> " " <> e2 <> " " <> e3)
            else return $ Left "Error"

-- | connect to database through TCP port, used in Client module.
tcpConnect ::
     ByteString
     -- ^ host name to connect
  -> ByteString
     -- ^ port name to connect. Default would be 8123
  -> ByteString
     -- ^ username. Default would be "default"
  -> ByteString
     -- ^ password. Default would be empty string
  -> ByteString
     -- ^ database. Default would be "default"
  -> Bool
     -- ^ choose if send and receive data in compressed form. Default would be False. 
  -> IO (Either String TCPConnection)
tcpConnect host port user password database compression = do
  (sock, sockaddr) <- TCP.connectSock (unpack host) (unpack port)
  sendHello (database, user, password) sock
  let isCompressed = if compression then Client._COMPRESSION_ENABLE else Client._COMPRESSION_DISABLE

  buf <- createBuffer _BUFFER_SIZE sock
  hello <- receiveHello buf
  case hello of
    Right x ->
      return $
        Right
          TCPConnection
            { tcpHost = host,
              tcpPort = port,
              tcpUsername = user,
              tcpPassword = password,
              tcpSocket = sock,
              tcpSockAdrr = sockaddr,
              context = Context{
                server_info = Just x,
                client_info = Nothing,
                client_setting = Just $ ClientSetting {
                  insert_block_size = _DEFAULT_INSERT_BLOCK_SIZE,
                  strings_as_bytes = False,
                  strings_encoding = _STRINGS_ENCODING
                }
              },
              tcpCompression = isCompressed
            }
    Left "Exception" -> return $ Left "exception"
    Left x -> do
      print ("error is " <> x)
      TCP.closeSock sock
      return $ Left "Connection error"

sendQuery ::TCPConnection-> ByteString -> Maybe ByteString -> IO ()
sendQuery TCPConnection{context=Context{server_info=Nothing}} _ _ = error "Empty server info"
sendQuery
  TCPConnection
    { tcpCompression = comp,
      tcpSocket = sock,
      context = Context{
        server_info=Just info,
        client_setting=client_setting
      }
    } 
  query
  query_id = do
    (_, r) <- runWriterT $ do
      writeVarUInt Client._QUERY
      writeBinaryStr
        ( case query_id of
            Nothing -> ""
            Just x -> x
        )
      let revis = revision info
      if revis >= _DBMS_MIN_REVISION_WITH_CLIENT_INFO
        then do
          let client_info = getDefaultClientInfo (_DBMS_NAME <> " " <> _CLIENT_NAME)
          writeInfo client_info info
        else return ()
      writeVarUInt 0 -- TODO add write settings
      writeVarUInt Stage._COMPLETE
      writeVarUInt comp
      writeBinaryStr query
    TCP.sendLazy sock (toLazyByteString r)

sendData :: TCPConnection->ByteString->Maybe Block->IO ()
sendData TCPConnection {tcpSocket = sock, context=ctx} table_name maybe_block = do
  -- TODO: ADD REVISION
  let info = Block.defaultBlockInfo
  r <- execWriterT $ do
    writeVarUInt Client._DATA
    writeBinaryStr table_name
    case maybe_block of
      Nothing -> do
        Block.writeInfo info
        writeVarUInt 0 -- #col
        writeVarUInt 0 -- #row
      Just block -> do
        Block.writeBlockOutputStream ctx block
  TCP.sendLazy sock $ toLazyByteString r

sendCancel :: TCPConnection -> IO ()
sendCancel TCPConnection {tcpSocket = sock} = do
  c <- execWriterT $ writeVarUInt Client._CANCEL
  TCP.sendLazy sock (toLazyByteString c)

processInsertQuery  ::  TCPConnection
                        -- ^ source
                      ->ByteString
                        -- ^ query without data
                      ->Maybe ByteString
                        -- ^ query id
                      ->[[ClickhouseType]]
                        -- ^ data in Haskell type.
                      ->IO ByteString
                        -- ^ return 1 if insertion successfully completed
processInsertQuery tcp@TCPConnection{tcpSocket=sock, context=Context{client_setting=client_setting}}
 query_without_data query_id items = do
  sendQuery tcp query_without_data query_id
  sendData tcp "" Nothing
  buf <- createBuffer 2048 sock
  let info = case getServerInfo tcp of
        Nothing -> error "empty server info"
        Just s -> s 
  (sample_block,_) <- runStateT 
    (iterateWhile (\case Block{..} -> False
                         MultiString _ -> True
                         Hello -> True
                         x -> error ("unexpected packet type: " ++ show x)
                  )
     $ receivePacket info) buf
  case client_setting of
    Nothing -> error "empty client settings"
    Just ClientSetting{insert_block_size=siz} -> do
        let chunks = chunksOf (fromIntegral siz) items
        mapM_ (\chunk -> do
                  let vectorized = V.map V.fromList (V.fromList $ List.transpose chunk)
                  let dataBlock = case sample_block of
                        Block typeinfo@ColumnOrientedBlock{
                          columns_with_type = cwt
                        } -> 
                          typeinfo{
                            cdata = vectorized
                          }
                        x -> error ("unexpected packet type: " ++ show x)
                  sendData tcp "" (Just dataBlock)
          ) chunks
        sendData tcp "" Nothing
        buf2 <- refill buf
        runStateT (receivePacket info) buf2
        return "1"

-- | read data from stream.
receiveData :: ServerInfo -> Reader Block.Block
receiveData ServerInfo {revision = revision} = do
  _ <-
    if revision >= _DBMS_MIN_REVISION_WITH_TEMPORARY_TABLES
      then readBinaryStr
      else return ""
  block <- Block.readBlockInputStream
  return block

receiveResult :: ServerInfo->QueryInfo->Reader (Either String CKResult) --TODO Change to either.
receiveResult info query_info = do
  packets <- packetGen
  let onlyDataPacket = filter isBlock packets
  let errors = (\(ErrorMessage str)->str) <$> filter isError packets
  case errors of
    []->do 
      let dataVectors = (ClickHouseDriver.Core.Column.transpose . Block.cdata . queryData) <$> onlyDataPacket
      let newQueryInfo = Prelude.foldl updateQueryInfo query_info packets
      return $ Right $ CKResult (V.concat dataVectors) newQueryInfo
    xs->do
      return $ Left $ Prelude.concat xs
  where
    updateQueryInfo :: QueryInfo->Packet->QueryInfo
    updateQueryInfo q (Progress prog) 
      = storeProgress q prog
    updateQueryInfo q (StreamProfileInfo profile) 
      = storeProfile q profile
    updateQueryInfo q _ = q

    isError :: Packet->Bool
    isError (ErrorMessage _) = True
    isError _ = False

    isBlock :: Packet -> Bool
    isBlock Block {queryData=Block.ColumnOrientedBlock{cdata=d}} 
            = (V.length d > 0 &&  V.length (d ! 0) > 0)
    isBlock _ = False

    packetGen :: Reader [Packet]
    packetGen = do
      packet <- receivePacket info
      case packet of
        EndOfStream -> return []
        error@(ErrorMessage _) -> return [error]
        _ -> do
          next <- packetGen
          return (packet : next)
  
receivePacket :: ServerInfo->Reader Packet
receivePacket info = do
  packet_type <- readVarInt
  -- The pattern matching does not support match with variable name,
  -- so here we use number instead.
  case packet_type of
    1 -> (receiveData info) >>= (return . Block) -- Data
    2 -> (Error.readException Nothing) >>= (return . ErrorMessage . show) -- Exception
    3 -> (readProgress $ revision info) >>= (return . Progress) -- Progress
    5 -> return EndOfStream -- End of Stream
    6 -> readBlockStreamProfileInfo >>= (return . StreamProfileInfo) --Profile
    7 -> (receiveData info) >>= (return . Block) -- Total
    8 -> (receiveData info) >>= (return . Block) -- Extreme
          -- 10 -> return undefined -- Log
    11 -> do -- MultiStrings message
      first <- readBinaryStr
      second <- readBinaryStr
      return $ MultiString (first, second)
    0 -> return Hello -- Hello
    _ -> do
      closeBufferSocket
      error $
        show
          Error.ServerException
            { code = Error._UNKNOWN_PACKET_FROM_SERVER,
              message = "Unknown packet from server",
              nested = Nothing
            }

closeConnection :: TCPConnection->IO ()
closeConnection TCPConnection{tcpSocket=sock} = TCP.closeSock sock

-- | write client information and server infomation to protocols
writeInfo :: 
    (MonoidMap ByteString w)=>
    ClientInfo->
    ServerInfo ->
    Writer w
writeInfo
  ( ClientInfo
      client_name
      interface
      client_version_major
      client_version_minor
      client_version_patch
      client_revision
      initial_user
      initial_query_id
      initial_address
      quota_key
      query_kind
    )
  ServerInfo {revision = server_revision, display_name = host_name}
    | server_revision < _DBMS_MIN_REVISION_WITH_CLIENT_INFO =
      error "Method writeInfo is called for unsupported server revision"
    | otherwise = do
      writeVarUInt 1
      writeBinaryStr initial_user
      writeBinaryStr initial_query_id
      writeBinaryStr initial_address
      writeVarUInt (if interface == HTTP then 0 else 1)
      writeBinaryStr "" -- os_user. Seems that haskell modules don't have support of getting system username yet.
      writeBinaryStr host_name
      writeBinaryStr _CLIENT_NAME --client_name
      writeVarUInt client_version_major
      writeVarUInt client_version_minor
      writeVarUInt client_revision
      if server_revision >= _DBMS_MIN_REVISION_WITH_QUOTA_KEY_IN_CLIENT_INFO
        then writeBinaryStr quota_key
        else return ()
      if server_revision >= _DBMS_MIN_REVISION_WITH_VERSION_PATCH
        then writeVarUInt client_version_patch
        else return ()
-------------------------------------------------------------------------------------------------------------------
---Helpers 
{-# INLINE closeBufferSocket #-}
closeBufferSocket :: Reader ()
closeBufferSocket = do
  buf <- get
  let sock = ClickHouseDriver.IO.BufferedReader.socket buf
  case sock of
    Just sock'->TCP.closeSock sock'
    Nothing -> return ()