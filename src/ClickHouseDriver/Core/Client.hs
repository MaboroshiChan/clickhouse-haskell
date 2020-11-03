-- Copyright (c) 2014-present, EMQX, Inc.
-- All rights reserved.
--
-- This source code is distributed under the terms of a MIT license,
-- found in the LICENSE file.

{-# LANGUAGE BlockArguments             #-}
{-# LANGUAGE CPP                        #-}
{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE OverloadedStrings          #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE TypeSynonymInstances       #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE NamedFieldPuns             #-}

module ClickHouseDriver.Core.Client
  ( query,
    queryWithInfo,
    deploySettings,
    client,
    defaultClient,
    closeClient,
    insertMany,
    insertOneRow,
    ping,
    withQuery,
    ClickHouseDriver.Core.Client.fetch,
    fetchWithInfo,
    execute,
    defaultClientPool,
    createClient,
    createClientPool
  )
where

import ClickHouseDriver.Core.Column ( ClickhouseType )
import ClickHouseDriver.Core.Connection
    ( ping',
      tcpConnect,
      sendQuery,
      sendData,
      processInsertQuery,
      receiveResult )
import ClickHouseDriver.Core.Pool ( createConnectionPool )
import ClickHouseDriver.Core.Defines ( _BUFFER_SIZE )
import qualified ClickHouseDriver.Core.Defines      as Defines
import ClickHouseDriver.Core.Types
    ( ConnParams(..),
      CKResult(CKResult, query_result),
      TCPConnection(TCPConnection, tcpSocket),
      getServerInfo,
      defaultQueryInfo )
import ClickHouseDriver.IO.BufferedReader ( createBuffer )
import Control.Concurrent.Async ( mapConcurrently )
import Control.Exception ( SomeException, try )
import Control.Monad.State ( StateT(runStateT) )
import qualified Data.ByteString                    as BS
import qualified Data.ByteString.Char8              as C8
import Data.Hashable ( Hashable(hashWithSalt) )
import Data.Typeable ( Typeable )
import Data.Vector ( Vector )
import Haxl.Core
    ( putFailure,
      putSuccess,
      dataFetch,
      initEnv,
      runHaxl,
      stateEmpty,
      stateGet,
      stateSet,
      BlockedFetch(..),
      DataSource(fetch),
      DataSourceName(..),
      PerformFetch(SyncFetch),
      Env(states),
      GenHaxl,
      ShowP(..),
      StateKey(State) )                        
import qualified Network.Simple.TCP                 as TCP
import Text.Printf ( printf )
import           Data.Pool                          (Pool(..), withResource, destroyAllResources)
import Data.Time.Clock ( NominalDiffTime )
import           Data.Default.Class                 (def)

{-# INLINE _DEFAULT_PING_WAIT_TIME #-}
_DEFAULT_PING_WAIT_TIME :: Integer
_DEFAULT_PING_WAIT_TIME = 10000

{-# INLINE _DEFAULT_USERNAME #-}
_DEFAULT_USERNAME :: [Char]
_DEFAULT_USERNAME = "default"

{-# INLINE _DEFAULT_HOST_NAME #-}
_DEFAULT_HOST_NAME :: [Char]
_DEFAULT_HOST_NAME = "localhost"

{-# INLINE _DEFAULT_PASSWORD #-}
_DEFAULT_PASSWORD :: [Char]
_DEFAULT_PASSWORD =  ""

{-# INLINE _DEFAULT_PORT_NAME #-}
_DEFAULT_PORT_NAME :: [Char]
_DEFAULT_PORT_NAME =  "9000"

{-# INLINE _DEFAULT_DATABASE#-}
_DEFAULT_DATABASE :: [Char]
_DEFAULT_DATABASE =  "default"

{-# INLINE _DEFAULT_COMPRESSION_SETTING #-}
_DEFAULT_COMPRESSION_SETTING :: Bool
_DEFAULT_COMPRESSION_SETTING =  False


data Query a where
  FetchData :: String
               -- ^ SQL statement such as "SELECT * FROM table"
             ->Query (Either String CKResult)
               -- ^ result data in Haskell type and additional information

deriving instance Show (Query a)

deriving instance Typeable Query

deriving instance Eq (Query a)

instance ShowP Query where showp = show

instance Hashable (Query a) where
  hashWithSalt salt (FetchData cmd) = hashWithSalt salt cmd

instance DataSourceName Query where
  dataSourceName _ = "ClickhouseServer"

instance DataSource u Query where
  fetch (resource) _flags env = SyncFetch $ \blockedFetches -> do
    printf "Fetching %d queries.\n" (length blockedFetches)
    mapConcurrently (fetchData resource) blockedFetches
    return ()

instance StateKey Query where
  data State Query = CKResource TCPConnection
                   | CKPool (Pool TCPConnection)

class Resource a where
  client :: Either String a->IO(Env () w)
            -- ^ Either wrong message of resource with type a

fetchData :: State Query-> BlockedFetch Query -> IO ()
fetchData (CKResource tcpconn)  fetch = do
  let (queryStr, var) = case fetch of
        BlockedFetch (FetchData q) var' -> (C8.pack q, var')
  e <- Control.Exception.try $ do
    sendQuery tcpconn queryStr Nothing
    sendData tcpconn "" Nothing
    let serverInfo = case getServerInfo tcpconn of
          Just info -> info
          Nothing   -> error "Empty server information"
    let sock = tcpSocket tcpconn
    buf <- createBuffer _BUFFER_SIZE sock
    (res, _) <- runStateT (receiveResult serverInfo defaultQueryInfo) buf
    return res
  either
    (putFailure var)
    (putSuccess var)
    (e :: Either SomeException (Either String CKResult))
fetchData (CKPool pool) fetch = do
  let (queryStr, var) = case fetch of
        BlockedFetch (FetchData q) var' -> (C8.pack q, var')
  e <- Control.Exception.try $ do
    withResource pool $ \conn->do
      sendQuery conn queryStr Nothing
      sendData conn "" Nothing
      let serverInfo = case getServerInfo conn of
            Just info -> info
            Nothing -> error "Empty server information"
      let sock = tcpSocket conn
      buf <- createBuffer _BUFFER_SIZE sock
      (res, _) <- runStateT (receiveResult serverInfo defaultQueryInfo) buf
      return res
  either
    (putFailure var)
    (putSuccess var)
    (e :: Either SomeException (Either String CKResult))

deploySettings :: TCPConnection -> IO (Env () w)
deploySettings tcp =
  initEnv (stateSet (CKResource tcp) stateEmpty) ()

defaultClient :: IO (Env () w)
defaultClient = createClient def 
  
createClient :: ConnParams->IO(Env () w)
createClient ConnParams{
                 username'   
                ,host'       
                ,port'       
                ,password'   
                ,compression'
                ,database'   
             } = do
          tcp <- tcpConnect  
                  host'       
                  port'
                  username'       
                  password'   
                  database'
                  compression'
          case tcp of
            Left e -> client ((Left e) :: Either String (Pool TCPConnection))
            Right conn -> client $ Right conn
  
defaultClientPool :: Int
                    -- ^ number of stripes
                   ->NominalDiffTime
                    -- ^ idle time for each stripe
                   ->Int
                    -- ^ maximum resources for reach stripe
                   ->IO (Env () w)
                    -- ^ Haxl env wrapped in IO monad.
defaultClientPool = createClientPool def

createClientPool :: ConnParams
                  -- ^ parameters for connection settings
                  ->Int
                  -- ^ number of stripes
                  ->NominalDiffTime
                  -- ^ idle time for each stripe
                  ->Int
                  -- ^ maximum resources for reach stripe
                  ->IO(Env () w)
createClientPool params numberStripes idleTime maxResources = do 
  pool <- createConnectionPool params numberStripes idleTime maxResources
  client $ Right pool

instance Resource TCPConnection where
  client (Left e) = error e
  client (Right src) = initEnv (stateSet (CKResource src) stateEmpty) ()

instance Resource (Pool TCPConnection) where
  client (Left e) = error e
  client (Right src) = initEnv (stateSet (CKPool src) stateEmpty) ()

-- | fetch data alone with query information
fetchWithInfo :: String->GenHaxl u w (Either String CKResult)
fetchWithInfo = dataFetch . FetchData

-- | fetch data only
fetch :: String->GenHaxl u w (Either String (Vector (Vector ClickhouseType)))
fetch str = do
  result_with_info <- fetchWithInfo str
  case result_with_info of
    Right CKResult{query_result=r}->return $ Right r
    Left err -> return $ Left err

-- | query result contains query information.
queryWithInfo :: String->Env () w->IO (Either String CKResult)
queryWithInfo query source = runHaxl source (executeQuery query)
  where
    executeQuery :: String->GenHaxl u w (Either String CKResult)
    executeQuery = dataFetch . FetchData

-- | query command
query :: Env () w
        -- ^ Haxl environment for connection
       ->String
        -- ^ Query command for "SELECT" and "SHOW" only
       ->IO (Either String (Vector (Vector ClickhouseType)))
query source cmd = do
  query_with_info <- queryWithInfo cmd source
  case query_with_info of
    Right CKResult{query_result=r}->return $ Right r
    Left err->return $ Left err

-- | for general use e.g. creating table,
-- | multiple queries, multiple insertions. 
execute :: Env u w -> GenHaxl u w a -> IO a
execute = runHaxl

withQuery :: Env () w
            -- ^ enviroment i.e. the database resource
           ->String
           -- ^ sql statement
           ->(Either String (Vector (Vector ClickhouseType))->IO a)
           -- ^ callback function that returns type a
           ->IO a
           -- ^ type a wrapped in IO monad.
withQuery source cmd f = query source cmd >>= f

insertMany :: Env () w->String->[[ClickhouseType]]->IO(BS.ByteString)
insertMany source cmd items = do
  let st :: Maybe (State Query) = stateGet $ states source
  case st of
    Nothing             -> error "No Connection."
    Just (CKResource tcp) -> processInsertQuery tcp (C8.pack cmd) Nothing items
    Just (CKPool pool) -> 
      withResource pool $ \tcp->do
        processInsertQuery tcp (C8.pack cmd) Nothing items

insertOneRow :: Env () w->String->[ClickhouseType]->IO(BS.ByteString)
insertOneRow source cmd items = insertMany source cmd [items]

-- | ping pong 
ping :: Env () w->IO()
ping source = do
  let get :: Maybe (State Query) = stateGet $ states source
  case get of
    Nothing -> print "empty source"
    Just (CKResource tcp)
     -> ping' Defines._DEFAULT_PING_WAIT_TIME tcp >>= print
    Just (CKPool pool)
     -> withResource pool $ \src->
       ping' Defines._DEFAULT_PING_WAIT_TIME src >>= print 

-- | close connection
closeClient :: Env () w -> IO()
closeClient source = do
  let get :: Maybe (State Query) = stateGet $ states source
  case get of
    Nothing -> return ()
    Just (CKResource TCPConnection{tcpSocket=sock})
     -> TCP.closeSock sock
    Just (CKPool pool)
     -> destroyAllResources pool