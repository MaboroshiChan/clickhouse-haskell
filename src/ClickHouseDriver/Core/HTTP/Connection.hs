-- Copyright (c) 2014-present, EMQX, Inc.
-- All rights reserved.
--
-- This source code is distributed under the terms of a MIT license,
-- found in the LICENSE file.

{-# LANGUAGE CPP  #-}
{-# LANGUAGE OverloadedStrings #-}

module ClickHouseDriver.Core.HTTP.Connection (
    httpConnect,
    defaultHttpConnection,
    HttpConnection(..),
    createHttpPool
) where
                                
import Network.HTTP.Client
    ( defaultManagerSettings, newManager)
import ClickHouseDriver.Core.HTTP.Types
    ( HttpConnection(..),
      HttpParams(HttpParams, httpUsername, httpPort, httpPassword,
                 httpHost) ) 
import Data.Default.Class ( Default(..) )
import Data.Pool ( createPool, Pool )
import Data.Time.Clock ( NominalDiffTime )

#define DEFAULT_USERNAME  "default"
#define DEFAULT_HOST_NAME "localhost"
#define DEFAULT_PASSWORD  ""
--TODO change default password to ""

defaultHttpConnection :: IO (HttpConnection)
defaultHttpConnection = httpConnect DEFAULT_USERNAME DEFAULT_PASSWORD 8123 DEFAULT_HOST_NAME

instance Default HttpParams where
  def = HttpParams{
     httpHost = DEFAULT_HOST_NAME,
     httpPassword = DEFAULT_PASSWORD,
     httpPort = 8123,
     httpUsername = DEFAULT_USERNAME
  }

createHttpPool :: HttpParams
                ->Int
                ->NominalDiffTime
                ->Int
                ->IO(Pool HttpConnection)
createHttpPool HttpParams{
                httpHost=host,
                httpPassword = password,
                httpPort = port,
                httpUsername = user
              } 
               numStripes 
               idleTime 
               maxResources 
  = createPool(
      do
        conn <- httpConnect user password port host
        return conn
  )(\HttpConnection{httpManager=mng}->return ())
  numStripes 
  idleTime 
  maxResources 

httpConnect :: String->String->Int->String->IO(HttpConnection)
httpConnect user password port host = do
  mng <- newManager defaultManagerSettings
  return HttpConnection {
    httpParams = HttpParams {
      httpHost = host,
      httpPassword = password,
      httpPort = port,
      httpUsername = user
    },
    httpManager = mng
  }
