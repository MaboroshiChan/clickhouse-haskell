{-# LANGUAGE CPP  #-}
{-# LANGUAGE OverloadedStrings #-}


module ClickHouseDriver.Core.HTTP.Connection (
    httpConnect,
    httpConnectDb,
    defaultHttpConnection,
    HttpConnection(..),
) where
                                
import Network.HTTP.Client

#define DEFAULT_USERNAME  "default"
#define DEFAULT_HOST_NAME "localhost"
#define DEFAULT_PASSWORD  "12345612341"
--TODO change default password to ""

data HttpConnection
  = HttpConnection
      { httpHost :: {-# UNPACK #-}     !String,
        httpPort :: {-# UNPACK #-}     !Int,
        httpUsername :: {-# UNPACK #-}  !String,
        httpPassword :: {-# UNPACK #-} !String,
        httpManager ::  {-# UNPACK #-} !Manager,
        httpDatabase :: {-# UNPACK #-} !(Maybe String)
      }

defaultHttpConnection :: IO (HttpConnection)
defaultHttpConnection = httpConnect DEFAULT_USERNAME DEFAULT_PASSWORD 8123 DEFAULT_HOST_NAME


httpConnect :: String->String->Int->String->IO(HttpConnection)
httpConnect user password port host = 
  httpConnectDb user password port host Nothing


httpConnectDb :: String->String->Int->String->Maybe String->IO(HttpConnection)
httpConnectDb user password port host database = do
  mng <- newManager defaultManagerSettings
  return HttpConnection {
    httpHost = host,
    httpPassword = password,
    httpPort = port,
    httpUsername = user,
    httpManager = mng,
    httpDatabase = database
  }
