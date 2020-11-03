-- Copyright (c) 2014-present, EMQX, Inc.
-- All rights reserved.
--
-- This source code is distributed under the terms of a MIT license,
-- found in the LICENSE file.

{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE TypeSynonymInstances #-}

module ClickHouseDriver.Core.HTTP.Helpers
  ( extract,
    genURL,
    toString
  )
where

import ClickHouseDriver.Core.Column
    ( ClickhouseType(CKNull, CKTuple, CKArray, CKString, CKInt32) )
import ClickHouseDriver.Core.HTTP.Connection
    ( HttpConnection(HttpConnection, httpParams) )
import ClickHouseDriver.Core.HTTP.Types ( Cmd, JSONResult, HttpParams(..))
import ClickHouseDriver.IO.BufferedWriter ( writeIn )
import Control.Monad.Writer ( WriterT(runWriterT) )
import qualified Data.Aeson                            as JP
import Data.Attoparsec.ByteString ( IResult(Done, Fail), parse )
import qualified Data.ByteString.Char8                 as C8
import qualified Data.HashMap.Strict                   as HM
import           Data.Text                             (pack)
import           Data.Vector                           (toList)
import qualified Network.URI.Encode                    as NE
import Data.Maybe ( fromMaybe )

-- | Trim JSON data
extract :: C8.ByteString -> JSONResult
extract val = getData $ parse JP.json val
  where
    getData (Fail e _ _)           = Left e
    getData (Done _ (JP.Object x)) = Right $ getData' x
    getData _                      = Right []

    getData' = map getObject . maybeArrToList . HM.lookup (pack "data")

    maybeArrToList Nothing = []
    maybeArrToList (Just x) = toList . getArray $ x

    getArray (JP.Array arr) = arr
    getObject (JP.Object x) = x

genURL :: HttpConnection->Cmd->IO String
genURL HttpConnection {
        httpParams = HttpParams{
            httpHost = host,
            httpPassword = pw, 
            httpPort = port, 
            httpUsername = usr,
            httpDatabase = db
        }
       }
         cmd = do
         (_,basicUrl) <- runWriterT $ do
           writeIn "http://"
           writeIn usr
           writeIn ":"
           writeIn pw
           writeIn "@"
           writeIn host
           writeIn ":"
           writeIn $ show port   
           writeIn "/"
           if cmd == "ping" then return () else writeIn "?query="
           writeIn $ dbUrl db
         let res = basicUrl ++ NE.encode cmd
         return res

-- | serialize column type into sql string
toString :: [ClickhouseType]->String
toString ck = "(" ++ toStr ck ++ ")"

toStr :: [ClickhouseType]->String
toStr [] = ""
toStr (x:[]) = toStr' x
toStr (x:xs) = toStr' x ++ "," ++ toStr xs

toStr' :: ClickhouseType->String
toStr' (CKInt32 n) = show n
toStr' (CKString str) = "'" ++ C8.unpack str ++ "'"
toStr' (CKArray arr) = "[" ++ (toStr $ toList arr) ++ "]"
toStr' (CKTuple arr) = "(" ++ (toStr $ toList arr) ++ ")"
toStr' CKNull = "null"
toStr' _ = error "unsupported writing type"

dbUrl :: (Maybe String) -> String
dbUrl = fromMaybe "" . fmap ("?database=" ++) 
