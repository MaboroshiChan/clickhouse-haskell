{-# LANGUAGE OverloadedStrings #-}
module Benchmark  where

import ClickHouseDriver.Core
    ( ConnParams(password'), createClient, query )
import Data.Default.Class (def)
import Data.ByteString ()
import Data.Time ( diffUTCTime, getCurrentTime ) 

benchmark1 :: IO()
benchmark1 = do
    start <- getCurrentTime
    putStrLn "Start benchmark for Clickhouse-Haskell"
    let params = def :: ConnParams
    conn <- createClient params{password'="12345612341"}
    query conn "SELECT * FROM customer LIMIT 10"
    putStrLn "success!"
    end <- getCurrentTime
    print $ diffUTCTime end start
