{-# LANGUAGE OverloadedStrings #-}
module Test.ColumnSpec (columnSpec) where

import ClickHouseDriver.Core
    ( ClickhouseType(CKNull, CKTuple, CKArray, CKInt8, CKInt16,
                     CKString),
      defaultClient,
      insertMany,
      query )
import Test.Hspec
    ( hspec, describe, it, parallel, runIO, shouldBe, Spec )
import Data.Vector (fromList)


columnSpec :: IO()
columnSpec = hspec $ parallel $ do
    --stringAndIntSpec
    arraySpec
    tupleAndEnumSpec
    lowCardinalitySpec
    ipAndDateSpec
    --aggregateFunctionSpec
    uuidSpec
    --nullableSpec

stringAndIntSpec :: Spec
stringAndIntSpec = describe "test string and int columns" $ do
    conn <- runIO $ defaultClient
    runIO $ query conn ("CREATE TABLE IF NOT EXISTS str_and_int_suite " ++ 
            "(`str` String, `int` Int16, `fix` FixedString(3))" ++ "ENGINE = Memory") 
    runIO $ ClickHouseDriver.Core.insertMany conn "INSERT INTO str_and_int_suite VALUES"
                [
                    [CKString "teststr1", CKInt16 323, CKString "abc"],
                    [CKString "teststr2", CKInt16 456 ,CKString "axy"],
                    [CKString "teststr2", CKInt16 220, CKString "ffa"]
                ]
    q <- runIO $ query conn "SELECT * FROM str_and_int_suite" 
    it "returns query result in standard format" $ do
        show q `shouldBe` "[[CKString \"teststr1\", CKInt16 323, CKString \"abc\"]," ++
                "[CKString \"teststr2\", CKInt16 456 ,CKString \"axy\"],[CKString \"teststr2\"," ++
                "CKInt16 220, CKString \"ffa\"]]"

arraySpec :: Spec
arraySpec = describe "test array" $ do
    conn <- runIO $ defaultClient
    runIO $ query conn ("CREATE TABLE IF NOT EXISTS array_suite " ++ 
            "(`id` Int8, `arr` Array(String))" ++ "ENGINE = Memory") 
    runIO $ ClickHouseDriver.Core.insertMany conn "INSERT INTO array_suite VALUES"
                [
                    [CKInt8 1, CKArray $ fromList [CKString "Clickhouse", CKString "Test1"]],
                    [CKInt8 2, CKArray $ fromList [CKString "Clickhouse", CKString "Test2"]],
                    [CKInt8 3, CKArray $ fromList [CKString "Clickhouse", CKString "Test3"]]
                ]
    q <- runIO $ query conn "SELECT * FROM array_suite" 
    it "returns query result in standard format" $ do
        show q `shouldBe` (show $ ([
                        [CKInt8 1, CKArray $ fromList [CKString "Clickhouse", CKString "Test1"]],
                        [CKInt8 2, CKArray $ fromList [CKString "Clickhouse", CKString "Test2"]],
                        [CKInt8 3, CKArray $ fromList [CKString "Clickhouse", CKString "Test3"]]
                    ]))

tupleAndEnumSpec :: Spec
tupleAndEnumSpec = describe "tuple and enum test" $ do
    conn <- runIO $ defaultClient
    runIO $ query conn ("CREATE TABLE IF NOT EXISTS tuple_enum_suite " ++ 
            "(`id` Int8, `tup` Tuple(Int8, String), `enum` Enum('hello' = 1, 'world' = 2))" ++ "ENGINE = Memory") 
    runIO $ ClickHouseDriver.Core.insertMany conn "INSERT INTO tuple_enum_suite VALUES"
                [
                    [CKInt8 1, CKTuple $ fromList [CKInt8 11, CKString "Test1"], CKString "hello"],
                    [CKInt8 2, CKTuple $ fromList [CKInt8 12, CKString "Test2"], CKString "world"],
                    [CKInt8 3, CKTuple $ fromList [CKInt8 13, CKString "Test3"], CKString "hello"]
                ]
    q <- runIO $ query conn "SELECT * FROM tuple_enum_suite" 
    it "returns query result in standard format" $ do
     show q `shouldBe` (show $ ([
                    [CKInt8 1, CKTuple $ fromList [CKInt8 11, CKString "Test1"], CKString "hello"],
                    [CKInt8 2, CKTuple $ fromList [CKInt8 12, CKString "Test2"], CKString "world"],
                    [CKInt8 3, CKTuple $ fromList [CKInt8 13, CKString "Test3"], CKString "hello"]
                ]))
    

lowCardinalitySpec :: Spec
lowCardinalitySpec = describe "lowcardinality type test" $ do
    conn <- runIO $ defaultClient
    runIO $ query  conn ("CREATE TABLE IF NOT EXISTS lowcardinality_suite " ++ 
            "(`id` Int8, `lowstr` LowCardinality(String))" ++ "ENGINE = Memory")
    runIO $ ClickHouseDriver.Core.insertMany conn "INSERT INTO lowcardinality_suite VALUES"
                [
                    [CKInt8 1, CKString "Clickhouse", CKInt8 123],
                    [CKInt8 2, CKString "driver", CKInt8 123],
                    [CKInt8 3, CKString "Clickhouse", CKInt8 120]
                ]
    q <- runIO $ query conn "SELECT * FROM lowcardinality_suite" 
    it "returns query result in standard format" $ do
        show q `shouldBe` "[[CKInt8 1, CKString \"Clickhouse\"]," ++
                    "[CKInt8 2, CKString \"driver\",]," ++
                    "[CKInt8 3, CKString \"Clickhouse\"]]"

ipAndDateSpec :: Spec
ipAndDateSpec = undefined

aggregateFunctionSpec :: Spec
aggregateFunctionSpec = undefined

uuidSpec :: Spec
uuidSpec = describe "uuid test" $ do
    conn <- runIO $ defaultClient
    runIO $ query conn ("CREATE TABLE IF NOT EXISTS uuid_suite " ++ 
            "(`id` Int8, `uuid` UUID)" ++ "ENGINE = Memory") 
    runIO $ ClickHouseDriver.Core.insertMany conn "INSERT INTO uuid_suite VALUES"
                [
                    [CKInt8 1, CKString "123e4567-e89b-12d3-a456-426614174000"],
                    [CKInt8 2, CKString "123e4567-e89b-12d3-a456-426614174000"],
                    [CKInt8 3, CKString "123e4567-e89b-12d3-a456-426614174000"]
                ]
    q <- runIO $ query conn "SELECT * FROM uuid_suite" 
    it "returns query result in standard format" $ do
        show q `shouldBe` "[[CKInt8 1,CKString \"123e4567-e89b-12d3-a456-426614174000\"]," ++ 
                    "[CKInt8 2,CKString \"123e4567-e89b-12d3-a456-426614174000\"]," ++
                    "[CKInt8 3,CKString \"123e4567-e89b-12d3-a456-426614174000\"]]"

nullableSpec :: Spec
nullableSpec = describe "nullable type test" $ do
    conn <- runIO $ defaultClient
    runIO $ query conn ("CREATE TABLE IF NOT EXISTS nullable_suite " ++ 
            "(`id` Int8, `nullableStr` Nullable(String), `nullableInt` Nullable(Int16), `nullableArr` Array(Nullable(Int16)))" ++ "ENGINE = Memory") 
    runIO $ ClickHouseDriver.Core.insertMany conn "INSERT INTO nullable_suie VALUES"
                [
                    [CKInt8 1, CKString "string", CKNull, CKArray $ fromList [CKNull, CKNull, CKInt16 155]],
                    [CKInt8 2, CKNull, CKInt16 1100, CKArray $ fromList [CKNull,CKInt16 110, CKNull]],
                    [CKInt8 3, CKString "string", CKNull, CKArray $ fromList [CKInt16 220, CKInt16 110, CKNull]]
                ]
    q <- runIO $ query conn "SELECT * FROM nullable_suite" 
    it "returns query result in standard format" $ do
        show q `shouldBe` "[[CKInt8 1, CKString \"17ddc5d-e556-4d27-95dd-a34d84e46a50\"]," ++
                    "[CKInt8 2, CKString \"17ddc5d-0000-4d27-95dd-a34d84e46a50\"]," ++
                    "[CKInt8 3, CKString \"17ddc5d-e556-4d27-0000-a34d84e46a50\"]]"
