**Clickhouse-haskell**
======================
ClickHouse Haskell Driver with HTTP and native (TCP) interface support.
Support both insert and ordinary query.
This project has been heavily referenced from the python version. Link: https://github.com/mymarilyn/clickhouse-driver

**Features**
========

* External Data for query processing
* Types support
    * [U]Int8/16/32/64
    * String/FixedString(N)
    * Array(T)
    * Nullable(T)
    * Decimal
    * SimpleAggregateFunction(F, T)
    * Tuple(T1, T2, ...)
    * Date/DateTime('timezone')/DateTime64('timezone')
    * Enum8/16
    * Nested
    * LowCardinality(T)
    * UUID
* Query progress information.
* Block by block results streaming.
* Reading query profile info.
* Receiving server logs.
* Applying Haxl(Concurrency library developed by Facebook) for caching query requests and concurrent querying processes.

**Usage of the HTTP Client**
=====

In the HTTP client, data can be fetched from Clickhouse server in the format of pure text string, and structured JSON in which user can read the value according to a given key.

## **Example of data fetch using http client**
```Haskell
import ClickHouseDriver.Core
import qualified Data.Text.IO as TIO

main :: IO()
main = do
    env <- httpClient "default" "" --username and password
    showtables <- runQuery env (getText "SHOW TABLES")
    TIO.putStr showtables
```
Result
```
    Fetching 1 queries.
    array0
    array1
    array_t
    array_test
    array_test2
    big
    cardin
    crd
    crd2
    dt
    int_test
    ip
    t
    tande
    tande2
    tande3
    test_table
    test_table2
    test_table3
    test_table4
    test_table5
    test_table6
    tuple
    tuple2
```
```Haskell

    puretext <- runQuery env (getText "SELECT * FROM test_table")
    TIO.putStr puretext
```
stdout: 
```
    Fetching 1 queries.
    9987654321      Suzuki  12507   [667]
    9987654321      Suzuki  12507   [667]
    0000000001      JOHN    1557    [45,45,45]
    1234567890      CONNOR  533     [1,2,3,4]
    3543364534      MARRY   220     [0,1,2,3,121,2]
    2258864346      JAME    4452    [42,-10988,66,676,0]
    0987654321      Connan  9984    [24]
    0987654321      Connan  9984    [24]
    9987654321      Suzuki  12507   [667]
```
```Haskell 
    json <- runQuery env (getJSON "SELECT * FROM test_table")
    print json
```
stdout:
```Haskell
    Right [fromList [("numArray",Array [Number 45.0,Number 45.0,Number 45.0]),("item",   String "JOHN"),("id",String "0000000001"),("number",Number 1557.0)],fromList [("numArray",Array [Number 1.0,Number 2.0,Number 3.0,Number 4.0]),("item",String "CONNOR"),("id",String "1234567890"),("number",Number 533.0)],fromList [("numArray",Array [Number 0.0,Number 1.0,Number 2.0,Number 3.0,Number 121.0,Number 2.0]),("item",String "MARRY"),("id",String "3543364534"),("number",Number 220.0)],fromList [("numArray",Array [Number 42.0,Number -10988.0,Number 66.0,Number 676.0,Number 0.0]),("item",String "JAME"),("id",String "2258864346"),("number",Number 4452.0)],fromList [("numArray",Array [Number 24.0]),("item",String "Connan"),("id",String "0987654321"),("number",Number 9984.0)],fromList [("numArray",Array [Number 24.0]),("item",String "Connan"),("id",String "0987654321"),("number",Number 9984.0)],fromList [("numArray",Array [Number 667.0]),("item",String "Suzuki"),("id",String "9987654321"),("number",Number 12507.0)],fromList [("numArray",Array [Number 667.0]),("item",String "Suzuki"),("id",String "9987654321"),("number",Number 12507.0)],fromList [("numArray",Array [Number 667.0]),("item",String "Suzuki"),("id",String "9987654321"),("number",Number 12507.0)]]
```

There is also a built-in Clickhouse type for user to send data in rows to Clickhouse server.

## **Algebraic data type for the Clickhouse types** 

```Haskell 
data ClickhouseType
  = CKBool Bool
  | CKInt8 Int8
  | CKInt16 Int16
  | CKInt32 Int32
  | CKInt64 Int64
  | CKUInt8 Word8
  | CKUInt16 Word16
  | CKUInt32 Word32
  | CKUInt64 Word64
  | CKString ByteString
  | CKFixedLengthString Int ByteString
  | CKTuple (Vector ClickhouseType)
  | CKArray (Vector ClickhouseType)
  | CKDecimal32 Float
  | CKDecimal64 Float
  | CKDecimal128 Float
  | CKIPv4 IP4
  | CKIPv6 IP6
  | CKDate {
    year :: !Integer,
    month :: !Int,
    day :: !Int 
  }
  | CKNull
  deriving (Show, Eq)
```

## **Example of sending data from memory**
```Haskell
main = do
  env <- httpClient "default" "12345612341"
  create <- exec "CREATE TABLE test (x Int32) ENGINE = Memory" env
  print create
  isSuccess <- insertOneRow "test" [CKInt32 100] env
  print isSuccess
  result <- runQuery env (getText "select * from test")
  TIO.putStr result
```
stdout:
```
Right "Inserted successfully"
Right "Inserted successfully"
Fetching 1 queries.
100
```

## **Example of sending data from CSV**
```Haskell
main :: IO()
main = do
    env <- httpClient "default" "12345612341"
    isSuccess <- insertFromFile "test_table" CSV "./test/example.csv" env
    putStr (case isSuccess of
        Right y -> y
        Left x -> CL8.unpack x)
    query <- runQuery env (getText "SELECT * FROM test_table")
    TIO.putStr query
```
where in example.csv
```CSV
0000000011,Bob,123,'[1,2,3]'
0000000012,Bob,124,'[4,5,6]'
0000000013,Bob,125,'[7,8,9,10]'
0000000014,Bob,126,'[11,12,13]'
```

stdout:

```
0000000010      Alice   123     [1,2,3]
0000000010      Alice   123     [1,2,3]
0000000010      Alice   123     [1,2,3]
0000000010      Alice   123     [1,2,3]
0000000011      Bob     123     [1,2,3]
0000000012      Bob     124     [4,5,6]
0000000013      Bob     125     [7,8,9,10]
0000000014      Bob     126     [11,12,13]
```

**Usage of the Native(TCP) interface**
==========================

## **Ping**
```Haskell
    conn <- defaultClient
    ClickHouseDriver.Core.ping conn
```
stdout:
```
Just "PONG!"
```

## **Example of making query with the native interface**
```Haskell
main :: IO ()
main = do
    env <- defaultClient --localhost 9000
    res <- query "SHOW TABLES" env
    print res
```
stdout:
``` Haskell
[[CKString "test"],[CKString "test_table"],[CKString "test_table2"]]
```

## **Example of making insert query with the native interface**
```
conn <- defaultClient
insertMany conn "INSERT INTO crd VALUES"
          [
            [CKString "123", CKString "hi"],
            [CKString "456", CKString "lo"]
          ]
```
In the terminal interface of clickhouse it will show:
```
id──┬─card─┐
│ 123 │ hi   │
│ 456 │ lo   │
```
## **Use of Haxl for concurrency**
We can perform multiple fetches concurrently like this:
```Haskell
queryTests :: GenHaxl u w (V.Vector (V.Vector ClickhouseType))
queryTests = do
    one <- fetch "SELECT * FROM UUID_test"
    two <- fetch "SELECT * FROM array_t"
    three <- fetch "SHOW DATABASES"
    four <- fetch "SHOW TABLES"
    return $ V.concat [one, two ,three, four]

```
```
Fetching 4 queries.
[[CKString "417ddc5d-e556-4d27-95dd-a34d84e46a50"],...
```

## **Stream profile and process infomation**

The native interface supports reading infomations coming from server. Originally they come with the queried data wrapped in the algebraic data types:

```Haskell
data CKResult = CKResult
 { query_result ::  Vector (Vector ClickhouseType),
   query_info :: {-# UNPACK #-} ! QueryInfo
 }

data QueryInfo = QueryInfo 
 { profile_info :: {-# UNPACK #-} !BlockStreamProfileInfo,
   progress :: {-# UNPACK #-} !Progress,
   elapsed :: {-# UNPACK #-} !Word
 }

 data BlockStreamProfileInfo = ProfileInfo
  { number_rows :: {-# UNPACK #-} !Word,
    blocks :: {-# UNPACK #-} !Word,
    number_bytes :: {-# UNPACK #-} !Word,
    applied_limit :: {-# UNPACK #-} !Bool,
    rows_before_limit :: {-# UNPACK #-} !Word,
    calculated_rows_before_limit :: {-# UNPACK #-} !Bool
  }

data Progress = Prog
  { rows :: {-# UNPACK #-} !Word,
    bytes :: {-# UNPACK #-} !Word,
    total_rows :: {-# UNPACK #-} !Word,
    written_rows :: {-# UNPACK #-} !Word,
    written_bytes :: {-# UNPACK #-} !Word
  }
```
One can use executeWithInfo to get results that come with those information.
For example:
```Haskell
main = do
    conn <- defaultClient
    res <- executeWithInfo "show databases" conn
    print $ query_result res
    print $ query_info res
```
The code above prints:
```Haskell
[[CKString "_temporary_and_external_tables"],[CKString "default"],[CKString "system"]]

QueryInfo {profile_info = ProfileInfo {number_rows = 3, blocks = 1, number_bytes = 4224, applied_limit = True, rows_before_limit = 0, calculated_rows_before_limit = True}, progress = Prog {rows = 3, bytes = 331, total_rows = 0, written_rows = 0, written_bytes = 0}, elapsed = 0}
```
