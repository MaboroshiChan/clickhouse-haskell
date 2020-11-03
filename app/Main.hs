{-# LANGUAGE CPP #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ForeignFunctionInterface #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE ApplicativeDo #-}
module Main where

import           ClickHouseDriver.Core
import qualified ClickHouseDriver.Core.HTTP as HTTP
import           Control.Monad.ST
import           Data.Text
import qualified Data.Text.IO as TIO
import           Network.HTTP.Client
import           Data.ByteString      hiding (putStrLn)  
import qualified Data.ByteString.Char8 as C8
import qualified Data.ByteString.Lazy.Char8 as CL8
import           Foreign.C
import           ClickHouseDriver.IO.BufferedWriter
import           ClickHouseDriver.IO.BufferedReader
import           Data.Monoid
import           Control.Monad.State.Lazy
import           Data.ByteString.Builder
import qualified Data.ByteString.Lazy as L
import           Data.Word
import           Network.Socket                                           
import qualified Network.Simple.TCP as TCP
import qualified Data.Binary as B
import           Data.Int
import           Control.Monad.Writer
import qualified Data.Vector as V
import qualified Control.Monad.Reader as R
import           Control.Monad.Reader (ask)
import qualified ClickHouseDriver.Core.Column as Col
import           ClickHouseDriver.Core.HTTP.Helpers
import qualified Network.URI.Encode as NE
import qualified System.IO.Streams as Streams
import           System.IO hiding (putStr)
import           Data.Int
import           Data.Bits
import           Haxl.Core hiding (fetch)
import           Data.Default.Class (def)
import           Data.Time

main :: IO ()
main = main'
    

benchmark1 :: IO()
benchmark1 = do
    putStrLn "Start benchmark for Clickhouse-Haskell"
    let params = def :: ConnParams
    conn <- createClient params{password'="12345612341"}
    start <- getCurrentTime
    q <- query conn "SELECT * FROM customer LIMIT 10000"
    case q of
        Left e->putStrLn e
        Right r -> Col.putStrLn r
    putStrLn "success!"
    end <- getCurrentTime
    print $ diffUTCTime end start


someReader :: R.Reader Int Int
someReader = do
    x <- ask
    return (x + 1)

xReader :: R.Reader Socket ByteString
xReader = undefined

readResult :: Reader Word
readResult = do
    r <- readVarInt
    r2 <- readVarInt
    r3 <- readVarInt
    return r3

readResult2 :: Reader ByteString
readResult2 = do
    r <- readBinaryStr
    r' <- readBinaryStr
    return (r <> r')

main3 :: IO()
main3 = do
    (sock, sockaddr) <- TCP.connectSock "localhost" "9000"
    print sock

class MyType a

instance MyType Int

instance MyType Char

instance MyType Bool

myfunc :: (MyType a)=>a->a
myfunc a = a

readManyVarInt :: Reader [Word]
readManyVarInt = do
    int <- readVarInt
    if int == 10 
        then
        return [10]
        else do
            next <- readManyVarInt
            return (int : next)

manualTCP :: IO()
manualTCP = do
    print "manual"
    conn' <- tcpConnect "localhost" "9000" "default" "12345612341" "default" False
    case conn' of
        Left _ -> return ()
        Right conn -> do
            print "connected"
            sendQuery conn "select * from tande" Nothing
            sendData conn "" Nothing
            case conn of
                TCPConnection {tcpSocket=sock}-> do
                    x <- TCP.recv sock 2048
                    print x
                    y <- TCP.recv sock 2048
                    print y
                    z <- TCP.recv sock 2048
                    print z
                    TCP.closeSock sock
        


mainTest :: IO()
mainTest = do
    print "Test Section"
    conn <- defaultClient
    print "connected"
    res <- query conn "select item from array_t" 
    closeClient conn
    print res


writes ::ByteString->IO (ByteString)
writes str = do
    r <- execWriterT $ do
        writeBinaryStr str
    return r

data SocketReader = SocketReader {
    sock :: Socket,
    readFromSock :: Socket->IO(ByteString)
}

main' :: IO()
main' = do
    env <- HTTP.httpClient "default" ""
    create <- HTTP.exec "CREATE TABLE test (x Int32) ENGINE = Memory" env
    print create
    isSuccess <- HTTP.insertOneRow "test" [CKInt32 100] env
    print isSuccess
    result <- HTTP.runQuery env (HTTP.getText "select * from test")
    TIO.putStr result

main'' :: IO()
main'' = do
    env <- HTTP.httpClient "default" "12345612341"
    isSuccess <- HTTP.insertFromFile "test_table" HTTP.CSV "./test/example.csv" env
    putStrLn (case isSuccess of
        Right y -> y
        Left x -> CL8.unpack x)
    query <- HTTP.runQuery env (HTTP.getText "SELECT * FROM test_table")
    TIO.putStr query

insertTest :: IO()
insertTest = do
    print "insertion test"
    conn <- defaultClient
    s <- insertMany conn "INSERT INTO simple_table VALUES" 
            [[CKString "0000000000", CKString "Clickhouse-Haskell", CKInt16 1]
            ,[CKString "1000000000", CKString "Clickhouse-Haskell2", CKInt16 12]
            ,[CKString "3000000000", CKString "Clickhouse-Haskell3", CKInt16 15]]
    print s
    q <- query conn "SELECT * FROM simple_table" 
    print q
    closeClient conn

readTest :: IO ()
readTest = do
    cmd <- System.IO.getLine
    conn <- defaultClient
    res <- query conn cmd 
    print res
    closeClient conn
--INSERT INTO nulls_table (`id`, `item`,`number`) VALUES (null, 'JOHN',1557),('1234567890', null,533),('3543364534', 'MARRY',null),('2258864346', 'JAME',4452)
insertTest2 :: IO()
insertTest2 = do
    conn <- defaultClient
    s <- insertMany conn "INSERT INTO nulls_table VALUES" 
            [[CKNull, CKString "Clickhouse-Haskell", CKInt16 1]
            ,[CKString "1000000000", CKNull, CKInt16 12]
            ,[CKString "3000000000", CKString "Clickhouse-Haskell3", CKNull]
            ,[CKString "2258864346", CKString "Jame", CKInt16 4452]]
    q <- query conn "SELECT * FROM nulls_table"
    print q 
    print "conn"
    closeClient conn

insertTest3 :: IO()
insertTest3 = do
    conn <- defaultClient
    s <- insertMany conn "INSERT INTO tande VALUES"
            [
                [CKString "ggo", CKTuple [CKInt16 0, CKString "hah", CKString "oxo", CKInt8 (-11)], CKString "hello"],
                [CKString "ggo", CKTuple [CKInt16 0, CKString "hah", CKString "oxo", CKInt8 (-11)], CKString "hello"],
                [CKString "gfo", CKTuple [CKInt16 0, CKString "hah", CKString "oxo", CKInt8 (-11)], CKString "world"]
            ]
    q <- query conn "SELECT * FROM tande" 
    print q
    print "conn"
    closeClient conn

insertTest4 :: IO()
insertTest4 = do
    conn <- defaultClient
    s <- insertMany conn "INSERT INTO array_table VALUES"
            [
                [CKNull, CKArray [CKArray [CKInt16 1], CKArray [CKInt16 2], CKArray [CKInt16 3]]],
                [CKNull, CKArray [CKArray [CKInt16 1, CKInt16 2], CKArray [CKInt16 3, CKInt16 4], CKArray $ V.fromList [CKInt16 5, CKInt16 6]]]
            ]
    q <- query conn "SELECT * FROM array_table" 
    print q
    print "conn"
    closeClient conn

insertTest5 :: IO()
insertTest5 = do
    conn <- defaultClient
    s <- insertMany conn "INSERT INTO array_nulls VALUES"
            [
                [CKNull, CKArray $ V.fromList [CKArray [CKString "ABC", CKNull, CKString "XYZ"], CKArray [CKString "DEF"], CKArray $ V.fromList [CKString "DEF"]]],
                [CKNull, CKArray $ V.fromList [CKArray [CKString "Clickhouse"],  CKArray [CKNull,CKNull]]]
            ]
    q <- query conn "SELECT * FROM array_nulls" 
    print q
    print "conn"
    closeClient conn

insertTest6 = do
    conn <- defaultClient
    q <- query conn "SELECT * FROM UUID_test" 
    print q
    closeClient conn

insertTest7 = do
    conn <- defaultClient
    q <- insertMany conn "INSERT INTO crd VALUES"
            [
                [CKString "123", CKString "hi"],
                [CKString "456", CKString "lo"]
            ]
    closeClient conn

pingTest = do
    conn <- defaultClient
    ping conn 

queryTests :: GenHaxl u w (String)
queryTests = do
    one <- fetch "SELECT * FROM UUID_test"
    two <- fetch "SELECT * FROM array_t"
    three <- fetch "SHOW DATABASES"
    four <- fetch "SHOW TABLES"
    return $ show one ++ show two ++ show three ++ show four

