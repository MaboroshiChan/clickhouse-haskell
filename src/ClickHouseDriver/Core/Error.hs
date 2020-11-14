-- Copyright (c) 2014-present, EMQX, Inc.
-- All rights reserved.
--
-- This source code is distributed under the terms of a MIT license,
-- found in the LICENSE file.
---------------------------------------------------------------------------
-- This file contains the details of handling error messages from server.
{-# LANGUAGE OverloadedStrings #-}

module ClickHouseDriver.Core.Error
  ( readException,
    ClickhouseException (..),
    _UNEXPECTED_PACKET_FROM_SERVER,
    _UNKNOWN_PACKET_FROM_SERVER,
  )
where

import ClickHouseDriver.IO.BufferedReader
  ( Reader,
    readBinaryInt32,
    readBinaryStr,
    readBinaryUInt8,
  )
import Data.ByteString.Char8 (unpack)

--import           Debug.Trace

data ClickhouseException = ServerException
  { message :: String,
    code :: !Integer,
    nested :: Maybe ClickhouseException
  }

instance Show ClickhouseException where
  show (ServerException message code nested) =
    "Code " ++ show code ++ "."
      ++ ( case nested of
             Nothing -> ""
             Just s -> show s
         )
      ++ " "
      ++ message

readException :: Maybe String -> Reader ClickhouseException
readException additional = do
  code <- readBinaryInt32
  name <- readBinaryStr
  messange <- readBinaryStr
  stack_trace <- readBinaryStr
  has_nested <- (== 1) <$> readBinaryUInt8
  let hasAdditional =
        ( case additional of
            Nothing -> ""
            Just msg -> msg ++ "."
        )
          ++ if name /= "DB::Exception"
            then unpack name
            else "" ++ "."
  let new_message = hasAdditional <> show messange <> ". Stack trace:\n\n" <> show stack_trace
  if has_nested
    then do
      nested <- readException Nothing
      return $ ServerException new_message (fromIntegral code) (Just nested)
    else do
      return $ ServerException new_message (fromIntegral code) Nothing

_UNSUPPORTED_METHOD = 1

_UNSUPPORTED_PARAMETER = 2

_UNEXPECTED_END_OF_FILE = 3

_EXPECTED_END_OF_FILE = 4

_CANNOT_PARSE_TEXT = 6

_INCORRECT_NUMBER_OF_COLUMNS = 7

_THERE_IS_NO_COLUMN = 8

_SIZES_OF_COLUMNS_DOESNT_MATCH = 9

_NOT_FOUND_COLUMN_IN_BLOCK = 10

_POSITION_OUT_OF_BOUND = 11

_PARAMETER_OUT_OF_BOUND = 12

_SIZES_OF_COLUMNS_IN_TUPLE_DOESNT_MATCH = 13

_DUPLICATE_COLUMN = 15

_NO_SUCH_COLUMN_IN_TABLE = 16

_DELIMITER_IN_STRING_LITERAL_DOESNT_MATCH = 17

_CANNOT_INSERT_ELEMENT_INTO_CONSTANT_COLUMN = 18

_SIZE_OF_FIXED_STRING_DOESNT_MATCH = 19

_NUMBER_OF_COLUMNS_DOESNT_MATCH = 20

_CANNOT_READ_ALL_DATA_FROM_TAB_SEPARATED_INPUT = 21

_CANNOT_PARSE_ALL_VALUE_FROM_TAB_SEPARATED_INPUT = 22

_CANNOT_READ_FROM_ISTREAM = 23

_CANNOT_WRITE_TO_OSTREAM = 24

_CANNOT_PARSE_ESCAPE_SEQUENCE = 25

_CANNOT_PARSE_QUOTED_STRING = 26

_CANNOT_PARSE_INPUT_ASSERTION_FAILED = 27

_CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER = 28

_CANNOT_PRINT_INTEGER = 29

_CANNOT_READ_SIZE_OF_COMPRESSED_CHUNK = 30

_CANNOT_READ_COMPRESSED_CHUNK = 31

_ATTEMPT_TO_READ_AFTER_EOF = 32

_CANNOT_READ_ALL_DATA = 33

_TOO_MANY_ARGUMENTS_FOR_FUNCTION = 34

_TOO_LESS_ARGUMENTS_FOR_FUNCTION = 35

_BAD_ARGUMENTS = 36

_UNKNOWN_ELEMENT_IN_AST = 37

_CANNOT_PARSE_DATE = 38

_TOO_LARGE_SIZE_COMPRESSED = 39

_CHECKSUM_DOESNT_MATCH = 40

_CANNOT_PARSE_DATETIME = 41

_NUMBER_OF_ARGUMENTS_DOESNT_MATCH = 42

_ILLEGAL_TYPE_OF_ARGUMENT = 43

_ILLEGAL_COLUMN = 44

_ILLEGAL_NUMBER_OF_RESULT_COLUMNS = 45

_UNKNOWN_FUNCTION = 46

_UNKNOWN_IDENTIFIER = 47

_NOT_IMPLEMENTED = 48

_LOGICAL_ERROR = 49

_UNKNOWN_TYPE = 50

_EMPTY_LIST_OF_COLUMNS_QUERIED = 51

_COLUMN_QUERIED_MORE_THAN_ONCE = 52

_TYPE_MISMATCH = 53

_STORAGE_DOESNT_ALLOW_PARAMETERS = 54

_STORAGE_REQUIRES_PARAMETER = 55

_UNKNOWN_STORAGE = 56

_TABLE_ALREADY_EXISTS = 57

_TABLE_METADATA_ALREADY_EXISTS = 58

_ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER = 59

_UNKNOWN_TABLE = 60

_ONLY_FILTER_COLUMN_IN_BLOCK = 61

_SYNTAX_ERROR = 62

_UNKNOWN_AGGREGATE_FUNCTION = 63

_CANNOT_READ_AGGREGATE_FUNCTION_FROM_TEXT = 64

_CANNOT_WRITE_AGGREGATE_FUNCTION_AS_TEXT = 65

_NOT_A_COLUMN = 66

_ILLEGAL_KEY_OF_AGGREGATION = 67

_CANNOT_GET_SIZE_OF_FIELD = 68

_ARGUMENT_OUT_OF_BOUND = 69

_CANNOT_CONVERT_TYPE = 70

_CANNOT_WRITE_AFTER_END_OF_BUFFER = 71

_CANNOT_PARSE_NUMBER = 72

_UNKNOWN_FORMAT = 73

_CANNOT_READ_FROM_FILE_DESCRIPTOR = 74

_CANNOT_WRITE_TO_FILE_DESCRIPTOR = 75

_CANNOT_OPEN_FILE = 76

_CANNOT_CLOSE_FILE = 77

_UNKNOWN_TYPE_OF_QUERY = 78

_INCORRECT_FILE_NAME = 79

_INCORRECT_QUERY = 80

_UNKNOWN_DATABASE = 81

_DATABASE_ALREADY_EXISTS = 82

_DIRECTORY_DOESNT_EXIST = 83

_DIRECTORY_ALREADY_EXISTS = 84

_FORMAT_IS_NOT_SUITABLE_FOR_INPUT = 85

_RECEIVED_ERROR_FROM_REMOTE_IO_SERVER = 86

_CANNOT_SEEK_THROUGH_FILE = 87

_CANNOT_TRUNCATE_FILE = 88

_UNKNOWN_COMPRESSION_METHOD = 89

_EMPTY_LIST_OF_COLUMNS_PASSED = 90

_SIZES_OF_MARKS_FILES_ARE_INCONSISTENT = 91

_EMPTY_DATA_PASSED = 92

_UNKNOWN_AGGREGATED_DATA_VARIANT = 93

_CANNOT_MERGE_DIFFERENT_AGGREGATED_DATA_VARIANTS = 94

_CANNOT_READ_FROM_SOCKET = 95

_CANNOT_WRITE_TO_SOCKET = 96

_CANNOT_READ_ALL_DATA_FROM_CHUNKED_INPUT = 97

_CANNOT_WRITE_TO_EMPTY_BLOCK_OUTPUT_STREAM = 98

_UNKNOWN_PACKET_FROM_CLIENT = 99

_UNKNOWN_PACKET_FROM_SERVER = 100

_UNEXPECTED_PACKET_FROM_CLIENT = 101

_UNEXPECTED_PACKET_FROM_SERVER = 102

_RECEIVED_DATA_FOR_WRONG_QUERY_ID = 103

_TOO_SMALL_BUFFER_SIZE = 104

_CANNOT_READ_HISTORY = 105

_CANNOT_APPEND_HISTORY = 106

_FILE_DOESNT_EXIST = 107

_NO_DATA_TO_INSERT = 108

_CANNOT_BLOCK_SIGNAL = 109

_CANNOT_UNBLOCK_SIGNAL = 110

_CANNOT_MANIPULATE_SIGSET = 111

_CANNOT_WAIT_FOR_SIGNAL = 112

_THERE_IS_NO_SESSION = 113

_CANNOT_CLOCK_GETTIME = 114

_UNKNOWN_SETTING = 115

_THERE_IS_NO_DEFAULT_VALUE = 116

_INCORRECT_DATA = 117

_ENGINE_REQUIRED = 119

_CANNOT_INSERT_VALUE_OF_DIFFERENT_SIZE_INTO_TUPLE = 120

_UNKNOWN_SET_DATA_VARIANT = 121

_INCOMPATIBLE_COLUMNS = 122

_UNKNOWN_TYPE_OF_AST_NODE = 123

_INCORRECT_ELEMENT_OF_SET = 124

_INCORRECT_RESULT_OF_SCALAR_SUBQUERY = 125

_CANNOT_GET_RETURN_TYPE = 126

_ILLEGAL_INDEX = 127

_TOO_LARGE_ARRAY_SIZE = 128

_FUNCTION_IS_SPECIAL = 129

_CANNOT_READ_ARRAY_FROM_TEXT = 130

_TOO_LARGE_STRING_SIZE = 131

_CANNOT_CREATE_TABLE_FROM_METADATA = 132

_AGGREGATE_FUNCTION_DOESNT_ALLOW_PARAMETERS = 133

_PARAMETERS_TO_AGGREGATE_FUNCTIONS_MUST_BE_LITERALS = 134

_ZERO_ARRAY_OR_TUPLE_INDEX = 135

_UNKNOWN_ELEMENT_IN_CONFIG = 137

_EXCESSIVE_ELEMENT_IN_CONFIG = 138

_NO_ELEMENTS_IN_CONFIG = 139

_ALL_REQUESTED_COLUMNS_ARE_MISSING = 140

_SAMPLING_NOT_SUPPORTED = 141

_NOT_FOUND_NODE = 142

_FOUND_MORE_THAN_ONE_NODE = 143

_FIRST_DATE_IS_BIGGER_THAN_LAST_DATE = 144

_UNKNOWN_OVERFLOW_MODE = 145

_QUERY_SECTION_DOESNT_MAKE_SENSE = 146

_NOT_FOUND_FUNCTION_ELEMENT_FOR_AGGREGATE = 147

_NOT_FOUND_RELATION_ELEMENT_FOR_CONDITION = 148

_NOT_FOUND_RHS_ELEMENT_FOR_CONDITION = 149

_NO_ATTRIBUTES_LISTED = 150

_INDEX_OF_COLUMN_IN_SORT_CLAUSE_IS_OUT_OF_RANGE = 151

_UNKNOWN_DIRECTION_OF_SORTING = 152

_ILLEGAL_DIVISION = 153

_AGGREGATE_FUNCTION_NOT_APPLICABLE = 154

_UNKNOWN_RELATION = 155

_DICTIONARIES_WAS_NOT_LOADED = 156

_ILLEGAL_OVERFLOW_MODE = 157

_TOO_MANY_ROWS = 158

_TIMEOUT_EXCEEDED = 159

_TOO_SLOW = 160

_TOO_MANY_COLUMNS = 161

_TOO_DEEP_SUBQUERIES = 162

_TOO_DEEP_PIPELINE = 163

_READONLY = 164

_TOO_MANY_TEMPORARY_COLUMNS = 165

_TOO_MANY_TEMPORARY_NON_CONST_COLUMNS = 166

_TOO_DEEP_AST = 167

_TOO_BIG_AST = 168

_BAD_TYPE_OF_FIELD = 169

_BAD_GET = 170

_BLOCKS_HAVE_DIFFERENT_STRUCTURE = 171

_CANNOT_CREATE_DIRECTORY = 172

_CANNOT_ALLOCATE_MEMORY = 173

_CYCLIC_ALIASES = 174

_CHUNK_NOT_FOUND = 176

_DUPLICATE_CHUNK_NAME = 177

_MULTIPLE_ALIASES_FOR_EXPRESSION = 178

_MULTIPLE_EXPRESSIONS_FOR_ALIAS = 179

_THERE_IS_NO_PROFILE = 180

_ILLEGAL_FINAL = 181

_ILLEGAL_PREWHERE = 182

_UNEXPECTED_EXPRESSION = 183

_ILLEGAL_AGGREGATION = 184

_UNSUPPORTED_MYISAM_BLOCK_TYPE = 185

_UNSUPPORTED_COLLATION_LOCALE = 186

_COLLATION_COMPARISON_FAILED = 187

_UNKNOWN_ACTION = 188

_TABLE_MUST_NOT_BE_CREATED_MANUALLY = 189

_SIZES_OF_ARRAYS_DOESNT_MATCH = 190

_SET_SIZE_LIMIT_EXCEEDED = 191

_UNKNOWN_USER = 192

_WRONG_PASSWORD = 193

_REQUIRED_PASSWORD = 194

_IP_ADDRESS_NOT_ALLOWED = 195

_UNKNOWN_ADDRESS_PATTERN_TYPE = 196

_SERVER_REVISION_IS_TOO_OLD = 197

_DNS_ERROR = 198

_UNKNOWN_QUOTA = 199

_QUOTA_DOESNT_ALLOW_KEYS = 200

_QUOTA_EXPIRED = 201

_TOO_MANY_SIMULTANEOUS_QUERIES = 202

_NO_FREE_CONNECTION = 203

_CANNOT_FSYNC = 204

_NESTED_TYPE_TOO_DEEP = 205

_ALIAS_REQUIRED = 206

_AMBIGUOUS_IDENTIFIER = 207

_EMPTY_NESTED_TABLE = 208

_SOCKET_TIMEOUT = 209

_NETWORK_ERROR = 210

_EMPTY_QUERY = 211

_UNKNOWN_LOAD_BALANCING = 212

_UNKNOWN_TOTALS_MODE = 213

_CANNOT_STATVFS = 214

_NOT_AN_AGGREGATE = 215

_QUERY_WITH_SAME_ID_IS_ALREADY_RUNNING = 216

_CLIENT_HAS_CONNECTED_TO_WRONG_PORT = 217

_TABLE_IS_DROPPED = 218

_DATABASE_NOT_EMPTY = 219

_DUPLICATE_INTERSERVER_IO_ENDPOINT = 220

_NO_SUCH_INTERSERVER_IO_ENDPOINT = 221

_ADDING_REPLICA_TO_NON_EMPTY_TABLE = 222

_UNEXPECTED_AST_STRUCTURE = 223

_REPLICA_IS_ALREADY_ACTIVE = 224

_NO_ZOOKEEPER = 225

_NO_FILE_IN_DATA_PART = 226

_UNEXPECTED_FILE_IN_DATA_PART = 227

_BAD_SIZE_OF_FILE_IN_DATA_PART = 228

_QUERY_IS_TOO_LARGE = 229

_NOT_FOUND_EXPECTED_DATA_PART = 230

_TOO_MANY_UNEXPECTED_DATA_PARTS = 231

_NO_SUCH_DATA_PART = 232

_BAD_DATA_PART_NAME = 233

_NO_REPLICA_HAS_PART = 234

_DUPLICATE_DATA_PART = 235

_ABORTED = 236

_NO_REPLICA_NAME_GIVEN = 237

_FORMAT_VERSION_TOO_OLD = 238

_CANNOT_MUNMAP = 239

_CANNOT_MREMAP = 240

_MEMORY_LIMIT_EXCEEDED = 241

_TABLE_IS_READ_ONLY = 242

_NOT_ENOUGH_SPACE = 243

_UNEXPECTED_ZOOKEEPER_ERROR = 244

_CORRUPTED_DATA = 246

_INCORRECT_MARK = 247

_INVALID_PARTITION_VALUE = 248

_NOT_ENOUGH_BLOCK_NUMBERS = 250

_NO_SUCH_REPLICA = 251

_TOO_MANY_PARTS = 252

_REPLICA_IS_ALREADY_EXIST = 253

_NO_ACTIVE_REPLICAS = 254

_TOO_MANY_RETRIES_TO_FETCH_PARTS = 255

_PARTITION_ALREADY_EXISTS = 256

_PARTITION_DOESNT_EXIST = 257

_UNION_ALL_RESULT_STRUCTURES_MISMATCH = 258

_CLIENT_OUTPUT_FORMAT_SPECIFIED = 260

_UNKNOWN_BLOCK_INFO_FIELD = 261

_BAD_COLLATION = 262

_CANNOT_COMPILE_CODE = 263

_INCOMPATIBLE_TYPE_OF_JOIN = 264

_NO_AVAILABLE_REPLICA = 265

_MISMATCH_REPLICAS_DATA_SOURCES = 266

_STORAGE_DOESNT_SUPPORT_PARALLEL_REPLICAS = 267

_CPUID_ERROR = 268

_INFINITE_LOOP = 269

_CANNOT_COMPRESS = 270

_CANNOT_DECOMPRESS = 271

_AIO_SUBMIT_ERROR = 272

_AIO_COMPLETION_ERROR = 273

_AIO_READ_ERROR = 274

_AIO_WRITE_ERROR = 275

_INDEX_NOT_USED = 277

_LEADERSHIP_LOST = 278

_ALL_CONNECTION_TRIES_FAILED = 279

_NO_AVAILABLE_DATA = 280

_DICTIONARY_IS_EMPTY = 281

_INCORRECT_INDEX = 282

_UNKNOWN_DISTRIBUTED_PRODUCT_MODE = 283

_UNKNOWN_GLOBAL_SUBQUERIES_METHOD = 284

_TOO_LESS_LIVE_REPLICAS = 285

_UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE = 286

_UNKNOWN_FORMAT_VERSION = 287

_DISTRIBUTED_IN_JOIN_SUBQUERY_DENIED = 288

_REPLICA_IS_NOT_IN_QUORUM = 289

_LIMIT_EXCEEDED = 290

_DATABASE_ACCESS_DENIED = 291

_LEADERSHIP_CHANGED = 292

_MONGODB_CANNOT_AUTHENTICATE = 293

_INVALID_BLOCK_EXTRA_INFO = 294

_RECEIVED_EMPTY_DATA = 295

_NO_REMOTE_SHARD_FOUND = 296

_SHARD_HAS_NO_CONNECTIONS = 297

_CANNOT_PIPE = 298

_CANNOT_FORK = 299

_CANNOT_DLSYM = 300

_CANNOT_CREATE_CHILD_PROCESS = 301

_CHILD_WAS_NOT_EXITED_NORMALLY = 302

_CANNOT_SELECT = 303

_CANNOT_WAITPID = 304

_TABLE_WAS_NOT_DROPPED = 305

_TOO_DEEP_RECURSION = 306

_TOO_MANY_BYTES = 307

_UNEXPECTED_NODE_IN_ZOOKEEPER = 308

_FUNCTION_CANNOT_HAVE_PARAMETERS = 309

_INVALID_SHARD_WEIGHT = 317

_INVALID_CONFIG_PARAMETER = 318

_UNKNOWN_STATUS_OF_INSERT = 319

_VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE = 321

_BARRIER_TIMEOUT = 335

_UNKNOWN_DATABASE_ENGINE = 336

_DDL_GUARD_IS_ACTIVE = 337

_UNFINISHED = 341

_METADATA_MISMATCH = 342

_SUPPORT_IS_DISABLED = 344

_TABLE_DIFFERS_TOO_MUCH = 345

_CANNOT_CONVERT_CHARSET = 346

_CANNOT_LOAD_CONFIG = 347

_CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN = 349

_INCOMPATIBLE_SOURCE_TABLES = 350

_AMBIGUOUS_TABLE_NAME = 351

_AMBIGUOUS_COLUMN_NAME = 352

_INDEX_OF_POSITIONAL_ARGUMENT_IS_OUT_OF_RANGE = 353

_ZLIB_INFLATE_FAILED = 354

_ZLIB_DEFLATE_FAILED = 355

_BAD_LAMBDA = 356

_RESERVED_IDENTIFIER_NAME = 357

_INTO_OUTFILE_NOT_ALLOWED = 358

_TABLE_SIZE_EXCEEDS_MAX_DROP_SIZE_LIMIT = 359

_CANNOT_CREATE_CHARSET_CONVERTER = 360

_SEEK_POSITION_OUT_OF_BOUND = 361

_CURRENT_WRITE_BUFFER_IS_EXHAUSTED = 362

_CANNOT_CREATE_IO_BUFFER = 363

_RECEIVED_ERROR_TOO_MANY_REQUESTS = 364

_OUTPUT_IS_NOT_SORTED = 365

_SIZES_OF_NESTED_COLUMNS_ARE_INCONSISTENT = 366

_TOO_MANY_FETCHES = 367

_BAD_CAST = 368

_ALL_REPLICAS_ARE_STALE = 369

_DATA_TYPE_CANNOT_BE_USED_IN_TABLES = 370

_INCONSISTENT_CLUSTER_DEFINITION = 371

_SESSION_NOT_FOUND = 372

_SESSION_IS_LOCKED = 373

_INVALID_SESSION_TIMEOUT = 374

_CANNOT_DLOPEN = 375

_CANNOT_PARSE_UUID = 376

_ILLEGAL_SYNTAX_FOR_DATA_TYPE = 377

_DATA_TYPE_CANNOT_HAVE_ARGUMENTS = 378

_UNKNOWN_STATUS_OF_DISTRIBUTED_DDL_TASK = 379

_CANNOT_KILL = 380

_HTTP_LENGTH_REQUIRED = 381

_CANNOT_LOAD_CATBOOST_MODEL = 382

_CANNOT_APPLY_CATBOOST_MODEL = 383

_PART_IS_TEMPORARILY_LOCKED = 384

_MULTIPLE_STREAMS_REQUIRED = 385

_NO_COMMON_TYPE = 386

_EXTERNAL_LOADABLE_ALREADY_EXISTS = 387

_CANNOT_ASSIGN_OPTIMIZE = 388

_INSERT_WAS_DEDUPLICATED = 389

_CANNOT_GET_CREATE_TABLE_QUERY = 390

_EXTERNAL_LIBRARY_ERROR = 391

_QUERY_IS_PROHIBITED = 392

_THERE_IS_NO_QUERY = 393

_QUERY_WAS_CANCELLED = 394

_FUNCTION_THROW_IF_VALUE_IS_NON_ZERO = 395

_TOO_MANY_ROWS_OR_BYTES = 396

_QUERY_IS_NOT_SUPPORTED_IN_MATERIALIZED_VIEW = 397

_CANNOT_PARSE_DOMAIN_VALUE_FROM_STRING = 441

_KEEPER_EXCEPTION = 999

_POCO_EXCEPTION = 1000

_STD_EXCEPTION = 1001

_UNKNOWN_EXCEPTION = 1002

_CONDITIONAL_TREE_PARENT_NOT_FOUND = 2001

_ILLEGAL_PROJECTION_MANIPULATOR = 2002