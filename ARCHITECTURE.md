# Architecture

How the objects and functions in this connector relate to each other. The connector is a gRPC
server implementing the Fivetran Partner SDK `DestinationConnector` service, backed by a DuckDB
connection to MotherDuck.

## Entity map

Every entity belongs to one of three lifetimes, which is the main thing to keep straight when
reading the code: process-scoped state is shared across all concurrent requests, request-scoped
state is created and destroyed per gRPC call, and file-scoped state lives only for the duration of
one batch file.

```mermaid
graph TD
    subgraph process["Process scope (created once in main)"]
        main["main()<br/>motherduck_destination.cpp"]
        preload["preload_extensions()"]
        runserver["RunServer(port)"]
        service["DestinationSdkImpl<br/>(gRPC service impl)"]
        factory["ConnectionFactory"]
        duckdb["duckdb::DuckDB<br/>(single instance, std::call_once)"]
        stdoutlog["Logger (stdout only)"]
    end

    subgraph request["Request scope (one per gRPC call)"]
        ctx["RequestContext"]
        con["duckdb::Connection"]
        logger["Logger (stdout + DuckDB)"]
        sqlgen["MdSqlGenerator"]
        tabledef["table_def"]
        coldefs["vector&lt;column_def&gt;"]
    end

    subgraph file["File scope (one per batch file)"]
        props["IngestProperties"]
        processfile["csv_processor::ProcessFile"]
        memfile["MemoryBackedFile"]
        staging["staging table<br/>(temp, in MotherDuck)"]
    end

    subgraph helpers["Stateless helpers"]
        interop["fivetran_duckdb_interop<br/>get_duckdb_type / get_fivetran_type"]
        cfg["config<br/>find_property / find_bool_property"]
        tester["config_tester<br/>get_test_cases / run_test"]
        decrypt["decryption<br/>decrypt_file / decrypt_stream"]
        openssl["openssl_helper"]
        mderror["md_error<br/>RecoverableError / truncate_for_grpc_header"]
    end

    main --> preload
    main --> runserver
    runserver --> service
    service -->|owns one| factory
    factory -->|lazily creates| duckdb
    factory -->|owns| stdoutlog
    factory -->|rewrites auth failures into| mderror

    service -->|each endpoint constructs| ctx
    ctx -->|reads token + database via| cfg
    ctx -->|CreateConnection| factory
    ctx -->|owns| con
    ctx -->|owns, outlived by con| logger
    logger -->|writes log rows through| con

    service -->|constructs per request| sqlgen
    sqlgen -->|borrows| logger
    sqlgen -->|runs SQL on| con
    sqlgen -->|addresses| tabledef
    sqlgen -->|reads / writes| coldefs
    sqlgen -->|OOM errors become| mderror

    service -->|converts Fivetran columns via| interop
    interop --> coldefs
    service -->|Test endpoint| tester
    service -->|ConfigurationForm| tester
    tester --> cfg
    tester -->|queries| con

    service -->|WriteBatch / WriteHistoryBatch build| props
    props --> coldefs
    service -->|calls| processfile
    processfile -->|if encrypted| decrypt
    decrypt --> openssl
    processfile -->|holds plaintext in| memfile
    processfile -->|CREATE TABLE AS read_csv| staging
    processfile -->|callback receives| staging
    staging -->|consumed by| sqlgen
```

## Data structures

```mermaid
classDiagram
    class DestinationSdkImpl {
        -ConnectionFactory connection_factory
        +ConfigurationForm()
        +Test()
        +Capabilities()
        +DescribeTable()
        +CreateTable()
        +AlterTable()
        +Truncate()
        +WriteBatch()
        +WriteHistoryBatch()
        +Migrate()
    }

    class ConnectionFactory {
        -Logger stdout_logger
        -once_flag db_init_flag
        -DuckDB db
        -string initial_md_token
        -string initial_db_name
        +CreateConnection(token, db_name) Connection
        -get_duckdb(token, db_name) DuckDB&
    }

    class RequestContext {
        -string endpoint_name
        -string db_name
        -string md_token
        -Connection con
        -Logger logger
        +GetConnection() Connection&
        +GetLogger() Logger&
        +GetDBName() string&
    }

    class Logger {
        -SinkType enabled_sinks
        -Connection* con
        +CreateNopLogger() Logger
        +CreateStdoutLogger() Logger
        +CreateMultiSinkLogger(con) Logger
        +debug/info/warning/severe(msg)
    }

    class MdSqlGenerator {
        -Logger& logger
        +table_exists() bool
        +create_table()
        +describe_table() vector~column_def~
        +alter_table()
        +upsert() / insert() / update_values()
        +delete_rows() / truncate_table()
        +create_latest_active_records_table() string
        +add_partial_historical_values()
        +deactivate_historical_records()
        +migrate_*()
        -run_query()
    }

    class table_def {
        +string db_name
        +string schema_name
        +string table_name
        +to_escaped_string() string
    }

    class column_def {
        +string name
        +LogicalTypeId type
        +optional~string~ column_default
        +bool primary_key
        +optional~uint8~ width
        +optional~uint8~ scale
        +quoted() string
    }

    class IngestProperties {
        +string filename
        +string decryption_key
        +vector~column_def~ columns
        +string null_value
        +bool allow_unmodified_string
        +uint32 max_record_size
    }

    class MemoryBackedFile {
        +int fd
        +string path
        +Create(size) MemoryBackedFile
    }

    class TestCase {
        +string name
        +string description
    }

    class TestResult {
        +bool success
        +string failure_message
    }

    class RecoverableError

    DestinationSdkImpl *-- ConnectionFactory : owns for process lifetime
    DestinationSdkImpl ..> RequestContext : creates per request
    ConnectionFactory *-- Logger : stdout sink only
    RequestContext ..> ConnectionFactory : CreateConnection
    RequestContext *-- Logger : con must outlive logger
    MdSqlGenerator o-- Logger : borrowed reference
    DestinationSdkImpl ..> MdSqlGenerator : creates per request
    MdSqlGenerator ..> table_def : operates on
    MdSqlGenerator ..> column_def : reads and writes
    table_def "1" --> "*" column_def : described by
    IngestProperties *-- column_def : expected CSV columns
    IngestProperties ..> MemoryBackedFile : decrypted into
    DestinationSdkImpl ..> TestCase : advertises in ConfigurationForm
    DestinationSdkImpl ..> TestResult : returns from Test
    MdSqlGenerator ..> RecoverableError : throws on OOM
    ConnectionFactory ..> RecoverableError : throws on bad/expired token
```

## Request lifecycle

`WriteBatch` is the most complete path, exercising nearly every entity:

```mermaid
sequenceDiagram
    participant FT as Fivetran
    participant Svc as DestinationSdkImpl
    participant Ctx as RequestContext
    participant CF as ConnectionFactory
    participant CSV as csv_processor
    participant SQL as MdSqlGenerator
    participant MD as MotherDuck

    FT->>Svc: WriteBatch(request)
    Svc->>Ctx: construct("WriteBatch", factory, config)
    Ctx->>CF: CreateConnection(token, db_name)
    CF-->>Ctx: duckdb::Connection
    Ctx-->>Svc: connection + multi-sink logger
    Svc->>Svc: get_duckdb_columns(request columns)
    Svc->>SQL: construct(logger)
    Svc->>Svc: find_primary_keys(cols)

    loop each replace / update / delete file
        Svc->>Svc: get_decryption_key + build IngestProperties
        Svc->>CSV: ProcessFile(con, props, logger, callback)
        CSV->>CSV: decrypt_file into MemoryBackedFile
        CSV->>CSV: determine_compression_type
        CSV->>SQL: generate_temp_table_name
        CSV->>MD: CREATE TABLE staging AS FROM read_csv(...)
        CSV->>SQL: callback: upsert / update_values / delete_rows
        SQL->>MD: MERGE / UPDATE / DELETE against target table
        CSV->>MD: DROP TABLE staging
    end

    Svc-->>FT: grpc::Status::OK
    Note over Svc,FT: A RecoverableError becomes a Fivetran task with an OK status,<br/>while any other exception becomes an INTERNAL status
```

## Endpoint to SQL operation mapping

Every endpoint follows the same shape: construct a `RequestContext`, construct an `MdSqlGenerator`
over its logger, then dispatch to one or more generator methods.

| Endpoint | Primary collaborators |
| --- | --- |
| `ConfigurationForm` | `config` property names, `config_tester::get_test_cases` |
| `Test` | `RequestContext` (connecting *is* the auth test), `config_tester::run_test`, `extract_readable_error` |
| `Capabilities` | none; hardcodes CSV batch format |
| `DescribeTable` | `table_exists`, `describe_table`, `get_fivetran_type` |
| `CreateTable` | `create_schema_if_not_exists_with_retries`, `create_table`, `get_duckdb_type` |
| `AlterTable` | `alter_table` (which picks `alter_table_in_place` or `alter_table_recreate`) |
| `Truncate` | `table_exists`, `truncate_table` |
| `WriteBatch` | `csv_processor::ProcessFile` plus `upsert` / `update_values` / `delete_rows` |
| `WriteHistoryBatch` | `create_latest_active_records_table`, `deactivate_historical_records`, `add_partial_historical_values`, `insert`, `delete_historical_rows`, `drop_latest_active_records_table` |
| `Migrate` | `drop_table`, `drop_column_in_history_mode`, `copy_table`, `copy_column`, `copy_table_to_history_mode`, `rename_table`, `rename_column`, `add_column`, `add_defaults`, `add_column_in_history_mode`, `update_column_value`, `migrate_*` sync-mode switches |

## Lifetime and concurrency constraints

These relationships are load-bearing and easy to break:

- `duckdb::DuckDB` is initialized exactly once per process. `ConnectionFactory::get_duckdb` throws
  if a later request arrives with a different token or database name.
- Inside `RequestContext`, `con` is declared before `logger` so the connection outlives the logger.
  The logger holds a raw `duckdb::Connection*` and writes its log rows through it.
- `MdSqlGenerator` holds a `Logger&`, so it must not outlive the `RequestContext` it was built from.
- Up to `MAX_PARALLEL_REQUESTS` (8) `WriteBatch` calls can be in flight at once, which is what
  bounds `MAX_RECORD_SIZE_DEFAULT` (24 MiB) against the container memory limit.
- `~RequestContext` rolls back any transaction still open and not in autocommit mode.
- In `WriteHistoryBatch`, `lar_table_name` is declared outside the `try` block so the
  latest-active-records table can be dropped from the `catch` blocks.
