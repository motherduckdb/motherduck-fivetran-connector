## MotherDuck Fivetran Destination connector

This is a MotherDuck implementation of Fivetran connector SDK 

This connector requires two user-configurable properties:
- **Authentication Token** Users can retrieve the token from app.motherduck.com ([documentation](https://motherduck.com/docs/authenticating-to-motherduck#authentication-using-a-service-token)).
- **Database** The MotherDuck database to load the data into. If the database does not exist, it will be created at the first Fivetran request mentioning it.

## Local build

To install dependencies:
```shell
make build_dependencies
```

To build the destination server:
```shell
make build_connector
OR 
make build_connector_debug
```

To run the destination server:
```shell
./build/Release/motherduck_destination [--port CUSTOM_PORT]
OR
./build/Debug/motherduck_destination [--port CUSTOM_PORT]
```

By default, the server runs on 0.0.0.0:50052.

## Local testing

One-time setup:
```shell
make build_test_dependencies
```

To run integration tests (which will create a database named `fivetran_test` in your production MotherDuck account):
```shell
make build_connector_debug
./build/Debug/integration_tests
```

## Upgrading DuckDB

This connector uses DuckDB's amalgamation sources.
To upgrade, change `DUCKDB_VERSION` in [Makefile](Makefile) and re-run `make get_duckdb`.

### Temporarily upgrade DuckDB by building the amalgamation

1. check out the new released version of DuckDB submodule.
2. build the amalgamation sources:
```
cd duckdb
EXTENSION_CONFIGS='./.github/config/bundled_extensions.cmake' ENABLE_EXTENSION_AUTOLOADING=1 ENABLE_EXTENSION_AUTOINSTALL=1  MAIN_BRANCH_VERSIONING=0 python scripts/amalgamation.py
```
3. copy them into the libduckdb-src directory
```
cp src/amalgamation/* ../libduckdb-src/
```

## TODO
- support GZIP compression
