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

You can also run the connector in Docker:
```shell
docker build --build-arg GIT_COMMIT_SHA_OVERRIDE=$(git rev-parse --short HEAD) -t motherduck-connector .
```

## Upgrading DuckDB

This connector builds DuckDB from source.
To upgrade, change the `GIT_TAG` in `FetchContent_Declare(duckdb)` in _CMakeLists.txt_ to the new commit SHA.
