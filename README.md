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


## Upgrading DuckDB

This connector uses DuckDB's amalgamation sources. To upgrade, copy the [amalgamation sources](https://duckdb.org/docs/installation/?version=latest&environment=cplusplus&installer=source) to [libduckdb-src](./libduckdb-src) folder. 