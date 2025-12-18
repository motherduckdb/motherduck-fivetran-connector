Find the readme for the destination tester [here](https://github.com/fivetran/fivetran_partner_sdk/tree/main/tools/destination-connector-tester).

Get the Docker image for the destination tester: 
```
docker pull us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:<tag>
docker tag us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:<tag> fivetran-destination-tester
``` 

Find the latest tag [here](https://console.cloud.google.com/artifacts/docker/build-286712/us/public-docker-us/sdktesters-v2%2Fsdk-tester). 

The MotherDuck destination server has to be up and running:

```shell
make build_connector
./build/Release/motherduck_destination
```

Then run it

```
cd test/files/destination_tester
mkdir -p generated_files
docker run --interactive --tty --rm \
  --network=host \
  --mount type=bind,source=$(pwd)/,target=/data \
  --attach STDIN --attach STDOUT --attach STDERR \
  --env WORKING_DIR=$(pwd) --env GRPC_HOSTNAME=host.docker.internal \
  fivetran-destination-tester \
  --tester-type destination --port 50052 --disable-operation-delay --batch-file-type CSV [--plain-text]
```

This generates a X.csv.zstd.aes file in the working directory.
Get the decryption key and put it into a file X.csv.zstd.aes.key (e.g., by letting the motherduck_destination_server generate this file).
Then use decrypt.py from test/utils to decrypt the file.

The generated files use the current time as timestamps, e.g. for `_fivetran_synced`.