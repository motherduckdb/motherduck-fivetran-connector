Get the Docker image for the destination tester: 
```
docker pull us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:<tag>
docker tag us-docker.pkg.dev/build-286712/public-docker-us/sdktesters-v2/sdk-tester:<tag> fivetran-destination-tester
``` 

Find the latest tag [here](https://console.cloud.google.com/artifacts/docker/build-286712/us/public-docker-us/sdktesters-v2%2Fsdk-tester). 

Then run it

```
cd test/destination_tester_input
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
