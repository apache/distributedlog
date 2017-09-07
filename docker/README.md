# Docker Images for Apache DistributedLog

This directory keeps all the files used for building docker images from master.

## Build DistributedLog

``` shell
mvn clean package assembly:single -DskipTests
```

## Build Docker Images

``` shell
./docker/build.sh
```

## Run Sandbox

``` shell
docker run -it -p 7000:7000 --env DLOG_ROOT_LOGGER=INFO,R distributedlog:nightly /distributedlog/bin/dlog local 7000
```
