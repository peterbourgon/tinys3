# tinys3

Extremely basic AWS S3 implementation that serves objects (files) from local disk.

## Install

```sh
go install github.com/peterbourgon/tinys3@latest
tinys3 --root=/tmp/tinys3 --addr=localhost:1234
```

## Usage

```sh
export AWS_ACCESS_KEY_ID=none
export AWS_SECRET_ACCESS_KEY=none
export AWS_ENDPOINT_URL=http://localhost:1234
echo 'hello' | aws s3 cp - s3://mybucket/hello.txt
aws s3 cp s3://mybucket/hello.txt s3://mybucket/some/other/place/world.txt
aws s3 ls --recursive s3://mybucket
find /tmp/tinys3
```
