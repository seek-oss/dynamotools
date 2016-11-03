# Dynamodb Tools
Tools to manage dynamo db

## Install
```
go get -u github.com/SEEK-Jobs/dynamotools
go install github.com/SEEK-Jobs/dynamotools
```
## Usage
```
dynamotools [command] [options...]
```

### Archive
Archive does a parallel scan on a dynamodb table and uplodas the data in chunks to a file in s3 bucket.

```
dynamotools archive -help

NAME:
   dynamotools archive - region [aws region name] table [dynamo table name] tableindex [index to use for scanning]
            partitions [scan partitions for parallel scanning] limit [limit for scanning no of records]
            bucket [s3 bucket name] chunksize [chunk sizes (in MB) to be uploaded to the bucket]
            concurrency [concurrency for uploads to the bucket]

USAGE:
   dynamotools archive [command options] [arguments...]

DESCRIPTION:
   archive scans the [table] using the specified [tableindex] and saves it the s3 [bucket]

OPTIONS:
   --region value, -r value         aws region name where your dynamodb table and s3 bucket is (default: "ap-southeast-2")
   --table value, -t value          dynamodb table name
   --tableindex value, -i value     index for scanning the dynamo table
   --partitions value, -p value     partitions for parallel scanning (default: 1)
   --limit value, -l value          limit for scanning records (default: 100)
   --bucket value, -b value         name of the bucket to store the archived data
   --chunksize value, --cs value    chunk sizes (in MB) to be uploaded to the bucket (default: 16)
   --concurrency value, --uc value  concurrency for uploads to the bucket (default: 10)
   --prefix value, --pf value       prefix for the folder where archived data will be stored (optional)
```
