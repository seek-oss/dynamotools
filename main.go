package main

import "github.com/SEEK-Jobs/dynamotools/cmd/archive"

func main() {
	archive.ToS3(&archive.S3Config{
		Region:         "ap-southeast-2",
		TableName:      "ca-applied-jobs",
		TableIndex:     "candidateId-appliedDateUTC-index",
		Bucket:         "ca-applied-jobs-dynamodb-backup",
		ScanPartitions: 5,
	})
}
