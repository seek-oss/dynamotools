package cmd

import (
	"github.com/SEEK-Jobs/dynamotools/cmd/archive"
	"github.com/urfave/cli"
)

// BuildArchive builds the cli command for archive funationality
func BuildArchive() cli.Command {
	return cli.Command{
		Name:        "archive",
		Usage:       "region [aws region name] table [dynamo table name] tableindex [index to use for scanning] partitions [scan partitions for parallel scanning] bucket [s3 bucket name]",
		Description: "archive scans the [table] using the specified [tableindex] and saves it the s3 [bucket]",
		Flags: []cli.Flag{
			cli.StringFlag{
				Name:  "region, r",
				Value: "ap-southeast-2",
				Usage: "aws region name where your dynamodb table and s3 bucket is",
			},
			cli.StringFlag{
				Name:  "table, t",
				Usage: "dynamodb table name",
			},
			cli.StringFlag{
				Name:  "tableindex, i",
				Usage: "index for scanning the dynamo table",
			},
			cli.IntFlag{
				Name:  "partitions, p",
				Value: 1,
				Usage: "partitions for parallel scanning",
			},
			cli.StringFlag{
				Name:  "bucket, b",
				Usage: "name of the bucket to store the archived data",
			},
			cli.StringFlag{
				Name:  "prefix, pf",
				Usage: "prefix for the folder where archived data will be stored (optional)",
			},
		},
		SkipFlagParsing: false,
		Before: func(c *cli.Context) error {
			if c.String("table") == "" && c.String("t") == "" {
				return cli.NewExitError("missing value for [table]", 86)
			} else if c.String("bucket") == "" && c.String("b") == "" {
				return cli.NewExitError("missing value for [bucket]", 86)
			}
			return nil
		},
		Action: func(c *cli.Context) error {
			return archive.ToS3(&archive.S3ArchiveConfig{
				Region:             c.String("region"),
				TableName:          c.String("table"),
				TableIndex:         c.String("tableindex"),
				Bucket:             c.String("bucket"),
				BackupFolderPrefix: c.String("prefix"),
				ScanPartitions:     c.Int("partitions"),
			})

		},
	}
}
