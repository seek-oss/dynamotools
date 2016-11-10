package cmd

import (
	"github.com/SEEK-Jobs/dynamotools/restore"
	"github.com/urfave/cli"
)

// BuildRestore builds the cli command for restore funationality
func BuildRestore() cli.Command {
	return cli.Command{
		Name:        "restore",
		Usage:       "region [aws region name] table [dynamo table name] bucket [s3 bucket name] file [restore file in the bucket]",
		Description: "restore downaloads the [file] from the [bucket] and inserts the records into the [table]",
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
			cli.IntFlag{
				Name:  "workers, w",
				Value: 1,
				Usage: "number of parallel workers putting data in dynamodb table",
			},
			cli.StringFlag{
				Name:  "bucket, b",
				Usage: "name of the bucket to store the archived data",
			},
			cli.StringFlag{
				Name:  "file, f",
				Value: "",
				Usage: "restore file in the bucket with json content",
			},
		},
		SkipFlagParsing: false,
		Before: func(c *cli.Context) error {
			if c.String("table") == "" && c.String("t") == "" {
				return cli.NewExitError("missing value for [table]", 86)
			} else if c.String("bucket") == "" && c.String("b") == "" {
				return cli.NewExitError("missing value for [bucket]", 86)
			} else if c.String("file") == "" && c.String("f") == "" {
				return cli.NewExitError("missing value for [file]", 86)
			}
			return nil
		},
		Action: func(c *cli.Context) error {
			return restore.ToDyanmo(&restore.DynamoResotreConfig{
				Region:      c.String("region"),
				TableName:   c.String("table"),
				Workers:     c.Int("workers"),
				Bucket:      c.String("bucket"),
				RestoreFile: c.String("file"),
			})

		},
	}
}
