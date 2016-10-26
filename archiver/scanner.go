package archiver

import (
	"context"
	"encoding/json"
	"io"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"golang.org/x/sync/errgroup"
)

type Scanner interface {
	Scan(writer io.Writer) error
}

type parallelScanner struct {
	db  dynamodbiface.DynamoDBAPI
	cfg *ScannerConfig
}

type ScannerConfig struct {
	tableName  string
	index      string
	partitions int
}

func NewScannerConfig(tableName, index string, partitions int) *ScannerConfig {
	if partitions < 1 {
		partitions = 1
	}

	return &ScannerConfig{tableName: tableName, index: index, partitions: partitions}
}

func NewParallelScanner(db dynamodbiface.DynamoDBAPI, cfg *ScannerConfig) Scanner {
	return &parallelScanner{db, cfg}
}

func (s *parallelScanner) Scan(writer io.Writer) error {
	grp, _ := errgroup.WithContext(context.Background())
	decoder := dynamodbattribute.NewDecoder()
	encoder := json.NewEncoder(writer)

	for index := 0; index < s.cfg.partitions; index++ {
		partitionSegment := index

		grp.Go(func() error {
			return s.db.ScanPages(s.buildScanInput(partitionSegment), func(p *dynamodb.ScanOutput, lastPage bool) (shouldContinue bool) {
				for _, item := range p.Items {
					var decodedItem map[string]interface{}
					decoder.Decode(&dynamodb.AttributeValue{M: item}, &decodedItem)
					encoder.Encode(decodedItem)
				}
				return false
			})
		})

	}
	if err := grp.Wait(); err != nil {
		return err
	}
	return nil
}

func (s *parallelScanner) buildScanInput(partitionIndex int) *dynamodb.ScanInput {
	return &dynamodb.ScanInput{
		TableName:     aws.String(s.cfg.tableName),
		IndexName:     aws.String(s.cfg.index),
		Segment:       aws.Int64(int64(partitionIndex)),
		TotalSegments: aws.Int64(int64(s.cfg.partitions)),
	}
}
