package archive

import (
	"context"
	"encoding/json"
	"io"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"golang.org/x/sync/errgroup"
)

type scanner interface {
	Scan(writer io.WriteCloser) error
}

type parallelScanner struct {
	db  dynamodbiface.DynamoDBAPI
	cfg *scannerConfig
}

type scannerConfig struct {
	tableName  string
	index      string
	partitions int
	limit      int
}

func newScannerConfig(tableName, index string, partitions int) *scannerConfig {
	return &scannerConfig{tableName: tableName, index: index, partitions: partitions}
}

func newParallelScanner(db dynamodbiface.DynamoDBAPI, cfg *scannerConfig) scanner {
	return &parallelScanner{db, cfg}
}

func (s *parallelScanner) Scan(writer io.WriteCloser) error {
	grp, _ := errgroup.WithContext(context.Background())

	log.Printf("started processing %d partitions....", s.cfg.partitions)

	for index := 0; index < s.cfg.partitions; index++ {
		partitionSegment := index
		grp.Go(func() error {
			if err := s.db.ScanPages(s.buildScanInput(partitionSegment), func(p *dynamodb.ScanOutput, lastPage bool) (shouldContinue bool) {
				items := make([]*dynamodb.AttributeValue, len(p.Items))
				for i, m := range p.Items {
					items[i] = &dynamodb.AttributeValue{M: m}
				}
				var decodedItems []map[string]interface{}
				if err := dynamodbattribute.NewDecoder().Decode(&dynamodb.AttributeValue{L: items}, &decodedItems); err != nil {
					log.Printf("error %s whilst decoding items %v", err, items)
				}
				if err := json.NewEncoder(writer).Encode(decodedItems); err != nil {
					log.Printf("error %s whilst encoding items %v", err, items)
				}

				return !lastPage
			}); err != nil {
				log.Printf("error %s whilst scanning items from dynamo partion %d", err, partitionSegment)
				return err
			}
			log.Println("finished processing partion no ", partitionSegment)
			return nil
		})
	}
	if err := grp.Wait(); err != nil {
		return err
	}
	if err := writer.Close(); err != nil {
		log.Printf("error %s whilst closing the writer", err)
		return err
	}

	log.Printf("finished processing %d partitions", s.cfg.partitions)
	return nil
}

func (s *parallelScanner) buildScanInput(partitionIndex int) *dynamodb.ScanInput {
	input := &dynamodb.ScanInput{
		TableName:     aws.String(s.cfg.tableName),
		Segment:       aws.Int64(int64(partitionIndex)),
		TotalSegments: aws.Int64(int64(s.cfg.partitions)),
		Limit:         aws.Int64(int64(s.cfg.limit)),
	}
	if s.cfg.index != "" {
		input.IndexName = aws.String(s.cfg.index)
	}
	return input
}
