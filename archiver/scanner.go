package archiver

import (
	"context"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"golang.org/x/sync/errgroup"
)

type scanner struct {
	db  *dynamodb.DynamoDB
	cfg *ScannerConfig
}

type ScannerConfig struct {
	tableName  string
	index      string
	partitions int
	pageSize   int
}

func (s *scanner) Scan() error {
	grp, ctx := errgroup.WithContext(context.Background())
	for index := 0; index < s.cfg.partitions; index++ {
		grp.Go(func() error {
			return s.db.ScanPages(s.buildScanInput(index), func(p *dynamodb.ScanOutput, lastPage bool) (shouldContinue bool) {

				return false
			})
		})

	}
	if err := grp.Wait(); err != nil {
		return err
	}
	return nil
}

func (s *scanner) buildScanInput(partitionIndex int) *dynamodb.ScanInput {

}
