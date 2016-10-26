package main

import (
	"log"
	"os"

	"github.com/SEEK-Jobs/dynamotools/archiver"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

func main() {
	c := defaults.Config().WithRegion("ap-southeast-2")
	c.Credentials = defaults.CredChain(c, defaults.Handlers())
	db := dynamodb.New(session.New(c))

	sc := archiver.NewParallelScanner(db, archiver.NewScannerConfig("ca-applied-jobs", "candidateId-appliedDateUTC-index", 1))
	if err := sc.Scan(os.Stdout); err != nil {
		log.Println(err)
	}
}
