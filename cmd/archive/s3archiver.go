package archive

import (
	"fmt"
	"io"
	"log"

	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/defaults"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// S3ArchiveConfig provides the configuration for archiving dynamo table to s3
type S3ArchiveConfig struct {
	Region         string
	TableName      string
	TableIndex     string
	ScanPartitions int
	Bucket         string
}

// ToS3 archives the dyanamo table to a file in s3 bucket
func ToS3(c *S3ArchiveConfig) error {
	s := getNewAwsSession(c.Region)

	db := dynamodb.New(s)

	sc := newParallelScanner(db, newScannerConfig(c.TableName, c.TableIndex, c.ScanPartitions))

	r, w := io.Pipe()

	u := s3manager.NewUploader(s, func(ul *s3manager.Uploader) {
		ul.PartSize = 32 * 1024 * 1024 //32MB
		ul.Concurrency = 10
	})

	go func() {
		if err := sc.Scan(w); err != nil {
			w.Close()
			log.Fatal(err)
		}
	}()

	_, err := u.Upload(&s3manager.UploadInput{
		Bucket:      &c.Bucket,
		Key:         aws.String(fmt.Sprintf("%s/%s.json", time.Now().Format("2006-01-02"), c.TableName)),
		Body:        r,
		ContentType: aws.String("application/json"),
	})
	if err != nil {
		log.Printf("error %s whilst uploading to s3", err)
		return err
	}
	log.Println("Backup Completed!")
	return nil
}

func getNewAwsSession(region string) *session.Session {
	awsconfig := defaults.Config().WithRegion(region) //.WithLogLevel(aws.LogDebugWithRequestErrors)
	awsconfig.Credentials = defaults.CredChain(awsconfig, defaults.Handlers())
	return session.New(awsconfig)
}
