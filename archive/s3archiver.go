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
	Region            string
	TableName         string
	TableIndex        string
	ScanPartitions    int
	ScanLimit         int
	ScanFilterName    string
	ScanFilterValue   string
	ScanFilterType    string
	ScanFilterOpertor string
	UploadBucket      string
	UploadChunkSize   int64
	UploadConcurrency int
	BackupPrefix      string
}

// ToS3 archives the dyanamo table to a file in s3 bucket
func ToS3(c *S3ArchiveConfig) error {
	s := getNewAwsSession(c.Region)

	db := dynamodb.New(s)

	sc := newParallelScanner(db, newScannerConfig(c.TableName, c.TableIndex, c.ScanPartitions, c.ScanLimit,
		c.ScanFilterName, c.ScanFilterType, c.ScanFilterOpertor, c.ScanFilterValue))

	r, w := io.Pipe()

	u := s3manager.NewUploader(s, func(ul *s3manager.Uploader) {
		ul.PartSize = c.UploadChunkSize * 1024 * 1024 //MB
		ul.Concurrency = c.UploadConcurrency
	})

	go func() {
		if err := sc.Scan(w); err != nil {
			w.Close()
			log.Fatal(err)
		}
	}()

	key := generateBackupFileName(c.BackupPrefix, c.TableName)
	log.Printf("backing up data in %s", key)

	_, err := u.Upload(&s3manager.UploadInput{
		Bucket:      &c.UploadBucket,
		Key:         aws.String(key),
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

func generateBackupFileName(prefix, fileName string) string {
	if prefix != "" {
		return fmt.Sprintf("%s/%s/%s.json", prefix, time.Now().Format("2006-01-02"), fileName)
	}

	return fmt.Sprintf("%s/%s.json", time.Now().Format("2006-01-02"), fileName)
}

func getNewAwsSession(region string) *session.Session {
	awsconfig := defaults.Config().WithRegion(region) //.WithLogLevel(aws.LogDebugWithRequestErrors)
	awsconfig.Credentials = defaults.CredChain(awsconfig, defaults.Handlers())
	return session.New(awsconfig)
}
